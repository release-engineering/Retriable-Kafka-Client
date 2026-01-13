"""
Module for managing retries in message consumption.

Each consumed topic can have settings for a retry topic.
Retry topic is a separate topic for each normal topic. If message
processing fails, the consumer commits the original message and
sends its contents to the retry topic. It also adds headers to
this message, specifying number of attempts and the timestamp,
after which the message can be retried.

The consumer consumes messages from all specified topics, (both
the original topics and retry topics) and if it encounters a message
that should not be processed yet due to its timestamp, it stores it
in memory and pauses the consumption of the partition where this message
originated. After its timestamp passes, it resumes the consumption
of this partition and sends the message for further processing and
committing.
"""

import datetime
import logging
from collections import defaultdict

from confluent_kafka import Message, KafkaException, TopicPartition

from .config import ProducerConfig, ConsumerConfig, ConsumeTopicConfig
from .producer import BaseProducer

_HEADER_PREFIX = "retriable_kafka_"
ATTEMPT_HEADER = _HEADER_PREFIX + "attempt"
TIMESTAMP_HEADER = _HEADER_PREFIX + "timestamp"

LOGGER = logging.getLogger(__name__)


def _get_retry_timestamp(message: Message) -> float | None:
    """
    Retrieves the timestamp (specifying when the message should be processed)
    from the message's header.
    Args:
        message: Kafka message object
    Returns:
        the timestamp in POSIX format or None if no timestamp was found
    """
    headers = message.headers()
    if headers is None:
        return None
    for header_name, header_value in headers:
        if header_name == TIMESTAMP_HEADER:
            return int.from_bytes(header_value, "big")
    return None


def _get_retry_attempt(message: Message) -> int:
    """
    Retrieves the attempt number from the message's header.
    Args:
        message: Kafka message object
    Returns:
        the number of attempt or 0 if no attempt header was found
    """
    headers = message.headers()
    if headers is None:
        return 0
    for header_name, header_value in headers:
        if header_name == ATTEMPT_HEADER:
            return int.from_bytes(header_value, "big")
    return 0


def _get_current_timestamp() -> float:
    """
    Returns the current timestamp in POSIX format
    Returns:
        the current timestamp in POSIX format
    """
    return datetime.datetime.now(tz=datetime.timezone.utc).timestamp()


class RetryScheduleCache:
    """
    Class for storing information about messages that are blocked
    from execution due to their header timestamp. Each message with this header
    is scheduled for execution only after the specified timestamp passes.
    """

    def __init__(self):
        self.__schedule: dict[float, list[Message]] = defaultdict(list)

    def pop_reprocessable(self) -> list[Message]:
        """
        Return and delete the messages whose timestamp allows reprocessing
        Returns:
            list of Kafka message objects to be processed
        """
        result = []
        current_timestamp = _get_current_timestamp()
        to_remove = set()
        for message_timestamp in sorted(self.__schedule.keys()):
            message_list = self.__schedule[message_timestamp]
            if message_timestamp <= current_timestamp:
                to_remove.add(message_timestamp)
                result.extend(message_list)
        for resolved_item in to_remove:
            self.__schedule.pop(resolved_item)
        return result

    def add_if_applicable(self, message: Message) -> bool:
        """
        Only add the message if it is currently blocked. If this
        method returns True, the message should not be processed
        further at this point and should be aborted instead.

        Further processing should then be checked with pop_reprocessable().
        Only when the message is popped, it can be reprocessed.
        Args:
            message: Kafka message object
        Returns: True if the message should be scheduled, False
            if the message can be processed already.
        """
        retry_timestamp = _get_retry_timestamp(message)
        if retry_timestamp is None:
            LOGGER.debug(
                "Message from topic %s was not configured to wait in a schedule",
                message.topic(),
                extra={"message_raw": message.value()},
            )
            return False
        if _get_current_timestamp() > retry_timestamp:
            # Ready for processing already
            return False
        LOGGER.debug(
            "Message from topic %s scheduled for processing at timestamp %s",
            message.topic(),
            retry_timestamp,
            extra={"message_raw": message.value()},
        )
        self.__schedule.setdefault(retry_timestamp, []).append(message)
        return True

    def _cleanup(self) -> None:
        """
        Clean up empty keys in the schedule (messages were deleted,
        but the dictionary key could remain).
        """
        keys_to_remove = set()
        for timestamp, messages in self.__schedule.items():
            if not messages:
                keys_to_remove.add(timestamp)
        for timestamp in keys_to_remove:
            self.__schedule.pop(timestamp)

    def register_revoke(self, partitions: list[TopicPartition]) -> None:
        """
        Handle revocation of partitions. This happens during
        cluster rebalancing.
        Args:
            partitions: list of partitions that are revoked
        Returns: Nothing
        """
        # Create a set of revoked partition information
        # in the form of tuple with topic name and partition
        # number for fast lookup
        fast_lookup_partition_info = {
            (partition.topic, partition.partition) for partition in partitions
        }
        for timestamp, messages in self.__schedule.items():
            indexes_to_remove: list[int] = []
            for idx, message in enumerate(messages):
                if (
                    message.topic(),
                    message.partition(),
                ) not in fast_lookup_partition_info:
                    # Message is not in one of the revoked partitions
                    continue
                LOGGER.info(
                    "Message from topic %s scheduled for processing at "
                    "timestamp %s will be discarded without committing "
                    "due to rebalancing. The next assigned consumer shall "
                    "retry reprocessing it.",
                    message.topic(),
                    timestamp,
                )
                indexes_to_remove.insert(0, idx)
            for idx in indexes_to_remove:
                # Delete message from schedule, this is in a reverse
                # order, ensured by inserting to index 0 instead of
                # appending
                messages.pop(idx)
        self._cleanup()


class RetryManager:
    """
    This class re-sends failed messages to a specified retry topic.
    """

    # pylint: disable=too-few-public-methods

    def __validate(self) -> None:
        """
        Check if the retry configuration is valid.
        Raises: AssertionError if the retry configuration is invalid
        """
        retry_topic_names = {
            retry_config.retry_topic
            for retry_config in self.__config.topics
            if retry_config.retry_topic is not None
        }
        assert len(retry_topic_names) == len(
            [
                retry_config.retry_topic
                for retry_config in self.__config.topics
                if retry_config.retry_topic is not None
            ]
        ), "Two consumed topics cannot share the same retry topic!"

    def __init__(self, config: ConsumerConfig) -> None:
        self.__config = config
        self.__validate()
        self.__retry_producers: dict[str, BaseProducer] = {}
        self.__topic_lookup: dict[str, ConsumeTopicConfig] = {}
        self.__populate_topics_and_producers()

    def __populate_topics_and_producers(self) -> None:
        """
        Help initiate the manager without polluting the constructor.
        """
        for topic_config in self.__config.topics:
            if topic_config.retry_topic is None:
                continue
            # Assign topic lookup
            self.__topic_lookup[topic_config.retry_topic] = topic_config
            self.__topic_lookup[topic_config.base_topic] = topic_config
            # Assign producer lookup
            producer_config = ProducerConfig(
                topics=[topic_config.retry_topic],
                kafka_hosts=self.__config.kafka_hosts,
                username=self.__config.username,
                password=self.__config.password,
                additional_settings=self.__config.additional_settings,
            )
            producer = BaseProducer(config=producer_config)
            self.__retry_producers[topic_config.retry_topic] = producer
            self.__retry_producers[topic_config.base_topic] = producer

    def _get_retry_headers(self, message: Message) -> dict[str, str | bytes] | None:
        """
        Create a dictionary of retry headers that will be used for the retried mechanism.
        The headers are generated based on the headers from the previous message.
        Args:
            message: Kafka message that will be retried
        Returns: dictionary of retry headers used for next sending
        """
        message_topic: str = message.topic()  # type: ignore[assignment]
        relevant_config = self.__topic_lookup.get(message_topic)
        if relevant_config is None:
            return None
        previous_attempt = _get_retry_attempt(message)
        retry_timestamp = _get_current_timestamp() + relevant_config.fallback_delay
        return {
            ATTEMPT_HEADER: (previous_attempt + 1).to_bytes(length=8, byteorder="big"),
            TIMESTAMP_HEADER: int(retry_timestamp).to_bytes(length=8, byteorder="big"),
        }

    def resend_message(self, message: Message) -> None:
        """
        Send the message's copy to the specified retry topic.
        Also update its headers so that it will only be retried after
        the specified amount of time. Skips empty messages.

        If the message has already exhausted its retry attempts, it will
        not be resent and will be logged as a warning.

        Args:
            message: the Kafka message that failed to be processed
        """
        message_topic = message.topic()
        message_value = message.value()
        if message_topic is None or message_value is None:
            return
        relevant_producer = self.__retry_producers.get(message_topic)
        if relevant_producer is None:
            LOGGER.debug(
                "Message %s from topic %s does not have configured retry topic.",
                message,
                message_topic,
            )
            return

        # Check if we've exhausted retry attempts
        relevant_config = self.__topic_lookup.get(message_topic)
        if relevant_config is not None:
            current_attempt = _get_retry_attempt(message)
            if current_attempt >= relevant_config.retries:
                LOGGER.warning(
                    "Message from topic %s exhausted all %d retry attempts. "
                    "Message will not be retried.",
                    message_topic,
                    relevant_config.retries,
                    extra={"message_raw": message.value()},
                )
                return

        try:
            relevant_producer.send_sync(
                message_value, headers=self._get_retry_headers(message)
            )
            LOGGER.debug(
                "Message from topic sent for reprocessing, %s",
                message_topic,
                extra={"message_raw": message.value()},
            )
        except (TypeError, BufferError, KafkaException):
            LOGGER.exception(
                "Cannot resend message from topic: %s to its retry topic %s",
                message_topic,
                relevant_producer.topics,
                extra={"message_raw": message.value()},
            )
