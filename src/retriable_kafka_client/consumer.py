"""Base Kafka Consumer module"""

import json
import logging
import sys
from concurrent.futures import Executor, Future
from concurrent.futures.process import BrokenProcessPool
from typing import Any

from confluent_kafka import Consumer, Message, KafkaException, TopicPartition

from .kafka_utils import message_to_partition
from .kafka_settings import KafkaOptions, DEFAULT_CONSUMER_SETTINGS
from .consumer_tracking import TrackingManager
from .config import ConsumerConfig
from .retry_utils import (
    RetryManager,
    RetryScheduleCache,
)

LOGGER = logging.getLogger(__name__)


class BaseConsumer:
    """
    Base class for consuming from Kafka topics in Python.
    The class utilizes executor for concurrent task processing,
    this executor can even be shared between multiple consumer classes
    (intended usage).

    To synchronize messages correctly, this class holds information about
    messages that are being processed and messages that are waiting to be
    committed. The messages may wait for commiting because some message
    received before them may not be processed yet. And we always want
    to commit only the latest processed message that is not preceded
    by any non-processed (pending) message.

    This consumer is also capable of using a retry mechanism, it uses
    RetryManager to get the correct producer object and resend the message
    to a retry topic. This message is marked with a special header
    that is checked in each consume. The header contains a timestamp
    and each message with this timestamp will only be consumed after
    that timestamp passes. To track messages pending for processing
    due to this timestamping, the class RetryScheduleCache is used.

    Consumption is stopped for each partition where a blocking message
    appears to spare local memory. Consumption is resumed when the blocking
    timestamp passes.
    """

    # pylint: disable=too-many-instance-attributes

    def __validate(self) -> None:
        base_topic_names = {
            retry_config.base_topic for retry_config in self._config.topics
        }
        assert len(base_topic_names) == len(
            [retry_config.base_topic for retry_config in self._config.topics]
        ), "Cannot consume twice from the same topic!"

    def __init__(
        self,
        config: ConsumerConfig,
        executor: Executor,
        max_concurrency: int = 16,
    ):
        """
        Initialize a consumer.
        Args:
            config: The configuration object
            executor: The executor pool used by this consumer.
            max_concurrency: The maximum number of messages that can be
                submitted to the executor from this consumer at the same time.
        """
        # Consumer configuration
        self._config = config
        # Validate the configuration
        self.__validate()
        # Executor for target tasks
        self._executor = executor
        # Used for Kafka connections
        self.__consumer_object: Consumer | None = None
        # Used for stopping the consumption
        self.__stop_flag: bool = False
        # Store information about offsets and tasks
        self.__tracking_manager = TrackingManager(
            max_concurrency, config.cancel_future_wait_time
        )
        # Manage re-sending messages to retry topics
        self.__retry_manager = RetryManager(config)
        # Store information about pending retried messages
        self.__schedule_cache = RetryScheduleCache()

    @property
    def _consumer(self) -> Consumer:
        """
        Create the consumer object, keep it in memory.
        Returns: Kafka consumer object.
        """
        if not self.__consumer_object:
            config_dict = {
                KafkaOptions.KAFKA_NODES: ",".join(self._config.kafka_hosts),
                KafkaOptions.USERNAME: self._config.username,
                KafkaOptions.PASSWORD: self._config.password,
                KafkaOptions.GROUP_ID: self._config.group_id,
                **DEFAULT_CONSUMER_SETTINGS,
            }
            config_dict.update(self._config.additional_settings)
            self.__consumer_object = Consumer(
                config_dict,
            )
        return self.__consumer_object

    ### Offset-related methods ###

    def __perform_commits(self) -> None:
        """
        Commit anything that is awaiting to be committed.
        """
        committable = self.__tracking_manager.pop_committable()
        if committable:
            self._consumer.commit(offsets=committable, asynchronous=False)

    def __on_revoke(self, _: Consumer, partitions: list[TopicPartition]) -> None:
        """
        Callback when partitions are revoked during rebalancing.
        This is called in the same thread as poll, directly by the underlying
        Kafka library.
        """
        self.__schedule_cache.register_revoke(partitions)
        self.__tracking_manager.register_revoke(partitions)
        self.__perform_commits()

    def __ack_message(self, message: Message, finished_future: Future) -> None:
        """
        Private method only ever intended to be used from within
        _process_message(). It commits offsets and releases
        semaphore for processing next messages.
        Args:
            message: The Kafka message to be acknowledged
            finished_future: The finished future which caused this call
        """
        if finished_future.cancelled():
            return
        try:
            if problem := finished_future.exception():
                LOGGER.error(
                    "Message could not be processed! Message: %s.",
                    message.value(),
                    exc_info=problem,
                )
                self.__retry_manager.resend_message(message)
        finally:
            self.__tracking_manager.schedule_commit(message)

    ### Retry methods ###

    def __process_retried_messages_from_schedule(self) -> None:
        """
        Perform all actions that are scheduled for processing and
        their schedule has passed already
        """
        reprocessable = self.__schedule_cache.pop_reprocessable()
        for message in reprocessable:
            LOGGER.debug(
                "Retrying processing message from topic: %s",
                message.topic(),
                extra={"raw_message": message.value()},
            )
            self._process_message(message)
        # Resume consumption from the topic, the latest message's schedule
        # has passed
        self._consumer.resume(
            [message_to_partition(message) for message in reprocessable]
        )

    ### Main processing function ###

    def _process_message(self, message: Message) -> Future[Any] | None:
        """
        Process this message with the specified target function
        with usage of the executor.
        Args:
            message: Kafka message object. Only its value will be used
                for deserialization and passing to the target function.
        Returns: Future of the target execution if the message can be processed.
            None otherwise.
        """
        message_value = message.value()
        if not message_value:
            # Discard empty messages
            self._consumer.commit(message)
            return None
        try:
            message_data = json.loads(message_value)
        except json.decoder.JSONDecodeError:
            # This message cannot be deserialized, just log and discard it
            LOGGER.exception("Decoding error: not a valid JSON: %s", message.value())
            self._consumer.commit(message)
            return None
        future = self._executor.submit(self._config.target, message_data)
        self.__tracking_manager.process_message(message, future)
        # The semaphore is released within this callback
        future.add_done_callback(lambda res: self.__ack_message(message, res))
        return future

    ### Public methods ###

    def run(self) -> None:
        """
        Run the consumer. This starts consuming messages from kafka
        and their processing within the process pool.
        """
        plain_topics = [topic.base_topic for topic in self._config.topics]
        retry_topics = [
            topic.retry_topic
            for topic in self._config.topics
            if topic.retry_topic is not None
        ]
        self._consumer.subscribe(
            [*plain_topics, *retry_topics], on_revoke=self.__on_revoke
        )
        while not self.__stop_flag:
            try:
                # First resolve local messages waiting in schedule
                self.__process_retried_messages_from_schedule()
                # Then poll Kafka messages
                msg = self._consumer.poll(1)
                if msg is None:
                    LOGGER.debug(
                        "No message received currently.",
                    )
                    # This allows interrupting the script
                    # each second
                    continue
                if error := msg.error():
                    LOGGER.debug("Consumer error: %s", error.str())
                    continue
                LOGGER.debug("Consumer received message at offset: %s", msg.offset())
                if self.__schedule_cache.add_if_applicable(msg):
                    # Skip messages not eligible for execution yet, schedule them
                    self._consumer.pause([message_to_partition(msg)])
                    continue
                self._process_message(msg)
                self.__perform_commits()
            except BrokenProcessPool:
                LOGGER.exception("Process pool got broken, stopping consumer.")
                self.stop()
                sys.exit(1)

    def connection_healthcheck(self) -> bool:
        """Programmatically check if we are able to read from Kafka."""
        try:
            self._consumer.list_topics(timeout=5)
            return True
        except KafkaException as e:
            LOGGER.debug("Error while connecting to Kafka %s", e)
            return False

    def stop(self) -> None:
        """
        Gracefully stop the consumer.
        """
        LOGGER.debug("Stopping retriable consumer...")
        self.__stop_flag = True
        self.__tracking_manager.register_revoke()
        self.__perform_commits()
        try:
            LOGGER.debug("Shutting down Kafka consumer...")
            self._consumer.close()
        except (RuntimeError, KafkaException):  # pragma: no cover
            LOGGER.debug("Consumer already closed.")
