"""Base Kafka Consumer module"""

import json
import logging
import sys
from concurrent.futures import Executor, Future
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import Semaphore
from typing import Any

from confluent_kafka import Consumer, Message, KafkaException, TopicPartition

from .kafka_settings import KafkaOptions, DEFAULT_CONSUMER_SETTINGS
from .offset_cache import OffsetCache
from .types import ConsumerConfig

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
    """

    def __init__(
        self,
        config: ConsumerConfig,
        executor: Executor,
        max_concurrency: int = 16,
        **additional_settings: Any,
    ):
        """
        Initialize a Consumer.
        :param config: The configuration object
        :param executor: The executor pool used by this consumer.
        :param max_concurrency: The maximum number of messages that can be
            submitted to the executor from this consumer at the same time.
        :param additional_settings: Additional settings to pass to the confluent_kafka
            consumer. This overrides the provided defaults from
            .kafka_settings.DEFAULT_CONSUMER_SETTINGS
        """
        self._config = config
        self._executor = executor
        # Used to handle max concurrency
        self.__semaphore = Semaphore(max_concurrency)
        # Used for Kafka connections
        self.__consumer_object: Consumer | None = None
        # Used for stopping the consumption
        self.__stop_flag: bool = False
        # Override settings for Kafka Consumer object
        self.__additional_settings = additional_settings
        # Store information about offsets
        self.__offset_cache = OffsetCache()

    @property
    def _consumer(self) -> Consumer:
        """
        Create the consumer object, keep it in memory.
        :return: Kafka consumer object.
        """
        if not self.__consumer_object:
            config_dict = {
                KafkaOptions.KAFKA_NODES: ",".join(self._config.kafka_hosts),
                KafkaOptions.USERNAME: self._config.username,
                KafkaOptions.PASSWORD: self._config.password,
                KafkaOptions.GROUP_ID: self._config.group_id,
                **DEFAULT_CONSUMER_SETTINGS,
            }
            config_dict.update(self.__additional_settings)
            self.__consumer_object = Consumer(
                config_dict,
            )
        return self.__consumer_object

    ### Offset-related methods ###

    def __perform_commits(self) -> None:
        committable = self.__offset_cache.pop_committable()
        if committable:
            self._consumer.commit(offsets=committable, asynchronous=False)

    def __on_revoke(self, _: Consumer, __: list[TopicPartition]) -> None:
        """
        Callback when partitions are revoked during rebalancing.
        """
        if not self.__offset_cache.has_cache():
            return
        self.__perform_commits()
        self.__offset_cache.register_revoke()

    def __ack_message(self, message: Message, finished_future: Future) -> None:
        """
        Private method only ever intended to be used from within
        _process_message(). It commits offsets and releases
        semaphore for processing next messages.
        :param message: The Kafka message to be acknowledged
        :param finished_future:
        :return: Nothing
        """
        self.__semaphore.release()
        self.__offset_cache.schedule_commit(message)
        if problem := finished_future.exception():
            LOGGER.error(
                "Message could not be processed! Message: %s.",
                message.value(),
                exc_info=problem,
            )

    ### Main processing function ###

    def _process_message(self, message: Message) -> Future[Any] | None:
        """
        Process this message with the specified target function
        with usage of the executor.
        :param message: Kafka message object. Only its value will be used
            for deserialization and passing to the target function.
        :return: Future of the target execution if the message can be processed.
            None otherwise.
        """
        if error := message.error():
            if error.retriable():
                LOGGER.debug("Consumer error: %s", error.str())
                return None
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
        self.__offset_cache.process_message(message)
        self.__semaphore.acquire()
        future = self._executor.submit(self._config.target, message_data)
        # The semaphore is released within this callback
        future.add_done_callback(lambda res: self.__ack_message(message, res))
        return future

    ### Public methods ###

    def run(self) -> None:
        """
        Run the consumer. This starts consuming messages from kafka
        and their processing within the process pool.
        :return: Nothing
        """
        self._consumer.subscribe(self._config.topics, on_revoke=self.__on_revoke)
        while not self.__stop_flag:
            try:
                msg = self._consumer.poll(1)
                if not msg:
                    # This allows interrupting the script
                    # each second
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
        :return: Nothing
        """
        LOGGER.debug("Stopping consumer...")
        self.__stop_flag = True
        while self.__offset_cache.has_cache():
            self.__perform_commits()
        try:
            LOGGER.debug("Unsubscribing from topics: %s", self._config.topics)
            self._consumer.unsubscribe()
            try:
                LOGGER.debug("Shutting down consumer...")
                self._executor.map(self._consumer.close, tuple(), timeout=10)
            except TimeoutError:  # pragma: no cover
                # See https://github.com/confluentinc/confluent-kafka-python/issues/1667
                LOGGER.warning(
                    "Kafka consumer failed to close! Next reconnection may be slow!"
                )
        except (RuntimeError, KafkaException):  # pragma: no cover
            LOGGER.debug("Consumer already closed.")
