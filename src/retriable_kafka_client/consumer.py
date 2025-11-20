import json
import logging
import sys
from concurrent.futures import Executor, Future
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import Semaphore
from typing import Any

from confluent_kafka import Consumer, Message, KafkaException

from .types import TopicConfig

LOGGER = logging.getLogger(__name__)


class BaseConsumer:
    def __init__(
        self,
        /,
        config: TopicConfig,
        executor: Executor,
        max_concurrency: int = 16,
    ):
        """
        Initialize a Consumer.
        :param config: The configuration object
        :param executor: The executor pool used by this consumer.
        :param max_concurrency: The maximum number of messages that can be
            submitted to the executor from this consumer at the same time.
        """
        self._config = config
        self._executor = executor
        self._max_concurrency = max_concurrency
        self.__semaphore = Semaphore(max_concurrency)
        self.__consumer_object: Consumer | None = None
        self.__stop_flag: bool = False

    @property
    def _consumer(self) -> Consumer:
        """
        Create the consumer object, keep it in memory.
        :return: The initialized consumer.
        """
        if not self.__consumer_object:
            self.__consumer_object = Consumer(
                {
                    "bootstrap.servers": ",".join(self._config.kafka_hosts),
                    "group.id": self._config.group_id,
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                    "sasl.mechanisms": "SCRAM-SHA-512",
                    "security.protocol": "SASL_SSL",
                    "sasl.username": self._config.user_name,
                    "sasl.password": self._config.password,
                }
            )
        return self.__consumer_object

    def connection_healthcheck(self) -> bool:
        """Programmatically check if we are able to read from Kafka."""
        try:
            self._consumer.list_topics(timeout=5)
            return True
        except KafkaException as e:
            LOGGER.debug("Error while connecting to Kafka %s" % e)
            return False

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
        self._consumer.commit(message)
        if problem := finished_future.exception():
            LOGGER.error(
                "Message could not be processed! Message: %s.",
                message.value(),
                exc_info=problem,
            )

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
            LOGGER.exception(f"Decoding error: not a valid JSON: {message.value()!r}")
            self._consumer.commit(message)
            return None
        self.__semaphore.acquire()
        future = self._executor.submit(self._config.target, message_data)
        # The semaphore is released within this callback
        future.add_done_callback(lambda res: self.__ack_message(message, res))
        return future

    def run(self):
        """
        Run the consumer. This starts consuming messages from kafka
        and their processing within the process pool.
        :return: Nothing
        """
        self._consumer.subscribe(self._config.topics)
        while not self.__stop_flag:
            try:
                msg = self._consumer.poll(1)
                if not msg:
                    # This allows interrupting the script
                    # each second
                    continue
                self._process_message(msg)
            except BrokenProcessPool:
                LOGGER.exception("Process pool got broken, stopping consumer.")
                self.stop()
                sys.exit(1)
        LOGGER.debug("Consumer stopped.")
        self.stop()

    def stop(self) -> None:
        """
        Gracefully stop the consumer.
        :return: Nothing
        """
        self.__stop_flag = True
        self._consumer.unsubscribe()
        self._consumer.close()
