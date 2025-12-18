"""Base Kafka producer module"""

import asyncio
import json
import logging
import time
from typing import Any

from confluent_kafka import Producer, KafkaException

from .kafka_settings import KafkaOptions, DEFAULT_PRODUCER_SETTINGS
from .config import ProducerConfig

LOGGER = logging.getLogger(__name__)


class BaseProducer:
    """
    Base class for producing to Kafka topics in Python.
    """

    def __init__(self, config: ProducerConfig):
        """
        Initialize a Producer.
        Parameters:
            config: The configuration object
        """
        self._config = config
        self.__producer_object: Producer | None = None

    @property
    def topics(self) -> list[str]:
        """Return topics this producer produces to."""
        return self._config.topics

    @property
    def _producer(self) -> Producer:
        """
        Get and cache the producer object.
        """
        if not self.__producer_object:
            config_dict = {
                KafkaOptions.KAFKA_NODES: ",".join(self._config.kafka_hosts),
                KafkaOptions.USERNAME: self._config.username,
                KafkaOptions.PASSWORD: self._config.password,
                **DEFAULT_PRODUCER_SETTINGS,
            }
            config_dict.update(**self._config.additional_settings)
            self.__producer_object = Producer(config_dict)
        return self.__producer_object

    @staticmethod
    def __serialize_message(message: dict[str, Any] | bytes) -> bytes:
        """Convert message to bytes if needed."""
        if isinstance(message, bytes):
            return message
        return json.dumps(message).encode("utf-8")

    def __calculate_backoff(self, attempt_idx: int) -> float:
        """Calculate exponential backoff time for a given attempt."""
        return self._config.fallback_base * self._config.fallback_factor**attempt_idx

    def __log_retry(self, attempt_idx: int, backoff_time: float) -> None:
        """Log retry attempt information."""
        LOGGER.debug(
            "Producing a message to topics %s failed, attempting "
            "to send again in %s seconds (this is attempt %s out of %s)",
            self._config.topics,
            backoff_time,
            attempt_idx,
            self._config.retries,
        )

    @staticmethod
    def __handle_problems(problems: dict[str, Exception]) -> None:
        """Log all problems and raise the first one if any exist."""
        if problems:
            for problem_topic, problem in problems.items():
                LOGGER.error("Cannot produce to topic %s: %s", problem_topic, problem)
            raise next(iter(problems.values()))

    def send_sync(
        self,
        message: dict[str, Any] | bytes,
        headers: dict[str, str | bytes] | None = None,
    ) -> None:
        """
        Send a message to the specified topics. Automatically retry several times with
        exponential backoff. Backoff is configurable. Synchronized method.
        Attributes:
            message: JSON-serializable or JSON-serialized data to be
                published to the specified topics
            headers: Kafka headers to add to the message
        Raises:
            TypeError: if message is not a JSON-serializable object nor bytes
            BufferError: if Kafka queue is full even after all attempts
            KafkaException: if some Kafka error occurs even after all attempts
        """
        byte_message = self.__serialize_message(message)
        problems: dict[str, Exception] = {}
        timestamp = int(time.time() * 1000)  # Kafka expects milliseconds
        for topic in self._config.topics:
            for attempt_idx in range(self._config.retries + 1):
                try:
                    self._producer.produce(
                        topic=topic,
                        value=byte_message,
                        timestamp=timestamp,
                        headers=headers,
                    )
                    break
                except (BufferError, KafkaException) as err:
                    if attempt_idx < self._config.retries:
                        backoff_time = self.__calculate_backoff(attempt_idx)
                        self.__log_retry(attempt_idx, backoff_time)
                        time.sleep(backoff_time)
                        continue
                    problems[topic] = err
        self._producer.flush()
        self.__handle_problems(problems)

    async def send(
        self,
        message: dict[str, Any] | bytes,
        headers: dict[str, str | bytes] | None = None,
    ) -> None:
        """
        Send a message to the specified topics. Automatically retry several times with
        exponential backoff. Backoff is configurable. Asynchronous method.
        Attributes:
            message: JSON-serializable or JSON-serialized data to be
                published to the specified topics
            headers: Kafka headers to add to the message
        Raises:
            TypeError: if message is not a JSON-serializable object nor bytes
            BufferError: if Kafka queue is full even after all attempts
            KafkaException: if some Kafka error occurs even after all attempts
        """
        byte_message = self.__serialize_message(message)
        problems: dict[str, Exception] = {}
        timestamp = int(time.time() * 1000)  # Kafka expects milliseconds
        for topic in self._config.topics:
            for attempt_idx in range(self._config.retries + 1):
                try:
                    self._producer.produce(
                        topic=topic,
                        value=byte_message,
                        timestamp=timestamp,
                        headers=headers,
                    )
                    break
                except (BufferError, KafkaException) as err:
                    if attempt_idx < self._config.retries:
                        backoff_time = self.__calculate_backoff(attempt_idx)
                        self.__log_retry(attempt_idx, backoff_time)
                        await asyncio.sleep(backoff_time)
                        continue
                    problems[topic] = err
        self._producer.flush()
        self.__handle_problems(problems)

    def close(self) -> None:
        """
        Finish sending all messages, block until complete.
        """
        while messages := self._producer.flush(1):
            LOGGER.debug("Remaining messages in send queue: %d", messages)
