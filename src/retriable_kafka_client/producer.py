"""Base Kafka producer module"""

import asyncio
import json
import logging
import time
from copy import copy
from typing import Any

from confluent_kafka import Producer, KafkaException

from .chunking import generate_group_id, calculate_header_size
from .headers import (
    CHUNK_GROUP_HEADER,
    NUMBER_OF_CHUNKS_HEADER,
    serialize_number_to_bytes,
    CHUNK_ID_HEADER,
)
from .health import perform_healthcheck_using_client
from .kafka_settings import (
    KafkaOptions,
    DEFAULT_PRODUCER_SETTINGS,
    DEFAULT_MESSAGE_SIZE,
    MESSAGE_OVERHEAD,
)
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
        self._config_dict = {
            KafkaOptions.KAFKA_NODES: ",".join(self._config.kafka_hosts),
            KafkaOptions.USERNAME: self._config.username,
            KafkaOptions.PASSWORD: self._config.password,
            **DEFAULT_PRODUCER_SETTINGS,
        }
        self._config_dict.update(**self._config.additional_settings)

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
            self.__producer_object = Producer(self._config_dict)
        return self.__producer_object

    def _get_chunk_size(
        self,
        headers: list[tuple[str, str | bytes]] | dict[str, str | bytes] | None,
    ) -> int:
        """Calculate chunk size to fit messages into Kafka."""
        chunk_size_base: int = self._config_dict.get(  # type: ignore[assignment]
            KafkaOptions.MAX_MESSAGE_SIZE, DEFAULT_MESSAGE_SIZE
        )
        return chunk_size_base - calculate_header_size(headers) - MESSAGE_OVERHEAD

    def __serialize_message(
        self,
        message: dict[str, Any] | bytes | list[bytes],
        headers: list[tuple[str, str | bytes]] | dict[str, str | bytes] | None,
        split_messages: bool,
    ) -> list[bytes]:
        """Convert message to bytes if needed."""
        # Get the information from rendered config dict
        # to take user overrides into consideration
        chunk_size = self._get_chunk_size(headers)
        if isinstance(message, list):
            if all(len(chunk) <= chunk_size for chunk in message) and split_messages:
                return message
            # Split is wrong, needs re-chunking
            message = b"".join(message)
        if isinstance(message, bytes):
            full_bytes = message
        else:
            full_bytes = json.dumps(message).encode("utf-8")
        if split_messages:
            result = []
            for i in range(0, len(full_bytes), chunk_size):
                result.append(full_bytes[i : i + chunk_size])
            return result
        return [full_bytes]

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
        message: dict[str, Any] | bytes | list[bytes],
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
        chunks = self.__serialize_message(message, headers, self._config.split_messages)
        number_of_chunks = len(chunks)
        problems: dict[str, Exception] = {}
        timestamp = int(time.time() * 1000)  # Kafka expects milliseconds
        for topic in self._config.topics:
            group_id = generate_group_id()
            for chunk_id, chunk in enumerate(chunks):
                headers = copy(headers) if headers else {}
                if self._config.split_messages:
                    headers[CHUNK_GROUP_HEADER] = group_id
                    headers[NUMBER_OF_CHUNKS_HEADER] = serialize_number_to_bytes(
                        number_of_chunks
                    )
                    headers[CHUNK_ID_HEADER] = serialize_number_to_bytes(chunk_id)
                for attempt_idx in range(self._config.retries + 1):
                    try:
                        self._producer.produce(
                            topic=topic,
                            value=chunk,
                            timestamp=timestamp,
                            headers=headers,
                            key=group_id,
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
        message: dict[str, Any] | bytes | list[bytes],
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
        chunks = self.__serialize_message(message, headers, self._config.split_messages)
        number_of_chunks = len(chunks)
        problems: dict[str, Exception] = {}
        timestamp = int(time.time() * 1000)  # Kafka expects milliseconds
        for topic in self._config.topics:
            group_id = generate_group_id()
            for chunk_id, chunk in enumerate(chunks):
                headers = copy(headers) if headers else {}
                if self._config.split_messages:
                    headers[CHUNK_GROUP_HEADER] = group_id
                    headers[NUMBER_OF_CHUNKS_HEADER] = serialize_number_to_bytes(
                        number_of_chunks
                    )
                    headers[CHUNK_ID_HEADER] = serialize_number_to_bytes(chunk_id)
                for attempt_idx in range(self._config.retries + 1):
                    try:
                        self._producer.produce(
                            topic=topic,
                            value=chunk,
                            timestamp=timestamp,
                            headers=headers,
                            key=group_id,
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

    def connection_healthcheck(self) -> bool:
        """Programmatically check if we are able to read from Kafka."""
        return perform_healthcheck_using_client(self._producer)

    def close(self) -> None:
        """
        Finish sending all messages, block until complete.
        """
        while messages := self._producer.flush(1):
            LOGGER.debug("Remaining messages in send queue: %d", messages)
