"""Base Kafka producer module"""

import asyncio
import json
import logging
import time
from typing import Any

from confluent_kafka import Producer, KafkaException

from .kafka_settings import KafkaOptions, DEFAULT_PRODUCER_SETTINGS
from .types import ProducerConfig

LOGGER = logging.getLogger(__name__)


class BaseProducer:
    """
    Base class for producing to Kafka topics in Python.
    """

    def __init__(self, config: ProducerConfig, **additional_settings: Any):
        """
        Initialize a Producer.
        :param config: The configuration object
        :param additional_settings: Additional settings to pass to the confluent_kafka
            producer. This overrides the provided defaults from
            .kafka_settings.DEFAULT_PRODUCER_SETTINGS
        """
        self._config = config
        self.__producer_object: Producer | None = None
        self.__additional_settings = additional_settings

    @property
    def _producer(self) -> Producer:
        """
        Get and cache the producer object.
        :return: Kafka producer object.
        """
        if not self.__producer_object:
            config_dict = {
                KafkaOptions.KAFKA_NODES: ",".join(self._config.kafka_hosts),
                KafkaOptions.USERNAME: self._config.user_name,
                KafkaOptions.PASSWORD: self._config.password,
                **DEFAULT_PRODUCER_SETTINGS,
            }
            config_dict.update(**self.__additional_settings)
            self.__producer_object = Producer(config_dict)
        return self.__producer_object

    async def send(self, message: dict[str, Any]) -> None:
        """
        Send a message to the specified topics. Automatically retry several times with
        exponential backoff. Backoff is configurable.
        Attributes:
            message: JSON-serializable data to be published to the specified topics
        Raises:
            TypeError: if message is not a JSON-serializable object
            BufferError: if Kafka queue is full even after all attempts
            KafkaException: if some Kafka error occurs even after all attempts
        """
        byte_message = json.dumps(message).encode("utf-8")
        for attempt_idx in range(self._config.retries + 1):
            try:
                # Get the timestamp so no surprises are raised from underlying C lib
                timestamp = int(time.time())
                for topic in self._config.topics:
                    self._producer.produce(
                        topic=topic, value=byte_message, timestamp=timestamp
                    )
                break
            except (BufferError, KafkaException) as err:
                if attempt_idx < self._config.retries:
                    await asyncio.sleep(
                        self._config.fallback_base
                        * self._config.fallback_factor**attempt_idx
                    )
                    continue
                raise err

    def close(self) -> None:
        """
        Finish sending all messages, block until complete.
        :return: Nothing
        """
        while messages := self._producer.flush(1):
            LOGGER.debug("Remaining messages in send queue: %d", messages)
