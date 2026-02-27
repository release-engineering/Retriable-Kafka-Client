"""Module for detached Kafka cluster healthcheck"""

import logging
from typing import TypeVar

from confluent_kafka import KafkaException, Consumer, Producer

from retriable_kafka_client.config import CommonConfig
from retriable_kafka_client.kafka_settings import (
    KafkaOptions,
    DEFAULT_PRODUCER_SETTINGS,
)

LOGGER = logging.getLogger(__name__)

Client = TypeVar("Client", Producer, Consumer)


def perform_healthcheck_using_client(client: Client) -> bool:
    """
    Programmatically check if we are able to read from Kafka
    using the provided client.
    """
    try:
        client.list_topics(timeout=5)
        return True
    except KafkaException as e:
        LOGGER.warning("Error while connecting to Kafka %s", e)
        return False


class HealthCheckClient:
    """
    Class for only performing health checks on Kafka cluster.
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, config: CommonConfig):
        self.config = config
        config_dict = {
            KafkaOptions.KAFKA_NODES: ",".join(config.kafka_hosts),
            KafkaOptions.USERNAME: config.username,
            KafkaOptions.PASSWORD: config.password,
            **DEFAULT_PRODUCER_SETTINGS,
        }
        config_dict.update(config.additional_settings)
        # We use producer so the group ID can stay unfilled
        self._client = Producer(
            config_dict,
        )

    def connection_healthcheck(self) -> bool:
        """Programmatically check if we are able to read from Kafka."""
        return perform_healthcheck_using_client(self._client)
