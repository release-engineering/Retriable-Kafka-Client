from copy import copy
from typing import Any

import pytest

from retriable_kafka_client.kafka_settings import KafkaOptions
from retriable_kafka_client.config import CommonConfig
from retriable_kafka_client.health import HealthCheckClient


@pytest.fixture(scope="session")
def config_object(kafka_config: dict[str, Any]) -> CommonConfig:
    return CommonConfig(
        kafka_hosts=[kafka_config[KafkaOptions.KAFKA_NODES]],
        username=kafka_config[KafkaOptions.USERNAME],
        password=kafka_config[KafkaOptions.PASSWORD],
        additional_settings={
            KafkaOptions.AUTH_MECHANISM: "SCRAM-SHA-512",
            KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT",
        },
    )


def test_health_success(config_object: CommonConfig) -> None:
    healthcheck_client = HealthCheckClient(config_object)
    assert healthcheck_client.connection_healthcheck() is True


def test_health_failure(config_object: CommonConfig) -> None:
    copied_config = copy(config_object)
    copied_config.password = "invalid"
    assert HealthCheckClient(copied_config).connection_healthcheck() is False
