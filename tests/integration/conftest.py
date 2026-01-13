"""
Shared fixtures for integration tests.

The IntegrationTestHarness in integration_utils.py handles topic creation
automatically. These fixtures provide the basic Kafka connectivity.
"""

import time
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient
from retriable_kafka_client.kafka_settings import KafkaOptions


@pytest.fixture(scope="session")
def kafka_config() -> dict[str, Any]:
    """Kafka connection configuration for local testing"""
    return {
        KafkaOptions.KAFKA_NODES: "localhost:9092",
        KafkaOptions.USERNAME: "testuser",
        KafkaOptions.PASSWORD: "testpassword",
    }


@pytest.fixture(scope="session")
def admin_client(kafka_config: dict[str, Any]) -> AdminClient:
    """Kafka AdminClient for managing topics and checking cluster health"""
    return AdminClient(
        {
            KafkaOptions.AUTH_MECHANISM: "SCRAM-SHA-512",
            KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT",
            **kafka_config,
        }
    )


@pytest.fixture(scope="session", autouse=True)
def wait_for_kafka(admin_client: AdminClient) -> None:
    """Wait for Kafka to be healthy before running any tests"""

    max_attempts = 30
    for attempt in range(1, max_attempts + 1):
        try:
            # Try to get cluster metadata
            metadata = admin_client.list_topics(timeout=5)
            if metadata.brokers:
                return
        except Exception:
            pass

        if attempt < max_attempts:
            time.sleep(1)

    pytest.fail(
        f"Kafka did not become healthy after {max_attempts} seconds. "
        "Ensure Kafka is running with: docker compose up -d"
    )
