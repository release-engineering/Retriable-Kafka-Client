from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from retriable_kafka_client.config import CommonConfig
from retriable_kafka_client.health import (
    perform_healthcheck_using_client,
    HealthCheckClient,
)


def test_perform_healthcheck_using_client() -> None:
    mock_client = MagicMock()

    result = perform_healthcheck_using_client(mock_client)
    assert result is True
    mock_client.list_topics.assert_called_once_with(timeout=5)


def test_perform_healthcheck_using_client_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    mock_client = MagicMock()
    mock_client.list_topics.side_effect = KafkaException

    result = perform_healthcheck_using_client(mock_client)
    assert result is False
    mock_client.list_topics.assert_called_once_with(timeout=5)
    assert any("Error while connecting to Kafka" in msg for msg in caplog.messages)


@patch("retriable_kafka_client.health.Producer")
def test_healthcheck_client(mock_consumer_class: MagicMock) -> None:
    client = HealthCheckClient(
        CommonConfig(kafka_hosts=["foo"], username="bar", password="spam")
    )
    client.connection_healthcheck()
    mock_consumer_class.return_value.list_topics.assert_called_once_with(timeout=5)
