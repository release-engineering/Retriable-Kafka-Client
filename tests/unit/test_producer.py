"""Unit tests for BaseProducer"""

import asyncio
import json
from typing import Generator, Any
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import KafkaException

from retriable_kafka_client.producer import BaseProducer
from retriable_kafka_client.config import ProducerConfig


@pytest.fixture
def fast_config() -> ProducerConfig:
    """Producer config with very short backoff times for fast testing."""
    return ProducerConfig(
        kafka_hosts=["example.com:9092"],
        topics=["test_topic"],
        username="test_user",
        password="test_pass",
        retries=2,
        fallback_factor=1.1,
        fallback_base=0.01,
        additional_settings={},
    )


@pytest.fixture
def base_producer(fast_config: ProducerConfig) -> Generator[BaseProducer, None, None]:
    """Create a BaseProducer with mocked Kafka producer."""
    with patch("retriable_kafka_client.producer.Producer"):
        producer = BaseProducer(config=fast_config)
        # Cache the producer while it's mocked
        _ = producer._producer
        yield producer


def _run_send_method(
    producer: BaseProducer, method: str, message: Any, headers: Any = None
) -> None:
    """Helper to run either send or send_sync method."""
    if method == "send":
        asyncio.run(producer.send(message, headers))
    else:
        producer.send_sync(message, headers)


@pytest.mark.parametrize("send_method", ["send", "send_sync"])
def test_producer_send_success_first_attempt(
    base_producer: BaseProducer, send_method: str
) -> None:
    """Test successful message send on first attempt."""
    message = {"key": "value", "number": 42}

    with patch("time.time", return_value=1):
        _run_send_method(base_producer, send_method, message)

    mock_kafka_producer: MagicMock = base_producer._producer
    # Should produce to all topics once
    assert mock_kafka_producer.produce.call_count == len(base_producer._config.topics)
    for topic in base_producer._config.topics:
        mock_kafka_producer.produce.assert_any_call(
            topic=topic,
            value=json.dumps(message).encode("utf-8"),
            timestamp=1000,
            headers=None,
        )


@pytest.mark.parametrize("send_method", ["send", "send_sync"])
def test_producer_send_exhausts_retries_buffer_error(
    base_producer: BaseProducer, send_method: str
) -> None:
    """Test that BufferError is raised after all retries are exhausted."""
    mock_kafka_producer: MagicMock = base_producer._producer
    # Fail on all attempts (retries=2, so 3 total attempts)
    mock_kafka_producer.produce.side_effect = BufferError("Queue always full")

    with pytest.raises(BufferError, match="Queue always full"):
        _run_send_method(base_producer, send_method, {"fail": "test"})

    # Should have attempted 3 times (initial + 2 retries)
    assert mock_kafka_producer.produce.call_count == 3


@pytest.mark.parametrize("send_method", ["send", "send_sync"])
def test_producer_send_exhausts_retries_kafka_exception(
    base_producer: BaseProducer, send_method: str
) -> None:
    """Test that KafkaException is raised after all retries are exhausted."""
    mock_kafka_producer: MagicMock = base_producer._producer
    # Fail on all attempts
    mock_kafka_producer.produce.side_effect = KafkaException(MagicMock())

    with pytest.raises(KafkaException):
        _run_send_method(base_producer, send_method, {"error": "test"})

    # Should have attempted 3 times (initial + 2 retries)
    assert mock_kafka_producer.produce.call_count == 3


@pytest.mark.parametrize("send_method", ["send", "send_sync"])
def test_producer_send_non_json_serializable_message(
    base_producer: BaseProducer, send_method: str
) -> None:
    """Test that TypeError is raised for non-JSON-serializable messages."""

    # Object is not JSON serializable
    class NonSerializable:
        pass

    message = {"object": NonSerializable()}

    with pytest.raises(TypeError):
        _run_send_method(base_producer, send_method, message)


@pytest.mark.parametrize("send_method", ["send", "send_sync"])
def test_producer_send_nested_message(
    base_producer: BaseProducer, send_method: str
) -> None:
    """Test sending a nested message."""
    mock_kafka_producer: MagicMock = base_producer._producer

    message = {
        "user": {"id": 123, "name": "Alice"},
        "items": [{"id": 1, "qty": 2}, {"id": 2, "qty": 5}],
        "total": 15.99,
        "metadata": {"timestamp": "2023-01-01", "version": "1.0"},
    }

    _run_send_method(base_producer, send_method, message)

    # Verify the message was JSON encoded correctly
    expected_bytes = json.dumps(message).encode("utf-8")
    mock_kafka_producer.produce.assert_called_once()
    call_kwargs = mock_kafka_producer.produce.call_args[1]
    assert call_kwargs["value"] == expected_bytes


@pytest.mark.asyncio
async def test_producer_send_retry_on_buffer_error(base_producer: BaseProducer) -> None:
    """Test retry mechanism on BufferError (async version with timing check)."""
    mock_kafka_producer: MagicMock = base_producer._producer
    # First two attempts fail, third succeeds
    mock_kafka_producer.produce.side_effect = [
        BufferError("Queue full"),
        BufferError("Queue full"),
        None,
    ]

    message = {"test": "data"}

    start_time = asyncio.get_running_loop().time()
    await base_producer.send(message)
    elapsed_time = asyncio.get_running_loop().time() - start_time

    # Should have retried twice (2 failures + 1 success = 3 calls)
    assert mock_kafka_producer.produce.call_count == 3
    # Should have waited approximately: 0.01 + 0.011 = 0.021 seconds
    assert elapsed_time >= 0.02  # Allow some timing flexibility


def test_producer_close(base_producer: BaseProducer) -> None:
    """Test that close method flushes all messages."""
    mock_kafka_producer: MagicMock = base_producer._producer
    # Simulate flush returning 5, then 2, then 0 (all messages sent)
    mock_kafka_producer.flush.side_effect = [5, 2, 0]

    base_producer.close()

    # Should have called flush 3 times (until it returns 0)
    assert mock_kafka_producer.flush.call_count == 3
    mock_kafka_producer.flush.assert_called_with(1)


@pytest.mark.parametrize(
    "input_message,expected_output",
    [
        pytest.param(b"raw bytes", b"raw bytes", id="bytes_passthrough"),
        pytest.param({"key": "value"}, b'{"key": "value"}', id="dict_to_json"),
        pytest.param(
            {"num": 42, "flag": True},
            b'{"num": 42, "flag": true}',
            id="dict_with_types",
        ),
        pytest.param([], b"[]", id="empty_list"),
    ],
)
def test_serialize_message(input_message, expected_output) -> None:
    """Test that messages are correctly serialized to bytes."""
    # Access the private static method via name mangling
    result = BaseProducer._BaseProducer__serialize_message(input_message)
    assert result == expected_output
