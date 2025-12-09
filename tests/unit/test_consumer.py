import json
import logging
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Generator
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import Message, KafkaException, TopicPartition

from retriable_kafka_client import BaseConsumer, ConsumerConfig


@pytest.fixture
def sample_config() -> ConsumerConfig:
    return ConsumerConfig(
        topics=["foo", "bar"],
        target=lambda _: None,
        kafka_hosts=["example.com"],
        group_id="baz",
        user_name="user",
        password="pass",
    )


@pytest.fixture
def concurrency() -> int:
    return 2


@pytest.fixture
def executor(concurrency: int) -> Executor:
    return ProcessPoolExecutor(max_workers=concurrency)


@pytest.fixture
def base_consumer(
    sample_config: ConsumerConfig, concurrency: int, executor: Executor
) -> Generator[BaseConsumer, None, None]:
    with patch("retriable_kafka_client.BaseConsumer._consumer"):
        yield BaseConsumer(
            config=sample_config, executor=executor, max_concurrency=concurrency
        )


def test_consumer__process_message_decode_fail(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    mock_message = MagicMock(
        spec=Message, value=lambda: b"---\nthis: is not a json\n", error=lambda: None
    )

    assert base_consumer._process_message(mock_message) is None
    assert "Decoding error: not a valid JSON" in caplog.messages[-1]


def test_consumer__process_message_with_retriable_error(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that retriable errors are logged and message is not processed."""
    caplog.set_level(logging.DEBUG)
    mock_error = MagicMock()
    mock_error.retriable.return_value = True
    mock_error.str.return_value = "Retriable error"
    mock_message = MagicMock(spec=Message, error=lambda: mock_error)

    result = base_consumer._process_message(mock_message)
    assert result is None
    assert any("Consumer error: Retriable error" in msg for msg in caplog.messages)


def test_consumer__process_message_with_non_retriable_error(
    base_consumer: BaseConsumer,
) -> None:
    """Test that non-retriable errors are handled and processing continues."""
    mock_error = MagicMock()
    mock_error.retriable.return_value = False
    mock_message = MagicMock(
        spec=Message, error=lambda: mock_error, value=lambda: b'{"key": "value"}'
    )

    with patch.object(base_consumer._executor, "submit") as mock_submit:
        mock_future = MagicMock(spec=Future)
        mock_submit.return_value = mock_future
        result = base_consumer._process_message(mock_message)
        assert result is not None
        mock_submit.assert_called_once()


def test_consumer__process_message_empty_value(
    base_consumer: BaseConsumer,
) -> None:
    """Test that empty messages are discarded and committed."""
    mock_message = MagicMock(spec=Message, value=lambda: None, error=lambda: None)
    mock_consumer = base_consumer._consumer
    mock_consumer.commit = MagicMock()

    result = base_consumer._process_message(mock_message)
    assert result is None
    mock_consumer.commit.assert_called_once_with(mock_message)


def test_consumer__process_message_valid_json(
    base_consumer: BaseConsumer,
) -> None:
    """Test that valid JSON messages are processed."""
    message_data = {"key": "value", "number": 42}
    message_json = json.dumps(message_data).encode()
    mock_message = MagicMock(
        spec=Message, value=lambda: message_json, error=lambda: None
    )

    with patch.object(base_consumer._executor, "submit") as mock_submit:
        mock_future = MagicMock(spec=Future)
        mock_submit.return_value = mock_future
        result = base_consumer._process_message(mock_message)

        assert result is not None
        mock_submit.assert_called_once_with(base_consumer._config.target, message_data)
        assert mock_future.add_done_callback.called


def test_consumer_connection_healthcheck_success(
    base_consumer: BaseConsumer,
) -> None:
    """Test successful connection healthcheck."""
    mock_consumer = base_consumer._consumer
    mock_consumer.list_topics.return_value = None

    result = base_consumer.connection_healthcheck()
    assert result is True
    mock_consumer.list_topics.assert_called_once_with(timeout=5)


def test_consumer_connection_healthcheck_failure(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test connection healthcheck failure handling."""
    caplog.set_level(logging.DEBUG)
    mock_consumer = base_consumer._consumer
    mock_consumer.list_topics.side_effect = KafkaException("Connection failed")

    result = base_consumer.connection_healthcheck()
    assert result is False
    assert any("Error while connecting to Kafka" in msg for msg in caplog.messages)


def test_consumer__consumer_property_reuses_instance(
    sample_config: ConsumerConfig,
    executor: Executor,
) -> None:
    """Test that consumer property returns the same instance on subsequent calls."""
    with patch("retriable_kafka_client.consumer.Consumer") as mock_consumer_class:
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        consumer = BaseConsumer(
            config=sample_config, executor=executor, max_concurrency=2
        )

        consumer1 = consumer._consumer
        consumer2 = consumer._consumer

        assert consumer1 is consumer2
        assert mock_consumer_class.call_count == 1



@pytest.mark.parametrize(
    "poll_behavior,stop_after_polls",
    [
        pytest.param("stop_after_first", 1, id="stop_after_first_poll"),
        pytest.param("continue_none", 2, id="continue_with_none_messages"),
        pytest.param("normal_message", 1, id="normal_message_processing"),
    ],
)
def test_consumer_run(
    base_consumer: BaseConsumer,
    poll_behavior: str,
    stop_after_polls: int,
) -> None:
    """Test run method with various poll behaviors."""
    mock_consumer = base_consumer._consumer
    mock_consumer.subscribe = MagicMock()

    call_count = 0
    mock_message = MagicMock() if poll_behavior == "normal_message" else None

    def poll_side_effect(*_, **__):
        nonlocal call_count
        call_count += 1
        if call_count >= stop_after_polls:
            base_consumer._BaseConsumer__stop_flag = True
        return mock_message

    mock_consumer.poll.side_effect = poll_side_effect

    if poll_behavior == "normal_message":
        with patch.object(base_consumer, "_process_message") as mock_process:
            # Set stop flag after processing message to exit loop
            base_consumer.run()
            mock_process.assert_called_once_with(mock_message)
    else:
        base_consumer.run()

    # subscribe is now called with on_revoke callback
    assert mock_consumer.subscribe.call_count == 1
    call_args = mock_consumer.subscribe.call_args
    assert call_args[0][0] == base_consumer._config.topics

    mock_consumer.poll.assert_called()
    if poll_behavior == "continue_none":
        assert call_count >= stop_after_polls


def test_consumer_run_handles_broken_process_pool(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that BrokenProcessPool exception is handled and consumer stops."""
    caplog.set_level(logging.ERROR)

    mock_consumer = base_consumer._consumer
    mock_consumer.poll.side_effect = BrokenProcessPool("Pool broken")
    mock_consumer.subscribe = MagicMock()

    with patch("sys.exit") as mock_exit:
        base_consumer.run()
        mock_exit.assert_called_once_with(1)
        assert base_consumer._BaseConsumer__stop_flag is True
        assert any("Process pool got broken" in msg for msg in caplog.messages)


def test_ack_message_with_exception(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test __ack_message when the future raises an exception."""
    caplog.set_level(logging.ERROR)

    # Mock the semaphore
    mock_semaphore = MagicMock()
    base_consumer._BaseConsumer__semaphore = mock_semaphore

    # Mock the offset cache
    mock_offset_cache = MagicMock()
    base_consumer._BaseConsumer__offset_cache = mock_offset_cache

    # Create a mock message
    mock_message = MagicMock(
        spec=Message,
        value=lambda: b'{"test": "data"}',
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 42,
    )

    # Create a mock future that raises an exception
    test_exception = ValueError("Test processing error")
    mock_future = MagicMock(spec=Future)
    mock_future.exception.return_value = test_exception

    # Call __ack_message
    base_consumer._BaseConsumer__ack_message(mock_message, mock_future)

    # Verify semaphore was released
    mock_semaphore.release.assert_called_once()

    # Verify error was logged
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "ERROR"
    assert "Message could not be processed!" in caplog.records[0].message
    assert '{"test": "data"}' in caplog.records[0].message
    assert caplog.records[0].exc_info[1] is test_exception
