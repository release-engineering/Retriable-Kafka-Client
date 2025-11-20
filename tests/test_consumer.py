import json
import logging
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from typing import Generator
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import Message, KafkaException

from retriable_kafka_client import BaseConsumer, TopicConfig


@pytest.fixture
def sample_config() -> TopicConfig:
    return TopicConfig(
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
    sample_config: TopicConfig, concurrency: int, executor: Executor
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


@pytest.mark.parametrize(
    "future_exception,expect_error_log",
    [
        (None, False),  # Success case
        (ValueError("Processing failed"), True),  # Exception case
        (RuntimeError("Another error"), True),  # Different exception type
    ],
)
def test_consumer__ack_message(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
    future_exception: Exception | None,
    expect_error_log: bool,
) -> None:
    """Test that __ack_message always releases semaphore and commits message."""
    if expect_error_log:
        caplog.set_level(logging.ERROR)
    
    mock_message = MagicMock(spec=Message, value=lambda: b"test message")
    mock_future = MagicMock(spec=Future)
    mock_future.exception.return_value = future_exception
    mock_consumer = base_consumer._consumer
    mock_consumer.commit = MagicMock()

    # Acquire semaphore first to test release
    initial_count = base_consumer._BaseConsumer__semaphore.get_value()
    base_consumer._BaseConsumer__semaphore.acquire()

    base_consumer._BaseConsumer__ack_message(mock_message, mock_future)

    # Semaphore should always be released
    assert base_consumer._BaseConsumer__semaphore.get_value() == initial_count
    
    # Message should always be committed
    mock_consumer.commit.assert_called_once_with(mock_message)
    
    # Error should be logged only if there was an exception
    if expect_error_log:
        assert any("Message could not be processed!" in msg for msg in caplog.messages)
    else:
        assert not any("Message could not be processed!" in msg for msg in caplog.messages)


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


def test_consumer__consumer_property_lazy_initialization(
    sample_config: TopicConfig,
    executor: Executor,
) -> None:
    """Test that consumer is only created when accessed."""
    with patch("retriable_kafka_client.consumer.Consumer") as mock_consumer_class:
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        consumer = BaseConsumer(
            config=sample_config, executor=executor, max_concurrency=2
        )
        assert consumer._BaseConsumer__consumer_object is None

        # Access the property
        result = consumer._consumer

        assert consumer._BaseConsumer__consumer_object is not None
        assert result is mock_consumer_instance
        mock_consumer_class.assert_called_once()
        call_args = mock_consumer_class.call_args[0][0]
        assert call_args["bootstrap.servers"] == "example.com"
        assert call_args["group.id"] == "baz"
        assert call_args["sasl.username"] == "user"
        assert call_args["sasl.password"] == "pass"


def test_consumer__consumer_property_reuses_instance(
    sample_config: TopicConfig,
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


def test_consumer_stop(
    base_consumer: BaseConsumer,
) -> None:
    """Test that stop method sets flag and closes consumer."""
    mock_consumer = base_consumer._consumer
    mock_consumer.unsubscribe = MagicMock()
    mock_consumer.close = MagicMock()
    assert base_consumer._BaseConsumer__stop_flag is False

    base_consumer.stop()

    assert base_consumer._BaseConsumer__stop_flag is True
    mock_consumer.unsubscribe.assert_called_once()
    mock_consumer.close.assert_called_once()


@pytest.mark.parametrize(
    "poll_behavior,stop_after_polls",
    [
        ("stop_after_first", 1),
        ("continue_none", 2),
        ("normal_message", 1),
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
    
    mock_consumer.subscribe.assert_called_once_with(base_consumer._config.topics)
    mock_consumer.poll.assert_called()
    if poll_behavior == "continue_none":
        assert call_count >= stop_after_polls


def test_consumer_run_handles_broken_process_pool(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that BrokenProcessPool exception is handled and consumer stops."""
    caplog.set_level(logging.ERROR)
    from concurrent.futures.process import BrokenProcessPool

    mock_consumer = base_consumer._consumer
    mock_consumer.poll.side_effect = BrokenProcessPool("Pool broken")
    mock_consumer.subscribe = MagicMock()

    with patch("sys.exit") as mock_exit:
        base_consumer.run()
        mock_exit.assert_called_once_with(1)
        assert base_consumer._BaseConsumer__stop_flag is True
        assert any("Process pool got broken" in msg for msg in caplog.messages)


def test_consumer_semaphore_limits_concurrency(
    base_consumer: BaseConsumer,
) -> None:
    """Test that semaphore limits concurrent message processing."""
    message_data = {"key": "value"}
    message_json = json.dumps(message_data).encode()
    mock_message = MagicMock(
        spec=Message, value=lambda: message_json, error=lambda: None
    )

    with patch.object(base_consumer._executor, "submit") as mock_submit:
        mock_future = MagicMock(spec=Future)
        mock_submit.return_value = mock_future

        # Process messages up to concurrency limit
        futures = []
        for _ in range(2):  # concurrency is 2
            future = base_consumer._process_message(mock_message)
            futures.append(future)

        # Verify semaphore state - should be at 0 (all permits acquired)
        assert base_consumer._BaseConsumer__semaphore.get_value() == 0
        assert len(futures) == 2
        assert mock_submit.call_count == 2
