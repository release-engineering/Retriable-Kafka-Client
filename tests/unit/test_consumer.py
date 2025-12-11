import json
import logging
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Generator
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import Message, KafkaException, TopicPartition

from retriable_kafka_client import BaseConsumer, ConsumerConfig
from retriable_kafka_client.offset_cache import _PartitionInfo


@pytest.fixture
def sample_config() -> ConsumerConfig:
    return ConsumerConfig(
        topics=["foo", "bar"],
        target=lambda _: None,
        kafka_hosts=["example.com"],
        group_id="baz",
        username="user",
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


def test_consumer_stop(
    base_consumer: BaseConsumer,
) -> None:
    """Test that stop method sets flag, drains cache, and attempts to close consumer."""
    mock_consumer = base_consumer._consumer
    mock_consumer.unsubscribe = MagicMock()
    mock_consumer.close = MagicMock()
    mock_consumer.commit = MagicMock()
    # Mock the executor.map to avoid actually calling it
    base_consumer._executor.map = MagicMock()

    # Pre-fill the offset cache
    partition_info = _PartitionInfo("test-topic", 0)
    offset_cache = base_consumer._BaseConsumer__offset_cache
    offset_cache._OffsetCache__to_commit[partition_info].update({100, 101, 102})

    assert base_consumer._BaseConsumer__stop_flag is False
    assert offset_cache.has_cache() is True

    base_consumer.stop()

    assert base_consumer._BaseConsumer__stop_flag is True
    # Verify cache was drained
    assert offset_cache.has_cache() is False
    # Verify commit was called to drain the cache
    mock_consumer.commit.assert_called()
    mock_consumer.unsubscribe.assert_called_once()
    # The close is now called via executor.map
    base_consumer._executor.map.assert_called_once()


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
    assert "on_revoke" in call_args[1]

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


@pytest.mark.parametrize(
    "to_process_offsets,to_commit_offsets,should_commit,expected_offset",
    [
        pytest.param(set(), {100, 101}, True, 102, id="no_processing_can_commit"),
        pytest.param(
            {50}, {100, 101}, False, None, id="older_processing_blocks_newer_commits"
        ),
        pytest.param(
            {100}, {100, 101}, False, None, id="processing_same_offset_blocks_commit"
        ),
        pytest.param({70}, {50, 60}, True, 61, id="partial_commit_below_processing"),
        pytest.param(
            {100}, {50, 60}, True, 61, id="commit_older_while_processing_newer"
        ),
        pytest.param(set(), set(), False, None, id="empty_queues_nothing_to_commit"),
    ],
)
def test_perform_commits_logic(
    base_consumer: BaseConsumer,
    to_process_offsets: set[int],
    to_commit_offsets: set[int],
    should_commit: bool,
    expected_offset: int | None,
) -> None:
    """Test various scenarios of commit logic with different offset combinations."""
    mock_consumer = base_consumer._consumer
    mock_consumer.commit = MagicMock()

    partition_info = _PartitionInfo("test-topic", 0)
    offset_cache = base_consumer._BaseConsumer__offset_cache
    if to_process_offsets:
        offset_cache._OffsetCache__to_process[partition_info].update(to_process_offsets)
    if to_commit_offsets:
        offset_cache._OffsetCache__to_commit[partition_info].update(to_commit_offsets)

    base_consumer._BaseConsumer__perform_commits()

    if should_commit:
        mock_consumer.commit.assert_called_once()
        call_args = mock_consumer.commit.call_args
        assert call_args[1]["offsets"][0].offset == expected_offset
    else:
        mock_consumer.commit.assert_not_called()


@pytest.mark.parametrize(
    "has_cache,to_commit_offsets,expected_offset",
    [
        pytest.param(True, {100, 101, 102}, 103, id="cache_filled_commits_offsets"),
        pytest.param(False, set(), None, id="cache_empty_no_commit"),
    ],
)
def test_on_revoke(
    base_consumer: BaseConsumer,
    has_cache: bool,
    to_commit_offsets: set[int],
    expected_offset: int | None,
) -> None:
    """Test __on_revoke callback behavior with and without cache."""
    mock_consumer = base_consumer._consumer
    mock_consumer.commit = MagicMock()

    # Pre-fill the cache if needed
    if has_cache:
        partition_info = _PartitionInfo("test-topic", 0)
        offset_cache = base_consumer._BaseConsumer__offset_cache
        offset_cache._OffsetCache__to_commit[partition_info].update(to_commit_offsets)

    # Create mock partitions list (required by on_revoke signature)
    mock_partitions = [TopicPartition("test-topic", 0)]

    # Call the private __on_revoke method
    base_consumer._BaseConsumer__on_revoke(mock_consumer, mock_partitions)

    if has_cache:
        # Should commit and register revoke
        mock_consumer.commit.assert_called_once()
        call_args = mock_consumer.commit.call_args
        assert call_args[1]["asynchronous"] is False
        assert call_args[1]["offsets"][0].offset == expected_offset
        # Cache should be cleared after register_revoke
        assert not base_consumer._BaseConsumer__offset_cache.has_cache()
    else:
        # Should not commit when cache is empty
        mock_consumer.commit.assert_not_called()


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

    # Verify offset cache schedule_commit was called
    mock_offset_cache.schedule_commit.assert_called_once_with(mock_message)

    # Verify error was logged
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "ERROR"
    assert "Message could not be processed!" in caplog.records[0].message
    assert '{"test": "data"}' in caplog.records[0].message
    assert caplog.records[0].exc_info[1] is test_exception
