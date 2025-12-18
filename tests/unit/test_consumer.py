import json
import logging
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Generator
from unittest.mock import patch, MagicMock

import pytest
from confluent_kafka import Message, KafkaException, TopicPartition, KafkaError

from retriable_kafka_client import BaseConsumer, ConsumerConfig
from retriable_kafka_client.config import ConsumeTopicConfig
from retriable_kafka_client.offset_cache import _PartitionInfo


@pytest.fixture
def sample_config() -> ConsumerConfig:
    return ConsumerConfig(
        topics=[
            ConsumeTopicConfig(base_topic="foo", retry_topic=None),
            ConsumeTopicConfig(base_topic="bar", retry_topic=None),
        ],
        target=lambda _: None,
        kafka_hosts=["example.com"],
        group_id="baz",
        username="user",
        password="pass",
        additional_settings={},
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


@pytest.mark.parametrize(
    "schedule_message",
    [
        pytest.param(False, id="normal_message_processing"),
        pytest.param(True, id="scheduled_message_pauses"),
    ],
)
def test_consumer_run(
    base_consumer: BaseConsumer,
    schedule_message: bool,
) -> None:
    """Test run method with various poll behaviors."""
    mock_consumer = base_consumer._consumer
    mock_consumer.subscribe = MagicMock()
    mock_consumer.pause = MagicMock()
    mock_consumer.resume = MagicMock()

    mock_message = MagicMock(spec=Message)
    mock_message.topic.return_value = "test-topic"
    mock_message.partition.return_value = 0
    mock_message.offset.return_value = 0
    mock_message.error.return_value = None

    def poll_side_effect(*_, **__):
        base_consumer._BaseConsumer__stop_flag = True
        return mock_message

    mock_consumer.poll.side_effect = poll_side_effect

    # Mock schedule_cache for message behavior
    mock_schedule_cache = MagicMock()
    mock_schedule_cache.add_if_applicable.return_value = schedule_message
    mock_schedule_cache.pop_reprocessable.return_value = []
    base_consumer._BaseConsumer__schedule_cache = mock_schedule_cache

    with patch.object(base_consumer, "_process_message") as mock_process:
        base_consumer.run()
        if schedule_message:
            # Message was scheduled, should pause and NOT process
            mock_consumer.pause.assert_called_once()
            mock_process.assert_not_called()
        else:
            mock_process.assert_called_once_with(mock_message)

    # subscribe is now called with on_revoke callback
    assert mock_consumer.subscribe.call_count == 1
    call_args = mock_consumer.subscribe.call_args
    # Topics are extracted as base_topic strings from RetryConfig objects
    expected_topics = [topic.base_topic for topic in base_consumer._config.topics]
    assert call_args[0][0] == expected_topics
    assert "on_revoke" in call_args[1]

    mock_consumer.poll.assert_called()


@pytest.mark.parametrize(
    "message",
    [
        pytest.param(None, id="None"),
        pytest.param(
            MagicMock(
                spec=Message,
                error=lambda: KafkaError(-1, "I stubbed my toe when fetching messages"),
            ),
            id="Error in message",
        ),
    ],
)
def test_consumer_run_no_message(
    message: MagicMock | None,
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")
    mock_consumer = base_consumer._consumer
    mock_consumer.subscribe = MagicMock()
    mock_consumer.pause = MagicMock()
    mock_consumer.resume = MagicMock()

    def poll_side_effect(*_, **__):
        base_consumer._BaseConsumer__stop_flag = True
        return message

    mock_consumer.poll.side_effect = poll_side_effect

    base_consumer.run()
    if message is None:
        assert "No message received currently." in caplog.messages
    else:
        assert (
            "Consumer error: I stubbed my toe when fetching messages" in caplog.messages
        )


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

    # Mock the retry manager
    mock_retry_manager = MagicMock()
    base_consumer._BaseConsumer__retry_manager = mock_retry_manager

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

    # Verify retry manager was called to resend the message
    mock_retry_manager.resend_message.assert_called_once_with(mock_message)

    # Verify error was logged
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "ERROR"
    assert "Message could not be processed!" in caplog.records[0].message
    assert '{"test": "data"}' in caplog.records[0].message
    assert caplog.records[0].exc_info[1] is test_exception


def test_consumer__process_retried_messages_from_schedule(
    base_consumer: BaseConsumer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test that scheduled messages are reprocessed when their time comes."""
    caplog.set_level(logging.DEBUG)

    mock_consumer = base_consumer._consumer
    mock_consumer.resume = MagicMock()

    # Create mock messages that are ready to be reprocessed
    mock_message_1 = MagicMock(
        spec=Message,
        value=lambda: b'{"id": 1}',
        error=lambda: None,
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 10,
    )
    mock_message_2 = MagicMock(
        spec=Message,
        value=lambda: b'{"id": 2}',
        error=lambda: None,
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 11,
    )

    # Mock the retry cache to return reprocessable messages
    mock_retry_cache = MagicMock()
    mock_retry_cache.pop_reprocessable.return_value = [mock_message_1, mock_message_2]
    # After messages are popped, add_if_applicable should return False (no longer scheduled)
    mock_retry_cache.add_if_applicable.return_value = False
    base_consumer._BaseConsumer__schedule_cache = mock_retry_cache

    # Mock process_message to track calls
    with patch.object(base_consumer, "_process_message") as mock_process:
        # Call the private method
        base_consumer._BaseConsumer__process_retried_messages_from_schedule()

        # Verify both messages were processed
        assert mock_process.call_count == 2
        mock_process.assert_any_call(mock_message_1)
        mock_process.assert_any_call(mock_message_2)

    # Verify partitions were resumed
    mock_consumer.resume.assert_called_once()

    # Verify debug logs for retrying
    retry_logs = [
        msg for msg in caplog.messages if "Retrying processing message" in msg
    ]
    assert len(retry_logs) == 2


def _make_consumer_config(topics: list[ConsumeTopicConfig]) -> ConsumerConfig:
    """Helper to create ConsumerConfig with given topics."""
    return ConsumerConfig(
        topics=topics,
        target=lambda _: None,
        kafka_hosts=["example.com"],
        group_id="test-group",
        username="user",
        password="pass",
        additional_settings={},
    )


@pytest.mark.parametrize(
    "topics",
    [
        pytest.param(
            [
                ConsumeTopicConfig(base_topic="topic-a", retry_topic=None),
                ConsumeTopicConfig(base_topic="topic-b", retry_topic=None),
            ],
            id="unique_base_topics",
        ),
        pytest.param(
            [
                ConsumeTopicConfig(base_topic="topic-a", retry_topic="retry-a"),
                ConsumeTopicConfig(base_topic="topic-b", retry_topic="retry-b"),
            ],
            id="unique_base_and_retry_topics",
        ),
    ],
)
def test_consumer_validation_valid_configs(
    executor: Executor, topics: list[ConsumeTopicConfig]
) -> None:
    """Test that consumer accepts valid configurations with unique base topics."""
    config = _make_consumer_config(topics)
    with patch("retriable_kafka_client.consumer.Consumer"):
        consumer = BaseConsumer(config=config, executor=executor, max_concurrency=2)
        assert consumer is not None


@pytest.mark.parametrize(
    "topics",
    [
        pytest.param(
            [
                ConsumeTopicConfig(base_topic="same-topic", retry_topic=None),
                ConsumeTopicConfig(base_topic="same-topic", retry_topic=None),
            ],
            id="duplicate_base_topics",
        ),
    ],
)
def test_consumer_validation_invalid_configs(
    executor: Executor, topics: list[ConsumeTopicConfig]
) -> None:
    """Test that consumer rejects configurations with duplicate base topics."""
    config = _make_consumer_config(topics)
    with pytest.raises(
        AssertionError, match="Cannot consume twice from the same topic"
    ):
        BaseConsumer(config=config, executor=executor, max_concurrency=2)
