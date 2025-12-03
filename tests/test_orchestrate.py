from concurrent.futures import ProcessPoolExecutor
from unittest.mock import patch, MagicMock

import pytest

from retriable_kafka_client import TopicConfig, consume_topics, BaseConsumer
from retriable_kafka_client.orchestrate import ConsumerThread


@pytest.fixture
def sample_config() -> TopicConfig:
    return TopicConfig(
        topics=["test_topic"],
        target=lambda _: None,
        kafka_hosts=["example.com"],
        group_id="test_group",
        user_name="user",
        password="pass",
    )


@pytest.fixture
def multiple_configs() -> list[TopicConfig]:
    return [
        TopicConfig(
            topics=[f"topic_{i}"],
            target=lambda _: None,
            kafka_hosts=["example.com"],
            group_id=f"group_{i}",
            user_name="user",
            password="pass",
        )
        for i in range(2)
    ]


def test_thread_init(sample_config: TopicConfig):
    consumer = BaseConsumer(
        config=sample_config, executor=MagicMock(), max_concurrency=1
    )
    thread = ConsumerThread(consumer)
    assert thread.consumer is consumer


def test_consume_topics_creates_executor_and_threads(
    multiple_configs: list[TopicConfig],
) -> None:
    """Test that consume_topics creates ProcessPoolExecutor and threads correctly."""
    with (
        patch("retriable_kafka_client.consumer.Consumer"),
        patch("retriable_kafka_client.orchestrate.ConsumerThread") as mock_thread_class,
    ):
        mock_consumers = [MagicMock() for _ in multiple_configs]
        mock_threads = [MagicMock(spec=ConsumerThread) for _ in multiple_configs]
        mock_thread_class.side_effect = mock_threads

        # Mock BaseConsumer to return our mock consumers and track executor usage
        created_executor = None
        call_index = 0
        with patch(
            "retriable_kafka_client.orchestrate.BaseConsumer"
        ) as mock_consumer_base:

            def consumer_side_effect(*_, **kwargs):
                nonlocal created_executor, call_index
                created_executor = kwargs.get("executor")
                result = mock_consumers[call_index]
                call_index += 1
                return result

            mock_consumer_base.side_effect = consumer_side_effect

            # Call consume_topics with small number of workers (2)
            # ProcessPoolExecutor will be created, but threads are mocked so it won't actually run
            consume_topics(multiple_configs, max_workers=2)

        # Verify ProcessPoolExecutor was actually created (not None)
        assert created_executor is not None
        assert isinstance(created_executor, ProcessPoolExecutor)

        # Verify BaseConsumer was created for each config
        assert mock_consumer_base.call_count == len(multiple_configs)
        for i, config in enumerate(multiple_configs):
            call_kwargs = mock_consumer_base.call_args_list[i][1]
            assert call_kwargs["config"] == config
            assert call_kwargs["max_concurrency"] == 2
            assert call_kwargs["executor"] is created_executor

        # Verify ConsumerThread was created for each consumer
        assert mock_thread_class.call_count == len(multiple_configs)
        for i, mock_consumer in enumerate(mock_consumers):
            assert mock_thread_class.call_args_list[i][0][0] == mock_consumer

        # Verify all threads were started
        for mock_thread in mock_threads:
            mock_thread.start.assert_called_once()

        # Verify all threads were joined
        for mock_thread in mock_threads:
            mock_thread.join.assert_called_once()

        # Cleanup: shutdown the executor
        created_executor.shutdown(wait=False)
