"""Unit tests for retry_utils module"""

import logging
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException, KafkaError, TopicPartition

from retriable_kafka_client.config import ConsumerConfig, ConsumeTopicConfig
from retriable_kafka_client.retry_utils import (
    ATTEMPT_HEADER,
    TIMESTAMP_HEADER,
    RetryManager,
    RetryScheduleCache,
    _get_retry_attempt,
    _get_retry_timestamp,
)


def _make_config(topics: list[ConsumeTopicConfig]) -> ConsumerConfig:
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


def _make_message(
    topic: str | None = "test-topic",
    partition: int | None = 0,
    offset: int | None = 0,
    value: bytes | None = b"test",
    headers: list[tuple[str, bytes]] | None = None,
    error: KafkaError | None = None,
) -> MagicMock:
    """Helper to create a mock Kafka message."""
    message = MagicMock()
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    message.value.return_value = value
    message.headers.return_value = headers
    message.error.return_value = error
    return message


@pytest.mark.parametrize(
    "headers,expected",
    [
        pytest.param(None, None, id="no_headers"),
        pytest.param([], None, id="empty_headers"),
        pytest.param([("other_header", b"value")], None, id="no_timestamp_header"),
        pytest.param(
            [(TIMESTAMP_HEADER, (12345).to_bytes(8, "big"))],
            12345,
            id="has_timestamp_header",
        ),
        pytest.param(
            [
                ("other", b"value"),
                (TIMESTAMP_HEADER, (99999).to_bytes(8, "big")),
            ],
            99999,
            id="timestamp_with_other_headers",
        ),
    ],
)
def test_get_retry_timestamp(
    headers: list[tuple[str, bytes]] | None, expected: float | None
) -> None:
    """Test _get_retry_timestamp extracts timestamp from headers correctly."""
    message = _make_message(headers=headers)
    assert _get_retry_timestamp(message) == expected


@pytest.mark.parametrize(
    "headers,expected",
    [
        pytest.param(None, 0, id="no_headers"),
        pytest.param([], 0, id="empty_headers"),
        pytest.param([("other_header", b"value")], 0, id="no_attempt_header"),
        pytest.param(
            [(ATTEMPT_HEADER, (3).to_bytes(8, "big"))], 3, id="has_attempt_header"
        ),
        pytest.param(
            [
                ("other", b"value"),
                (ATTEMPT_HEADER, (7).to_bytes(8, "big")),
            ],
            7,
            id="attempt_with_other_headers",
        ),
    ],
)
def test_get_retry_attempt(
    headers: list[tuple[str, bytes]] | None, expected: int
) -> None:
    """Test _get_retry_attempt extracts attempt number from headers correctly."""
    message = _make_message(headers=headers)
    assert _get_retry_attempt(message) == expected


class TestRetryScheduleCache:
    """Tests for RetryScheduleCache class."""

    def test_pop_reprocessable_returns_due_messages(self) -> None:
        """Test pop_reprocessable returns messages whose timestamp has passed."""
        cache = RetryScheduleCache()
        past_timestamp = 1000.0
        message = _make_message(
            headers=[(TIMESTAMP_HEADER, int(past_timestamp).to_bytes(8, "big"))]
        )

        with patch(
            "retriable_kafka_client.retry_utils._get_current_timestamp",
            return_value=past_timestamp - 10,
        ):
            added = cache.add_if_applicable(message)
            assert added is True

        with patch(
            "retriable_kafka_client.retry_utils._get_current_timestamp",
            return_value=past_timestamp + 10,
        ):
            result = cache.pop_reprocessable()
            assert len(result) == 1
            assert result[0] == message

            # After pop, schedule should be empty
            assert cache.pop_reprocessable() == []

    def test_pop_reprocessable_keeps_future_messages(self) -> None:
        """Test pop_reprocessable does not return messages scheduled for the future."""
        cache = RetryScheduleCache()
        future_timestamp = 2000.0
        message = _make_message(
            headers=[(TIMESTAMP_HEADER, int(future_timestamp).to_bytes(8, "big"))]
        )

        with patch(
            "retriable_kafka_client.retry_utils._get_current_timestamp",
            return_value=future_timestamp - 100,
        ):
            cache.add_if_applicable(message)
            result = cache.pop_reprocessable()
            assert result == []

    def test_add_if_applicable_no_timestamp_header(self) -> None:
        """Test add_if_applicable returns False for messages without timestamp."""
        cache = RetryScheduleCache()
        message = _make_message(headers=None)
        assert cache.add_if_applicable(message) is False

    def test_add_if_applicable_timestamp_already_passed(self) -> None:
        """Test add_if_applicable returns False if timestamp already passed."""
        cache = RetryScheduleCache()
        past_timestamp = 1000.0
        message = _make_message(
            headers=[(TIMESTAMP_HEADER, int(past_timestamp).to_bytes(8, "big"))]
        )

        with patch(
            "retriable_kafka_client.retry_utils._get_current_timestamp",
            return_value=past_timestamp + 100,
        ):
            assert cache.add_if_applicable(message) is False

    def test_add_if_applicable_schedules_future_message(self) -> None:
        """Test add_if_applicable returns True and schedules future messages."""
        cache = RetryScheduleCache()
        future_timestamp = 2000.0
        message = _make_message(
            headers=[(TIMESTAMP_HEADER, int(future_timestamp).to_bytes(8, "big"))]
        )

        with patch(
            "retriable_kafka_client.retry_utils._get_current_timestamp",
            return_value=future_timestamp - 100,
        ):
            assert cache.add_if_applicable(message) is True

    def test_clean_empty_removes_empty_timestamp_keys(self) -> None:
        """Test _clean_empty removes timestamps with empty message lists."""
        cache = RetryScheduleCache()
        cache._RetryScheduleCache__schedule = {
            100: [],
            200: _make_message(
                topic="topic-a",
                partition=0,
                headers=[(TIMESTAMP_HEADER, int(200).to_bytes(8, "big"))],
            ),
        }

        cache._cleanup()

        # Verify empty keys are removed, non-empty remain
        assert len(cache._RetryScheduleCache__schedule) == 1
        assert 100 not in cache._RetryScheduleCache__schedule
        assert 200 in cache._RetryScheduleCache__schedule

    @pytest.mark.parametrize(
        "scheduled_messages,partitions_to_revoke,expected_log_count,expected_remaining_count",
        [
            pytest.param(
                [],
                [],
                0,
                0,
                id="empty_schedule_no_revoke",
            ),
            pytest.param(
                [
                    ("topic-a", 0, 1000.0),
                    ("topic-a", 1, 1000.0),
                ],
                [("topic-a", 0), ("topic-a", 1)],
                2,
                0,
                id="revoke_all_scheduled_messages",
            ),
            pytest.param(
                [
                    ("topic-a", 0, 1000.0),
                    ("topic-a", 1, 1500.0),
                    ("topic-b", 0, 2000.0),
                ],
                [("topic-a", 0)],
                1,
                2,
                id="partial_revoke_one_partition",
            ),
            pytest.param(
                [
                    ("topic-a", 0, 1000.0),
                    ("topic-a", 1, 1000.0),
                    ("topic-b", 0, 2000.0),
                    ("topic-c", 0, 3000.0),
                ],
                [("topic-a", 0), ("topic-a", 1)],
                2,
                2,
                id="partial_revoke_multiple_partitions_same_topic",
            ),
            pytest.param(
                [
                    ("topic-a", 0, 1000.0),
                    ("topic-b", 0, 1000.0),
                    ("topic-c", 0, 1000.0),
                ],
                [("topic-a", 0), ("topic-c", 0)],
                2,
                1,
                id="partial_revoke_multiple_topics",
            ),
            pytest.param(
                [
                    ("topic-a", 0, 1000.0),
                    ("topic-a", 1, 1000.0),
                ],
                [("topic-b", 0)],
                0,
                2,
                id="revoke_untracked_partition_no_effect",
            ),
            pytest.param(
                [
                    ("topic-a", 0, 1000.0),
                    ("topic-a", 0, 2000.0),
                    ("topic-a", 1, 3000.0),
                ],
                [("topic-a", 0)],
                2,
                1,
                id="revoke_partition_with_multiple_timestamps",
            ),
        ],
    )
    def test_register_revoke(
        self,
        caplog: pytest.LogCaptureFixture,
        scheduled_messages: list[tuple[str, int, float]],
        partitions_to_revoke: list[tuple[str, int]],
        expected_log_count: int,
        expected_remaining_count: int,
    ) -> None:
        """Test register_revoke handles partition revocation correctly."""
        caplog.set_level(logging.INFO)
        cache = RetryScheduleCache()

        # Create messages and add them to schedule
        created_messages = []
        for topic, partition, timestamp in scheduled_messages:
            message = _make_message(
                topic=topic,
                partition=partition,
                headers=[(TIMESTAMP_HEADER, int(timestamp).to_bytes(8, "big"))],
            )
            created_messages.append((message, timestamp))
            # Manually add to schedule to bypass time checks
            cache._RetryScheduleCache__schedule.setdefault(timestamp, []).append(
                message
            )

        # Create TopicPartition objects for revocation
        revoke_partitions = [
            TopicPartition(topic, partition)
            for topic, partition in partitions_to_revoke
        ]

        # Call register_revoke
        cache.register_revoke(revoke_partitions)

        # Verify log count
        assert len(caplog.records) == expected_log_count

        # Verify log content
        if expected_log_count > 0:
            for record in caplog.records:
                assert "rebalancing" in record.message.lower()
                assert "discarded" in record.message.lower()

        # Count remaining messages in schedule
        remaining_count = sum(
            len(messages) for messages in cache._RetryScheduleCache__schedule.values()
        )
        assert remaining_count == expected_remaining_count

        # Verify revoked messages are removed
        revoked_set = set(partitions_to_revoke)
        for message, timestamp in created_messages:
            message_partition = (message.topic(), message.partition())
            if message_partition in revoked_set:
                # This message should be removed
                if timestamp in cache._RetryScheduleCache__schedule:
                    assert message not in cache._RetryScheduleCache__schedule[timestamp]
            else:
                # This message should remain
                assert message in cache._RetryScheduleCache__schedule[timestamp]


@pytest.mark.parametrize(
    "topics",
    [
        pytest.param(
            [
                ConsumeTopicConfig(base_topic="topic-a", retry_topic="topic-a-retry"),
                ConsumeTopicConfig(base_topic="topic-b", retry_topic="topic-b-retry"),
            ],
            id="unique_retry_topics",
        ),
        pytest.param(
            [
                ConsumeTopicConfig(base_topic="topic-a", retry_topic=None),
                ConsumeTopicConfig(base_topic="topic-b", retry_topic=None),
            ],
            id="none_retry_topics",
        ),
        pytest.param(
            [
                ConsumeTopicConfig(base_topic="topic-a", retry_topic="topic-a-retry"),
                ConsumeTopicConfig(base_topic="topic-b", retry_topic=None),
                ConsumeTopicConfig(base_topic="topic-c", retry_topic="topic-c-retry"),
            ],
            id="mixed_retry_config",
        ),
    ],
)
def test_retry_manager_validation_valid_configs(
    topics: list[ConsumeTopicConfig],
) -> None:
    """Test that RetryManager accepts valid configurations."""
    config = _make_config(topics)
    manager = RetryManager(config)
    assert manager is not None


@pytest.mark.parametrize(
    "topics",
    [
        pytest.param(
            [
                ConsumeTopicConfig(base_topic="topic-a", retry_topic="shared-retry"),
                ConsumeTopicConfig(base_topic="topic-b", retry_topic="shared-retry"),
            ],
            id="duplicate_retry_topics",
        ),
    ],
)
def test_retry_manager_validation_invalid_configs(
    topics: list[ConsumeTopicConfig],
) -> None:
    """Test that RetryManager rejects configurations with duplicate retry topics."""
    config = _make_config(topics)
    with pytest.raises(
        AssertionError, match="Two consumed topics cannot share the same retry topic"
    ):
        RetryManager(config)


@pytest.mark.parametrize(
    "prev_attempt,delay,timestamp,expected_attempt,expected_ts",
    [
        pytest.param(None, 60, 1000.0, 1, 1060, id="first_attempt"),
        pytest.param(2, 30, 2000.0, 3, 2030, id="increments_attempt"),
    ],
)
def test__get_retry_headers(
    prev_attempt, delay, timestamp, expected_attempt, expected_ts
) -> None:
    """Test _get_retry_headers computes correct attempt and timestamp."""
    config = _make_config(
        [ConsumeTopicConfig(base_topic="t", retry_topic="t-r", fallback_delay=delay)]
    )
    headers = (
        [(ATTEMPT_HEADER, prev_attempt.to_bytes(8, "big"))] if prev_attempt else None
    )
    with patch(
        "retriable_kafka_client.retry_utils._get_current_timestamp",
        return_value=timestamp,
    ):
        manager = RetryManager(config)
        result = manager._get_retry_headers(_make_message(topic="t", headers=headers))
    assert int.from_bytes(result[ATTEMPT_HEADER], "big") == expected_attempt
    assert int.from_bytes(result[TIMESTAMP_HEADER], "big") == expected_ts


def test__get_retry_headers_no_config() -> None:
    config = _make_config(
        [
            ConsumeTopicConfig(base_topic="t", retry_topic=None),
            ConsumeTopicConfig(base_topic="o", retry_topic="o-r"),
        ]
    )
    manager = RetryManager(config)
    result = manager._get_retry_headers(_make_message(topic="t", headers=[]))
    assert result is None


@pytest.mark.parametrize(
    "topics,message_topic,send_error,expect_send",
    [
        pytest.param(
            [ConsumeTopicConfig(base_topic="t", retry_topic="t-retry")],
            "t",
            None,
            True,
            id="success",
        ),
        pytest.param(
            [ConsumeTopicConfig(base_topic="t", retry_topic=None)],
            "t",
            None,
            False,
            id="no_retry_configured",
        ),
        pytest.param(
            [ConsumeTopicConfig(base_topic="t", retry_topic="t-retry")],
            "unknown",
            None,
            False,
            id="unknown_topic",
        ),
        pytest.param(
            [ConsumeTopicConfig(base_topic="t", retry_topic="t-retry")],
            "t",
            KafkaException(KafkaError(1, "I don't feel like working today")),
            True,
            id="exception_logged",
        ),
    ],
)
def test_resend_message(topics, message_topic, send_error, expect_send) -> None:
    """Test resend_message handles various scenarios."""
    config = _make_config(topics)
    with patch("retriable_kafka_client.retry_utils.BaseProducer") as mock_cls:
        mock_cls.return_value.send_sync.side_effect = send_error
        manager = RetryManager(config)
        manager.resend_message(_make_message(topic=message_topic))
        assert mock_cls.return_value.send_sync.called == expect_send


def test_resend_message_no_value() -> None:
    message = _make_message(value=None)
    config = _make_config([])
    with patch("retriable_kafka_client.retry_utils.BaseProducer") as mock_cls:
        manager = RetryManager(config)
        manager.resend_message(message)
        mock_cls.return_value.send_sync.assert_not_called()


def test_resend_message_exhausted_attempts(caplog: pytest.LogCaptureFixture) -> None:
    message = _make_message(
        topic="t-retry",
        value=b'{"foo": "bar"}',
        headers=[
            (ATTEMPT_HEADER, (1).to_bytes(8, "big")),
            (TIMESTAMP_HEADER, (0).to_bytes(8, "big")),
        ],
    )
    caplog.set_level("DEBUG")
    config = _make_config(
        [ConsumeTopicConfig(base_topic="t", retry_topic="t-retry", retries=1)]
    )
    with patch("retriable_kafka_client.retry_utils.BaseProducer") as mock_cls:
        manager = RetryManager(config)
        manager.resend_message(message)
        mock_cls.return_value.send_sync.assert_not_called()
        print(caplog.messages)
        assert "Message will not be retried." in caplog.messages[-1]
