"""Unit tests for retry_utils module"""

from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException, KafkaError

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
