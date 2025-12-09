"""Tests for OffsetCache module"""

import logging
import pytest
from unittest.mock import MagicMock
from confluent_kafka import Message, TopicPartition

from retriable_kafka_client.offset_cache import OffsetCache, _PartitionInfo


def test_partition_info_round_trip() -> None:
    """Test that _PartitionInfo can be converted to TopicPartition and maintains data integrity."""
    # Create a mock Kafka message
    mock_message = MagicMock(
        spec=Message,
        topic=lambda: "test-topic",
        partition=lambda: 3,
        offset=lambda: 150,
    )

    # Convert Message to _PartitionInfo
    partition_info = _PartitionInfo.from_message(mock_message)

    # Verify extracted data
    assert partition_info.topic == "test-topic"
    assert partition_info.partition == 3

    # Convert back to TopicPartition with the same offset
    topic_partition = partition_info.to_offset_info(150)

    # Verify data integrity after round-trip
    assert topic_partition.topic == "test-topic"
    assert topic_partition.partition == 3
    assert topic_partition.offset == 150
    assert isinstance(topic_partition, TopicPartition)


@pytest.mark.parametrize(
    "partition_states,expected_commits,expected_remaining_to_commit",
    [
        pytest.param(
            [{"partition": 0, "to_commit": set(), "to_process": set()}],
            [],
            {0: set()},
            id="empty_to_commit_nothing_to_commit",
        ),
        pytest.param(
            [{"partition": 0, "to_commit": {100, 101, 102}, "to_process": set()}],
            [(0, 103)],
            {0: set()},
            id="no_pending_to_process_commit_all_and_cleanup",
        ),
        pytest.param(
            [{"partition": 0, "to_commit": {100, 101}, "to_process": {50}}],
            [],
            {0: {100, 101}},
            id="all_offsets_gte_min_pending_nothing_committable",
        ),
        pytest.param(
            [{"partition": 0, "to_commit": {50, 60, 100, 101}, "to_process": {70}}],
            [(0, 61)],
            {0: {100, 101}},
            id="some_offsets_lt_min_pending_partial_commit",
        ),
        pytest.param(
            [
                {"partition": 0, "to_commit": {10, 11, 12}, "to_process": set()},
                {"partition": 1, "to_commit": set(), "to_process": {20}},
                {"partition": 2, "to_commit": {30, 31, 40, 41}, "to_process": {35}},
                {"partition": 3, "to_commit": {50, 51}, "to_process": {45}},
            ],
            [(0, 13), (2, 32)],
            {0: set(), 1: set(), 2: {40, 41}, 3: {50, 51}},
            id="multiple_partitions_different_states",
        ),
        pytest.param(
            [
                {
                    "partition": 0,
                    "to_commit": {10, 20, 30, 40},
                    "to_process": {25, 35, 45},
                }
            ],
            [(0, 21)],
            {0: {30, 40}},
            id="multiple_pending_to_process_use_minimum",
        ),
    ],
)
def test_offset_cache_pop_committable(
    partition_states: list[dict],
    expected_commits: list[tuple[int, int]],
    expected_remaining_to_commit: dict[int, set[int]],
) -> None:
    """Test pop_committable covers all branches with various partition states."""
    cache = OffsetCache()

    # Setup the cache state
    for state in partition_states:
        partition_info = _PartitionInfo("test-topic", state["partition"])
        # Handle special case for explicitly creating empty set in to_commit
        cache._OffsetCache__to_commit[partition_info] = state["to_commit"]
        cache._OffsetCache__to_process[partition_info] = state["to_process"]

    # Call pop_committable
    result = cache.pop_committable()

    # Verify the returned commits
    assert len(result) == len(expected_commits)
    for tp in result:
        assert isinstance(tp, TopicPartition)
        assert tp.topic == "test-topic"
        # Find matching expected commit
        matching = [
            (partition, offset)
            for partition, offset in expected_commits
            if partition == tp.partition
        ]
        assert len(matching) == 1, f"Unexpected partition {tp.partition}"
        assert tp.offset == matching[0][1]

    # Verify remaining state in to_commit
    for partition, expected_offsets in expected_remaining_to_commit.items():
        partition_info = _PartitionInfo("test-topic", partition)
        actual_offsets = cache._OffsetCache__to_commit.get(partition_info, set())
        assert actual_offsets == expected_offsets, (
            f"Partition {partition}: expected {expected_offsets}, got {actual_offsets}"
        )


def test_offset_cache_schedule_commit_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test schedule_commit successfully moves offset from to_process to to_commit."""
    cache = OffsetCache()

    # Create a mock message
    mock_message = MagicMock(
        spec=Message,
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 42,
        value=lambda: b'{"test": "data"}',
    )

    # First, mark the message as being processed
    cache.process_message(mock_message)

    # Verify it's in to_process
    partition_info = _PartitionInfo("test-topic", 0)
    assert 42 in cache._OffsetCache__to_process[partition_info]
    assert 42 not in cache._OffsetCache__to_commit.get(partition_info, set())

    # Now schedule it for commit
    result = cache.schedule_commit(mock_message)

    # Verify success
    assert result is True

    # Verify it moved from to_process to to_commit
    assert 42 not in cache._OffsetCache__to_process[partition_info]
    assert 42 in cache._OffsetCache__to_commit[partition_info]

    # No warning should be logged
    assert len(caplog.records) == 0


def test_offset_cache_schedule_commit_failure_rebalancing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test schedule_commit fails when partition not in to_process (e.g., after rebalancing)."""
    caplog.set_level(logging.WARNING)
    cache = OffsetCache()

    # Create a mock message
    mock_message = MagicMock(
        spec=Message,
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 42,
        value=lambda: b'{"test": "data"}',
    )

    # Don't add it to to_process (simulating rebalancing cleared it)
    # Try to schedule commit
    result = cache.schedule_commit(mock_message)

    # Verify failure
    assert result is False

    # Verify offset was not added to to_commit
    partition_info = _PartitionInfo("test-topic", 0)
    assert 42 not in cache._OffsetCache__to_commit.get(partition_info, set())

    # Verify warning was logged
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == "WARNING"
    assert "will not be committed" in caplog.records[0].message
    assert "Rebalancing happened" in caplog.records[0].message
    assert "test-topic" in caplog.records[0].message
    assert "42" in caplog.records[0].message


@pytest.mark.parametrize(
    "to_commit_data,to_process_data,expected_log_count,expected_log_content",
    [
        pytest.param(
            {},
            {},
            0,
            [],
            id="both_empty_return_early_no_logs",
        ),
        pytest.param(
            {_PartitionInfo("test-topic", 0): {100, 101, 102}},
            {},
            1,
            [
                "test-topic",
                "partition",
                "0",
                "offsets",
                "failed",
                "commited",
                "rebalancing",
            ],
            id="only_to_commit_has_data_log_uncommitted",
        ),
        pytest.param(
            {},
            {_PartitionInfo("test-topic", 1): {50, 51}},
            1,
            [
                "test-topic",
                "partition",
                "1",
                "offset",
                "failed",
                "processed",
                "rebalancing",
            ],
            id="only_to_process_has_data_log_unprocessed",
        ),
    ],
)
def test_offset_cache_register_revoke(
    caplog: pytest.LogCaptureFixture,
    to_commit_data: dict,
    to_process_data: dict,
    expected_log_count: int,
    expected_log_content: list[str],
) -> None:
    """Test register_revoke logs warnings for uncommitted/unprocessed messages."""
    caplog.set_level(logging.WARNING)
    cache = OffsetCache()

    # Setup cache state
    for partition_info, offsets in to_commit_data.items():
        cache._OffsetCache__to_commit[partition_info].update(offsets)
    for partition_info, offsets in to_process_data.items():
        cache._OffsetCache__to_process[partition_info].update(offsets)

    # Call register_revoke
    cache.register_revoke()

    # Verify log count
    assert len(caplog.records) == expected_log_count

    # Verify log content
    if expected_log_count > 0:
        log_messages = " ".join([record.message for record in caplog.records])
        for expected_text in expected_log_content:
            assert expected_text.lower() in log_messages.lower(), (
                f"Expected '{expected_text}' in log messages: {log_messages}"
            )

    # Verify caches are cleared
    assert len(cache._OffsetCache__to_commit) == 0
    assert len(cache._OffsetCache__to_process) == 0
