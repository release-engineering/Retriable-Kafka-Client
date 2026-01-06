"""Tests for TrackingManager module"""

import logging
import pytest
from unittest.mock import MagicMock
from concurrent.futures import Future
from confluent_kafka import Message, TopicPartition

from retriable_kafka_client.consumer_tracking import (
    TrackingManager,
    _PartitionInfo as PartitionInfo,
)


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
    partition_info = PartitionInfo.from_message(mock_message)

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
    cache = TrackingManager(concurrency=16, cancel_wait_time=30.0)

    # Setup the cache state
    for state in partition_states:
        partition_info = PartitionInfo("test-topic", state["partition"])
        # Handle special case for explicitly creating empty set in to_commit
        cache._TrackingManager__to_commit[partition_info] = state["to_commit"]
        # to_process now stores dict[int, Future], convert sets to dicts with mock futures
        cache._TrackingManager__to_process[partition_info] = {
            offset: MagicMock(spec=Future) for offset in state["to_process"]
        }

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
        partition_info = PartitionInfo("test-topic", partition)
        actual_offsets = cache._TrackingManager__to_commit.get(partition_info, set())
        assert actual_offsets == expected_offsets, (
            f"Partition {partition}: expected {expected_offsets}, got {actual_offsets}"
        )


def test_offset_cache_schedule_commit_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test schedule_commit successfully moves offset from to_process to to_commit."""
    cache = TrackingManager(concurrency=16, cancel_wait_time=30.0)

    # Create a mock message
    mock_message = MagicMock(
        spec=Message,
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 42,
        value=lambda: b'{"test": "data"}',
    )

    # Create a mock future
    mock_future = MagicMock(spec=Future)

    # First, mark the message as being processed
    cache.process_message(mock_message, mock_future)

    # Verify it's in to_process
    partition_info = PartitionInfo("test-topic", 0)
    assert 42 in cache._TrackingManager__to_process[partition_info]
    assert 42 not in cache._TrackingManager__to_commit.get(partition_info, set())

    # Now schedule it for commit
    result = cache.schedule_commit(mock_message)

    # Verify success
    assert result is True

    # Verify it moved from to_process to to_commit
    assert 42 not in cache._TrackingManager__to_process[partition_info]
    assert 42 in cache._TrackingManager__to_commit[partition_info]

    # No warning should be logged
    assert len(caplog.records) == 0


def test_offset_cache_schedule_commit_without_prior_processing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test schedule_commit behavior when message wasn't marked for processing first."""
    caplog.set_level(logging.WARNING)
    cache = TrackingManager(concurrency=16, cancel_wait_time=30.0)

    # Create a mock message
    mock_message = MagicMock(
        spec=Message,
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 42,
        value=lambda: b'{"test": "data"}',
    )

    # Don't add it to to_process (simulating it was never marked for processing)
    # Try to schedule commit
    result = cache.schedule_commit(mock_message)

    # schedule_commit always returns True in the new implementation
    assert result is True

    # Verify offset was added to to_commit (new behavior)
    partition_info = PartitionInfo("test-topic", 0)
    assert 42 in cache._TrackingManager__to_commit.get(partition_info, set())

    # No warning is logged in the new implementation
    assert len(caplog.records) == 0


@pytest.mark.parametrize(
    "to_process_data,revoked_partitions,expected_cancelled,expected_not_cancelled",
    [
        pytest.param(
            {
                PartitionInfo("topic-a", 0): {
                    100: "future_1",
                    101: "future_2",
                    102: "future_3",
                },
            },
            {PartitionInfo("topic-a", 0)},
            ["future_1", "future_3"],
            ["future_2"],
            id="mixed_some_cancelled_some_not",
        ),
        pytest.param(
            {
                PartitionInfo("topic-a", 0): {100: "future_1", 101: "future_2"},
                PartitionInfo("topic-b", 0): {200: "future_3"},
            },
            {PartitionInfo("topic-a", 0)},
            ["future_1", "future_2"],
            [],
            id="partial_revoke_keeps_non_revoked",
        ),
        pytest.param(
            {
                PartitionInfo("topic-a", 0): {100: "future_1", 101: "future_2"},
            },
            {PartitionInfo("topic-b", 0)},
            [],
            ["future_1", "future_2"],
            id="different_topic",
        ),
    ],
)
def offset_cache_revoke_processing(
    to_process_data: dict[PartitionInfo, dict[int, str]],
    revoked_partitions: set[PartitionInfo],
    expected_cancelled: list[str],
    expected_not_cancelled: list[str],
) -> None:
    tracking_manager = TrackingManager(concurrency=16, cancel_wait_time=30.0)

    # Track created mock futures by name
    future_registry = {}

    # Setup the __to_process with mock futures
    for partition_info, offset_dict in to_process_data.items():
        for offset, future_name in offset_dict.items():
            if future_name not in future_registry:
                mock_future = MagicMock(spec=Future)
                mock_future.name = future_name  # For debugging

                # Configure cancellation behavior based on expected results
                if future_name in expected_cancelled:
                    mock_future.cancel.return_value = True
                else:
                    mock_future.cancel.return_value = False

                future_registry[future_name] = mock_future

            tracking_manager._TrackingManager__to_process[partition_info][offset] = (
                future_registry[future_name]
            )

    mock_semaphore = MagicMock()
    tracking_manager._TrackingManager__semaphore = mock_semaphore

    # Call _revoke_processing
    result = tracking_manager._revoke_processing(revoked_partitions)

    # Verify only non-cancelled futures are returned
    assert len(result) == len(expected_not_cancelled), (
        f"Expected {len(expected_not_cancelled)} futures to be returned, got {len(result)}"
    )

    # Verify the returned futures are the correct ones
    returned_future_names = [f.name for f in result]
    assert set(returned_future_names) == set(expected_not_cancelled), (
        f"Expected futures {expected_not_cancelled}, got {returned_future_names}"
    )

    # Verify all futures in revoked partitions had cancel() called
    for partition_info in revoked_partitions:
        if partition_info in to_process_data:
            for future_name in to_process_data[partition_info].values():
                future_registry[future_name].cancel.assert_called_once()

    # Verify semaphore was released for each successfully cancelled future
    assert mock_semaphore.release.call_count == len(expected_cancelled), (
        f"Expected semaphore.release() to be called {len(expected_cancelled)} times, "
        f"got {mock_semaphore.release.call_count}"
    )

    # Verify revoked partitions are cleared from __to_process
    for partition_info in revoked_partitions:
        assert partition_info not in tracking_manager._TrackingManager__to_process, (
            f"Partition {partition_info} should have been removed from __to_process"
        )

    # Verify futures in non-revoked partitions were NOT cancelled
    for partition_info, offset_dict in to_process_data.items():
        if partition_info not in revoked_partitions:
            for future_name in offset_dict.values():
                future_registry[future_name].cancel.assert_not_called()


@pytest.mark.parametrize(
    "to_process_data,partitions_to_revoke,expected_remaining_process",
    [
        pytest.param(
            {},
            [TopicPartition("test-topic", 0)],
            {},
            id="only_to_commit_has_data_remains_after_revoke",
        ),
        pytest.param(
            {PartitionInfo("test-topic", 1): {50: MagicMock(), 51: MagicMock()}},
            [TopicPartition("test-topic", 1)],
            {},
            id="only_to_process_has_data_all_cancelled",
        ),
        pytest.param(
            {
                PartitionInfo("topic-a", 0): {12: MagicMock()},
                PartitionInfo("topic-b", 1): {22: "FakeFuture"},
            },
            [TopicPartition("topic-a", 0)],
            {PartitionInfo("topic-b", 1): {22: "FakeFuture"}},
            id="partial",
        ),
        pytest.param(
            {
                PartitionInfo("topic-a", 0): {12: MagicMock(cancel=lambda: False)},
            },
            [TopicPartition("topic-a", 0)],
            {},
            id="uncancellable",
        ),
    ],
)
def test_offset_cache_register_revoke(
    to_process_data: dict,
    partitions_to_revoke: list[TopicPartition],
    expected_remaining_process: dict[PartitionInfo, set[int]],
) -> None:
    cache = TrackingManager(concurrency=16, cancel_wait_time=30.0)
    cache._TrackingManager__to_process = to_process_data

    # Call register_revoke with partitions
    cache.register_revoke(partitions_to_revoke)

    assert cache._TrackingManager__to_process == expected_remaining_process


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(TimeoutError, id="TimeoutError"),
        pytest.param(TypeError, id="other"),
    ],
)
def test_offset_cache_register_revoke_err(
    error: type[Exception],
) -> None:
    stub_partition = PartitionInfo("topic-a", 0)
    cache = TrackingManager(concurrency=16, cancel_wait_time=30.0)
    mock_future = MagicMock(spec=Future)
    mock_future.cancel.return_value = False
    mock_future.result.side_effect = error("Whoops")
    cache._TrackingManager__to_process = {stub_partition: {1: mock_future}}
    cache.register_revoke()

    mock_future.result.assert_called_once()


def test_offset_cache_schedule_commit_offset_not_in_partition(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test schedule_commit when partition exists but the specific offset doesn't.
    In the new implementation, schedule_commit always succeeds and adds to to_commit.
    """
    caplog.set_level(logging.WARNING)
    cache = TrackingManager(concurrency=16, cancel_wait_time=30.0)

    # Create a mock message
    mock_message = MagicMock(
        spec=Message,
        topic=lambda: "test-topic",
        partition=lambda: 0,
        offset=lambda: 42,
        value=lambda: b'{"test": "data"}',
    )

    partition_info = PartitionInfo("test-topic", 0)

    # Add partition but with DIFFERENT offsets (not including 42)
    for offset in [100, 101, 102]:
        mock_future = MagicMock(spec=Future)
        cache._TrackingManager__to_process[partition_info][offset] = mock_future

    # Try to schedule commit for offset 42 which doesn't exist in the partition
    result = cache.schedule_commit(mock_message)

    # In new implementation, schedule_commit always succeeds
    assert result is True

    # No warning is logged in the new implementation
    assert len(caplog.records) == 0

    # The offset IS added to to_commit (new behavior)
    assert 42 in cache._TrackingManager__to_commit.get(partition_info, set())

    # Verify original offsets remain in to_process (42 wasn't there to remove)
    assert set(cache._TrackingManager__to_process[partition_info].keys()) == {
        100,
        101,
        102,
    }
