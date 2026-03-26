"""Module for handling local Kafka offsets memory"""

import logging
from collections import defaultdict
from concurrent.futures import Future
from itertools import chain
from threading import Lock, Semaphore
from typing import Any, Iterable

from confluent_kafka import TopicPartition

from retriable_kafka_client.kafka_utils import TrackingInfo, MessageGroup

LOGGER = logging.getLogger(__name__)


def _flatten_offsets(done_offsets: Iterable[tuple[int, ...]]) -> list[int]:
    return list(chain(*done_offsets))


class TrackingManager:
    """
    Class for handling local memory containing offset information for correct
    offsets committing and tracks pending tasks (futures).

    Each message can be either:
    - untracked (committed or not polled)
    - pending for processing (then we also track its task object)
    - pending for committing

    Offsets are integers, specifying an index of each message in a cluster.
    Each Kafka topic is divided into partitions, each partitions can be
    consumed at most by ONE consumer within a consumer group. A consumer group
    is identified by its group ID (string).

    Offsets are tracked in cluster per partition per consumer group. The goal
    of this cache is to hold information about offsets that cannot be committed
    yet and also about offsets that can be committed already.

    It is enough to commit only the offset of the last committable messages
    +1 (Kafka tracks the next offset, not the current one),
    the cluster cannot hold more information than the latest offset for each
    partition and consumer group.

    The offsets cannot be committed when their message is finished processing,
    because the order of finishing is not guaranteed. If this cache was not used,
    then we could even overwrite a bigger offset with a smaller one if the message
    process out of order, meaning that the latest message would be consumed again
    on consumer restart.

    This tracking manager also blocks if too many tasks are submitted to it,
    using semaphore. On task finish or cancellation, the semaphore is released.
    """

    def __init__(self, concurrency: int, cancel_wait_time: float):
        self.__to_process: dict[TrackingInfo, dict[tuple[int, ...], Future]] = (
            defaultdict(dict)
        )
        self.__to_commit: dict[TrackingInfo, set[tuple[int, ...]]] = defaultdict(set)
        self.__access_lock = Lock()  # For handling multithreaded access to this object
        self.__semaphore = Semaphore(concurrency)
        self.__cancel_wait_time = cancel_wait_time

    def pop_committable(self) -> list[TopicPartition]:
        """
        Read the commits stashed in memory and return committable message offsets.
        Committable are only messages that are older than any message pending
        for processing in each partition. This ensures that no message is committed
        before it is fully handled.

        Clean up memory after fetching relevant data (returned offsets should
        be immediately committed).

        Returns: list of committable message offsets
        """
        to_commit = []
        with self.__access_lock:
            for partition_info, tuples_pending_to_commit in self.__to_commit.items():
                if not tuples_pending_to_commit:
                    # Nothing to commit
                    continue

                tuples_pending_to_process = self.__to_process.get(partition_info, None)
                if not tuples_pending_to_process:
                    # Nothing is blocking the committing
                    max_to_commit = max(_flatten_offsets(tuples_pending_to_commit))
                    to_commit.append(
                        TopicPartition(
                            topic=partition_info.topic,
                            partition=partition_info.partition,
                            offset=max_to_commit,
                        )
                    )
                    self.__to_commit[partition_info] = set()
                    continue
                min_pending_to_process = min(
                    _flatten_offsets(tuples_pending_to_process)
                )
                commit_candidates = {
                    offset_tuple
                    for offset_tuple in tuples_pending_to_commit
                    if all(offset < min_pending_to_process for offset in offset_tuple)
                }
                if not commit_candidates:
                    # Nothing to commit
                    continue
                max_to_commit = max(_flatten_offsets(commit_candidates))
                to_commit.append(
                    TopicPartition(
                        topic=partition_info.topic,
                        partition=partition_info.partition,
                        offset=max_to_commit,
                    )
                )
                # Clean up committed
                for committed in commit_candidates:
                    self.__to_commit[partition_info].remove(committed)
        self._cleanup()
        return to_commit

    def reschedule_uncommittable(
        self, failed_committable: list[TopicPartition]
    ) -> None:
        """
        Add back data that could not be committed at the moment.
        The committing of this data will be retried later.
        Args:
            failed_committable: list of data that failed to be committed
        """
        for failed in failed_committable:
            self.__to_commit[
                TrackingInfo(topic=failed.topic, partition=failed.partition)
            ].add((failed.offset,))

    def process_message(self, message: MessageGroup, future: Future[Any]) -> None:
        """
        Mark message as pending for processing.
        Args:
            message: Kafka message object
            future: The task associated with whis message
        """
        # We cannot really use context manager, the semaphore is released in
        # future's callback or when the future is cancelled

        self.__semaphore.acquire()  # pylint: disable=consider-using-with
        message_offsets = message.offsets
        with self.__access_lock:
            # Mark the message as being processed
            self.__to_process[TrackingInfo.from_message_group(message)][
                tuple(message_offset + 1 for message_offset in message_offsets)
            ] = future

    def schedule_commit(self, message: MessageGroup) -> bool:
        """
        Mark message as pending for committing when its processing is fully done.
        Args:
            message: Kafka message object
        Returns:
            True if successful (the message was previously marked
            as pending for processing), False otherwise
        """
        self.__semaphore.release()
        partition_info = TrackingInfo.from_message_group(message)
        message_offsets = message.offsets
        stored_offsets = tuple(message_offset + 1 for message_offset in message_offsets)
        with self.__access_lock:
            self.__to_process[partition_info].pop(stored_offsets, None)
            self.__to_commit.setdefault(partition_info, set()).add(stored_offsets)
        self._cleanup()
        return True

    def _cleanup(self) -> None:
        """
        Clean up empty keys in the schedule (messages were deleted,
        but the dictionary key could remain).
        """
        with self.__access_lock:
            for cache_to_clean in self.__to_process, self.__to_commit:
                keys_to_pop = set()
                for partition_info, offsets in cache_to_clean.items():
                    if not offsets:
                        keys_to_pop.add(partition_info)
                for key in keys_to_pop:
                    cache_to_clean.pop(key, None)

    def _revoke_processing(
        self, revoked_partitions: set[TrackingInfo]
    ) -> list[Future[Any]]:
        """
        Cancel all pending tracked futures related to the given partitions.
        Clean up memory. Return all futures which cannot be cancelled.
        Args:
            revoked_partitions: revoked partitions (hashable)
        Returns: list of futures that couldn't be cancelled, these
            should be awaited later
        """
        to_await = []
        keys_to_pop = set()
        with self.__access_lock:
            for partition_info, offset_dict in self.__to_process.items():
                if partition_info not in revoked_partitions:
                    continue
                for future in offset_dict.values():
                    if not future.cancel():
                        to_await.append(future)
                    else:
                        self.__semaphore.release()
                keys_to_pop.add(partition_info)
            for key in keys_to_pop:
                self.__to_process.pop(key, None)
        self._cleanup()
        return to_await

    def register_revoke(self, partitions: list[TopicPartition] | None = None) -> None:
        """
        Handle revocation of partitions. This happens during
        cluster rebalancing. During this time, cancel pending
        tasks and await tasks in progress
        Args:
            partitions: list of partitions that are revoked.
                If omitted, all partitions are revoked.
        Returns: Nothing
        """
        if partitions is None:
            revoked_partition_keys = set(self.__to_process.keys())
        else:
            revoked_partition_keys = {
                TrackingInfo(partition=partition.partition, topic=partition.topic)
                for partition in partitions
            }
        pending_futures = self._revoke_processing(revoked_partition_keys)
        for future in pending_futures:
            try:
                future.result(timeout=self.__cancel_wait_time)
            except TimeoutError:
                LOGGER.error(
                    "Timeout waiting for processing of in-flight message ",
                )
            except Exception:  # pylint: disable=broad-exception-caught
                # Exceptions are already handled in __ack_message
                pass
