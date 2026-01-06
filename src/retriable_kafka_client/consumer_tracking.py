"""Module for handling local Kafka offsets memory"""

import logging
from collections import defaultdict
from concurrent.futures import Future
from threading import Lock, Semaphore
from typing import NamedTuple, Any

from confluent_kafka import Message, TopicPartition

LOGGER = logging.getLogger(__name__)


class _PartitionInfo(NamedTuple):
    """
    Consistently hashable dataclass for storing information about a partition,
    namely offset information. Can be used as keys in a dictionary.
    """

    topic: str
    partition: int

    @staticmethod
    def from_message(message: Message) -> "_PartitionInfo":
        """
        Create a PartitionInfo from a Kafka message.
        Args:
             message: Kafka message object
        Returns: hashable info about a partition
        """
        message_topic = message.topic()
        message_partition = message.partition()
        # This should never happen with polled messages. Polled messages need
        # the information asserted to be valid Kafka messages. This can
        # happen only for custom-created messages objects, which this
        # method is not intended to be used for
        assert message_topic is not None and message_partition is not None, (
            "Invalid message cannot be converted to partition info"
        )
        return _PartitionInfo(message_topic, message_partition)

    def to_offset_info(self, offset: int) -> TopicPartition:
        """
        Create a Kafka-committable object using the provided offset.
        Args:
            offset: The offset to be committed. Make sure to commit
                offset one higher than the latest processed message.
        Returns: The committable Kafka object
        """
        return TopicPartition(topic=self.topic, partition=self.partition, offset=offset)


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

    It is enough to commit only the offset of the last committable messages,
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
        self.__to_process: dict[_PartitionInfo, dict[int, Future]] = defaultdict(dict)
        self.__to_commit: dict[_PartitionInfo, set[int]] = defaultdict(set)
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
            for partition_info, pending_to_commit in self.__to_commit.items():
                if not pending_to_commit:
                    # Nothing to commit
                    continue

                pending_to_process = self.__to_process.get(partition_info, None)
                if not pending_to_process:
                    # Nothing is blocking the committing
                    max_to_commit = max(pending_to_commit)
                    to_commit.append(
                        TopicPartition(
                            topic=partition_info.topic,
                            partition=partition_info.partition,
                            offset=max_to_commit + 1,
                        )
                    )
                    self.__to_commit[partition_info] = set()
                    continue

                min_pending_to_process = min(pending_to_process)
                commit_candidates = {
                    offset
                    for offset in pending_to_commit
                    if offset < min_pending_to_process
                }
                if not commit_candidates:
                    # Nothing to commit
                    continue
                max_to_commit = max(commit_candidates)
                to_commit.append(
                    TopicPartition(
                        topic=partition_info.topic,
                        partition=partition_info.partition,
                        offset=max_to_commit + 1,
                    )
                )
                # Clean up committed
                for committed in commit_candidates:
                    self.__to_commit[partition_info].remove(committed)
        self._cleanup()
        return to_commit

    def process_message(self, message: Message, future: Future[Any]) -> None:
        """
        Mark message as pending for processing.
        Args:
            message: Kafka message object
            future: The task associated with whis message
        """
        # We cannot really use context manager, the semaphore is released in
        # future's callback or when the future is cancelled
        self.__semaphore.acquire()  # pylint: disable=consider-using-with
        message_offset: int = message.offset()  # type: ignore[assignment]
        with self.__access_lock:
            # Mark the message as being processed
            self.__to_process[_PartitionInfo.from_message(message)][message_offset] = (
                future
            )

    def schedule_commit(self, message: Message) -> bool:
        """
        Mark message as pending for committing when its processing is fully done.
        Args:
            message: Kafka message object
        Returns:
            True if successful (the message was previously marked
            as pending for processing), False otherwise
        """
        self.__semaphore.release()
        partition_info = _PartitionInfo.from_message(message)
        message_offset: int = message.offset()  # type: ignore[assignment]
        with self.__access_lock:
            self.__to_process[partition_info].pop(message_offset, None)
            self.__to_commit.setdefault(partition_info, set()).add(message_offset)
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
        self, revoked_partitions: set[_PartitionInfo]
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
                _PartitionInfo(partition=partition.partition, topic=partition.topic)
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
