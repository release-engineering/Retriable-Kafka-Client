"""Module for handling local Kafka offsets memory"""

import logging
from collections import defaultdict
from threading import Lock
from typing import NamedTuple

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
        Create a _PartitionInfo from a Kafka message.
        :param message: Kafka message object
        :return: hashable info about a partition
        """
        message_topic = message.topic()
        message_partition = message.partition()
        assert message_topic is not None and message_partition is not None, (
            "Invalid message cannot be converted to partition info"
        )
        return _PartitionInfo(message_topic, message_partition)

    def to_offset_info(self, offset: int) -> TopicPartition:
        """
        Create a Kafka-committable object using the provided offset.
        :param offset: The offset to be committed. Make sure to commit
            offset one higher than the latest processed message.
        :return: The committable Kafka object
        """
        return TopicPartition(topic=self.topic, partition=self.partition, offset=offset)


class OffsetCache:
    """
    Class for handling local memory containing offset information for correct
    offsets committing.

    Each message can be either:
    - untracked (committed or not polled)
    - pending for processing
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
    """

    def __init__(self):
        self.__to_process: dict[_PartitionInfo, set[int]] = defaultdict(set)
        self.__to_commit: dict[_PartitionInfo, set[int]] = defaultdict(set)
        self.__commit_lock = Lock()

    def pop_committable(self) -> list[TopicPartition]:
        """
        Read the commits stashed in memory and return committable message offsets.
        Committable are only messages that are older than any message pending
        for processing in each partition. This ensures that no message is committed
        before it is fully handled.

        Clean up memory after fetching relevant data (returned offsets should
        be immediately committed).

        :return: list of committable message offsets
        """
        to_commit = []
        with self.__commit_lock:
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
        return to_commit

    def process_message(self, message: Message) -> None:
        """
        Mark message as pending for processing.
        :param message: Kafka message object
        :return: Nothing
        """
        message_offset: int = message.offset()  # type: ignore[assignment]
        with self.__commit_lock:
            # Mark the message as being processed
            self.__to_process.setdefault(
                _PartitionInfo.from_message(message), set()
            ).add(message_offset)

    def schedule_commit(self, message: Message) -> bool:
        """
        Mark message as pending for committing.
        :param message: Kafka message object
        :return: True if successful (the message was previously marked
            as pending for processing), False otherwise
        """
        partition_info = _PartitionInfo.from_message(message)
        message_offset: int = message.offset()  # type: ignore[assignment]
        with self.__commit_lock:
            if partition_info not in self.__to_process:
                # This can happen if rebalancing took place
                LOGGER.warning(
                    "Message in topic %s and partition %d with offset %s "
                    "will not be committed. Rebalancing happened while this message was "
                    "processed.",
                    message.topic(),
                    message.partition(),
                    message_offset,
                    extra={"message_raw": message.value()},
                )
                return False
            self.__to_process[partition_info].remove(message_offset)
            self.__to_commit.setdefault(partition_info, set()).add(message_offset)
        return True

    def register_revoke(self) -> None:
        """
        Handle revocation of partitions. This happens during
        cluster rebalancing.
        :return: Nothing
        """
        if not self.has_cache():
            return
        for partition_info, offsets in self.__to_commit.items():
            LOGGER.warning(
                "Messages in topic %s and partition %d with offsets %s failed "
                "to be commited due to rebalancing. The message may be processed "
                "again after rebalancing is complete.",
                partition_info.topic,
                partition_info.partition,
                offsets,
            )
        for partition_info, offsets in self.__to_process.items():
            LOGGER.warning(
                "Messages in topic %s and partition %d with offset %s failed "
                "to be fully processed due to rebalancing. The message may be "
                "processed again after rebalancing is complete.",
                partition_info.topic,
                partition_info.partition,
                offsets,
            )
        with self.__commit_lock:
            self.__to_process.clear()
            self.__to_commit.clear()

    def has_cache(self) -> bool:
        """
        Determine if there is anything pending to process or to commit.
        :return: True if there is cache, False otherwise
        """
        return any(self.__to_process.values()) or any(self.__to_commit.values())
