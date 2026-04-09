"""Module for kafka utility functions"""

import json
import logging
from dataclasses import dataclass
from typing import NamedTuple, Any

from confluent_kafka import Message, TopicPartition

LOGGER = logging.getLogger(__name__)


def message_to_partition(message: Message) -> TopicPartition:
    """Convert message to info about a partition."""
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    assert topic is not None and partition is not None and offset is not None, (
        "Message has to have topic, partition and offset to be converted! "
        "Maybe an error was unchecked?"
    )
    return TopicPartition(topic, partition, offset)


@dataclass
class MessageGroup:
    """
    Class for grouping messages which share chunked data.
    Attributes:
        topic: The topic of the messages.
        partition: The partition of the messages
            (all should share the same partition).
        messages: The messages within this group.
            The keys are chunk IDs (different from
            offsets).
        group_id: The group ID, UUID4. Is None
            when the received message is standalone.
    """

    topic: str
    partition: int
    messages: dict[int, Message]
    group_id: bytes | None = None

    @property
    def all_chunks(self) -> list[bytes]:
        """
        Return all data, chunked according to messages.
        """
        result = []
        for _, message in sorted(self.messages.items(), key=lambda i: i[0]):
            message_value = message.value()
            if message_value is not None:
                result.append(message_value)
        return result

    def deserialize(self) -> dict[str, Any] | None:
        """Deserialize messages into dict."""
        cumulative_value = b""
        for _, message in sorted(self.messages.items(), key=lambda i: i[0]):
            message_value = message.value()
            if not message_value:
                # Discard empty messages
                continue
            cumulative_value += message_value
        try:
            return json.loads(cumulative_value)
        except json.decoder.JSONDecodeError:
            # This message cannot be deserialized, just log and discard it
            LOGGER.exception("Decoding error: not a valid JSON: %s", cumulative_value)
            return None

    @property
    def offsets(self) -> list[int]:
        """Return all offsets of messages within this group."""
        return list(
            message.offset()  # type: ignore[misc]
            for message in self.messages.values()
        )

    def headers(self) -> list[tuple[str, bytes]] | None:
        """Return headers of these messages (first message's headers)."""
        first_message = next(iter(self.messages.values()), None)
        return first_message.headers() if first_message else None


class TrackingInfo(NamedTuple):
    """
    Consistently hashable dataclass for storing information about a partition,
    namely offset information. Can be used as keys in a dictionary.
    """

    topic: str
    partition: int

    @staticmethod
    def from_message_group(message_group: MessageGroup) -> "TrackingInfo":
        """
        Create a PartitionInfo from a Kafka message.
        Args:
             message_group: Kafka composite message object
        Returns: hashable info about a partition
        """
        message_topic = message_group.topic
        message_partition = message_group.partition
        return TrackingInfo(message_topic, message_partition)
