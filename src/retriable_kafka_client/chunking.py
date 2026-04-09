"""Module for tracking chunked messages"""

from datetime import datetime, timedelta, timezone
import logging
import sys
import uuid

from confluent_kafka import Message

from retriable_kafka_client.headers import (
    CHUNK_GROUP_HEADER,
    NUMBER_OF_CHUNKS_HEADER,
    CHUNK_ID_HEADER,
    deserialize_number_from_bytes,
    get_header_value,
)
from retriable_kafka_client.kafka_utils import MessageGroup, TrackingInfo

LOGGER = logging.getLogger(__name__)


def generate_group_id() -> bytes:
    """Generate a random group id."""
    return uuid.uuid4().bytes


def calculate_header_size(
    headers: dict[str, str | bytes] | list[tuple[str, str | bytes]] | None,
) -> int:
    """Approximate the space needed for headers within a message."""
    if not headers:
        return 0
    result = 0
    if not isinstance(headers, dict):
        headers = dict(headers)
    for header_key, header_value in headers.items():
        result += len(header_key) + sys.getsizeof(header_value) + 8
        # Two 32-bit numbers specifying lengths of fields are also
        # present, therefore adding 8
    return result


class MessageGroupBuilder:
    """Class for gathering data from message chunks."""

    def __init__(self, max_wait_time: timedelta) -> None:
        self._last_update_time = datetime.now(tz=timezone.utc)
        self._max_wait_time = max_wait_time
        self._messages: list[Message] = []
        self.partition_info: TrackingInfo | None = None
        self.group_id: bytes | None = None
        self.full_length: int = 0
        self._present_chunks: set[int] = set()

    @staticmethod
    def get_group_id_from_message(message: Message) -> bytes | None:
        """Get the group id from the message."""
        return get_header_value(message, CHUNK_GROUP_HEADER)

    @staticmethod
    def get_chunk_id_from_message(message: Message) -> int | None:
        """Get the chunk id from the message."""
        if header_value := get_header_value(message, CHUNK_ID_HEADER):
            return deserialize_number_from_bytes(header_value)
        return None

    @staticmethod
    def get_number_of_chunks_from_message(message: Message) -> int | None:
        """Get the number of chunks from the message."""
        if header_value := get_header_value(message, NUMBER_OF_CHUNKS_HEADER):
            return deserialize_number_from_bytes(header_value)
        return None

    def add(self, message: Message) -> None:
        """
        Add a message to the message group builder.
        Args:
            message: The Kafka message to add.
        """
        if (
            (new_group_id := self.get_group_id_from_message(message)) is None
            or (new_number_of_chunks := self.get_number_of_chunks_from_message(message))
            is None
            or ((new_chunk_id := self.get_chunk_id_from_message(message)) is None)
        ):
            raise ValueError("The new message is missing required chunk headers!")
        if self.group_id is None:
            self.group_id = new_group_id
            self.full_length = new_number_of_chunks
            self.partition_info = TrackingInfo(
                message.topic(),  # type: ignore[arg-type]
                message.partition(),  # type: ignore[arg-type]
            )
        self._last_update_time = datetime.now(tz=timezone.utc)
        self._messages.append(message)
        self._present_chunks.add(new_chunk_id)

    @property
    def is_complete(self) -> bool:
        """Does this builder contain all the needed chunks?"""
        if not self._messages:
            return False
        return all(i in self._present_chunks for i in range(self.full_length))

    @property
    def offsets(self) -> tuple[int, ...]:
        """Return the offsets of the chunks."""
        return tuple(message.offset() for message in self._messages)  # type: ignore[misc]

    def is_still_valid(self) -> bool:
        """Isn't this builder stale? Useful for discarding corrupted data."""
        return (
            datetime.now(tz=timezone.utc) - self._last_update_time < self._max_wait_time
        )

    def get_message_group(self, allow_incomplete: bool = False) -> MessageGroup | None:
        """
        Generate the message group object from the builder.
        Args:
            allow_incomplete: If true, this will return a messageGroup
            object even if not all chunks have been gathered.
        Returns: MessageGroup object if all required data is available,
            None otherwise.
        """
        if (
            (not allow_incomplete and not self.is_complete)
            or not self._messages
            or not self.partition_info
        ):
            return None
        return MessageGroup(
            topic=self.partition_info.topic,
            partition=self.partition_info.partition,
            messages={
                self.get_chunk_id_from_message(message): message  # type: ignore[misc]
                for message in self._messages
            },
            group_id=self.group_id,
        )
