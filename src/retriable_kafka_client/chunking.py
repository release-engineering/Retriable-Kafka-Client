"""Module for tracking chunked messages"""

import logging
import sys
import uuid
from collections import defaultdict

from confluent_kafka import Message

from retriable_kafka_client.headers import (
    CHUNK_GROUP_HEADER,
    NUMBER_OF_CHUNKS_HEADER,
    CHUNK_ID_HEADER,
    deserialize_number_from_bytes,
    get_header_value,
)
from retriable_kafka_client.kafka_utils import MessageGroup

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


class ChunkingCache:
    # pylint: disable=too-few-public-methods
    """Class for storing information about received message fragments."""

    def __init__(self):
        # The tuple holds group ID, topic and partition
        # in case of group ID collision (that could mean an attack attempt).
        # If the same consumer was used for different topics, an adversary
        # may want to override split messages to execute different operations.
        # By using group id as well as topic and partition, this attack is
        # made impossible.
        self._message_chunks: dict[tuple[bytes, str, int], dict[int, Message]] = (
            defaultdict(dict)
        )

    def receive(self, message: Message) -> MessageGroup | None:
        """
        Receive a message. If the message is whole, or it is
        the last fragment, returns the whole message group
        and flushes cache of this group. Otherwise, returns None
        and the message fragment shall not be processed.
        """
        topic: str = message.topic()  # type: ignore[assignment]
        partition: int = message.partition()  # type: ignore[assignment]
        if (
            (group_id := get_header_value(message, CHUNK_GROUP_HEADER)) is not None
            and (
                number_of_chunks_raw := get_header_value(
                    message, NUMBER_OF_CHUNKS_HEADER
                )
            )
            is not None
            and (chunk_id_raw := get_header_value(message, CHUNK_ID_HEADER)) is not None
        ):
            number_of_chunks = deserialize_number_from_bytes(number_of_chunks_raw)
            chunk_id = deserialize_number_from_bytes(chunk_id_raw)
            identifier = (group_id, topic, partition)
            stored_message_ids_from_group = set(
                self._message_chunks.get(identifier, {}).keys()
            )
            stored_message_ids_from_group.add(chunk_id)
            if not all(
                i in stored_message_ids_from_group for i in range(number_of_chunks)
            ):
                LOGGER.debug(
                    "Received a message chunk, waiting for the other chunks..."
                )
                self._message_chunks[identifier][chunk_id] = message
                return None
            LOGGER.debug(
                "Received all message chunks, assembling group %s composed of %s messages.",
                group_id.hex(),
                number_of_chunks,
            )
            # Clear cache and reassemble, cache can be empty if
            # this is just one-message-sized value
            messages = self._message_chunks.pop(identifier, {})
            messages[chunk_id] = message
            return MessageGroup(topic, partition, messages, group_id)
        return MessageGroup(topic, partition, {0: message}, None)
