"""
Module with definitions and utilities
related to Kafka headers
"""

from confluent_kafka import Message

from retriable_kafka_client.kafka_utils import MessageGroup

_HEADER_PREFIX = "retriable_kafka_"
# Header name which holds the number of retry attempts
ATTEMPT_HEADER = _HEADER_PREFIX + "attempt"
# Header name which holds the timestamp of next reprocessing
TIMESTAMP_HEADER = _HEADER_PREFIX + "timestamp"
# Header name which holds the ID of a chunk
CHUNK_GROUP_HEADER = _HEADER_PREFIX + "chunk_group"
# Header name which holds the total number of chunks within group
NUMBER_OF_CHUNKS_HEADER = _HEADER_PREFIX + "number_of_chunks"
# Header name which holds the serial number of chunk within a group
CHUNK_ID_HEADER = _HEADER_PREFIX + "chunk_id"


def serialize_number_to_bytes(value: int | float) -> bytes:
    """Store a number as bytes, converts to integers first."""
    return int(value).to_bytes(length=8, byteorder="big")


def deserialize_number_from_bytes(value: bytes) -> int:
    """Restore integer from bytes."""
    return int.from_bytes(value, byteorder="big")


def get_header_value(
    message: Message | MessageGroup, searched_header_name: str
) -> bytes | None:
    """Fetch header value from message."""
    message_headers = message.headers()
    if not message_headers:
        return None
    for header_name, header_value in message_headers:
        if header_name == searched_header_name:
            return header_value
    return None
