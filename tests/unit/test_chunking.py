from unittest.mock import MagicMock

import pytest
from confluent_kafka import Message

from retriable_kafka_client.chunking import ChunkingCache, calculate_header_size
from retriable_kafka_client.headers import (
    CHUNK_GROUP_HEADER,
    NUMBER_OF_CHUNKS_HEADER,
    CHUNK_ID_HEADER,
    serialize_number_to_bytes,
)


def test_chunking_receive():
    message1 = MagicMock(spec=Message)
    message2 = MagicMock(spec=Message)
    message3 = MagicMock(spec=Message)
    for message_id, message in enumerate([message1, message2, message3]):
        message.headers.return_value = [
            (CHUNK_GROUP_HEADER, b"foo"),
            (NUMBER_OF_CHUNKS_HEADER, serialize_number_to_bytes(3)),
            (CHUNK_ID_HEADER, serialize_number_to_bytes(message_id)),
        ]
        message.topic.return_value = "foo_topic"
        message.partition.return_value = 0
    message1.value.return_value = b'{"hello'
    message2.value.return_value = b'": "wor'
    message3.value.return_value = b'ld"}'
    chunking_cache = ChunkingCache()
    assert chunking_cache.receive(message1) is None
    assert chunking_cache.receive(message2) is None
    assert chunking_cache.receive(message3).deserialize() == {"hello": "world"}


@pytest.mark.parametrize(
    ["headers", "expected_size"],
    [(None, 0), ({"foo": "bar"}, 55), ([("foo", "bar"), ("spam", "ham")], 111)],
)
def test_calculate_header_size(
    headers: dict[str, str | bytes] | list[tuple[str, str | bytes]] | None,
    expected_size: int,
) -> None:
    assert calculate_header_size(headers) == expected_size
