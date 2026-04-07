from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from confluent_kafka import Message

from retriable_kafka_client.chunking import MessageGroupBuilder, calculate_header_size
from retriable_kafka_client.consumer_tracking import TrackingManager
from retriable_kafka_client.headers import (
    CHUNK_GROUP_HEADER,
    NUMBER_OF_CHUNKS_HEADER,
    CHUNK_ID_HEADER,
    serialize_number_to_bytes,
)


def _make_chunk_message(
    group_id: bytes,
    chunk_id: int,
    total_chunks: int,
    value: bytes,
    topic: str = "foo_topic",
    partition: int = 0,
    offset: int = 0,
) -> MagicMock:
    message = MagicMock(spec=Message)
    message.headers.return_value = [
        (CHUNK_GROUP_HEADER, group_id),
        (NUMBER_OF_CHUNKS_HEADER, serialize_number_to_bytes(total_chunks)),
        (CHUNK_ID_HEADER, serialize_number_to_bytes(chunk_id)),
    ]
    message.topic.return_value = topic
    message.partition.return_value = partition
    message.offset.return_value = offset
    message.value.return_value = value
    return message


def test_message_group_builder_complete():
    builder = MessageGroupBuilder(max_wait_time=timedelta(minutes=15))
    messages = [
        _make_chunk_message(b"foo", i, 3, v, offset=i)
        for i, v in enumerate([b'{"hello', b'": "wor', b'ld"}'])
    ]
    for msg in messages[:2]:
        builder.add(msg)
        assert not builder.is_complete
    builder.add(messages[2])
    assert builder.is_complete
    group = builder.get_message_group()
    assert group is not None
    assert group.deserialize() == {"hello": "world"}


def test_message_group_builder_complete_empty():
    builder = MessageGroupBuilder(max_wait_time=timedelta(minutes=15))
    assert builder.is_complete is False


def test_message_group_builder_incomplete_returns_none():
    builder = MessageGroupBuilder(max_wait_time=timedelta(minutes=15))
    msg = _make_chunk_message(b"foo", 0, 3, b'{"hello', offset=0)
    builder.add(msg)
    assert not builder.is_complete
    assert builder.get_message_group() is None


def test_tracking_manager_receive_chunked():
    tracker = TrackingManager(
        concurrency=16,
        cancel_wait_time=30.0,
        max_chunk_wait_time=timedelta(minutes=15),
    )
    messages = [
        _make_chunk_message(b"foo", i, 3, v, offset=i)
        for i, v in enumerate([b'{"hello', b'": "wor', b'ld"}'])
    ]
    assert tracker.receive(messages[0]) is None
    assert tracker.receive(messages[1]) is None
    result = tracker.receive(messages[2])
    assert result is not None
    assert result.deserialize() == {"hello": "world"}


@pytest.mark.parametrize(
    ["headers", "expected_size"],
    [(None, 0), ({"foo": "bar"}, 55), ([("foo", "bar"), ("spam", "ham")], 111)],
)
def test_calculate_header_size(
    headers: dict[str, str | bytes] | list[tuple[str, str | bytes]] | None,
    expected_size: int,
) -> None:
    assert calculate_header_size(headers) == expected_size
