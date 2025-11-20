from typing import Any
from unittest.mock import MagicMock

from confluent_kafka import Consumer


def get_kafka_consumer(mocked_messages: list[bytes]) -> Consumer:
    """
    Mocks a Kafka consumer
    :param mocked_messages: List of messages to return for each poll call.
    :return: A MagicMock object acting as Kafka consumer in disguise.
    """
    mocked_consumer = MagicMock(spec=Consumer)
    mocked_consumer.message_no = 0

    def mocked_poll(*_: Any, **__: Any) -> bytes | None:
        if mocked_consumer.message_no >= len(mocked_messages):
            return None
        result = mocked_messages[mocked_consumer.message_no]
        mocked_consumer.message_no += 1
        return result

    mocked_consumer.poll = mocked_poll
    return mocked_consumer
