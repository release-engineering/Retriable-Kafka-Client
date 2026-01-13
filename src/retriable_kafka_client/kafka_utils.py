"""Module for kafka utility functions"""

from confluent_kafka import Message, TopicPartition


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
