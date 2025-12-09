"""Types used in this library"""

from dataclasses import dataclass, field
from typing import Callable, Any


@dataclass
class _CommonConfig:
    """
    Topic configuration common for consumers and producers.
    Attributes:
        kafka_hosts: list of Kafka node URLs to connect to
        topics: list of topic names to connect to
        user_name: consumer username
        password: consumer password
    """

    kafka_hosts: list[str]
    topics: list[str]
    user_name: str
    password: str


@dataclass
class ProducerConfig(_CommonConfig):
    """
    Topic configuration common each producer, including backoff settings.
    Attributes:
        kafka_hosts: list of Kafka node URLs to connect to
        topics: list of topic names to connect to
        user_name: consumer username
        password: consumer password
        retries: number of attempts to publish the message
        fallback_factor: how many times longer should each backoff take
        fallback_base: what is the starting backoff in seconds
    """

    retries: int = field(default=3)
    fallback_factor: float = field(default=2.0)
    fallback_base: float = field(default=5.0)


@dataclass
class ConsumerConfig(_CommonConfig):
    """
    Topic configuration for each consumer.
    Attributes:
        kafka_hosts: list of Kafka node URLs to connect to
        topics: list of topic names to connect to
        user_name: consumer username
        password: consumer password
        group_id: consumer group ID to use when consuming
        target: Callable to execute on all parsed messages
    """

    group_id: str
    target: Callable[[dict[str, Any]], Any]
