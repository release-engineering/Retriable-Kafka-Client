"""Types used in this library"""

from dataclasses import dataclass
from typing import Callable, Any


@dataclass
class TopicConfig:
    """
    Topic configuration for each consumer
    Attributes:
        kafka_hosts: list of Kafka node URLs to connect to
        topics: list of topic names to connect to
        group_id: consumer group ID to use when consuming
        user_name: consumer username
        password: consumer password
        target: Callable to execute on all parsed messages
    """

    kafka_hosts: list[str]
    topics: list[str]
    group_id: str
    user_name: str
    password: str
    target: Callable[[dict[str, Any]], Any]
