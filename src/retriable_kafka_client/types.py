from dataclasses import dataclass
from typing import Callable, Any


@dataclass
class TopicConfig:
    kafka_hosts: list[str]
    topics: list[str]
    group_id: str
    user_name: str
    password: str
    target: Callable[[dict[str, Any]], Any]
