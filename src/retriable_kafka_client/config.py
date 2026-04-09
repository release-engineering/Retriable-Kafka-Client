"""Types used in this library"""

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable, Any

from confluent_kafka import Message


@dataclass(kw_only=True)
class CommonConfig:
    """
    Topic configuration common for consumers and producers.
    Attributes:
        kafka_hosts: list of Kafka node URLs to connect to
        username: Kafka username
        password: Kafka password
        additional_settings: additional settings to pass directly to Kafka
    """

    kafka_hosts: list[str]
    username: str
    password: str
    additional_settings: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProducerConfig(CommonConfig):
    """
    Topic configuration common each producer, including backoff settings.
    Attributes:
        kafka_hosts: list of Kafka node URLs to connect to
        topics: list of topic names to publish to
        username: producer username
        password: producer password
        additional_settings: additional settings to pass directly to Kafka consumer
        retries: number of attempts to publish the message
        fallback_factor: how many times longer should each backoff take
        fallback_base: what is the starting backoff in seconds
    """

    topics: list[str]
    retries: int = field(default=3)
    fallback_factor: float = field(default=2.0)
    fallback_base: float = field(default=5.0)
    split_messages: bool = field(default=False)


@dataclass
class ConsumeTopicConfig:
    """
    Configuration for retry mechanism of a consumer.
    Must be used from within ConsumerConfig.
    Attributes:
        base_topic: Topic that this consumer subscribes to
        retry_topic: Topic used for resending failed messages
        retries: maximal number of attempts to re-process the
            message originated from base_topic
        fallback_delay: Number of seconds to wait before a message
            should be re-processed. This is a non-blocking event.
    """

    base_topic: str
    retry_topic: str | None = field(default=None)
    retries: int = field(default=5)
    fallback_delay: float = field(default=15.0)


@dataclass
class ConsumerConfig(CommonConfig):
    """
    Topic configuration for each consumer.
    Attributes:
        kafka_hosts: list of Kafka node URLs to connect to
        topics: list of configuration for topics and their
            retry policies
        cancel_future_wait_time: Maximal time to wait for a task
            to finish before discarding it on rebalance or soft shutdown.
            Doesn't affect tasks which are ran in normal circumstances.
        username: consumer username
        password: consumer password
        additional_settings: additional settings to pass directly to Kafka producer
        group_id: consumer group ID to use when consuming
        target: Callable to execute on all parsed messages
        filter_function: Filters messages based on the user-provided function.
            Returns True if the message will be processed
            or False if skipped. In case False or exception is returned,
            message will be committed without processing.
        max_chunk_reassembly_wait_time: Maximal time to wait for all the chunks
            of the message to arrive. Has any effect only if chunking is enabled.
            If some chunks are still waiting for reassembly after this threshold,
            they are deleted and a warning is logged. This happens if the producer
            crashed during producing of the chunked message, data cannot be salvaged.
    """

    group_id: str
    target: Callable[[dict[str, Any]], Any]
    topics: list[ConsumeTopicConfig] = field(default_factory=list)
    cancel_future_wait_time: float = field(default=30.0)
    filter_function: Callable[[Message], bool] | None = field(default=None)
    max_chunk_reassembly_wait_time: timedelta = field(default=timedelta(minutes=15))
