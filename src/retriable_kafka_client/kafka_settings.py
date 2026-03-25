"""Settings definitions for Kafka. Introduces intended defaults."""

DEFAULT_MESSAGE_SIZE = 1000000
MESSAGE_OVERHEAD = 400  # When splitting messages,
# the exact length cannot be easily computed.
# Therefore, we check the size of passed objects
# and subtract additional 400 B out of default 1 MB
# This number also includes custom headers for


class KafkaOptions:
    """
    Definitions for Kafka settings.
    """

    # pylint: disable=too-few-public-methods
    KAFKA_NODES = "bootstrap.servers"
    GROUP_ID = "group.id"
    OFFSET_RESET = "auto.offset.reset"
    AUTO_COMMIT = "enable.auto.commit"
    AUTH_MECHANISM = "sasl.mechanisms"
    SECURITY_PROTO = "security.protocol"
    USERNAME = "sasl.username"
    PASSWORD = "sasl.password"
    PARTITION_ASSIGNMENT_STRAT = "partition.assignment.strategy"
    MAX_MESSAGE_SIZE = "message.max.bytes"


_DEFAULT_COMMON_SETTINGS = {
    KafkaOptions.AUTH_MECHANISM: "SCRAM-SHA-512",
    KafkaOptions.SECURITY_PROTO: "SASL_SSL",
}

DEFAULT_CONSUMER_SETTINGS = {
    KafkaOptions.AUTO_COMMIT: False,
    KafkaOptions.OFFSET_RESET: "earliest",
    KafkaOptions.PARTITION_ASSIGNMENT_STRAT: "cooperative-sticky",
    **_DEFAULT_COMMON_SETTINGS,
}

DEFAULT_PRODUCER_SETTINGS = {
    KafkaOptions.MAX_MESSAGE_SIZE: DEFAULT_MESSAGE_SIZE,
    **_DEFAULT_COMMON_SETTINGS,
}
