"""Settings definitions for Kafka. Introduces intended defaults."""


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

DEFAULT_PRODUCER_SETTINGS = {**_DEFAULT_COMMON_SETTINGS}
