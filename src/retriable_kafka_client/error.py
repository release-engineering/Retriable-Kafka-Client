"""Module for error definitions"""

from confluent_kafka import KafkaError


class SendError(RuntimeError):
    """Class for raising problems with message producing."""

    def __init__(
        self, *args, retriable: bool, fatal: bool, kafka_error: KafkaError | None
    ) -> None:
        self.retriable = retriable
        self.fatal = fatal
        self.kafka_error = kafka_error
        super().__init__(*args)

    @staticmethod
    def format_err(err: KafkaError) -> str:
        """Propose human-readable error message constructed from Kafka error."""
        return (
            f"Underlying error details: {err.__class__.__name__}(name={err.name()}, "
            f"retriable={err.retriable()}, fatal={err.fatal()})"
        )
