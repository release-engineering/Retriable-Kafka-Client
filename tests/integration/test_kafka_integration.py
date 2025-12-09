"""Integration tests for Kafka producer and consumer"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Generator

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

from retriable_kafka_client.consumer import BaseConsumer
from retriable_kafka_client.orchestrate import ConsumerThread
from retriable_kafka_client.producer import BaseProducer
from retriable_kafka_client.types import ProducerConfig, ConsumerConfig
from retriable_kafka_client.kafka_settings import KafkaOptions


@pytest.fixture(scope="session")
def kafka_config() -> dict[str, Any]:
    """Kafka connection configuration for local testing"""
    return {
        KafkaOptions.KAFKA_NODES: "localhost:9092",
        KafkaOptions.USERNAME: "testuser",
        KafkaOptions.PASSWORD: "testpassword",
    }


@pytest.fixture(scope="session")
def admin_client(kafka_config: dict[str, Any]) -> AdminClient:
    return AdminClient(
        {
            KafkaOptions.AUTH_MECHANISM: "SCRAM-SHA-512",
            KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT",
            **kafka_config,
        }
    )


@pytest.fixture(scope="session", autouse=True)
def wait_for_kafka(admin_client: AdminClient) -> None:
    """Wait for Kafka to be healthy before running any tests"""

    max_attempts = 30
    for attempt in range(1, max_attempts + 1):
        try:
            # Try to get cluster metadata
            metadata = admin_client.list_topics(timeout=5)
            if metadata.brokers:
                return
        except KafkaException:
            pass
        except Exception:
            pass

        if attempt < max_attempts:
            time.sleep(1)

    pytest.fail(
        f"Kafka did not become healthy after {max_attempts} seconds. "
        "Ensure Kafka is running with: docker compose up -d"
    )


@pytest.fixture(scope="module")
def topic_names() -> list[str]:
    """Topic names for testing"""
    return ["test-topic-1", "test-topic-2"]


@pytest.fixture(scope="module", autouse=True)
def create_topics(admin_client: AdminClient, topic_names: list[str]) -> Generator[None]:
    """Create test topics before running tests"""
    # Note: With auto.create.topics.enable=true, this is optional,
    # but we'll create them explicitly for better control

    new_topics = [
        NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topic_names
    ]

    # Create topics
    fs = admin_client.create_topics(new_topics)

    # Wait for topics to be created
    for topic, f in fs.items():
        try:
            f.result()
        except Exception:
            # Topic might already exist, that's fine
            pass
    yield

    # Cleanup: delete topics after tests
    # admin_client.delete_topics(topic_names, operation_timeout=30)


@pytest.fixture
def producer(kafka_config, topic_names) -> Generator[BaseProducer, None, None]:
    """Create a producer instance"""
    config = ProducerConfig(
        kafka_hosts=[kafka_config[KafkaOptions.KAFKA_NODES]],
        topics=topic_names,
        user_name=kafka_config[KafkaOptions.USERNAME],
        password=kafka_config[KafkaOptions.PASSWORD],
        retries=3,
        fallback_base=0.1,
        fallback_factor=2.0,
    )

    # Override the security protocol for local testing (no SSL)
    producer = BaseProducer(config, **{KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT"})

    yield producer

    # Cleanup
    producer.close()


@pytest.mark.asyncio
async def test_producer_consumer_integration(producer, kafka_config, topic_names):
    """Test producing messages to 2 topics and consuming them"""

    # Create shared list for message collection
    shared_messages = []
    group_id = "test-consumer-group"

    # Create consumer config
    config = ConsumerConfig(
        kafka_hosts=[kafka_config[KafkaOptions.KAFKA_NODES]],
        topics=topic_names,
        user_name=kafka_config[KafkaOptions.USERNAME],
        password=kafka_config[KafkaOptions.PASSWORD],
        group_id=group_id,
        target=shared_messages.append,
    )

    # Create ProcessPoolExecutor with spawn context to avoid fork warnings
    executor = ThreadPoolExecutor(max_workers=2)

    # Create consumer and wrap it in ConsumerThread
    consumer = BaseConsumer(
        config,
        executor,
        max_concurrency=4,
        **{KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT"},
    )
    consumer_thread = ConsumerThread(consumer)

    try:
        # Start the consumer thread
        consumer_thread.start()

        # Wait a bit for consumer to be ready
        await asyncio.sleep(2)

        # Produce messages to both topics
        messages_to_send = [
            {"id": 1, "message": "Hello from topic 1 and 2", "timestamp": time.time()},
            {"id": 2, "message": "Second message", "data": {"key": "value"}},
            {"id": 3, "message": "Third message", "numbers": [1, 2, 3, 4, 5]},
        ]

        for msg in messages_to_send:
            await producer.send(msg)

        # Flush to ensure all messages are sent
        producer.close()

        # Wait for messages to be consumed
        max_wait = 10  # seconds
        start_time = time.time()

        # Each message goes to 2 topics, so we expect 6 total consumed messages
        expected_count = len(messages_to_send) * 2

        iteration = 0
        while len(shared_messages) < expected_count:
            iteration += 1
            elapsed = time.time() - start_time
            print(
                f"Iteration {iteration}: Got {len(shared_messages)}/{expected_count} messages (elapsed: {elapsed:.1f}s)"
            )

            if time.time() - start_time > max_wait:
                print(
                    f"TIMEOUT: Waited {max_wait} seconds but only got {len(shared_messages)} messages"
                )
                break
            await asyncio.sleep(0.5)

        # Stop the consumer thread
        consumer_thread.stop()
        consumer_thread.join(timeout=5)

        # Convert manager.list to regular list for easier manipulation
        consumed_list = list(shared_messages)

        # Assertions
        assert len(consumed_list) == expected_count, (
            f"Expected {expected_count} messages (3 messages × 2 topics), "
            f"but got {len(consumed_list)}"
        )

        # Verify all original messages are in the consumed set
        consumed_ids = [msg["id"] for msg in consumed_list]

        for original_msg in messages_to_send:
            count = consumed_ids.count(original_msg["id"])
            # Each message should appear twice (once per topic)
            assert count == 2, (
                f"Message with id={original_msg['id']} should appear twice "
                f"(once per topic), but appeared {count} times"
            )

    finally:
        # Ensure cleanup even if test fails
        consumer_thread.stop()
        executor.shutdown(wait=True)
