"""Integration tests for Kafka consumer reconnection scenarios"""

import asyncio
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client import ConsumeTopicConfig

from .integration_utils import IntegrationTestScaffold, ScaffoldConfig


@pytest.mark.asyncio
async def test_consumer_reconnection_mid_consumption(
    kafka_config: dict[str, Any], admin_client: AdminClient
) -> None:
    """
    Test that when a consumer is shut down mid-consumption,
    a new consumer can continue processing and all messages are eventually consumed.
    """
    total_messages = 10

    config = ScaffoldConfig(
        topics=[ConsumeTopicConfig(base_topic="test-reconnection-topic")],
        group_id="test-reconnection-consumer-group",
        timeout=20.0,
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        # Start first consumer with slow processing
        consumer1 = scaffold.start_consumer(
            delay=0.3,
            max_concurrency=2,
            max_workers=2,
        )

        # Send messages
        await scaffold.send_messages(total_messages)

        # Wait for first consumer to process some messages (but not all)
        await scaffold.wait_for(lambda a, b: a or b, 5)

        # Shut down first consumer mid-consumption (this properly leaves the group)
        with scaffold.tracker.lock:
            first_consumer_count = len(scaffold.tracker.success_counts)

        consumer1.stop()

        # First consumer should have processed some messages but not all
        assert first_consumer_count > 0, (
            "First consumer should have processed at least one message "
            f"before shutdown, but processed {first_consumer_count}"
        )
        assert first_consumer_count < total_messages, (
            f"First consumer should NOT have processed all {total_messages} messages, "
            f"but processed {first_consumer_count}"
        )

        # Start second consumer with fast processing
        await asyncio.sleep(1)

        scaffold.start_consumer(
            delay=0,
            max_concurrency=4,
            max_workers=2,
        )

        # Wait for all messages to be processed
        success = await scaffold.wait_for_success()

        assert success, (
            f"Expected {total_messages} messages to be processed. "
            f"Success counts: {scaffold.tracker.success_counts}"
        )

        # Verify no duplicate processing (each message processed exactly once)
        with scaffold.tracker.lock:
            duplicates = {
                k: v for k, v in scaffold.tracker.success_counts.items() if v > 1
            }
        assert not duplicates, (
            f"Some messages were processed multiple times: {duplicates}"
        )
