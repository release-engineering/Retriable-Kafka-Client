"""
Integration tests for Kafka producer and consumer
using multiple topics.
"""

import asyncio
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client import ConsumeTopicConfig

from .integration_utils import (
    IntegrationTestScaffold,
    ScaffoldConfig,
)


@pytest.mark.asyncio
async def test_producer_consumer_integration(
    kafka_config: dict[str, Any], admin_client: AdminClient
) -> None:
    """Test producing messages to 2 topics and consuming them"""

    message_count = 3

    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(base_topic="test-topic-1"),
            ConsumeTopicConfig(base_topic="test-topic-2"),
        ],
        group_id="test-consumer-group",
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer()
        await asyncio.sleep(2)  # Wait for consumer to be ready

        await scaffold.send_messages(message_count)
        success = await scaffold.wait_for_success()

        assert success, (
            f"Expected {scaffold.messages_sent} messages × {len(config.topics)} topics. "
            f"Got success_counts: {scaffold.tracker.success_counts}"
        )

        # Verify each message was received once per topic
        for msg_id in range(scaffold.messages_sent):
            assert scaffold.tracker.success_counts.get(msg_id) == len(config.topics), (
                f"Message {msg_id} should appear {len(config.topics)} times, "
                f"got {scaffold.tracker.success_counts.get(msg_id)}"
            )
