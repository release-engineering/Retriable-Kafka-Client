"""
Integration tests for Kafka producer and consumer
using larger amount of messages than other tests.
"""

import asyncio
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client import ConsumeTopicConfig

from .integration_utils import (
    IntegrationTestScaffold,
    ScaffoldConfig,
    RandomDelay,
)


@pytest.mark.asyncio
async def test_high_volume_with_random_delays_and_retries(
    kafka_config: dict[str, Any], admin_client: AdminClient
) -> None:
    """
    Test that a large number of messages (30+) can be processed without deadlocks
    when using:
    - A small number of workers (2)
    - Random delays in processing
    - Exceptions on first attempt that trigger retries

    This test ensures the consumer can handle concurrent processing with retries
    without getting into a deadlock state.
    """
    message_count = 30

    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(
                base_topic="test-large-base-topic",
                retry_topic="test-large-retry-topic",
                retries=3,
                fallback_delay=0.5,
            ),
        ],
        group_id="test-large",
        timeout=30.0,
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer(
            delay=RandomDelay(min_delay=0, max_delay=0.05),
            fail_chance_on_first=0.7,
            max_workers=2,
            max_concurrency=4,
        )
        await asyncio.sleep(2)  # Wait for consumer to be ready

        await scaffold.send_messages(message_count)
        success = await scaffold.wait_for_success()

        assert success, (
            f"Expected {scaffold.messages_sent} successful messages after retries, "
            f"got {len(scaffold.tracker.success_counts)}. "
            f"This may indicate a deadlock. "
            f"Call counts: {scaffold.tracker.call_counts}"
        )

        # Verify all messages were processed
        for msg_id in range(scaffold.messages_sent):
            assert scaffold.tracker.success_counts.get(msg_id) == 1, (
                f"Message {msg_id} should have succeeded 1 time, "
                f"got {scaffold.tracker.success_counts.get(msg_id)}"
            )
