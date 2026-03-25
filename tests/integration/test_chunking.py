"""
Integration tests for Kafka producer and consumer
using larger amount of messages than other tests.
"""

import asyncio
import re
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client.kafka_settings import KafkaOptions
from retriable_kafka_client import ConsumeTopicConfig

from .integration_utils import (
    IntegrationTestScaffold,
    ScaffoldConfig,
    RandomDelay,
)


@pytest.mark.asyncio
async def test_chunking(
    kafka_config: dict[str, Any],
    admin_client: AdminClient,
    caplog: pytest.LogCaptureFixture,
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
    message_count = 5
    very_large_message = {"sample": "a" * 10000}

    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(
                base_topic="test-chunks-base-topic",
                retry_topic="test-chunks-retry-topic",
                retries=3,
                fallback_delay=0.5,
            ),
        ],
        group_id="test-chunks",
        timeout=30.0,
        additional_settings={KafkaOptions.MAX_MESSAGE_SIZE: 1000},
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer(
            delay=RandomDelay(min_delay=0, max_delay=0.05),
            fail_chance_on_first=0.7,
            max_workers=2,
            max_concurrency=4,
        )
        await asyncio.sleep(2)  # Wait for consumer to be ready

        await scaffold.send_messages(message_count, extra_fields=very_large_message)
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
    assert any(
        re.match(
            r"Received all message chunks, assembling group [a-f0-9]+ composed of [0-9]{2} messages\.",
            message,
        )
        for message in caplog.messages
    )
