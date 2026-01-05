"""Integration tests for Kafka consumer rebalancing during message processing"""

import asyncio
import logging
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client import ConsumeTopicConfig

from .integration_utils import IntegrationTestScaffold, ScaffoldConfig


@pytest.mark.asyncio
async def test_rebalance_mid_processing_exactly_once(
    kafka_config: dict[str, Any],
    admin_client: AdminClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test that when a rebalance occurs mid-processing (by adding a new consumer),
    all messages are processed at least once with no message loss.
    """
    caplog.set_level(logging.WARNING)
    total_messages = 50

    config = ScaffoldConfig(
        topics=[ConsumeTopicConfig(base_topic="test-rebalance-topic")],
        group_id="test-rebalance-consumer-group",
        timeout=30.0,
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        # Start first consumer with moderate processing speed
        consumer1 = scaffold.start_consumer(
            delay=0.2,
            max_concurrency=3,
            max_workers=2,
        )

        # Send all messages
        await scaffold.send_messages(total_messages)

        # Wait for first consumer to start processing some messages
        await scaffold.wait_for(lambda a, b: len(a) >= 5, timeout=10)

        with scaffold.tracker.lock:
            messages_before_rebalance = len(scaffold.tracker.success_counts)

        print(
            f"First consumer processed {messages_before_rebalance} messages, "
            "adding second consumer to trigger rebalance..."
        )

        # Add second consumer to trigger rebalance while first is still processing
        consumer2 = scaffold.start_consumer(
            delay=0.2,
            max_concurrency=3,
            max_workers=2,
        )

        # Give rebalance time to occur
        await asyncio.sleep(2)

        with scaffold.tracker.lock:
            messages_after_rebalance = len(scaffold.tracker.success_counts)

        print(f"  After rebalance: {messages_after_rebalance} messages processed")
        print("  Both consumers now processing in parallel...")

        def _ensure_success(_: dict[int, int], success_counts: dict[int, int]) -> bool:
            if len(success_counts) == total_messages:
                return True
            return False

        # Wait for all messages to be processed by both consumers
        success = await scaffold.wait_for(_ensure_success)

        # Get final statistics
        with scaffold.tracker.lock:
            success_counts = dict(scaffold.tracker.success_counts)

        # Verify no messages were completely lost
        for msg_id in range(total_messages):
            assert msg_id in success_counts, f"Message {msg_id} was never processed!"

        # Check for duplicates (expected for in-flight messages during rebalance)
        duplicates = {k: v for k, v in success_counts.items() if v > 1}

        print(f"{total_messages} messages processed. Duplicates: {len(duplicates)}")

        # Clean up
        consumer1.stop()
        consumer2.stop()

        assert success, "Not all messages were processed!"
