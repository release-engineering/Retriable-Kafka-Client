"""
Integration tests for Kafka producer and consumer
using larger amount of messages than other tests.
"""

import asyncio
from datetime import timedelta
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client.chunking import generate_group_id
from retriable_kafka_client.headers import (
    CHUNK_GROUP_HEADER,
    CHUNK_ID_HEADER,
    serialize_number_to_bytes,
    NUMBER_OF_CHUNKS_HEADER,
)
from retriable_kafka_client import ConsumeTopicConfig

from .integration_utils import (
    IntegrationTestScaffold,
    ScaffoldConfig,
    RandomDelay,
)


@pytest.mark.asyncio
async def test_lost_chunk_recover(
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

    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(
                base_topic="test-chunks-recover",
            ),
        ],
        group_id="test-chunks",
        timeout=30.0,
        split_messages=False,
        max_chunk_reassembly_wait_time=timedelta(seconds=0.5),
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        group_id = generate_group_id()
        consumer = scaffold.start_consumer(
            delay=RandomDelay(min_delay=0, max_delay=0.05),
            max_workers=2,
            max_concurrency=4,
        )
        await asyncio.sleep(2)

        # This is missing the first message from the group
        await scaffold.send_messages(
            1,
            headers={
                CHUNK_GROUP_HEADER: group_id,
                CHUNK_ID_HEADER: serialize_number_to_bytes(1),
                NUMBER_OF_CHUNKS_HEADER: serialize_number_to_bytes(2),
            },
        )
        # Send the next message only after the first message builder expired
        # and this expiration can be handled during the next message processing
        await asyncio.sleep(5)
        await scaffold.send_messages(1)

        assert await scaffold.wait_for(
            lambda _, success_counts: (
                any(
                    "Removing stale message builder that failed to assemble message in time. "
                    "Lost message topic: test-chunks-recover" in message
                    for message in caplog.messages
                )
                and success_counts.get(1) == 1
            ),
            config.timeout,
        )
        # Check that the cache has been cleared
        assert not consumer._consumer_thread.consumer._BaseConsumer__tracking_manager._TrackingManager__message_builders
