"""
Integration tests for Kafka retry mechanism
"""

import asyncio
import datetime
import time
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client import BaseProducer, ConsumeTopicConfig, ProducerConfig
from retriable_kafka_client.kafka_settings import KafkaOptions
from retriable_kafka_client.retry_utils import TIMESTAMP_HEADER, ATTEMPT_HEADER

from .integration_utils import (
    IntegrationTestScaffold,
    ScaffoldConfig,
)


@pytest.mark.asyncio
async def test_retry_mechanism_on_failure(
    kafka_config: dict[str, Any], admin_client: AdminClient
) -> None:
    """
    Test that when a message processing fails, it gets retried via the retry topic
    and eventually succeeds.
    """
    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(
                base_topic="test-base-topic",
                retry_topic="test-retry-topic",
                retries=3,
                fallback_delay=1.0,
            ),
        ],
        group_id="test-retry-consumer-group",
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer(fail_chance_on_first=1.0)
        await asyncio.sleep(2)  # Wait for consumer to be ready

        await scaffold.send_messages(1)
        success = await scaffold.wait_for_success()

        assert success, (
            f"Expected 1 successful message after retry. "
            f"Call counts: {scaffold.tracker.call_counts}, "
            f"Success counts: {scaffold.tracker.success_counts}"
        )

        # Verify the message was attempted at least twice (1 failure + 1 success)
        assert scaffold.tracker.call_counts.get(0, 0) >= 2, (
            f"Expected at least 2 attempts (1 failure + 1 success), "
            f"got {scaffold.tracker.call_counts.get(0, 0)}"
        )


@pytest.mark.asyncio
async def test_scheduled_message_with_future_timestamp(
    kafka_config: dict[str, Any],
    admin_client: AdminClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test that a message with a retry timestamp header set in the future
    is scheduled and only processed after the timestamp passes.
    """
    caplog.set_level("DEBUG")

    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(base_topic="test-scheduled-topic"),
        ],
        group_id="test-scheduled-consumer-group",
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer()
        await asyncio.sleep(2)  # Wait for consumer to be ready

        # Calculate a timestamp 3 seconds into the future
        schedule_delay = 3
        future_timestamp = int(
            datetime.datetime.now(tz=datetime.timezone.utc).timestamp() + schedule_delay
        )

        headers = {
            TIMESTAMP_HEADER: future_timestamp.to_bytes(length=8, byteorder="big"),
            ATTEMPT_HEADER: (1).to_bytes(length=8, byteorder="big"),
        }

        # Send message with custom headers (can't use scaffold.send_messages for this)
        producer_config = ProducerConfig(
            kafka_hosts=[kafka_config[KafkaOptions.KAFKA_NODES]],
            topics=["test-scheduled-topic"],
            username=kafka_config[KafkaOptions.USERNAME],
            password=kafka_config[KafkaOptions.PASSWORD],
            retries=3,
            fallback_base=0.1,
            fallback_factor=2.0,
            additional_settings={KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT"},
        )
        producer = BaseProducer(producer_config)

        test_message = {"id": 0, "data": "scheduled message test"}
        send_time = time.time()
        await producer.send(test_message, headers=headers)
        producer.close()

        # Manually update tracker since we didn't use scaffold.send_messages
        scaffold.messages_sent = 1
        scaffold.tracker._message_count = 1

        # Wait a short time - message should NOT be processed yet
        await asyncio.sleep(1)
        with scaffold.tracker.lock:
            early_count = len(scaffold.tracker.success_counts)
        assert early_count == 0, (
            f"Message should not be processed yet "
            f"(only {time.time() - send_time:.1f}s elapsed), "
            f"but got {early_count} messages"
        )

        success = await scaffold.wait_for_success()

        # Verify the "scheduled for processing" log message appeared
        schedule_logs = [
            record
            for record in caplog.records
            if "scheduled for processing at timestamp" in record.message
        ]
        assert len(schedule_logs) >= 1, (
            f"Expected log message about scheduling, but found none. "
            f"Log messages: {[r.message for r in caplog.records]}"
        )

        assert success, (
            f"Expected message to be processed. "
            f"Success counts: {scaffold.tracker.success_counts}"
        )

        # Verify the message was processed after the scheduled time
        process_time = time.time()
        assert process_time >= send_time + schedule_delay - 1, (
            f"Message should have been processed after {schedule_delay}s delay, "
            f"but was processed after {process_time - send_time:.1f}s"
        )


@pytest.mark.asyncio
async def test_exhausted_retries_stops_processing(
    kafka_config: dict[str, Any],
    admin_client: AdminClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """
    Test that when a message exhausts all retry attempts, it is not retried anymore.

    This test sends a message that always fails processing. After the configured
    number of retries is exhausted, the message should be dropped and a warning
    should be logged.
    """
    caplog.set_level("DEBUG")

    max_retries = 2  # Total attempts: 1 initial + 2 retries = 3
    expected_attempts = max_retries + 1

    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(
                base_topic="test-exhausted-retry-base-topic",
                retry_topic="test-exhausted-retry-topic",
                retries=max_retries,
                fallback_delay=0.5,
            ),
        ],
        group_id="test-exhausted-retry-consumer-group",
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer(fail_consistently=True)
        await asyncio.sleep(2)  # Wait for consumer to be ready

        await scaffold.send_messages(1)

        # Wait for all retry attempts to be exhausted on message with ID 0
        def check_attempts(call_counts: dict[int, int], _: dict[int, int]) -> bool:
            return call_counts.get(0, 0) >= expected_attempts

        await scaffold.wait_for(check_attempts)

        # Wait a bit more to ensure no additional retries happen
        await asyncio.sleep(2)

        # Verify the message was attempted exactly expected_attempts times
        with scaffold.tracker.lock:
            final_attempts = scaffold.tracker.call_counts.get(0, 0)

        assert final_attempts == expected_attempts, (
            f"Expected exactly {expected_attempts} attempts "
            f"(1 initial + {max_retries} retries), but got {final_attempts}"
        )

        # Verify the "exhausted all retry attempts" log message appeared
        exhausted_logs = [
            record
            for record in caplog.records
            if "exhausted all" in record.message.lower()
            and "retry attempts" in record.message.lower()
        ]
        assert len(exhausted_logs) >= 1, (
            f"Expected log message about exhausted retries, but found none. "
            f"Log messages with 'retry': "
            f"{[r.message for r in caplog.records if 'retry' in r.message.lower()]}"
        )
