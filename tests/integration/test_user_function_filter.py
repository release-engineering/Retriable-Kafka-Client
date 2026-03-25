"""
Integration test to check for messages in headers

"""

import asyncio
from typing import Any

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka import Message

from retriable_kafka_client import ConsumeTopicConfig

from .integration_utils import (
    IntegrationTestScaffold,
    ScaffoldConfig,
)


@pytest.mark.asyncio
async def test_filter_for_message_headers(
    kafka_config: dict[str, Any], admin_client: AdminClient
) -> None:
    """
    Test that filter_function can access messages in headers
    and then filters them
    """
    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(base_topic="test-filter-headers-topic"),
        ],
        group_id="test-filter-headers-group",
    )

    # Helper filter function that accesses headers
    def filter_headers(msg: Message) -> bool:
        headers = msg.headers()
        if headers is None:
            return False
        for header_name, header_value in headers:
            if header_name == "repository_name":
                return b"helm-charts" in header_value
        return False

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer(filter_function=filter_headers)
        await asyncio.sleep(2)

        await scaffold.send_messages(1, headers={"repository_name": b"helm-charts"})
        await scaffold.send_messages(1, headers={"repository_name": b"other-repo"})
        await scaffold.send_messages(
            1, headers={"repository_name": b"my/helm-charts/repo"}
        )
        await scaffold.send_messages(1)

        # Helper function that processes only filtered related messages
        def check_filtered_messages(
            _: dict[int, int], successful_messages: dict[int, int]
        ) -> bool:
            return set(successful_messages.keys()) == {0, 2}

        expected = await scaffold.wait_for(check_filtered_messages, timeout=10)
        assert expected
