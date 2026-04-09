"""
Integration tests to check sender

"""

from typing import Any

import pytest
from confluent_kafka.admin import AdminClient

from retriable_kafka_client import ConsumeTopicConfig, SendError

from .integration_utils import (
    IntegrationTestScaffold,
    ScaffoldConfig,
)


@pytest.mark.asyncio
async def test_send_error(
    kafka_config: dict[str, Any], admin_client: AdminClient
) -> None:
    """
    Test that send_error is raised if the topic cannot handle
    messages
    """
    config = ScaffoldConfig(
        topics=[
            ConsumeTopicConfig(base_topic="test-send-error-topic"),
        ],
        group_id="test-send-error-group",
        topic_config={"max.message.bytes": "1000"},
    )
    # We set topic-level constraint, producer is not aware of it. Without callbacks,
    # this wouldn't raise any exceptions, it would just silently ignore it
    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        with pytest.raises(SendError) as err:
            await scaffold.send_messages(1, extra_fields={"large": 10000 * "a"})
        assert err.value.retriable is False  # This problem cannot be easily retried
        assert err.value.kafka_error.name() == "MSG_SIZE_TOO_LARGE"
