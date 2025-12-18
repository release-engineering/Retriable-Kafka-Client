# Integration Testing

This integration test framework uses `integration_utils.py` for simplification
of test code.

## IntegrationTestScaffold

The `IntegrationTestScaffold` is the primary tool for writing integration tests.
It bundles topic creation, producer, consumer, and message tracking together.

### Basic Usage

```python
from retriable_kafka_client import ConsumeTopicConfig
from tests.integration.integration_utils import IntegrationTestScaffold, ScaffoldConfig


@pytest.mark.asyncio
async def test_example(kafka_config, admin_client):
    config = ScaffoldConfig(
        topics=[ConsumeTopicConfig(base_topic="my-topic")],
        group_id="my-consumer-group",
    )

    async with IntegrationTestScaffold(kafka_config, admin_client, config) as scaffold:
        scaffold.start_consumer()

        await scaffold.send_messages(10)
        success = await scaffold.wait_for_success()
        assert success
```

## Lower-Level Components

For edge cases, you can use components directly:

- `MessageGenerator`: Creates messages with predictable IDs
- `MessageTracker`: Tracks processing results
- `MockTarget`: Simulates message processing with configurable behavior
- `RandomDelay`: Generates random delays within a range
- `ConsumerHandle`: Bundles consumer thread and executor for clean shutdown
