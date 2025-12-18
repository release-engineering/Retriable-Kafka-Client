"""Integration testing framework"""

import asyncio
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable

from confluent_kafka.admin import AdminClient, NewTopic

from retriable_kafka_client import (
    BaseConsumer,
    BaseProducer,
    ConsumerConfig,
    ConsumerThread,
    ConsumeTopicConfig,
    ProducerConfig,
)
from retriable_kafka_client.kafka_settings import KafkaOptions


@dataclass
class RandomDelay:
    """
    Class for creating a random delay from uniform range specified
    by the upper and lower end of the range.
    """

    min_delay: float
    max_delay: float

    def get_delay(self) -> float:
        """Generate  the delay"""
        return random.uniform(self.min_delay, self.max_delay)


class MessageGenerator:
    """
    This class helps to generate messages with predictable IDs for tracking.
    """

    def __init__(self) -> None:
        self._call_count = 0

    def generate(self, count: int) -> list[dict[str, Any]]:
        """
        Generate a chunk of messages.
        Args:
            count: Number of messages to generate in this chunk
        """
        result = []
        for _ in range(count):
            result.append({"id": self._call_count, "message": "This is a test message"})
            self._call_count += 1
        return result


class MessageTracker:
    """
    Class for tracking results of message processing
    Attributes:
        call_counts:
            Tracks the number of calls on each message,
            the keys are IDs in the messages, the values
            are counts of processing
        success_counts:
            Tracks the number of successful processing calls,
            format is the same as call_counts
        lock:
            Threading lock that should be used when manipulating
            the other attributes
    """

    def __init__(self, message_count: int, topics: int, lock: threading.Lock) -> None:
        """
        Initialize a message tracker.
        Args:
            message_count:
                Number of messages that is expected to be sent and received
                in each topic
            topics:
                Number of topics used for producing and consuming (doesn't
                include retry topics)
            lock:
                threading lock for avoiding race conditions when populating
                or reading results
        """
        self.call_counts: dict[int, int] = {}
        self.success_counts: dict[int, int] = {}
        self._message_count = message_count
        self._topics = topics
        self.lock = lock

    async def wait_for(
        self, func: Callable[[dict[int, int], dict[int, int]], bool], max_timeout: float
    ) -> bool:
        """
        Wait until a certain condition is met or until this times out.
        Args:
            func:
                A callable that determines if the conditions are met.
                This callable must return True if the condition is met,
                False otherwise. To determine the result, this function
                accepts two parameters. The first parameter is the
                call_counts attribute, the second one is the success_counts.
            max_timeout:
                Time in seconds to wait for the conditions to be met.
                After this time frame is over, the function returns False.
        Returns:
            True when the condition is met, False if the timeout is reached
            before the conditions are true.
        """
        time_ref = time.perf_counter()
        time_delta = 0
        success = False
        while time_delta < max_timeout:
            with self.lock:
                if func(self.call_counts, self.success_counts):
                    success = True
                    break
            await asyncio.sleep(0.5)
            time_delta = time.perf_counter() - time_ref
        if not success:
            print("Timed out after %s seconds." % time_delta)
        return success


class MockTarget:
    """
    Mock target for consumers to track result of tests
    """

    def __init__(
        self,
        delay: float | RandomDelay,
        fail_chance_on_first: float,
        tracker: MessageTracker,
        fail_consistently: bool = False,
    ) -> None:
        """
        Initialize a mock target.
        Args:
            delay:
                The simulated delay before this message is processed (or failed)
            fail_chance_on_first:
                Number between 0 and 1 that indicates the probability of failing
                on first processing of the message
            tracker:
                The tracker object to keep track of results of processing
            fail_consistently:
                If True, every attempt to call this object will raise a ValueError
        """
        self.delay = delay
        self.fail_chance_on_first = fail_chance_on_first
        self.tracker = tracker
        self.fail_consistently = fail_consistently

    def __call__(self, message: dict[str, Any]) -> None:
        """
        Simulate the execution of this target
        Args:
            message:
                The message to process. Must include
                the key "id" with an integer value
        Raises:
            ValueError: if simulated failure occurs
        """
        if self.delay:
            if isinstance(self.delay, RandomDelay):
                time.sleep(self.delay.get_delay())
            elif isinstance(self.delay, float):
                time.sleep(self.delay)
        message_id = message["id"]
        with self.tracker.lock:
            call_count = self.tracker.call_counts.get(message_id, 0)
            self.tracker.call_counts[message_id] = call_count + 1
        if self.fail_consistently or (
            call_count == 0 and random.random() < self.fail_chance_on_first
        ):
            raise ValueError("Simulated error")
        with self.tracker.lock:
            success_count = self.tracker.success_counts.get(message_id, 0)
            self.tracker.success_counts[message_id] = success_count + 1


class ConsumerHandle:
    """
    Handle for a consumer that bundles the thread and executor together.
    Provides a clean way to stop a consumer mid-test.
    """

    def __init__(
        self, consumer_thread: ConsumerThread, executor: ThreadPoolExecutor
    ) -> None:
        self._consumer_thread = consumer_thread
        self._executor = executor
        self._stopped = False

    def stop(self, timeout: float = 5) -> None:
        """
        Stop the consumer and shut down its executor.
        This ensures the consumer properly leaves the consumer group.
        """
        if self._stopped:
            return

        # Stop the consumer thread first
        self._consumer_thread.stop()
        self._consumer_thread.join(timeout=timeout)

        # Then shutdown the executor to complete any in-flight tasks
        self._executor.shutdown(wait=True)
        self._stopped = True

    @property
    def is_stopped(self) -> bool:
        return self._stopped


@dataclass
class ScaffoldConfig:
    """Configuration for the test harness"""

    topics: list[ConsumeTopicConfig]
    group_id: str
    timeout: float = 15.0


class IntegrationTestScaffold:
    """
    A unified test harness that bundles topic creation, producer, consumer,
    tracker, and generator together for cleaner integration tests.

    Usage:
        async with IntegrationTestHarness(kafka_config, admin_client, config) as harness:
            harness.get_consumer_thread()  # Start a consumer
            await harness.send_messages(10)
            success = await harness.wait_for_success()
            assert success
    """

    def __init__(
        self,
        kafka_config: dict[str, Any],
        admin_client: AdminClient,
        config: ScaffoldConfig,
    ) -> None:
        self.kafka_config = kafka_config
        self.admin_client = admin_client
        self.config = config

        # Internal state
        self._lock = threading.Lock()
        self._producer: BaseProducer | None = None

        # Track created consumer resources for cleanup
        self._consumer_handles: list[ConsumerHandle] = []

        # Track how many messages have been sent
        self.messages_sent: int = 0

        # Public components
        self.tracker = MessageTracker(
            message_count=0,  # Will be updated when send_messages is called
            topics=len(config.topics),
            lock=self._lock,
        )
        self.generator = MessageGenerator()

    def _create_topics(self) -> None:
        """Create all topics needed for this test"""
        all_topic_names: list[str] = []
        for tc in self.config.topics:
            all_topic_names.append(tc.base_topic)
            if tc.retry_topic:
                all_topic_names.append(tc.retry_topic)

        new_topics = [
            NewTopic(topic, num_partitions=1, replication_factor=1)
            for topic in all_topic_names
        ]

        fs = self.admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
            except Exception:
                # Topic might already exist
                pass

    def _create_producer(self) -> BaseProducer:
        """Create the producer"""
        # Producer sends to all base topics
        producer_topics = [tc.base_topic for tc in self.config.topics]

        producer_config = ProducerConfig(
            kafka_hosts=[self.kafka_config[KafkaOptions.KAFKA_NODES]],
            topics=producer_topics,
            username=self.kafka_config[KafkaOptions.USERNAME],
            password=self.kafka_config[KafkaOptions.PASSWORD],
            fallback_base=0.1,
            additional_settings={KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT"},
        )
        return BaseProducer(producer_config)

    def start_consumer(
        self,
        delay: float | RandomDelay = 0,
        fail_chance_on_first: float = 0,
        fail_consistently: bool = False,
        max_concurrency: int = 4,
        max_workers: int = 2,
    ) -> ConsumerHandle:
        """
        Create and start a consumer with the specified configuration.

        Args:
            delay: Processing delay (fixed or random range)
            fail_chance_on_first: Probability (0-1) of failing first attempt
            fail_consistently: If True, always fail processing
            max_concurrency: Consumer concurrency limit
            max_workers: Thread pool size

        Returns:
            A ConsumerHandle that can be used to stop the consumer.
            Call handle.stop() to cleanly stop the consumer and its executor.
        """
        # Create target with shared tracker
        target = MockTarget(
            delay=delay,
            fail_chance_on_first=fail_chance_on_first,
            tracker=self.tracker,
            fail_consistently=fail_consistently,
        )

        # Create executor
        executor = ThreadPoolExecutor(max_workers=max_workers)

        # Create consumer
        consumer_config = ConsumerConfig(
            kafka_hosts=[self.kafka_config[KafkaOptions.KAFKA_NODES]],
            topics=self.config.topics,
            username=self.kafka_config[KafkaOptions.USERNAME],
            password=self.kafka_config[KafkaOptions.PASSWORD],
            group_id=self.config.group_id,
            target=target,
            additional_settings={KafkaOptions.SECURITY_PROTO: "SASL_PLAINTEXT"},
        )

        consumer = BaseConsumer(
            consumer_config,
            executor,
            max_concurrency=max_concurrency,
        )

        # Create and start consumer thread
        consumer_thread = ConsumerThread(consumer)
        consumer_thread.start()

        # Create handle and track it
        handle = ConsumerHandle(consumer_thread, executor)
        self._consumer_handles.append(handle)

        return handle

    async def __aenter__(self) -> "IntegrationTestScaffold":
        """Set up the test harness (topics and producer only)"""
        # Create topics
        self._create_topics()

        # Create producer
        self._producer = self._create_producer()

        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Clean up all resources"""
        # Stop all consumer handles (this stops threads and shuts down executors)
        for handle in self._consumer_handles:
            try:
                handle.stop()
            except Exception:
                pass

    async def send_messages(self, count: int) -> list[dict[str, Any]]:
        """
        Generate and send messages.

        Args:
            count: Number of messages to send.

        Returns:
            The list of messages that were sent.
        """
        if self._producer is None:
            raise RuntimeError("Harness not started. Use 'async with' context manager.")

        messages = self.generator.generate(count)

        for msg in messages:
            await self._producer.send(msg)

        # Update tracking
        self.messages_sent += count
        self.tracker._message_count = self.messages_sent

        self._producer.close()
        return messages

    async def wait_for_success(self, timeout: float | None = None) -> bool:
        """
        Wait for all messages to be successfully processed.
        Args:
            timeout: Max time to wait. Defaults to config.timeout.
        Returns:
            True if all messages processed successfully, False on timeout.
        """

        def _ensure_success(_: dict[int, int], success_counts: dict[int, int]) -> bool:
            if len(success_counts) == self.messages_sent:
                number_of_topics = len(self.config.topics)
                return all(
                    call_count == number_of_topics
                    for call_count in success_counts.values()
                )
            return False

        max_timeout = timeout if timeout is not None else self.config.timeout
        return await self.wait_for(_ensure_success, max_timeout)

    async def wait_for(
        self,
        condition: Callable[[dict[int, int], dict[int, int]], bool],
        timeout: float | None = None,
    ) -> bool:
        """
        Wait for a custom condition.
        Args:
            condition: Function that takes (call_counts, success_counts) and returns bool.
            timeout: Max time to wait. Defaults to config.timeout.
        Returns:
            True if condition met, False on timeout.
        """
        max_timeout = timeout if timeout is not None else self.config.timeout
        return await self.tracker.wait_for(condition, max_timeout)
