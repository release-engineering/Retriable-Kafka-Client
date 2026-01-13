"""Module for Kafka orchestration for multiple topics"""

from concurrent.futures import ProcessPoolExecutor
from threading import Thread
from typing import Iterable

from .config import ConsumerConfig
from .consumer import BaseConsumer


class ConsumerThread(Thread):
    """
    Class for handling each topic consumer in a non-blocking manner.
    """

    def __init__(self, consumer: BaseConsumer):
        self.consumer = consumer
        super().__init__(target=consumer.run)

    def stop(self) -> None:
        """
        Gracefully stop the consumer and wait for its thread to exit.
        Returns: Nothing.
        """
        self.consumer.stop()
        self.join()


def consume_topics(topics: Iterable[ConsumerConfig], max_workers: int):
    """
    Function for parallel consuming of multiple topics using executor pool
    for processing the messages and threads for dispatching the listeners.
    Args:
        topics: Collection of topic configs to consume. Each config
            gets its own thread, which will not block the others.
        max_workers: Maximum number of workers in pool for processing
            messages.
    """
    executor = ProcessPoolExecutor(max_workers=max_workers)
    threads: list[ConsumerThread] = []
    try:
        for config in topics:
            consumer = BaseConsumer(
                config=config, max_concurrency=max_workers, executor=executor
            )
            thread = ConsumerThread(consumer)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
    finally:
        executor.shutdown()
