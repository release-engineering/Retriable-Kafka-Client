# Retriable Kafka Client

This is an opinionated wrapper for `confluent_kafka` Python library.
It only uses specific parts of the underlying library while introducing
additional functionalities to these selected parts.

## Features

The aim is to provide a fault-tolerant platform for parallel message
processing.

### Parallel processing

Each consumer requires an executor pool, which will be used for message
processing. Each consumer can consume from multiple topics and processes
the messages from these topics by a single callable. The callable must be
specified by the user of this library.

The library also ensures exactly-once processing when used correctly.
To ensure this, the tasks should take short enough time that all
of them finish before the cluster forces rebalancing. The library tries to
finish tasks from revoked partitions before the rebalance while stopping
additional non-started tasks. The default timeout for each task to finish is
30 seconds, but can be changed. Kafka cluster behavior change may also be 
needed with longer tasks. This behavior only appears during rebalancing and 
graceful stopping of the consumer.

### Fault-tolerance

Each consumer accepts configuration with retry topics. A retry topic is
a Kafka topic used for asynchronous retrying of message processing. If the
specified target callable fails, the consumer will commit the original message
and resends the same message to a retry topic with special headers. The headers
include information about the next timestamp at which the message should be
processed again (to give some time to the error to disappear if the processing
depends on some outside infrastructure).

The retry topic is polled alongside the original topic. If a message contains
the special timestamp header, its Kafka partition of origin will be paused and
the message will be stored locally. The processing will resume only after the
specified timestamp passes. The message will not be processed before the
timestamp, it can only gather delay (depending on the occupation of the
worker pool). Once the message is sent to the pool for re-processing, the
consumption of the blocked partition is resumed.

This whole mechanism **does not ensure message ordering**. When a message is
sent to be retried, another message processing from the same topic is still
unblocked.

## Contributing guidelines

To check contributing guidelines, please check `CONTRIBUTING.md` in the
GitHub repository.
