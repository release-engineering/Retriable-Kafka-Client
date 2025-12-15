# Retriable Kafka Client

This is an opinionated wrapper for `confluent_kafka` Python library.
It only uses specific parts of the underlying library while introducing
additional functionalities to these selected parts.

## Features

As this library is currently under development, this library should not be
used.

The aim is to provide a fault-tolerant platform for parallel message
processing.

## Local testing

This project uses [`uv`][1]. To set up the project locally, use

```bash
uv pip install .
```

To test, you also need development tools, add them to your local environment
using this command:

```bash
uv pip install --group dev
```

Then you can use [`tox`][2] to run linting and unit tests.

For integration tests you also need [`podman`][3] or [`docker`][4] with
`compose`. Run:

```bash
docker compose up -d 
```

Wait a while and then run:

```bash
tox -e integration
```

Don't forget to clean up the test environment afterward. Use

```bash
podman compose down
```

to do that (or switch `podman` with `docker` depending on your tool of choice).

[1]: https://docs.astral.sh/uv/
[2]: https://tox.wiki/en/4.32.0/
[3]: https://podman.io/
[4]: https://www.docker.com/
