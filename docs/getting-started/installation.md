# Installation

## Requirements

- Python **3.11+** (`>=3.11,<4`)
- A supported async runtime (examples use `asyncio`)

## Install the package

```bash
pip install linkr
```

With Poetry:

```bash
poetry add linkr
```

## Optional dependencies

### RabbitMQ backend

`RmqBackend` requires `aio-pika`:

```bash
pip install "linkr[rabbitmq]"
```

```bash
poetry add "linkr[rabbitmq]"
```

This installs `aio-pika>=9.0`.

## Verify installation

```python
import linkr

print(linkr.__version__)
```

## Development setup (from source)

```bash
poetry install --with test
```

If you use the docs extras/group defined in this repository:

```bash
poetry install --with docs
```

Run quality checks:

```bash
make lint
make typecheck
make test
```

!!! tip
    For local development and tests without external services, start with [`LocalBackend`](../backends/local.md). Use [`RmqBackend`](../backends/rabbitmq.md) when you need broker-backed delivery.
