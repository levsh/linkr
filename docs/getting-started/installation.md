# Installation

## Requirements

- Python **3.11+** (`>=3.11,<4`)
- A supported async runtime (examples use `asyncio`)

## Install the package

```bash
pip install linkr
```

## Optional dependencies

### RabbitMQ backend

`RmqBackend` requires `aio-pika`:

```bash
pip install "linkr[rabbitmq]"
```

This installs `aio-pika>=9.0`.

## Verify installation

```python
import linkr

print(linkr.__version__)
```

!!! tip
    To run without external services, start with [`LocalBackend`](../backends/local.md). Use [`RmqBackend`](../backends/rabbitmq.md) when you need broker-backed delivery.
