# linkr

Async messaging framework with three delivery modes: **RPC**, **Task**, and **Event**.

`linkr` provides a unified `App` (lifecycle, hooks, middleware, client API) and per-scope `Server` instances (handler registration and consuming) over pluggable backends (in-process and RabbitMQ).

## Features

- **Delivery modes:** request/response RPC, fire-and-forget tasks, and fan-out pub/sub events
- **Server scopes:** per-server queues and independent consume/stop control by mode
- **Backends:** `LocalBackend` (in-process) and `RmqBackend` (RabbitMQ via `aio-pika`)
- **Extensibility:** app/wire middleware, lifecycle and request hooks, serializers (Linkr JSON and JSON-RPC 2.0), dependency injection, argument validation
- **Contracts:** runtime introspection and export (`describe`, JSON/HTML)

## Quick example

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    async with App("orders", backend=LocalBackend()) as app:
        server = app.server()

        @server.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await server.consume(mode="rpc")
        result = await app.client.call("orders.add", (2, 3), {})
        print(result)  # 5


asyncio.run(main())
```

## Where to go next

- [Installation](getting-started/installation.md)
- [Quickstart](getting-started/quickstart.md)
- [Delivery modes](concepts/delivery-modes.md)
- [Routing and naming](concepts/routing-and-naming.md)
- [RabbitMQ backend](backends/rabbitmq.md)
- [API reference](reference/api.md)
