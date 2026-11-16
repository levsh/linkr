# Overview

**linkr** is an async messaging framework for Python that provides a unified API for request/response calls, background tasks, and pub/sub events over pluggable backends.

## What linkr provides

- A single `App` for lifecycle, hooks, middleware, and client operations (`call`/`send`/`emit`).
- `Server` scopes for handler registration and per-server/per-mode consuming (`consume`/`stop_consume`).
- Three delivery modes with consistent handler registration and client APIs:
  - **RPC** (`@server.method`) — request/response
  - **Task** (`@server.task`) — fire-and-forget to one consumer
  - **Event** (`@server.subscribe`) — fan-out to matching subscribers
- Pluggable backends (`LocalBackend` for in-process, `RmqBackend` for RabbitMQ) behind a common interface.
- Extensibility: middleware (app-level and wire-level), hooks, serializers, dependency injection, and validation.

## Core concepts

| Concept | Description |
|---|---|
| `App` | Application: lifecycle, hooks, middleware, serializers, client API, and server factory. |
| `Server` | Server scope bound to a queue/group: handler registration and consume/stop control by mode. |
| Backend | Transport implementation responsible for queues, delivery, and request/response correlation. |
| Handler | A function registered on a server for a mode (RPC/task/event). |
| Topic | Event routing key matched against subscription patterns. |

## Delivery modes at a glance

| Mode | Register | Invoke | Consumers | Response |
|---|---|---|---|---|
| RPC | `@server.method` | `app.client.call` | One (competing) | Yes |
| Task | `@server.task` | `app.client.send` | One (competing) | No |
| Event | `@server.subscribe` | `app.client.emit` | Matching subscribers on consuming servers (fan-out) | No |

See [Delivery modes](delivery-modes.md) for details and [Routing and naming](routing-and-naming.md) for addressing rules.

## Minimal example

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
        print(await app.client.call("orders.add", (2, 3), {}))  # 5


asyncio.run(main())
```
