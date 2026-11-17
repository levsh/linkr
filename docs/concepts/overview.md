# Overview

**linkr** is an async messaging framework for Python that provides a unified API for request/response calls, background tasks, and pub/sub events over pluggable backends.

## What linkr provides

- A single `App` for lifecycle, hooks, middleware, and client operations (`call`/`send`/`emit`).
- Mode-specific servers for handler registration and consuming:
  - **`RPC`** (`app.rpc(...)`, `@rpc.method`) — request/response
  - **`Worker`** (`app.worker(...)`, `@worker.task`) — fire-and-forget to one consumer
  - **`Bus`** (`app.bus(...)`, `@bus.subscribe`) — fan-out to matching subscribers
- Pluggable backends (`LocalBackend` for in-process, `RmqBackend` for RabbitMQ) behind a common interface.
- Extensibility: middleware (app-level and wire-level), hooks, serializers, dependency injection, and validation.

## Core concepts

| Concept | Description |
|---|---|
| `App` | Application: lifecycle, hooks, middleware, serializers, client API, and server factories. |
| `RPC` / `Worker` / `Bus` | Mode servers bound to a queue scope: handler registration and consume/stop control. |
| Backend | Transport implementation responsible for queues, delivery, and request/response correlation. |
| Handler | A function registered on a mode server (RPC method, task, or event subscriber). |
| Topic | Event routing key matched against subscription patterns. |

## Delivery modes at a glance

| Mode | Register | Invoke | Consumers | Response |
|---|---|---|---|---|
| RPC | `@rpc.method` | `app.client.call` | One (competing) | Yes |
| Task | `@worker.task` | `app.client.send` | One (competing) | No |
| Event | `@bus.subscribe` | `app.client.emit` | Matching subscribers on consuming buses (fan-out) | No |

See [Delivery modes](delivery-modes.md) for details and [Routing and naming](routing-and-naming.md) for addressing rules.

## Minimal example

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    async with App("orders", backend=LocalBackend()) as app:
        rpc = app.rpc()

        @rpc.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await rpc.consume()
        print(await app.client.call("orders.add", (2, 3), {}))  # 5


asyncio.run(main())
```
