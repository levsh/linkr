# linkr

`linkr` is an async messaging framework for Python with three delivery modes: **RPC**, **task**, and **event**.

It provides a single application model (`App`) for registering handlers, sending messages, and composing middleware, serializers, and hooks, with pluggable backends for transport.

## Key features

- **Three delivery modes**
    - RPC: one consumer handles a call and returns a result.
    - Task: one consumer handles a job, no result (fire-and-forget).
    - Event: every matching subscriber receives the event (fan-out).
- **Backend abstraction**: in-process `LocalBackend` (no external broker) and `RmqBackend` for RabbitMQ.
- **Middleware**: app-level (`Request`/`Response`) and wire-level (raw bytes/headers) with onion-style dispatch.
- **Hooks**: observer events for consume lifecycle, client publish/response, server receive/dispatch, handler execution, and dropped messages.
- **Serialization**: `JsonSerializer` (`linkr1.0`) by default, `JsonRpcSerializer` (`jsonrpc2.0`, RPC-only), server-side auto-detection when multiple serializers are configured.
- **Dependency injection**: `Depends[Type, factory]` with per-call resolution and `get_current_request()`.
- **Typed errors**: structured `RPCError`/`ErrorCode` and customizable exception handlers.

## Quick example

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    backend = LocalBackend()

    async with App("orders", backend=backend) as app:
        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        print(await app.call("orders.add", (2, 3), {}))  # 5


asyncio.run(main())
```

## Where to go next

- [Installation](getting-started/installation.md) and [Quickstart](getting-started/quickstart.md)
- [Concepts](concepts/overview.md) for delivery modes, routing, and message model
- [Guides](guides/rpc.md) for RPC, tasks, events, lifecycle, middleware, hooks, and more
- [Backends](backends/overview.md) and [Reference](reference/api.md) for contracts and API details

!!! note
    `linkr` requires Python 3.11+.
