# Quickstart

This quickstart shows the three delivery modes using `LocalBackend` (in-process, no broker required).

`App` provides lifecycle/client APIs; `RPC`/`Worker`/`Bus` servers own handler registration and consuming per mode.

## RPC: request and response

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
        result = await app.client.call("orders.add", (2, 3), {})
        print(result)  # 5


asyncio.run(main())
```

- Register on an RPC server with `@rpc.method(...)` (local leaf name).
- Start consuming with `await rpc.consume()`.
- Call with `await app.client.call(name, args_tuple, kwds_dict)` using the queue-qualified path.

## Task: fire-and-forget

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    received: list[int] = []

    async with App("worker", backend=LocalBackend()) as app:
        worker = app.worker("email")

        @worker.task("send")
        async def send_email(user_id: int) -> None:
            received.append(user_id)

        await worker.consume()
        await app.client.send("worker.email.send", (), {"user_id": 42})
        await asyncio.sleep(0)  # let the task handler run

    print(received)  # [42]


asyncio.run(main())
```

- Register with `@worker.task(...)`.
- Send with `await app.client.send(name, args_tuple, kwds_dict)`.
- No response is returned; exactly one consumer handles each task when consumers compete on the same queue.

## Event: fan-out

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    seen: list[int] = []

    async with App("billing", backend=LocalBackend()) as app:
        bus = app.bus()

        @bus.subscribe("user.created")
        def on_user_created(user_id: int) -> None:
            seen.append(user_id)

        await bus.consume()
        await app.client.emit("user.created", {"user_id": 7})
        await asyncio.sleep(0)  # let subscriber run

    print(seen)  # [7]


asyncio.run(main())
```

- Register with `@bus.subscribe(topic_pattern)`.
- Emit with `await app.client.emit(topic, event_dict)` (payload is passed as keyword arguments).
- Each bus has its own event queue; matching subscribers on consuming buses receive the event.

## Cross-app example (same backend)

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    backend = LocalBackend()
    server_app = App("server", backend=backend)
    client_app = App("client", backend=backend)
    rpc = server_app.rpc()

    @rpc.method("ping")
    def ping() -> str:
        return "pong"

    async with server_app, client_app:
        await rpc.consume()
        print(await client_app.client.call("server.ping", (), {}))  # pong


asyncio.run(main())
```

## Next steps

- Learn naming and queue resolution in [Routing and naming](../concepts/routing-and-naming.md).
- See lifecycle control in [Lifecycle](../guides/lifecycle.md).
- Move to a broker with [RabbitMQ backend](../backends/rabbitmq.md).
