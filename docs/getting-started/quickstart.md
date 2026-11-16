# Quickstart

This quickstart shows the three delivery modes using `LocalBackend` (in-process, no broker required).

`App` provides lifecycle/client APIs; `Server` owns handler registration and consuming.

## RPC: request and response

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

- Register on a server with `@server.method(...)` (local leaf name).
- Start consuming with `await server.consume(...)` (or `mode="rpc"`).
- Call with `await app.client.call(name, args_tuple, kwds_dict)` using the queue-qualified path.

## Task: fire-and-forget

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    received: list[int] = []

    async with App("worker", backend=LocalBackend()) as app:
        server = app.server("email")

        @server.task("send")
        async def send_email(user_id: int) -> None:
            received.append(user_id)

        await server.consume(mode="task")
        await app.client.send("worker.email.send", (), {"user_id": 42})
        await asyncio.sleep(0)  # let the task handler run

    print(received)  # [42]


asyncio.run(main())
```

- Register with `@server.task(...)`.
- Send with `await app.client.send(name, args_tuple, kwds_dict)`.
- No response is returned; exactly one consumer handles each task when consumers compete on the same queue.

## Event: fan-out

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    seen: list[int] = []

    async with App("billing", backend=LocalBackend()) as app:
        server = app.server()

        @server.subscribe("user.created")
        def on_user_created(user_id: int) -> None:
            seen.append(user_id)

        await server.consume(mode="event")
        await app.client.emit("user.created", {"user_id": 7})
        await asyncio.sleep(0)  # let subscriber run

    print(seen)  # [7]


asyncio.run(main())
```

- Register with `@server.subscribe(topic_pattern)`.
- Emit with `await app.client.emit(topic, event_dict)` (payload is passed as keyword arguments).
- Each server has its own event queue; matching subscribers on consuming servers receive the event.

## Cross-app example (same backend)

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    backend = LocalBackend()
    server_app = App("server", backend=backend)
    client_app = App("client", backend=backend)
    srv = server_app.server()

    @srv.method("ping")
    def ping() -> str:
        return "pong"

    async with server_app, client_app:
        await srv.consume(mode="rpc")
        print(await client_app.client.call("server.ping", (), {}))  # pong


asyncio.run(main())
```

## Next steps

- Learn naming and queue resolution in [Routing and naming](../concepts/routing-and-naming.md).
- See lifecycle control in [Lifecycle](../guides/lifecycle.md).
- Move to a broker with [RabbitMQ backend](../backends/rabbitmq.md).
