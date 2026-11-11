# Quickstart

This quickstart shows the three delivery modes using `LocalBackend` (in-process, no broker required).

## RPC: request and response

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
        result = await app.call("orders.add", (2, 3), {})
        print(result)  # 5


asyncio.run(main())
```

- Register with `@app.method(...)`.
- Start consumers with `await app.consume()` (or `consume_rpc()`).
- Call with `await app.call(name, args_tuple, kwds_dict)`.

## Task: fire-and-forget

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    backend = LocalBackend()
    received: list[int] = []

    async with App("worker", backend=backend) as app:
        @app.task("email.send")
        async def send_email(user_id: int) -> None:
            received.append(user_id)

        await app.consume()
        await app.send("worker.email.send", (), {"user_id": 42})
        await asyncio.sleep(0)  # let the task handler run

    print(received)  # [42]


asyncio.run(main())
```

- Register with `@app.task(...)`.
- Send with `await app.send(name, args_tuple, kwds_dict)`.
- No response is returned; exactly one consumer handles each task when consumers compete.

## Event: fan-out

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    backend = LocalBackend()
    seen: list[int] = []

    async with App("billing", backend=backend) as app:
        @app.subscribe("user.created")
        def on_user_created(user_id: int) -> None:
            seen.append(user_id)

        await app.consume()
        await app.emit("user.created", {"user_id": 7})
        await asyncio.sleep(0)  # let subscriber run

    print(seen)  # [7]


asyncio.run(main())
```

- Register with `@app.subscribe(topic_pattern)`.
- Emit with `await app.emit(topic, event_dict)` (payload is passed as keyword arguments).
- Each subscribed app instance receives matching events.

## Cross-app example (same backend)

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    backend = LocalBackend()
    server = App("server", backend=backend)
    client = App("client", backend=backend)

    @server.method("ping")
    def ping() -> str:
        return "pong"

    async with server, client:
        await server.consume()
        print(await client.call("server.ping", (), {}))  # pong


asyncio.run(main())
```

## Next steps

- Learn naming and queue resolution in [Routing and naming](../concepts/routing-and-naming.md).
- See lifecycle control in [Lifecycle](../guides/lifecycle.md).
- Move to a broker with [RabbitMQ backend](../backends/rabbitmq.md).
