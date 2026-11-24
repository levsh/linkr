# linkr

Async messaging framework with three delivery modes: RPC, task and event.

## Install

```
pip install linkr
```

## Core model: `App` and mode servers

- **`App`** — application infrastructure and client: lifecycle (`init/close`), hooks, middleware, exception handlers, serializers, and client operations (`call`, `send`, `emit`).
- **Mode servers** — servers bound to an `App` and a queue scope. Each mode has its own server type and API:
  - `rpc = app.rpc(path=None, *, options=None)` → `RPC`: `@rpc.method(...)`, `rpc.consume()`, `rpc.stop_consume()`
  - `worker = app.worker(path=None, *, options=None)` → `Worker`: `@worker.task(...)`, `worker.consume()`, `worker.stop_consume()`
  - `bus = app.bus(path=None, *, options=None)` → `Bus`: `@bus.subscribe(...)`, `bus.consume()`, `bus.stop_consume()`

`path=None` (root) uses base queue `app.name`; `path="group"` uses base queue `app.name.group`. `options` configures backend-specific queue options for that server (`QueueOptions`, a sequence, or `None`). Repeated calls for the same mode/path return the same server when options match.

## Delivery modes

| Mode | Registration | Client (`App`) | Behaviour |
|---|---|---|---|
| RPC | `@rpc.method("foo")` | `await app.client.call("app.foo", (), {})` | One consumer handles the call and returns a result. |
| Task | `@worker.task("send")` | `await app.client.send("app.email.send", (), {"user_id": 1})` | One of the consumers handles the task. No result. |
| Event | `@bus.subscribe("user.created")` | `await app.client.emit("user.created", {"user_id": 1})` | Matching subscribers receive the event. No result. |

Client methods take positional arguments as a tuple and keyword arguments as a dict:

```python
await app.client.call("app.add", (2, 3), {})
await app.client.call("app.add", (), {"x": 2, "y": 3})
await app.client.send("app.email.send", (), {"user_id": 1})
await app.client.emit("user.created", {"user_id": 1})
```

## Health checks

`App` and backends expose `healthcheck(fast: bool | None = None) -> HealthStatus(ready, health, details)`:

- `ready=False, health=None` — app/backend is not ready or health check failed at transport level.
- `ready=True, health=...` — backend is ready and `health` reflects dependency/consumer health (backend-specific).
- `details` is a read-only mapping with stable, public diagnostics:
  - `connection`: `"ok" | "error"`
  - `queues`: `"ok" | [{"mode", "queue", "reason"}, ...]`
  - queue issue `reason`: `"no_consumer" | "unavailable"`
- `fast=True` checks connection/transport only. `fast=False` or `fast=None` performs full health checks, including queue/consumer checks where supported. If a check is not requested or a fail-fast connection issue is reported, the corresponding key is omitted from `details`.

`App.healthcheck()` returns `ready=False, health=None` without `details` when the app is closed. If the backend is not ready, it returns `ready=False, health=None` and preserves backend `details` when present. Backend exceptions are not swallowed. No backend-internal transport/channel/probe/exception details are exposed.

## Handlers, local names, and addressing

Handler names in decorators are **local to the server** (leaf names, without `.`). The server scope defines the queue:

- `rpc = app.rpc()` → base queue `app`
  - `@rpc.method("method")` registers RPC leaf `method` on queue `app` (fully qualified: `app.method`)
- `rpc = app.rpc("group")` → base queue `app.group`
  - `@rpc.method("method")` registers RPC leaf `method` on queue `app.group` (fully qualified: `app.group.method`)

Calls use the queue-qualified path:

```python
await app.client.call("app.method", (), {})        # root RPC server
await app.client.call("app.group.method", (), {})  # group RPC server
```

The last segment is the handler leaf; everything before it resolves to the target queue (same queue-qualified convention for `send`).

## Example

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    app = App("app", backend=LocalBackend())

    root_rpc = app.rpc()
    root_worker = app.worker()

    @root_rpc.method("method")
    def method() -> str:
        return "root"

    @root_worker.task("task")
    async def task() -> None:
        ...

    group_rpc = app.rpc("group")
    group_worker = app.worker("group")

    @group_rpc.method("method")  # local name "method", not "group.method"
    def group_method() -> str:
        return "group"

    @group_worker.task("task")
    async def group_task() -> None:
        ...

    await app.init()

    await root_rpc.consume()
    await root_worker.consume()
    await group_rpc.consume()
    await group_worker.consume()

    print(await app.client.call("app.method", (), {}))        # root server
    print(await app.client.call("app.group.method", (), {}))  # group server

    await root_rpc.stop_consume()
    await root_worker.stop_consume()
    await group_rpc.stop_consume()
    await group_worker.stop_consume()

    await app.close()


asyncio.run(main())
```

## RPC

Each `RPC` server consumes its queue and dispatches by leaf method name within that queue. Multiple RPC methods registered on the same server share that server's queue.

```python
async with App("orders", backend=LocalBackend()) as app:
    rpc = app.rpc()

    @rpc.method("foo")
    def foo() -> str:
        return "foo"

    @rpc.method("bar")
    def bar() -> str:
        return "bar"

    await rpc.consume()
    print(await app.client.call("orders.foo", (), {}))
    print(await app.client.call("orders.bar", (), {}))
```

Use separate servers for separate queues/groups:

```python
app = App("orders", backend=LocalBackend())
sa = app.rpc("serviceA")
sb = app.rpc("serviceB")

@sa.method("foo")
def foo() -> str:
    return "foo"

@sb.method("bar")
def bar() -> str:
    return "bar"

await app.init()
await sa.consume()
await sb.consume()
await app.client.call("orders.serviceA.foo", (), {})
await app.client.call("orders.serviceB.bar", (), {})
```

## Tasks

A task has no response. Multiple consumers may listen on the same task queue; each task is handled by exactly one of them.

```python
async with App("worker", backend=LocalBackend()) as app:
    worker = app.worker("email")

    @worker.task("send")
    async def send_email(user_id: int) -> None:
        ...

    await worker.consume()
    await app.client.send("worker.email.send", (), {"user_id": 1})
```

## Events

Events are published by topic (`app.client.emit`) and delivered to subscribers whose patterns match. **Each `Bus` server has its own event queue** and consumes only the subscriptions registered on that server.

```python
async with App("billing", backend=LocalBackend()) as app:
    bus = app.bus()

    @bus.subscribe("user.created")
    def on_user_created(user_id: int) -> None:
        ...

    await bus.consume()
    await app.client.emit("user.created", {"user_id": 1})
```

Wildcards are supported:

- `*` matches exactly one segment: `user.*` matches `user.created`.
- `#` matches zero or more segments: `user.#` matches `user.created` and `user.profile.updated`.

Multiple subscribers in the same bus for the same topic are all called. Different bus servers (each with its own event queue) that subscribe to a matching topic will each receive the event.

## Lifecycle and consuming

```python
rpc = app.rpc()
worker = app.worker("email")
bus = app.bus()

await app.init()
await rpc.consume()
await worker.consume()
await bus.consume()

await rpc.stop_consume()     # pause/resume per server
await worker.stop_consume()
await bus.stop_consume()

await app.close()
```

- `App.init()` initializes the backend and middleware. `App.close()` stops all servers, closes middleware and backend.
- `server.consume()` starts consuming for that server (idempotent when already consuming).
- `server.stop_consume()` stops consuming for that server (pause). Handler registrations and subscriptions remain, so consuming can be started again.
- There are no global `app.consume()` / `app.stop_consume()` methods and no mode argument; manage consuming per mode server.

## Queue options

Backend-specific queue options are configured per server when creating it:

```python
from linkr.backends.rmq import RmqQueueOptions  # backend-specific

rpc = app.rpc("serviceA", options=[RmqQueueOptions(prefetch_count=5, durable=True)])
email = app.worker("email", options=[RmqQueueOptions(prefetch_count=50)])
bus = app.bus(options=[RmqQueueOptions(prefetch_count=10)])

@rpc.method("ping")
def ping() -> str:
    return "pong"

await app.init()
await rpc.consume()
```

Options are opaque to `App`; the active backend selects the types it understands. `LocalBackend` ignores transport options. `RmqQueueOptions` also supports `ack_mode`: `after_reply` (default) ack after successful processing and, for RPC, after successful reply publish; for task/event it is equivalent to `after_handler`. `after_handler` acks after successful handler execution. `before_handler` acks before handler execution.

## Hooks and middleware

Hooks and middleware are configured on `App` and apply across its servers:

```python
from linkr import App, AppMiddleware, LocalBackend
from linkr.models import Request, Response

class LoggingMiddleware(AppMiddleware):
    async def dispatch_server(self, call_next, request: Request) -> Response | None:
        return await call_next()

app = App("orders", backend=LocalBackend(), middlewares=[LoggingMiddleware()])
```

See `docs/` for details on hooks, middleware, dependency injection, validation, serializers, contracts, and backends.

## Backends

- `LocalBackend` — in-process backend for development/tests (no broker).
- `RmqBackend` — RabbitMQ backend (install extra: `pip install "linkr[rabbitmq]"`).

## License

MIT
