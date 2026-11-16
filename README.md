# linkr

Async messaging framework with three delivery modes: RPC, task and event.

## Install

```
pip install linkr
```

## Core model: `App` and `Server`

- **`App`** — application infrastructure and client: lifecycle (`init/close`), hooks, middleware, exception handlers, serializers, and client operations (`call`, `send`, `emit`).
- **`Server`** — a server scope bound to an `App` and a queue/group. It owns handler registration and consuming: `@server.method(...)`, `@server.task(...)`, `@server.subscribe(...)`, and `server.consume(...)` / `server.stop_consume(...)`.

Create servers from an app:

```python
app = App("app", backend=...)

root = app.server()                 # root scope, base queue "app"
group = app.server("group")         # named scope, base queue "app.group"
```

`app.server(path, rpc_options=..., task_options=..., event_options=...)` configures backend-specific queue options for that server (per mode). Repeated calls for the same `path` return the same server when options match.

## Delivery modes

| Mode | Registration (on `Server`) | Client (`App`) | Behaviour |
|---|---|---|---|
| RPC | `@server.method("foo")` | `await app.client.call("app.foo", (), {})` | One consumer handles the call and returns a result. |
| Task | `@server.task("send")` | `await app.client.send("app.email.send", (), {"user_id": 1})` | One of the consumers handles the task. No result. |
| Event | `@server.subscribe("user.created")` | `await app.client.emit("user.created", {"user_id": 1})` | Matching subscribers receive the event. No result. |

Client methods take positional arguments as a tuple and keyword arguments as a dict:

```python
await app.client.call("app.add", (2, 3), {})
await app.client.call("app.add", (), {"x": 2, "y": 3})
await app.client.send("app.email.send", (), {"user_id": 1})
await app.client.emit("user.created", {"user_id": 1})
```

## Handlers, local names, and addressing

Handler names in decorators are **local to the server** (leaf names, without `.`). The server scope defines the queue:

- `root = app.server()` → base queue `app`
  - `@root.method("method")` registers RPC leaf `method` on queue `app` (fully qualified: `app.method`)
- `group = app.server("group")` → base queue `app.group`
  - `@group.method("method")` registers RPC leaf `method` on queue `app.group` (fully qualified: `app.group.method`)

Calls use the queue-qualified path:

```python
await app.client.call("app.method", (), {})        # root server
await app.client.call("app.group.method", (), {})  # group server
```

The last segment is the handler/method leaf; everything before it resolves to the target queue (same queue-qualified convention for `send`).

## Example

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    app = App("app", backend=LocalBackend())

    root = app.server()

    @root.method("method")
    def method() -> str:
        return "root"

    @root.task("task")
    async def task() -> None:
        ...

    group = app.server("group")

    @group.method("method")  # local name "method", not "group.method"
    def group_method() -> str:
        return "group"

    @group.task("task")
    async def group_task() -> None:
        ...

    await app.init()

    await root.consume()
    await group.consume()                 # or consume(mode="rpc"/"task"/"event")

    print(await app.client.call("app.method", (), {}))        # root server
    print(await app.client.call("app.group.method", (), {}))  # group server

    await root.stop_consume()
    await group.stop_consume(mode="rpc")
    await group.stop_consume(mode="task")
    await group.stop_consume(mode="event")

    await app.close()


asyncio.run(main())
```

## RPC

Each server consumes its RPC queue and dispatches by leaf method name within that queue. Multiple RPC methods registered on the same server share that server's queue.

```python
async with App("orders", backend=LocalBackend()) as app:
    root = app.server()

    @root.method("foo")
    def foo() -> str:
        return "foo"

    @root.method("bar")
    def bar() -> str:
        return "bar"

    await root.consume(mode="rpc")
    print(await app.client.call("orders.foo", (), {}))
    print(await app.client.call("orders.bar", (), {}))
```

Use separate servers for separate queues/groups:

```python
app = App("orders", backend=LocalBackend())
sa = app.server("serviceA")
sb = app.server("serviceB")

@sa.method("foo")
def foo() -> str:
    return "foo"

@sb.method("bar")
def bar() -> str:
    return "bar"

await app.init()
await sa.consume(mode="rpc")
await sb.consume(mode="rpc")
await app.client.call("orders.serviceA.foo", (), {})
await app.client.call("orders.serviceB.bar", (), {})
```

## Tasks

A task has no response. Multiple consumers may listen on the same task queue; each task is handled by exactly one of them.

```python
async with App("worker", backend=LocalBackend()) as app:
    email = app.server("email")

    @email.task("send")
    async def send_email(user_id: int) -> None:
        ...

    await email.consume(mode="task")
    await app.client.send("worker.email.send", (), {"user_id": 1})
```

## Events

Events are published by topic (`app.client.emit`) and delivered to subscribers whose patterns match. **Each server has its own event queue** and consumes only the subscriptions registered on that server.

```python
async with App("billing", backend=LocalBackend()) as app:
    root = app.server()

    @root.subscribe("user.created")
    def on_user_created(user_id: int) -> None:
        ...

    await root.consume(mode="event")
    await app.client.emit("user.created", {"user_id": 1})
```

Wildcards are supported:

- `*` matches exactly one segment: `user.*` matches `user.created`.
- `#` matches zero or more segments: `user.#` matches `user.created` and `user.profile.updated`.

Multiple subscribers in the same server for the same topic are all called. Different servers (each with its own event queue) that subscribe to a matching topic will each receive the event.

## Lifecycle and consuming

```python
server = app.server()

await app.init()
await server.consume()                # rpc + task + event (for this server)
await server.consume(mode="rpc")      # only RPC
await server.consume(mode="task")     # only task
await server.consume(mode="event")    # only event

await server.stop_consume()           # stop all modes for this server
await server.stop_consume(mode="rpc")
await server.stop_consume(mode="task")
await server.stop_consume(mode="event")

await app.close()
```

- `App.init()` initializes the backend and middleware. `App.close()` stops all servers, closes middleware and backend.
- `server.consume(...)` starts consuming for the given mode(s) on this server (idempotent for already-consuming modes/queues).
- `server.stop_consume(...)` stops consuming for the given mode(s) on this server (pause). Handler registrations and subscriptions remain, so consuming can be started again with `server.consume(...)`.
- There are no global `app.consume()` / `app.stop_consume()` methods; manage consuming per server.

## Queue options

Backend-specific queue options are configured per server and mode when creating the server:

```python
from linkr.backends.rmq import RmqQueueOptions  # backend-specific

rpc = app.server("serviceA", rpc_options=[RmqQueueOptions(prefetch_count=5, durable=True)])
email = app.server("email", task_options=[RmqQueueOptions(prefetch_count=50)])
root = app.server(event_options=[RmqQueueOptions(prefetch_count=10)])

@rpc.method("ping")
def ping() -> str:
    return "pong"

await app.init()
await rpc.consume(mode="rpc")
```

Options are opaque to `App`; the active backend selects the types it understands. `LocalBackend` ignores transport options.

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
