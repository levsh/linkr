# linkr

Async messaging framework with three delivery modes: RPC, task and event.

## Install

```
pip install linkr
```

## Delivery modes

| Mode | Registration | Client | Behaviour |
|---|---|---|---|
| RPC | `@app.method("foo")` | `await app.call("orders.foo", (), {})` | One consumer handles the call and returns a result. |
| Task | `@app.task("email.send")` | `await app.send("worker.email.send", (), {"user_id": 1})` | One of the consumers handles the task. No result. |
| Event | `@app.subscribe("user.created")` | `await app.emit("user.created", {"user_id": 1})` | Every subscriber receives the event. No result. |

The client methods take positional arguments as a tuple and keyword arguments
as a dict:

```python
await app.call("orders.add", (2, 3), {})
await app.call("orders.add", (), {"x": 2, "y": 3})
await app.send("worker.email.send", (), {"user_id": 1})
await app.emit("user.created", {"user_id": 1})
```

### RPC

By default all RPC methods of an app share one queue. Dispatch happens by
method name.

```python
from linkr import App, LocalBackend

backend = LocalBackend()

async with App("orders", backend=backend) as app:
    @app.method("foo")
    def foo() -> str:
        return "foo"

    @app.method("bar")
    def bar() -> str:
        return "bar"

    await app.consume()

    print(await app.call("orders.foo", (), {}))  # foo
    print(await app.call("orders.bar", (), {}))  # bar
```

Both `foo` and `bar` are consumed from the `orders` queue.

A method name may contain a group prefix. A grouped method gets its own queue:

```python
@app.method("group1.foo")
def foo() -> str:
    return "foo"

@app.method("group2.bar")
def bar() -> str:
    return "bar"
```

For the app named `orders`:

| Method | Queue |
|---|---|
| `foo` | `orders` |
| `group1.foo` | `orders.group1` |
| `group2.bar` | `orders.group2` |

Calls use the fully qualified queue path:

```python
await app.call("orders.group1.foo", (), {})
await app.call("orders.group2.bar", (), {})
```

To call another app, use its queue prefix:

```python
await client.call("orders.foo", (), {})
await client.call("orders.group1.foo", (), {})
```

Dotted method names are queue-qualified paths. A plain name targets the
calling application's default queue.

### Task

A task has no response. Multiple consumers may listen on the same task queue;
each task is handled by exactly one of them.

```python
async with App("worker", backend=backend) as app:
    @app.task("email.send")
    async def send_email(user_id: int) -> None:
        ...

    await app.consume()

    # From the same app:
    await app.send("worker.email.send", (), {"user_id": 1})

    # From another app:
    await client.send("worker.email.send", (), {"user_id": 1})
```

Queue naming:

| Task method | Queue |
|---|---|
| `send` | `worker` |
| `email.send` | `worker.email` |

Task names passed to `send()` use the same queue-qualified convention as RPC
calls.

### Event

An event is delivered to every subscriber. Each app instance gets its own
event queue, so multiple instances all receive the event.

Event queue naming:

| App name | Instance ID | Queue |
|---|---|---|
| `billing` | `abc123` | `billing.events.abc123` |
| `billing` | `def456` | `billing.events.def456` |

```python
async with App("billing", backend=backend) as app:
    @app.subscribe("user.created")
    def on_user_created(user_id: int) -> None:
        ...

    await app.consume()

    await app.emit("user.created", {"user_id": 1})
```

The event payload is passed to subscribers as keyword arguments.

Wildcards are supported:

- `*` matches exactly one segment: `user.*` matches `user.created`.
- `#` matches zero or more segments: `user.#` matches `user.created` and
  `user.profile.updated`.

Multiple subscribers for the same topic in one app are all called.

### Lifecycle

```python
await app.consume()                 # start RPC, task and event consumers
await app.consume_rpc()             # start RPC consumers only
await app.consume_tasks()           # start task consumers only
await app.consume_events()          # start event consumers only

await app.stop_consume()            # stop all consumers
await app.stop_consume_rpc()        # stop all RPC consumers
await app.stop_consume_rpc("foo")   # stop a specific RPC method (queue stops when unused)
await app.stop_consume_tasks("email.send")
await app.stop_consume_events("user.created")
await app.close()
```

Stopping a specific RPC method or task removes that handler from the app. The
backend queue consumer is stopped only when no other handlers of the same mode
share that queue. Stopping a specific event topic removes that subscription; if
other topic subscriptions remain, only that topic is removed. Backends expose
`stop_consume_rpc(queue=None)`, `stop_consume_tasks(queue=None)` and
`stop_consume_events(queue=None, topic=None)` as execution commands; `App`
decides when a queue/topic should be stopped.

### Queue/router options

Use `app.router(...)` to configure backend-specific options for a queue and (optionally) register handlers on it. Options are opaque to `App`: pass a list and the active backend selects the types it understands.

```python
from linkr import App, LocalBackend
from linkr.backends.rmq import RmqQueueOptions  # backend-specific

app = App("orders", backend=LocalBackend())

# default queue (orders)
rpc = app.router("rpc", options=[RmqQueueOptions(prefetch_count=5, durable=True)])

@rpc("ping")
def ping() -> str:
    return "pong"

# named queue (orders.email), shared by handlers
email = app.router("task", path="email", options=[RmqQueueOptions(prefetch_count=50)])

@email("send")
async def send(user_id: int) -> None:
    ...

await app.consume()
```

- `path=None` targets the default app queue for rpc/task (or the instance event queue for `mode="event"`); `path="..."` targets `app.<path>`.
- `@router("name")` registers a leaf handler on that queue (`<path>.<name>` or `<name>` for the default queue). Handlers registered via `@app.method/task/subscribe` on the same resolved queue use the same configured options.
- Configure before `consume_*`. One queue has one option set (identical re-declaration is idempotent, conflicting raises; cannot reconfigure while consuming). No declaration keeps backend/mode defaults. `LocalBackend` ignores transport options.

## Middleware

### App-level middleware

`AppMiddleware` works with deserialized `Request`/`Response` objects.
`Request.type` tells which delivery mode is being processed.

```python
import logging

from typing import Any

from linkr import AppMiddleware, LocalBackend, App
from linkr.models import Request, Response


class LoggingMiddleware(AppMiddleware):
    async def dispatch_client(
        self,
        call_next,
        request: Request,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> Response | None:
        logging.info("[%s] Calling %s", request.id, request.data.get("method") or request.data.get("topic"))
        response = await call_next()
        if response:
            logging.info("[%s] Done", request.id)
        return response

    async def dispatch_server(
        self,
        call_next,
        request: Request,
        request_message: Message,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> Response | None:
        logging.info("[%s] Calling %s", request.id, request.data.get("method") or request.data.get("topic"))
        return await call_next()


app = App("orders", backend=LocalBackend())
app.add_middleware(LoggingMiddleware())
```

For task and event messages the response path is skipped.

### Wire-level middleware

Compression and encryption operate on raw bytes and wire headers.

```python
from linkr.middleware.gzip import GzipMiddleware

app.add_middleware(GzipMiddleware())
```

Custom wire middleware inherits from `WireMiddleware`:

```python
import gzip

from typing import Any

from linkr import WireMiddleware
from linkr.models import Message, Request, Response


class CustomCompression(WireMiddleware):
    async def dispatch_client(
        self,
        call_next,
        request_message: Message,
        request: Request,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> Message | None:
        if len(request_message.data) >= 1024:
            request_message.data = gzip.compress(request_message.data)
            request_message.headers["content_encoding"] = "gzip"
        response_message = await call_next()
        if response_message and "gzip" in response_message.headers.get("content_encoding", ""):
            response_message.data = gzip.decompress(response_message.data)
        return response_message

    async def dispatch_server(
        self,
        call_next,
        request_message: Message,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> tuple[Message, Response] | tuple[None, None]:
        if "gzip" in request_message.headers.get("content_encoding", ""):
            request_message.data = gzip.decompress(request_message.data)
        result = await call_next()
        if result is None or result[0] is None:
            return None, None
        response_message, response = result
        if len(response_message.data) >= 1024:
            response_message.data = gzip.compress(response_message.data)
            response_message.headers["content_encoding"] = "gzip"
        return response_message, response
```

## Dependency Injection

```python
from linkr import App, Depends, LocalBackend, get_current_request


class Database:
    def __init__(self, url: str) -> None:
        self.url = url


backend = LocalBackend()

async with App("orders", backend=backend) as app:
    @app.method("ping")
    def ping(db: Depends[Database, lambda: Database("postgres://...")]) -> str:
        req = get_current_request()
        return f"{req.type}:{db.url}"

    await app.consume()
    print(await app.call("orders.ping", (), {}))
```

`Depends[Type, factory]` resolves a dependency by calling `factory()` for each
handler execution. `get_current_request()` returns the request being processed
in the current async context for RPC, task and event handlers.

## Error Handling

Type validation is enabled per handler with `validate_types=True` or globally
with `App(..., validate_types=True)`:

```python
from linkr import App, LocalBackend, RPCError

backend = LocalBackend()

async with App("orders", backend=backend) as app:
    @app.method("add", validate_types=True)
    def add(x: int, y: int) -> int:
        return x + y

    @app.task("process", validate_types=True)
    async def process(data: dict[str, int]) -> None:
        ...

    @app.subscribe("user.created", validate_types=True)
    def on_user_created(user_id: int, name: str) -> None:
        ...

    await app.consume()
    try:
        await app.call("orders.add", (), {"x": "not", "y": 3})
    except RPCError as e:
        print(e.error_code)     # ValidationError
        print(e.error_message)

    # Task validation failure logs warning, handler not called
    await app.send("orders.process", (), {"data": {"x": "bad"}})

    # Event validation failure logs warning, handler not called
    await app.emit("user.created", {"user_id": "bad", "name": "John"})
```

Custom exception handlers:

```python
@app.exception_handler(ValueError)
def handle_value_error(exc: Exception):
    return ("InvalidValueError", str(exc), {"field": "x"})
```

Task and event handlers cannot return an error response, so their exceptions
are logged.

## Serializers

The default is `JsonSerializer` (`linkr1.0`). JSON-RPC 2.0 is available via
`JsonRpcSerializer` and supports RPC requests only:

```python
from linkr import App, JsonRpcSerializer, JsonSerializer, LocalBackend

app = App("orders", backend=LocalBackend(), serializer=JsonRpcSerializer())
```

A list of serializers enables auto-detection on the server; the first entry is
the default for outgoing calls:

```python
app = App(
    "orders",
    backend=LocalBackend(),
    serializer=[JsonRpcSerializer(), JsonSerializer()],
)
```

Tasks and events should use `JsonSerializer` (or a multi-serializer
configuration where the outgoing call explicitly selects it).

## Backends

| Backend | Status |
|---|---|
| `LocalBackend` | In-process, for tests and local development |
| `RmqBackend` | Implemented; requires RabbitMQ |

## License

MIT
