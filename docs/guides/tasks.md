# Tasks

Tasks are fire-and-forget handlers: no result is returned to the sender. They run on a `Server` and are sent via `app.client.send(...)`.

## Registering handlers

```python
from linkr import App, LocalBackend

app = App("worker", backend=LocalBackend())
email = app.server("email")

@email.task("send")
async def send_email(user_id: int, template: str = "welcome") -> None:
    ...  # perform work
```

- `@server.task(name, **options)` registers a task handler with a local leaf name on the server's queue.
- Supported handler options (see [Configuration](../reference/configuration.md)): e.g. `validate_types`.
- Sync and async handlers are supported.

## Sending

```python
app = App("worker", backend=LocalBackend())
email = app.server("email")

@email.task("send")
async def send_email(user_id: int, template: str = "welcome") -> None:
    ...  # perform work

await app.init()
await email.consume(mode="task")

await app.client.send("worker.email.send", (), {"user_id": 42})
await app.client.send("worker.email.send", (42,), {})
```

Signature: `app.client.send(task, args: tuple, kwds: dict, *, timeout=None, ttl=None, serializer=None, request_id=None, context=None)`.

- `task` is the queue-qualified path; the last segment is dispatched as the handler leaf.
- `send` returns `None` (no response from the handler).

## Delivery semantics

- **Competing consumers:** if multiple consumers listen on the same task queue, each task is handled by exactly one of them (backend-dependent load distribution).
- If no consumer is available for the queue, behavior depends on the backend (e.g. in-memory backends may drop/no-op; broker backends may queue according to broker semantics).

## Errors and validation

- Handler exceptions are logged on the server side (no client response). Custom exception handlers registered via `@app.exception_handler(...)` do not produce a client reply for tasks.
- Validation failures (when `validate_types` is enabled) prevent handler execution and are logged/dropped according to hooks.
- See [Error handling](error-handling.md).

## Consuming

```python
server = app.server()

await server.consume(mode="task")      # start task consuming for this server
await server.stop_consume(mode="task") # pause task consuming (resumable)
```

Consuming is controlled per server and mode (no global app consume/stop).
