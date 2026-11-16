# Lifecycle

`App` manages backend/middleware initialization and shutdown. `Server` manages handler registration and consuming per scope/mode.

## Initialization and shutdown

```python
app = App("orders", backend=backend)

await app.init()
try:
    ...
finally:
    await app.close()
```

Or use the async context manager:

```python
async with App("orders", backend=backend) as app:
    server = app.server()
    await server.consume()
    ...
```

- `app.init()` initializes the backend and middleware.
- `app.close(timeout=None)` stops consuming on all servers, closes middleware and backend, and marks the app closed. The `timeout` argument is reserved for future graceful-shutdown control and is currently unused. Operations after close raise `RuntimeError`.
- There are no global `app.consume()` / `app.stop_consume()` methods.

## Servers and queue scopes

Create servers from an app:

```python
root = app.server()                              # base queue: "orders"
svc = app.server("serviceA")                     # base queue: "orders.serviceA"
email = app.server("email", task_options=[...])  # named task queue with options
```

- `path=None` (or omitted) is the root server with base queue `app.name`.
- `path="group"` creates/returns a server bound to queue `app.name.group`.
- Repeated `app.server(path, ...)` with the same path returns the same server if options match; conflicting options raise `ValueError`.
- Handler names registered on a server are local leaf names (must not contain `.`). The queue is determined by the server scope.

## Register handlers, then consume

Register handlers on servers, then start consuming on each server:

```python
root = app.server()
email = app.server("email")

@root.method("ping")
def ping() -> str:
    return "pong"

@email.task("send")
async def send(user_id: int) -> None:
    ...

await app.init()
await root.consume()              # rpc + task + event for this server (as applicable)
await email.consume(mode="task")  # only task mode for this server
```

- `server.consume(mode=None)` starts consuming for all modes in this server that have registrations; `mode` can be `"rpc"`, `"task"`, or `"event"`.
- Consuming is idempotent for modes/queues already consuming.
- Consuming on a closed app raises `RuntimeError`.

## Stopping and resuming

```python
server = app.server()

await server.stop_consume()             # pause all modes for this server
await server.stop_consume(mode="rpc")   # pause RPC only
await server.stop_consume(mode="task")
await server.stop_consume(mode="event")
```

- `server.stop_consume(...)` pauses consuming for the specified mode(s) on that server.
- Registrations (handlers/subscriptions) are **not removed**; call `server.consume(...)` again to resume listening.
- Stopping is idempotent. Each server is controlled independently.

## Events are per-server

Each server has its own event queue (e.g. `<server.queue_base>.<app.instance_id>`). Subscriptions registered on a server are consumed only by that server's event consumer. Publishing (`app.client.emit`) sends to the topic; servers with matching subscriptions on their own queues receive the event.

## Hooks

Lifecycle operations emit hook events:

- `on_consume_start` — when consuming starts for a server/mode/queue
- `on_consume_stop` — when consuming stops for a server/mode/queue

See [Hooks](hooks.md).
