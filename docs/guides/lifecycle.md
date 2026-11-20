# Lifecycle

`App` manages backend/middleware initialization and shutdown. `RPC`/`Worker`/`Bus` servers manage handler registration and consuming per scope.

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
    rpc = app.rpc()
    await rpc.consume()
    ...
```

- `app.init()` initializes the backend and middleware.
- `app.close(timeout=None)` stops consuming on all servers, closes middleware and backend, and marks the app closed. The `timeout` argument is reserved for future graceful-shutdown control and is currently unused. Operations after close raise `RuntimeError`.
- There are no global `app.consume()` / `app.stop_consume()` methods.

## Servers and queue scopes

Create mode servers from an app:

```python
root_rpc = app.rpc()                               # base queue: "orders"
svc_rpc = app.rpc("serviceA")                      # base queue: "orders.serviceA"
email_worker = app.worker("email", options=[...])  # named task queue with options
bus = app.bus(options=[...])                       # event bus with options
```

- `path=None` (or omitted) is the root scope with base queue `app.name`.
- `path="group"` creates/returns a server bound to queue `app.name.group` for that mode.
- Repeated `app.rpc/worker/bus(path, ...)` with the same path returns the same server if options match; conflicting options raise `ValueError`.
- Handler names registered on a server are local leaf names (must not contain `.`). The queue is determined by the server scope.

## Register handlers, then consume

Register handlers on mode servers, then start consuming on each server:

```python
rpc = app.rpc()
worker = app.worker("email")

@rpc.method("ping")
def ping() -> str:
    return "pong"

@worker.task("send")
async def send(user_id: int) -> None:
    ...

await app.init()
await rpc.consume()
await worker.consume()
```

- `server.consume()` starts consuming for that server.
- Consuming is idempotent when already consuming.
- Consuming on a closed app raises `RuntimeError`.
- Declarative decorators `@rpc.method(...)`, `@worker.task(...)`, `@bus.subscribe(...)` must not be used while the server is consuming.
- For runtime changes while consuming, use async APIs: `rpc.add_method()` / `rpc.remove_method()`, `worker.add_task()` / `worker.remove_task()`, `bus.add_subscriber()` / `bus.remove_subscriber()` (no `stop_consume()`/`consume()` cycle required). For Bus, adding a new topic pattern while consuming requires backend support for dynamic event binding.

## Stopping and resuming

```python
rpc = app.rpc()
worker = app.worker()
bus = app.bus()

await rpc.stop_consume()     # pause RPC server
await worker.stop_consume()  # pause worker server
await bus.stop_consume()     # pause bus server
```

- `server.stop_consume()` pauses consuming for that server.
- Registrations (handlers/subscriptions) are **not removed**; call `server.consume()` again to resume listening.
- Stopping is idempotent. Each server is controlled independently.

## Events are per-bus

Each `Bus` has its own event queue (e.g. `<bus.queue_base>.<app.instance_id>`). Subscriptions registered on a bus are consumed only by that bus. Publishing (`app.client.emit`) sends to the topic; buses with matching subscriptions on their own queues receive the event.

## Hooks

Lifecycle operations emit hook events:

- `on_consume_start` — when consuming starts for a server/mode/queue
- `on_consume_stop` — when consuming stops for a server/mode/queue

See [Hooks](hooks.md).
