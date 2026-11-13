# Backends overview

Backends provide the transport for RPC, task, and event delivery. `App` depends on the `Backend` interface and implements routing, serialization, middleware, hooks, and handler dispatch on top of it.

## Available backends

| Backend | Module | Use case | Notes |
|---|---|---|---|
| `LocalBackend` | `linkr.backends.local` / `linkr.LocalBackend` | Tests, local/dev, in-process apps | No broker, process-local routing |
| `RmqBackend` | `linkr.backends.rmq` | Distributed messaging via RabbitMQ | Requires `aio-pika` (`linkr[rabbitmq]`) |

## Backend contract

A backend implements three delivery capabilities:

- **RPC:** `consume_rpc(queue, handler, *, options=None)`, `request(queue, message, *, context=None) -> Awaitable[Message]`
  - **Task:** `consume_task(queue, handler, *, options=None)`, `send(queue, message, *, context=None) -> None`
  - **Event:** `consume_event(queue, topics, handler)`, `emit(topic, message, *, context=None) -> None`

And lifecycle/stop operations:

- `init()`, `close(timeout=None)`
- `stop_consume_rpc(queue=None)`, `stop_consume_tasks(queue=None)`, `stop_consume_events(queue=None, topic=None)`
- Deprecated shim: `stop_consume(mode=None)` (delegates to the mode-specific methods)

`App` decides *when* to stop a queue or topic subscription (based on remaining handlers/subscriptions) and issues execution commands to the backend. It also passes per-queue router `options` (backend-specific, `QueueOptions` objects) to `consume_rpc`/`consume_task` when starting a named RPC/task queue; backends select options they understand and ignore the rest. Event consumers use backend-static configuration.

## `request` returns a Future

`async def request(...) -> asyncio.Future[Message]` has two phases:

1. `await backend.request(...)` completes once the request is published and response waiting is registered, returning a `Future`.
2. `await future` yields the response `Message`.

```python
future = await backend.request(queue, message)
response_message = await future
```

`App.call` manages this internally and also emits client hooks (`RequestPublishedEvent` after publish/registration, `ResponseReceivedEvent` after the RPC outcome).

## Responsibilities split

- **Backend:** transport I/O, queue/binding/consumer mechanics, delivering messages to registered handlers, returning response futures (RPC).
- **App:** naming/routing rules, serialization, app/wire middleware, handler dispatch and validation, error mapping, hooks, and deciding which backend stop commands to issue.

!!! note "Semantics vary by backend"
    Delivery guarantees, persistence, and behavior when no consumer exists depend on the backend and its configuration. Always verify with the specific backend you deploy.
