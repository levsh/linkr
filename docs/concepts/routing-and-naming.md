# Routing and naming

Handler names and event topics determine which queue receives a message and how it is dispatched. Understanding this is essential for correct addressing, especially across apps and shared queues.

## Registration queue vs call target

`App` derives queues from dotted names:

- **Registration queue** (where a handler is consumed):
  - no dot: `<app_name>`
  - dotted: `<app_name>.<name without last segment>`
- **Client-resolved queue** (where `call`/`send` targets):
  - no dot: `<app_name>` (the caller app's default queue)
  - dotted: `<name without last segment>` (used as the full queue path)

The handler is identified by the **last segment** of the name (`method`/`task`).

## RPC and task naming

### Same-app, default queue

```python
app = App("orders", backend=backend)

@app.method("foo")        # registered on queue "orders" as method "foo"
async def foo() -> None:
    ...

@app.task("send")         # registered on queue "orders" as task "send"
async def send_job() -> None:
    ...

await app.call("orders.foo", (), {})
await app.send("orders.send", (), {})
```

### Grouped (dedicated) queue

```python
@app.method("serviceA.foo")   # queue "orders.serviceA", method "foo"
@app.method("serviceB.bar")   # queue "orders.serviceB", method "bar"

await app.call("orders.serviceA.foo", (), {})
await app.call("orders.serviceB.bar", (), {})
```

### Calling another app

Use the target app's queue-qualified path:

```python
client = App("client", backend=backend)
# server app is named "orders"
await client.call("orders.foo", (), {})
await client.call("orders.serviceA.foo", (), {})
await client.send("orders.email.send", (), {"user_id": 1})
```

!!! important "Qualified names"
    Pass the fully qualified path expected by the target queue (typically `<target_app>.<group>.<handler>` or `<target_app>.<handler>`). A plain name resolves to the **calling** app's default queue.

## Shared queues and dispatch

- Multiple RPC methods or tasks can be registered such that they share the same queue (e.g. `foo` and `bar` on `<app>`, or `email.send` and `email.notify` on `<app>.email`).
- When a queue is consumed, the app dispatches to the handler matching the message's `method`/`task` name.
- Stopping a specific method/task removes that handler; the backend consumer for the queue stops only when no handlers of that mode remain on the queue (see [Lifecycle](../guides/lifecycle.md)).

## Queue configuration with RPC/task routers

Use `app.rpc_router(...)` / `app.task_router(...)` to bind backend-specific
transport options to a **named** RPC/task queue and, optionally, register
handlers on it:

```python
# named RPC queue ("orders.serviceA")
rpc = app.rpc_router("serviceA", options=[...])

@rpc("foo")          # -> orders.serviceA.foo
def foo() -> str:
    return "ok"

# named task queue ("orders.email"), shared by handlers
email = app.task_router("email", options=[...])

@email("send")       # -> orders.email.send
async def send(user_id: int) -> None:
    ...

@email("notify")     # -> orders.email.notify
async def notify(user_id: int) -> None:
    ...
```

- `path` is required and must be a non-empty string; it targets queue
  `<app>.<path>`.
- `@router("<name>")` registers a leaf handler `<path>.<name>` on that queue.
  The handler name must not contain `.`.
- The default app queue (`<app>`) and event queues are not configured via
  routers; they use backend-static defaults.
- `options` is a `QueueOptions` instance or a list of backend-specific option
  types (the active backend selects the ones it understands). One queue has one
  configured option set; configure before `consume_*`.
- Handlers registered with `@app.method` / `@app.task` on the same resolved
  queue receive the same configured options.

See [Configuration](../reference/configuration.md) and backend docs for
concrete option types (e.g. `RmqQueueOptions`).

## Event topics and subscriptions

Events are routed by **topic** (not by app queue name) and delivered to subscribers whose patterns match.

```python
@app.subscribe("user.created")
def on_created(user_id: int) -> None:
    ...

await app.emit("user.created", {"user_id": 1})
```

### Wildcards

- `*` matches **exactly one** segment: `user.*` matches `user.created`, not `user.profile.updated`.
- `#` matches **zero or more** segments: `user.#` matches `user.created` and `user.profile.updated`; `#` alone matches any topic (including empty segments behavior as implemented).

Patterns are matched as full topics.

### Event queues

Each app instance consumes events from its own queue: `<app_name>.<instance_id>`. This enables fan-out so every instance with a matching subscription receives the event. Event queue/consumer configuration is static on the backend (not configured via `App` routers/options).

## Backend queue naming (transport)

`LocalBackend` routes by logical queue name in-process. `RmqBackend` maps logical queues to broker resources (e.g. `linkr.rpc.<queue>`, `linkr.task.<queue>`, `linkr.event.<queue>` and exchanges) — see [RabbitMQ backend](../backends/rabbitmq.md).
