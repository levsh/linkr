# Lifecycle

`App` manages backend/middleware initialization, consumer registration, and shutdown.

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
    await app.consume()
    ...
```

- `init()` initializes the backend and middleware.
- `close(timeout=None)` stops consuming, closes middleware and backend, and marks the app closed. Operations after close raise `RuntimeError`.

## Named queue/router configuration and consume

Per-queue transport options for **named RPC/task queues** are configured with
`app.rpc_router(path, *, options=[...])` / `app.task_router(path, *, options=[...])`
and applied when the queue starts consuming. `path` is required and targets
queue `app.name.<path>`.

The default app queue (`app.name`) and event queues are not configured via
routers; they use backend-static defaults.

```python
rpc = app.rpc_router("serviceA", options=[...])      # named RPC queue (orders.serviceA)
jobs = app.task_router("jobs", options=[...])        # named task queue (orders.jobs)

@rpc("ping")
def ping() -> str:
    return "pong"
```

Rules:

- Declare/configure a named queue **before** `consume_*` starts it. Configuring or changing options for an already-consuming queue raises `RuntimeError`.
- The configuration unit is the named queue `(mode, app.name.<path>)` (shared by all handlers on that queue). Re-declaring the same queue with identical options is idempotent; different options raise `ValueError`. Duplicate option types in one options list are rejected.
- `options=None` does not change an existing configuration (can be used to obtain a router handle). With no declaration, backend defaults apply for that queue.
- `consume_*` passes a named rpc/task queue's configured options to the backend (`consume_rpc/task(..., options=...)`); backends select option types they understand and ignore the rest. Event consumers are started without app-level queue options.
- A stopped queue can be consumed again; `consume_*` re-reads the current queue configuration.

## Starting consumers

Register handlers and configure named queues first, then start consuming:

```python
@rpc("ping")
def ping() -> str:
    return "pong"

await app.consume()        # RPC + tasks + events
# or selectively:
await app.consume_rpc()
await app.consume_tasks()
await app.consume_events()
```

- `consume_rpc()` starts consumers for all RPC queues required by registered methods (including the app default queue when methods exist).
- `consume_tasks()` starts consumers for registered task queues.
- `consume_events()` starts consuming on this instance's event queue when subscriptions exist.
- Calling consume on a closed app raises `RuntimeError`. Already-consumed queues are not re-registered.

## Stopping consumers

```python
await app.stop_consume()                 # stop all modes

await app.stop_consume_rpc()             # stop all RPC consumers
await app.stop_consume_rpc("ping")       # stop a specific RPC method
await app.stop_consume_rpc("group.ping") # grouped method

await app.stop_consume_tasks()           # stop all task consumers
await app.stop_consume_tasks("email.send")

await app.stop_consume_events()          # stop all event consumers and subscriptions
await app.stop_consume_events("user.created")
```

### Granular stop semantics

`App` decides what to stop; the backend executes transport commands.

**RPC / Task (by handler name):**

- `method`/`task=None`: stop all consumers of that mode.
- Specific name: removes the handler from the app registry.
  - If other handlers of the same mode still share the same queue, the backend queue consumer **remains active**.
  - If it was the last handler on that queue, the app instructs the backend to stop that queue consumer.
- Unknown handler name raises `ValueError`.

**Events (by topic pattern):**

- `topic=None`: stops event consuming and clears subscriptions.
- Specific `topic`: removes that subscription.
  - If other subscriptions remain, only that topic binding/subscription is removed (backend queue consumer stays active).
  - If no subscriptions remain, the event queue consumer is stopped.
- Unknown topic pattern raises `ValueError`.

### Backend stop operations

Backends implement execution commands:

```python
await backend.stop_consume_rpc(queue=None)
await backend.stop_consume_tasks(queue=None)
await backend.stop_consume_events(queue=None, topic=None)
```

- `queue=None` — stop all consumers of the mode (for this backend instance).
- `queue=<name>` — stop the specified queue consumer.
- `queue=<event_queue>, topic=<pattern>` — remove only that topic subscription.

`Backend.consume_rpc/task` accept an optional `options` argument for per-queue backend configuration of named rpc/task queues. `Backend.consume_event` receives `(queue, topics, handler)` and uses backend-static configuration. `Backend.stop_consume(mode=None)` exists as a deprecated compatibility shim.

## Re-consuming

After stopping specific handlers/queues (or all), you can register new handlers and call `consume_*()`/`consume()` again to resume consuming as needed (subject to current registrations and app state).

## Hooks

Lifecycle operations emit hook events:

- `on_consume_start` — per queue/mode when consuming starts
- `on_consume_stop` — with reasons such as `all`, `queue_drained`, or `topic_unsubscribed`

See [Hooks](hooks.md).
