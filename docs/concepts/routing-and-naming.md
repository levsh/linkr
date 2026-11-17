# Routing and naming

Handler names and event topics determine which queue receives a message and how it is dispatched.

## Servers, queues, and local handler names

`App` is the client/infrastructure; `RPC`, `Worker`, and `Bus` are mode servers bound to a queue scope.

- `rpc = app.rpc()` / `worker = app.worker()` / `bus = app.bus()` → base queue `app.name`
- `rpc = app.rpc("group")` (etc.) → base queue `f"{app.name}.group"`

Handlers are registered on a mode server with **local (leaf) names** (no `.`):

```python
app = App("orders", backend=backend)
rpc = app.rpc()
group_rpc = app.rpc("serviceA")

@rpc.method("foo")        # queue "orders", leaf "foo"   -> FQ "orders.foo"
async def foo() -> None: ...

@group_rpc.method("foo")   # queue "orders.serviceA", leaf "foo" -> FQ "orders.serviceA.foo"
async def group_foo() -> None: ...
```

The fully qualified handler path is `<server.queue_base>.<leaf>`.

## Client addressing (`call` / `send`)

`app.client.call(name, args, kwds)` and `app.client.send(name, args, kwds)` target by queue-qualified name:

- If `name` contains a dot: queue = `name.rsplit('.', 1)[0]`, method/task leaf = last segment.
- If `name` has no dot: queue = calling app's default queue (`app.name`), leaf = `name`.

Examples:

```python
await app.client.call("orders.foo", (), {})          # queue "orders", leaf "foo"
await app.client.call("orders.serviceA.foo", (), {}) # queue "orders.serviceA", leaf "foo"
await app.client.send("orders.email.send", (), {"user_id": 1})
```

Pass the fully qualified path expected by the target server/queue. A plain name resolves to the **calling** app's default queue.

## Shared handlers on the same server queue

Multiple handlers registered on the same mode server share that server's queue and are dispatched by leaf name:

```python
rpc = app.rpc()

@rpc.method("foo")
def foo(): ...

@rpc.method("bar")
def bar(): ...
# both on queue "orders"
```

## Consuming and stopping

Consuming is controlled per mode server:

```python
rpc = app.rpc()

await rpc.consume()       # start consuming for this RPC server
await rpc.stop_consume()  # pause; registrations remain, can consume again
```

There is no per-method/task stop in the server API, no mode argument, and no global `app.consume()/stop_consume()`.

## Event topics and subscriptions

Events are routed by **topic** and delivered to bus subscriptions whose patterns match.

```python
bus = app.bus()

@bus.subscribe("user.created")
def on_created(user_id: int) -> None:
    ...

await app.init()
await bus.consume()
await app.client.emit("user.created", {"user_id": 1})
```

### Wildcards

- `*` matches **exactly one** segment: `user.*` matches `user.created`, not `user.profile.updated`.
- `#` matches **zero or more** segments: `user.#` matches `user.created` and `user.profile.updated`.

### Per-bus event queues

Each `Bus` has its own event queue (scoped to `bus.queue_base` and the app instance). Subscriptions registered on a bus are bound/consumed only on that bus's event queue. Different buses that subscribe to a matching topic each receive the event on their own queue.

## Backend queue naming (transport)

`LocalBackend` routes by logical queue name in-process. `RmqBackend` maps logical queues to broker resources (e.g. `linkr.rpc.<queue>`, `linkr.tasks.<queue>`, `linkr.events.<queue>` and exchanges) — see [RabbitMQ backend](../backends/rabbitmq.md).
