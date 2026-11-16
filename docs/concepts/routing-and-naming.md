# Routing and naming

Handler names and event topics determine which queue receives a message and how it is dispatched.

## Servers, queues, and local handler names

`App` is the client/infrastructure; `Server` is a server scope bound to a queue.

- `root = app.server()` → base queue `app.name`
- `group = app.server("group")` → base queue `f"{app.name}.group"`

Handlers are registered on a server with **local (leaf) names** (no `.`):

```python
app = App("orders", backend=backend)
root = app.server()
group = app.server("serviceA")

@root.method("foo")        # queue "orders", leaf "foo"   -> FQ "orders.foo"
async def foo() -> None: ...

@group.method("foo")       # queue "orders.serviceA", leaf "foo" -> FQ "orders.serviceA.foo"
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

Multiple handlers registered on the same server share that server's queue and are dispatched by leaf name:

```python
root = app.server()

@root.method("foo")
def foo(): ...

@root.method("bar")
def bar(): ...
# both on queue "orders"
```

## Consuming and stopping

Consuming is controlled per server and per mode:

```python
server = app.server()

await server.consume()                # rpc + task + event for this server
await server.consume(mode="rpc")
await server.stop_consume(mode="rpc") # pause; registrations remain, can consume again
```

There is no per-method/task stop in the server API and no global `app.consume()/stop_consume()`.

## Event topics and subscriptions

Events are routed by **topic** and delivered to server subscriptions whose patterns match.

```python
server = app.server()

@server.subscribe("user.created")
def on_created(user_id: int) -> None:
    ...

await app.init()
await server.consume(mode="event")
await app.client.emit("user.created", {"user_id": 1})
```

### Wildcards

- `*` matches **exactly one** segment: `user.*` matches `user.created`, not `user.profile.updated`.
- `#` matches **zero or more** segments: `user.#` matches `user.created` and `user.profile.updated`.

### Per-server event queues

Each server has its own event queue (scoped to `server.queue_base` and the app instance). Subscriptions registered on a server are bound/consumed only on that server's event queue. Different servers that subscribe to a matching topic each receive the event on their own queue.

## Backend queue naming (transport)

`LocalBackend` routes by logical queue name in-process. `RmqBackend` maps logical queues to broker resources (e.g. `linkr.rpc.<queue>`, `linkr.tasks.<queue>`, `linkr.events.<queue>` and exchanges) — see [RabbitMQ backend](../backends/rabbitmq.md).
