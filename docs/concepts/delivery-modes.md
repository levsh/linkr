# Delivery modes

linkr provides three messaging patterns over the same app/backend model. `App` is the client/infrastructure; handlers and consuming live on `Server`.

## Comparison

| Aspect | RPC | Task | Event |
|---|---|---|---|
| Decorator | `@server.method(name, **options)` | `@server.task(name, **options)` | `@server.subscribe(topic, **options)` |
| Client call | `app.client.call(target, args, kwds)` | `app.client.send(target, args, kwds)` | `app.client.emit(topic, event_dict)` |
| Response | Yes (`Response` / result) | No | No |
| Consumers | One handler processes a request (competing on a queue) | One consumer processes each task (competing) | All matching subscribers on consuming servers receive it (fan-out) |
| Typical use | Queries/commands needing a result | Background jobs | Notifications/state changes |

## RPC

```python
app = App("orders", backend=backend)
math = app.server("math")

@math.method("add")
def add(x: int, y: int) -> int:
    return x + y

await app.init()
await math.consume(mode="rpc")
result = await app.client.call("orders.math.add", (2, 3), {})
```

- Request/response with correlation.
- Errors propagate as error responses (client receives `RPCError`).
- Timeout/TTL can be controlled per call and by app defaults.

## Task

```python
app = App("worker", backend=backend)
email = app.server("email")

@email.task("send")
async def send_email(user_id: int) -> None:
    ...

await app.init()
await email.consume(mode="task")
await app.client.send("worker.email.send", (), {"user_id": 1})
```

- No response payload to the caller.
- Suitable for asynchronous/background processing.
- With multiple workers on the same queue, one handles each task.

## Event

```python
app = App("billing", backend=backend)
server = app.server()

@server.subscribe("user.*")
def on_user_event(user_id: int) -> None:
    ...

await app.init()
await server.consume(mode="event")
await app.client.emit("user.created", {"user_id": 1})
```

- Topic-based fan-out delivery.
- Each server has its own event queue; subscriptions on a server are consumed by that server.
- Supports `*` (single segment) and `#` (zero or more segments) wildcards.

## Choosing a mode

- Need a result or success/failure from the handler: **RPC**.
- Need background execution without a reply, load-balanced across workers: **Task**.
- Need to notify multiple interested servers/subscribers: **Event**.
