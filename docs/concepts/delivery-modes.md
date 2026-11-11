# Delivery modes

`linkr` supports three delivery modes. The mode determines registration, client API, consumer behavior, and response semantics.

## Comparison

| Aspect | RPC | Task | Event |
|---|---|---|---|
| Decorator | `@app.method(name, **options)` | `@app.task(name, **options)` | `@app.subscribe(topic, **options)` |
| Client call | `await app.call(name, args, kwds, ...)` | `await app.send(name, args, kwds, ...)` | `await app.emit(topic, event, ...)` |
| Consumers | Competing: exactly one handler processes a request | Competing: exactly one handler processes a task | Fan-out: every matching subscriber receives the event |
| Response to caller | Yes (`result` or `RPCError`) | No | No |
| Typical use | Query/command requiring result | Background job | Domain notification/broadcast |

## RPC

RPC is request/response. The caller awaits a result (or an error).

```python
@app.method("math.add")
def add(x: int, y: int) -> int:
    return x + y

result = await app.call("app.math.add", (1, 2), {})
```

- Method dispatch is by name within a queue.
- Supports `timeout`, `ttl`, and `rttl` headers/parameters.
- Server-side failures are mapped to error responses; on the client this raises `RPCError`.

## Task

A task is dispatched to one of the consumers of its queue. The caller does not receive a result.

```python
@app.task("email.send")
async def send_email(user_id: int) -> None:
    ...

await app.send("worker.email.send", (), {"user_id": 1})
```

- If no consumer is available, behavior depends on the backend (e.g. `LocalBackend` logs a warning and drops; broker-backed backends may queue according to broker semantics).
- Handler exceptions and validation failures are logged; no error is returned to the sender.

## Event

An event is published to a topic and delivered to all matching subscribers. Each app instance has its own event queue.

```python
@app.subscribe("user.*")
def on_user_event(user_id: int) -> None:
    ...

await app.emit("user.created", {"user_id": 1})
```

- Topic patterns support `*` (one segment) and `#` (zero or more segments).
- Multiple subscribers in the same app for the same topic are all invoked.
- Subscriber exceptions and validation failures are logged; no response is returned.

## Choosing a mode

- Use **RPC** when the caller needs a result, confirmation, or typed error.
- Use **task** when work should be processed once asynchronously without a reply.
- Use **event** when multiple independent consumers must react to the same occurrence.

!!! warning "No reply modes"
    For tasks and events, `send`/`emit` completing means the client-side publish completed (subject to backend behavior), not that handlers succeeded. Use hooks, logging, or out-of-band mechanisms for observability and reliability guarantees.
