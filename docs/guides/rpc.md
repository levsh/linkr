# RPC

RPC (request/response) handlers run on a `Server`, return a result to the caller, and are invoked via `app.client.call(...)`.

## Registering handlers

```python
from linkr import App, LocalBackend

app = App("orders", backend=LocalBackend())
server = app.server()

@server.method("ping")
def ping() -> str:
    return "pong"

# note: method/task names must be local leaf names (no '.').
# to place a handler on a different queue, use a different server scope.
```

Correct grouped example:

```python
app = App("orders", backend=LocalBackend())
math = app.server("math")

@math.method("add")
def add(x: int, y: int) -> int:
    return x + y
# FQ: orders.math.add, queue: orders.math
```

- `@server.method(name, **options)` registers an RPC handler with a local leaf name on the server's queue.
- Supported handler options (see [Configuration](../reference/configuration.md)): e.g. `validate_types`.
- Sync and async handler functions are supported.

## Calling

```python
# after registering handlers on `server` and `math` (see above)
await app.init()
await server.consume(mode="rpc")
await math.consume(mode="rpc")

result = await app.client.call("orders.ping", (), {})
result = await app.client.call("orders.math.add", (2, 3), {})
result = await app.client.call("orders.math.add", (), {"x": 2, "y": 3})
```

Signature: `app.client.call(method, args: tuple, kwds: dict, *, timeout=None, ttl=None, rttl=None, serializer=None, request_id=None, context=None)`.

- `method` is the queue-qualified path; the last segment is dispatched as the handler leaf.
- `args` and `kwds` are passed to the handler (subject to server-side middleware/validation/DI).
- A plain method name targets the calling app's default queue.

## Errors and timeouts

- Handler exceptions are converted to error responses (`RPCError` on the client) unless handled by a registered `@app.exception_handler(...)`.
- `METHOD_NOT_FOUND` is returned when no handler matches on the target queue.
- Client-side timeout/rttl produce `RPCError` with `error_code="Timeout"`.
- See [Error handling](error-handling.md).

## Consuming

```python
server = app.server()

await server.consume(mode="rpc")      # start RPC for this server
await server.stop_consume(mode="rpc") # pause RPC for this server (resumable)
```

Consuming is controlled per server and mode (no per-method stop, no global app consume/stop).
