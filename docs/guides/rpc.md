# RPC

RPC (request/response) handlers run on an `RPC` server, return a result to the caller, and are invoked via `app.client.call(...)`.

## Registering handlers

```python
from linkr import App, LocalBackend

app = App("orders", backend=LocalBackend())
rpc = app.rpc()

@rpc.method("ping")
def ping() -> str:
    return "pong"

# note: method names must be local leaf names (no '.').
# to place a handler on a different queue, use a different RPC server scope.
```

Correct grouped example:

```python
app = App("orders", backend=LocalBackend())
math = app.rpc("math")

@math.method("add")
def add(x: int, y: int) -> int:
    return x + y
# FQ: orders.math.add, queue: orders.math
```

- `@rpc.method(name, **options)` registers an RPC handler with a local leaf name on the server's queue.
- Supported handler options (see [Configuration](../reference/configuration.md)): e.g. `validate_types`.
- Sync and async handler functions are supported.

## Calling

```python
# after registering handlers on `rpc` and `math` (see above)
await app.init()
await rpc.consume()
await math.consume()

result = await app.client.call("orders.ping", (), {})
result = await app.client.call("orders.math.add", (2, 3), {})
result = await app.client.call("orders.math.add", (), {"x": 2, "y": 3})
```

Signature: `app.client.call(method, args: tuple, kwds: dict, *, request_id=None, client_timeout=None, server_timeout=None, ttl=None, rttl=None, serializer=None, context=None)`.

- `method` is the queue-qualified path; the last segment is dispatched as the handler leaf.
- `args` and `kwds` are passed to the handler (subject to server-side middleware/validation/DI).
- A plain method name targets the calling app's default queue.
- `client_timeout` limits client wait for the response; `server_timeout` limits handler execution on the server (sent as `headers["server_timeout"]`). `ttl`/`rttl` are transport lifetimes. Negative timeout values are normalized to `0`.

## Errors and timeouts

- Handler exceptions are converted to error responses (`RPCError` on the client) unless handled by a registered `@app.exception_handler(...)`.
- `METHOD_NOT_FOUND` is returned when no handler matches on the target queue.
- Client wait timeout raises `RPCError` with `error_code="Timeout"` (`"Client wait timed out"`). Server handler execution timeout is returned as `ErrorCode.TIMEOUT` (`"Handler execution timed out"`).
- See [Error handling](error-handling.md).

## Consuming

```python
rpc = app.rpc()

await rpc.consume()       # start consuming for this RPC server
await rpc.stop_consume()  # pause this RPC server (resumable)
```

Consuming is controlled per RPC server (no per-method stop, no global app consume/stop, no mode argument).
