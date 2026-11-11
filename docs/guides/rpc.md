# RPC

RPC provides request/response calls: one consumer handles the request and returns a result (or an error).

## Registering methods

```python
from linkr import App, LocalBackend

app = App("orders", backend=LocalBackend())

@app.method("ping")
def ping() -> str:
    return "pong"

@app.method("math.add")
def add(x: int, y: int) -> int:
    return x + y
```

- `@app.method(name, **options)` registers an RPC handler.
- Dotted names affect queue routing (see [Routing and naming](../concepts/routing-and-naming.md)).
- `options` can include `validate_types=True|False` (overrides app-level default).

Handlers may be sync or async and may declare dependencies via `Depends` (see [Dependency injection](dependency-injection.md)).

## Calling methods

```python
await app.consume()

result = await app.call("orders.ping", (), {})
sum_ = await app.call("orders.math.add", (1, 2), {})
sum_kw = await app.call("orders.math.add", (), {"x": 1, "y": 2})
```

Signature:

```python
await app.call(
    method: str,
    args: tuple[Any, ...],
    kwds: dict[str, Any],
    *,
    request_id: UUID | None = None,
    timeout: float | None = None,
    ttl: float | None = None,
    rttl: float | None = None,
    serializer: str | None = None,
    context: dict[str, Any] | None = None,
) -> Any
```

- `method` is the fully qualified path targeting the queue (e.g. `orders.ping`, `orders.math.add`).
- Positional and keyword arguments are passed as separate `args`/`kwds`.
- On success, returns the handler result. On error/timeout, raises `RPCError`.

## Timeouts and TTL

- Per-call: `timeout`, `ttl`, `rttl`.
- App defaults: `App(..., timeout=..., ttl=..., rttl=...)`.
- Built-in RPC defaults: `RPC_TIMEOUT = 3600.0`, `RPC_TTL = 3600.0`.

Client-side waiting is bounded by the effective timeout/ttl window; if no response arrives in time, `RPCError` with `ErrorCode.TIMEOUT` is raised.

## Errors

Common cases surfaced as `RPCError`:

- `ErrorCode.METHOD_NOT_FOUND` — no handler for the requested method on the queue
- `ErrorCode.VALIDATION_ERROR` — invalid request/arguments (when validation applies)
- `ErrorCode.TIMEOUT` — handler or call timed out
- `ErrorCode.INTERNAL_ERROR` (or custom mapped code) — handler failure

```python
from linkr import ErrorCode, RPCError

try:
    await app.call("orders.missing", (), {})
except RPCError as exc:
    print(exc.error_code, exc.error_message, exc.error_details)
```

Custom exception mapping for RPC handlers is available via `@app.exception_handler(...)` (see [Error handling](error-handling.md)).

## Cross-app calls

```python
server = App("server", backend=backend)
client = App("client", backend=backend)

@server.method("echo")
def echo(value): return value

async with server, client:
    await server.consume()
    assert await client.call("server.echo", ("hi",), {}) == "hi"
```

Both apps must use a backend/topology that allows them to reach the target queue.
