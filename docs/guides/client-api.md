# Client API

`App` provides three client methods corresponding to delivery modes: `call` (RPC), `send` (task), and `emit` (event).

## `call` — RPC

```python
result = await app.client.call(
    "orders.math.add",
    (1, 2),
    {},
    request_id=None,
    client_timeout=5.0,
    server_timeout=5.0,
    ttl=30.0,
    rttl=30.0,
    serializer="linkr1.0",
    context={"trace_id": "..."},
)
```

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `method` | `str` | Fully qualified method path (queue-resolved) |
| `args` | `tuple[Any, ...]` | Positional arguments |
| `kwds` | `dict[str, Any]` | Keyword arguments |
| `request_id` | `UUID \| None` | Optional request id (generated if omitted) |
| `client_timeout` | `float \| None` | Client wait timeout for the response (seconds) |
| `server_timeout` | `float \| None` | Handler execution timeout on the server (seconds), sent in request headers |
| `ttl` | `float \| None` | Request message TTL in transport (seconds) |
| `rttl` | `float \| None` | Response message TTL in transport (seconds) |
| `serializer` | `str \| None` | Serializer name for this call |
| `context` | `dict[str, Any] \| None` | Context forwarded to middleware/backend |

**Returns:** handler result (`Any`). **Raises:** `RPCError` on error or timeout.

- `client_timeout` limits how long the client waits for a response (`"Client wait timed out"` on expiry). It is local and not sent to the server.
- `server_timeout` limits handler execution on the server (`"Handler execution timed out"` in the error response on expiry).
- `ttl` / `rttl` are transport lifetimes (e.g. broker expiration) and are independent from client/server timeouts.
- Negative `client_timeout` / `server_timeout` are normalized to `0`.

## `send` — Task

```python
await app.client.send(
    "worker.email.send",
    (),
    {"user_id": 42},
    request_id=None,
    client_timeout=5.0,
    server_timeout=30.0,
    ttl=60.0,
    serializer="linkr1.0",
    context={"source": "api"},
)
```

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `task` | `str` | Fully qualified task path (queue-resolved) |
| `args` | `tuple[Any, ...]` | Positional arguments |
| `kwds` | `dict[str, Any]` | Keyword arguments |
| `request_id` | `UUID \| None` | Optional request id (generated if omitted) |
| `client_timeout` | `float \| None` | Client wait timeout for publish/send (seconds) |
| `server_timeout` | `float \| None` | Handler execution timeout on the server (seconds), sent in request headers |
| `ttl` | `float \| None` | Message TTL in transport (seconds) |
| `serializer` | `str \| None` | Serializer name for this call |
| `context` | `dict[str, Any] \| None` | Context forwarded to middleware/backend |

**Returns:** `None` (fire-and-forget). Completion of `send` means the client-side publish completed (within `client_timeout`, if set); no handler result or error is returned to the caller.

## `emit` — Event

```python
await app.client.emit(
    "user.created",
    {"user_id": 42, "email": "u@example.com"},
    request_id=None,
    client_timeout=5.0,
    ttl=60.0,
    serializer="linkr1.0",
    context={"trace_id": "..."},
)
```

**Parameters**

| Parameter | Type | Description |
|---|---|---|
| `topic` | `str` | Event topic to publish |
| `event` | `dict[str, Any]` | Event payload passed to subscribers as keyword arguments |
| `request_id` | `UUID \| None` | Optional request id (generated if omitted) |
| `client_timeout` | `float \| None` | Client wait timeout for publish/emit (seconds) |
| `ttl` | `float \| None` | Message TTL in transport (seconds) |
| `serializer` | `str \| None` | Serializer name for this call |
| `context` | `dict[str, Any] \| None` | Context forwarded to middleware/backend |

**Returns:** `None`. `emit` does not accept `server_timeout` (subscriber execution is not time-limited by a server timeout).

## Argument conventions

- `args` and `kwds` are always explicit parameters (not `*args/**kwargs`):
  - positional-only call: `await app.client.call("app.m", (1, 2), {})`
  - keyword call: `await app.client.call("app.m", (), {"x": 1, "y": 2})`
- The routed method/task name in the message payload is the **last segment** of the provided path; the target queue is resolved from the path (see [Routing and naming](../concepts/routing-and-naming.md)).

## Defaults and headers

- App-level defaults: `App(..., rpc_client_timeout=..., rpc_server_timeout=..., rpc_ttl=..., rpc_rttl=..., task_client_timeout=..., task_server_timeout=..., task_ttl=..., event_client_timeout=..., event_ttl=...)`.
  - Class defaults: `RPC_CLIENT_TIMEOUT = 60.0`, `RPC_SERVER_TIMEOUT = 60.0`, `RPC_TTL = 60.0`, `RPC_RTTL = 60.0`, `TASK_CLIENT_TIMEOUT = 30.0`, `TASK_SERVER_TIMEOUT = 3600.0`, `TASK_TTL = 3600.0`, `EVENT_CLIENT_TIMEOUT = 30.0`, `EVENT_TTL = 3600.0`.
- Per-call `client_timeout` / `server_timeout` / `ttl` / `rttl` (where applicable) override the corresponding app defaults.
- Effective request headers: `queue`, `server_timeout` (rpc/task), `ttl`, `rttl` (rpc). `client_timeout` is not sent over the wire.
- Negative `client_timeout` / `server_timeout` are normalized to `0`. `None` means no limit for that dimension (or fall back to default where applicable).
- `ttl` / `rttl` are transport-level lifetimes and are independent from client/server timeouts (there is no `client_timeout + ttl` wait calculation).

## Serializer selection

If multiple serializers are configured on `App`, the first is used by default for outgoing messages; pass `serializer="<name>"` (e.g. `"linkr1.0"`, `"jsonrpc2.0"`) to select one explicitly where supported.

!!! note "JSON-RPC scope"
    `JsonRpcSerializer` (`jsonrpc2.0`) supports RPC requests only. Use `JsonSerializer` for tasks and events (or choose it explicitly in mixed configurations).

## `context` — call-level metadata

`context` is an optional `dict[str, Any]` passed to `call`/`send`/`emit` that carries **out-of-band** data for the current invocation. It is:

- **Not serialized** — it does not appear in `Request.data` (the handler never sees it; use `args`/`kwds` or `event` for that).
- **Not forwarded to the server handler** — only `Request` is available server-side (via `get_current_request()`).
- **Propagated along the client pipeline** as `context` to:
  - `AppMiddleware.dispatch_client(call_next, request, *, context)` (app-level)
  - `WireMiddleware.dispatch_client(call_next, request, request_message, *, context)` (wire-level)
  - `Backend.request/send/emit(..., context=...)` (transport)

Typical uses: tracing/correlation ids, per-call headers, backend-specific options, metrics tags. Interpretation is middleware- and backend-specific (built-in `LocalBackend` accepts but ignores it).
