# Client API

`App` provides three client methods corresponding to delivery modes: `call` (RPC), `send` (task), and `emit` (event).

## `call` — RPC

```python
result = await app.call(
    "orders.math.add",
    (1, 2),
    {},
    request_id=None,
    timeout=5.0,
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
| `timeout` | `float \| None` | RPC timeout (seconds), sent in request headers |
| `ttl` | `float \| None` | Message TTL (seconds) |
| `rttl` | `float \| None` | Response TTL (seconds) |
| `serializer` | `str \| None` | Serializer name for this call |
| `context` | `dict[str, Any] \| None` | Context forwarded to middleware/backend |

**Returns:** handler result (`Any`). **Raises:** `RPCError` on error or timeout. The client waits for a response up to `timeout + ttl` seconds.

## `send` — Task

```python
await app.send(
    "worker.email.send",
    (),
    {"user_id": 42},
    request_id=None,
    timeout=None,
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
| `timeout` | `float \| None` | Task timeout (seconds), sent in request headers |
| `ttl` | `float \| None` | Message TTL (seconds) |
| `serializer` | `str \| None` | Serializer name for this call |
| `context` | `dict[str, Any] \| None` | Context forwarded to middleware/backend |

**Returns:** `None` (fire-and-forget). Completion of `send` means the client-side publish completed; no handler result or error is returned to the caller.

## `emit` — Event

```python
await app.emit(
    "user.created",
    {"user_id": 42, "email": "u@example.com"},
    request_id=None,
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
| `ttl` | `float \| None` | Message TTL (seconds) |
| `serializer` | `str \| None` | Serializer name for this call |
| `context` | `dict[str, Any] \| None` | Context forwarded to middleware/backend |

**Returns:** `None`.

## Argument conventions

- `args` and `kwds` are always explicit parameters (not `*args/**kwargs`):
  - positional-only call: `await app.call("app.m", (1, 2), {})`
  - keyword call: `await app.call("app.m", (), {"x": 1, "y": 2})`
- The routed method/task name in the message payload is the **last segment** of the provided path; the target queue is resolved from the path (see [Routing and naming](../concepts/routing-and-naming.md)).

## Defaults and headers

- App-level defaults: `App(..., timeout=..., ttl=..., rttl=...)`.
- RPC built-in defaults when unset: `RPC_TIMEOUT = 300.0`, `RPC_TTL = 300.0`.
- Effective values are carried in request headers (`queue`, `timeout`, `ttl`, `rttl`) and influence client waiting/handler behavior as applicable.

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
