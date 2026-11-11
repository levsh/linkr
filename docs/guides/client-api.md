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
    ttl=60.0,
    context={"source": "api"},
)
```

**Parameters:** `task, args, kwds, *, request_id, timeout, ttl, serializer, context`

**Returns:** `None` (fire-and-forget). No handler result or error is returned to the caller.

## `emit` — Event

```python
await app.emit(
    "user.created",
    {"user_id": 42, "email": "u@example.com"},
    ttl=60.0,
)
```

**Parameters:** `topic, event, *, request_id, ttl, serializer, context`

- `event` is a `dict` passed to subscribers as keyword arguments.

**Returns:** `None`.

## Argument conventions

- `args` and `kwds` are always explicit parameters (not `*args/**kwargs`):
  - positional-only call: `await app.call("app.m", (1, 2), {})`
  - keyword call: `await app.call("app.m", (), {"x": 1, "y": 2})`
- The routed method/task name in the message payload is the **last segment** of the provided path; the target queue is resolved from the path (see [Routing and naming](../concepts/routing-and-naming.md)).

## Defaults and headers

- App-level defaults: `App(..., timeout=..., ttl=..., rttl=...)`.
- RPC built-in defaults when unset: `RPC_TIMEOUT = 3600.0`, `RPC_TTL = 3600.0`.
- Effective values are carried in request headers (`queue`, `timeout`, `ttl`, `rttl`) and influence client waiting/handler behavior as applicable.

## Serializer selection

If multiple serializers are configured on `App`, the first is used by default for outgoing messages; pass `serializer="<name>"` (e.g. `"linkr1.0"`, `"jsonrpc2.0"`) to select one explicitly where supported.

!!! note "JSON-RPC scope"
    `JsonRpcSerializer` (`jsonrpc2.0`) supports RPC requests only. Use `JsonSerializer` for tasks and events (or choose it explicitly in mixed configurations).
