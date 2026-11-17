# Error handling

Error behavior differs by delivery mode: RPC can return structured errors to the caller, while tasks and events report failures through logging and hooks.

## Exception hierarchy

All exceptions raised by linkr derive from `LinkrError`:

- `RPCError` — error resulting from an RPC call (server error response or client-side timeout). Carries `error_code`, `error_message`, `error_details`.
- `RequestError` — local or transport-level error when preparing/sending a request (e.g. unknown serializer, serialization failure). Not tied to an RPC response.

```python
from linkr import LinkrError, RPCError, RequestError

try:
    await app.client.call("orders.missing", (), {})
except RPCError as exc:
    print(exc.error_code)     # e.g. "MethodNotFound"
except RequestError as exc:
    print(exc.error_message)
except LinkrError as exc:
    print(exc)
```

## `RPCError` and `ErrorCode`

RPC failures raised to callers use `RPCError`:

```python
from linkr import ErrorCode, RPCError

try:
    await app.client.call("orders.missing", (), {})
except RPCError as exc:
    print(exc.error_code)     # e.g. "MethodNotFound"
    print(exc.error_message)
    print(exc.error_details)
```

`ErrorCode` values:

| Code | Meaning |
|---|---|
| `InternalError` | Unhandled/internal handler failure |
| `MethodNotFound` | No RPC handler registered for the target method |
| `Timeout` | Call or handler timed out |
| `ValidationError` | Request/argument validation failure |

## Argument validation (`validate_types`)

Type validation can be enabled globally or per handler:

```python
from linkr import App, LocalBackend

app = App("orders", backend=LocalBackend(), validate_types=True)
rpc = app.rpc()
worker = app.worker()

@rpc.method("add", validate_types=True)
def add(x: int, y: int) -> int:
    return x + y

@worker.task("process", validate_types=False)
def process(data: dict) -> None:
    ...
```

- Per-handler `validate_types` overrides the app default.
- **RPC:** validation failures return an error response and raise `RPCError` (`ValidationError`) on the client.
- **Task/Event:** invalid arguments prevent handler execution; the failure is logged and reported via hooks (no error returned to sender).

## Custom exception handlers (RPC)

Map exceptions from RPC handlers to structured error responses:

```python
@app.exception_handler(ValueError)
def handle_value_error(exc: Exception) -> tuple[str, str, dict[str, str]]:
    return ("InvalidValueError", str(exc), {"field": "x"})

# other supported return forms include dict/str/ErrorInfo-compatible data
```

Registered handlers are used when converting server-side exceptions to RPC error responses.

## Timeouts

- **RPC client wait:** `client_timeout` limits how long the client waits for a response; expiry raises `RPCError` with `ErrorCode.TIMEOUT` (`"Client wait timed out"`).
- **RPC/task handler execution:** `server_timeout` limits handler execution on the server; expiry produces `ErrorCode.TIMEOUT` (`"Handler execution timed out"`) for RPC responses and handler timeout handling for tasks.
- **Publish wait (task/event):** `client_timeout` limits client wait for backend publish/send.
- **Transport TTL:** `ttl` / `rttl` are transport message lifetimes and are independent from client/server timeouts.
- Negative `client_timeout` / `server_timeout` are normalized to `0`. There is no server execution timeout for events.

Configure defaults via `App(..., rpc_client_timeout=..., rpc_server_timeout=..., rpc_ttl=..., rpc_rttl=..., task_client_timeout=..., task_server_timeout=..., task_ttl=..., event_client_timeout=..., event_ttl=...)` and per-call parameters where supported (see [Client API](client-api.md)).

## Tasks and events

Tasks and events have no response channel to the caller:

- Handler exceptions: logged (and emitted via `on_handler_error` hooks).
- Validation failures: logged and emitted via `on_message_dropped` (`validation_failed`); handler not invoked.
- Missing handler/subscribers: logged and/or emitted via `on_message_dropped` (`handler_not_found`, `no_subscribers`).

Use [hooks](hooks.md) and application logging for monitoring and reaction (metrics, alerting, audits).

## Example

```python
from linkr import App, LocalBackend, RPCError

app = App("svc", backend=LocalBackend(), validate_types=True)
rpc = app.rpc()

@app.exception_handler(PermissionError)
def on_permission(exc: Exception) -> tuple[str, str]:
    return ("PermissionDenied", str(exc))

@rpc.method("secure")
def secure() -> None:
    raise PermissionError("denied")

async def main() -> None:
    async with app:
        await rpc.consume()
        try:
            await app.client.call("svc.secure", (), {})
        except RPCError as exc:
            assert exc.error_code == "PermissionDenied"
```
