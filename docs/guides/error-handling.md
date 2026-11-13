# Error handling

Error behavior differs by delivery mode: RPC can return structured errors to the caller, while tasks and events report failures through logging and hooks.

## Exception hierarchy

All exceptions raised by linkr derive from `LinkrError`:

- `RPCError` — error resulting from an RPC call (server error response or client-side timeout). Carries `error_code`, `error_message`, `error_details`.
- `RequestError` — local or transport-level error when preparing/sending a request (e.g. unknown serializer, serialization failure). Not tied to an RPC response.

```python
from linkr import LinkrError, RPCError, RequestError

try:
    await app.call("orders.missing", (), {})
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
    await app.call("orders.missing", (), {})
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

@app.method("add", validate_types=True)
def add(x: int, y: int) -> int:
    return x + y

@app.task("process", validate_types=False)
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

- Client/RPC call timeout behavior can raise `RPCError` with `ErrorCode.TIMEOUT` when a response is not received in time.
- Handler execution may also time out (surfaced as timeout errors per mode).

Configure defaults via `App(..., timeout=..., ttl=..., rttl=...)` and per-call parameters where supported (see [Client API](client-api.md)).

## Tasks and events

Tasks and events have no response channel to the caller:

- Handler exceptions: logged (and emitted via `on_handler_error` hooks).
- Validation failures: logged and emitted via `on_message_dropped` (`validation_failed`); handler not invoked.
- Missing handler/subscribers: logged and/or emitted via `on_message_dropped` (`handler_not_found`, `no_subscribers`).

Use [hooks](hooks.md) and application logging for monitoring and reaction (metrics, alerting, audits).

## Example

```python
from linkr import App, ErrorCode, LocalBackend, RPCError

app = App("svc", backend=LocalBackend(), validate_types=True)

@app.exception_handler(PermissionError)
def on_permission(exc: Exception) -> tuple[str, str]:
    return ("PermissionDenied", str(exc))

@app.method("secure")
def secure() -> None:
    raise PermissionError("denied")

async def main() -> None:
    async with app:
        await app.consume()
        try:
            await app.call("svc.secure", (), {})
        except RPCError as exc:
            assert exc.error_code == "PermissionDenied"
```
