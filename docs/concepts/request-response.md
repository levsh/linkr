# Request and response model

`linkr` uses typed models for in-app processing and a raw `Message` for wire transport.

## Core models

### `Request`

Represents an incoming/outgoing operation.

| Field | Type | Description |
|---|---|---|
| `id` | `UUID` | Request identifier (default generated) |
| `type` | `RequestType` (`"rpc" \| "task" \| "event"`) | Delivery mode |
| `data` | `dict[str, Any]` | Payload (mode-specific) |
| `headers` | `dict[str, Any]` | Transport/app headers |

`Request` is frozen (`pydantic` model, immutable).

**Payload conventions:**

- RPC: `{"method": "<name>", "args": <tuple>, "kwds": <dict>}`
- Task: `{"task": "<name>", "args": <tuple>, "kwds": <dict>}`
- Event: `{"topic": "<topic>", "event": <dict>}` (event payload passed to subscribers as kwargs)

**Common headers:**

- `queue`: resolved target/physical queue context
- `server_timeout`: handler execution timeout on the server (seconds, rpc/task)
- `ttl`: message TTL in transport (seconds)
- `rttl`: response TTL in transport (seconds, RPC)

`client_timeout` is client-local and is not included in request headers.

### `Response`

Represents an RPC response.

| Field | Type | Description |
|---|---|---|
| `id` | `UUID` | Matches the request id |
| `type` | `ResponseType` (`"result" \| "error"`) | Outcome |
| `data` | `Any` | Result value or error payload |
| `headers` | `dict[str, Any]` | Response headers (e.g. propagated `rttl`) |

`Response` is frozen. Task and event flows do not produce responses to callers.

### `ErrorInfo`

Structured error payload used in error responses:

| Field | Type | Description |
|---|---|---|
| `error_code` | `str` | Machine-readable code |
| `error_message` | `str` | Human-readable message |
| `error_details` | `dict[str, Any] \| None` | Optional structured details |

### `Message`

Raw wire envelope:

| Field | Type | Description |
|---|---|---|
| `data` | `bytes` | Serialized payload |
| `headers` | `dict[str, Any]` | Wire headers (e.g. `content_type`, `serializer`, `content_encoding`) |

### `HandlerInfo`

Metadata for registered handlers (name, request type, function, signature, options, dependency info, queue).

## Client parameters and defaults

`App` accepts default timing parameters per delivery mode:

- `rpc_client_timeout`, `rpc_server_timeout`, `rpc_ttl`, `rpc_rttl`: defaults for RPC client wait, handler execution, request TTL, and response TTL (seconds)
- `task_client_timeout`, `task_server_timeout`, `task_ttl`: defaults for task client publish wait, handler execution, and request TTL (seconds)
- `event_client_timeout`, `event_ttl`: defaults for event client publish wait and event TTL (seconds)

Built-in class defaults: `RPC_CLIENT_TIMEOUT = 60.0`, `RPC_SERVER_TIMEOUT = 60.0`, `RPC_TTL = 60.0`, `RPC_RTTL = 60.0`, `TASK_CLIENT_TIMEOUT = 30.0`, `TASK_SERVER_TIMEOUT = 3600.0`, `TASK_TTL = 3600.0`, `EVENT_CLIENT_TIMEOUT = 30.0`, `EVENT_TTL = 3600.0`.

- `client_timeout` limits client-side waiting (RPC response or publish for task/event) and is not sent in request headers.
- `server_timeout` limits handler execution on the server (`rpc`/`task`) and is sent as `headers["server_timeout"]`.
- `ttl` / `rttl` are transport lifetimes and are independent from client/server timeouts (no `client_timeout + ttl` wait calculation).
- Negative `client_timeout` / `server_timeout` values are normalized to `0`. There is no server execution timeout for events.

Per-call overrides are available on `call`/`send`/`emit` (where applicable): `request_id`, `client_timeout`, `server_timeout` (`call`/`send`), `ttl`, `rttl` (RPC). See [Client API](../guides/client-api.md).

## Error responses

On the server, RPC failures are converted to `Response(type="error", data=ErrorInfo(...))` (based on `ErrorCode`, validation, handler exceptions, or custom exception handlers). On the client, error responses raise `RPCError`.

Tasks and events do not return error responses; failures are surfaced via logging and hooks. See [Error handling](../guides/error-handling.md).
