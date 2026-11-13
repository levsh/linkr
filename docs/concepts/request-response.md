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
- `timeout`: client/handler timeout (seconds)
- `ttl`: message TTL (seconds)
- `rttl`: response TTL (seconds, RPC)

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

`App` accepts default timing parameters:

- `timeout`: default RPC timeout (seconds) for outgoing requests (sent in request headers)
- `ttl`: default message TTL (seconds) for outgoing requests
- `rttl`: default response TTL (seconds) for outgoing RPC requests

Built-in defaults used for RPC when not set: `RPC_TIMEOUT = 300.0`, `RPC_TTL = 300.0`. The client waits for an RPC response up to `timeout + ttl` seconds.

Per-call overrides are available on `call`/`send`/`emit` (where applicable): `request_id`, `timeout`, `ttl`, `rttl` (RPC). See [Client API](../guides/client-api.md).

## Error responses

On the server, RPC failures are converted to `Response(type="error", data=ErrorInfo(...))` (based on `ErrorCode`, validation, handler exceptions, or custom exception handlers). On the client, error responses raise `RPCError`.

Tasks and events do not return error responses; failures are surfaced via logging and hooks. See [Error handling](../guides/error-handling.md).
