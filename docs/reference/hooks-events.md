# Hooks events reference

All hook events are `NamedTuple` types emitted by `App` through `app.hooks` / `AppHooks`. Listeners may be sync or async; exceptions in listeners are logged and isolated.

## Consume lifecycle

### `ConsumeStartEvent` — `on_consume_start`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `mode` | `"rpc" \| "task" \| "event"` | Delivery mode |
| `queue` | `str` | Queue name (event: instance event queue) |
| `topics` | `tuple[str, ...]` | Subscribed topic patterns for event mode; empty otherwise |

Emitted when consuming starts for a queue.

### `ConsumeStopEvent` — `on_consume_stop`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `mode` | `"rpc" \| "task" \| "event"` | Delivery mode |
| `queue` | `str \| None` | Queue name; `None` for mass stop in some cases |
| `topic` | `str \| None` | Event topic pattern when a specific subscription is removed |
| `reason` | `"all" \| "queue_drained" \| "topic_unsubscribed"` | Stop reason |

- `all`: all consumers of the mode stopped
- `queue_drained`: queue consumer stopped (no remaining handlers/subscriptions)
- `topic_unsubscribed`: only the specified topic subscription removed; consumer remains active

## Client-side

### `RequestPublishedEvent` — `on_request_published`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `request` | `Request` | Full request object |
| `outcome` | `"ok" \| "error"` | Publish/send succeeded or failed |
| `error` | `BaseException \| None` | Error when `outcome="error"` |

Emitted after the publish phase completes. For RPC this occurs once the backend request is registered and a response future is obtained (before awaiting the response). For task/event it occurs after successful backend send/emit. Emitted with `outcome="error"` if publishing fails.

### `ResponseReceivedEvent` — `on_response_received` (RPC)

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `request` | `Request` | Full request object |
| `response` | `Any` | Result data when `outcome="ok"`, else `None` |
| `outcome` | `"ok" \| "error" \| "timeout"` | RPC outcome |
| `error` | `BaseException \| None` | Error (e.g. `RPCError`) on failure/timeout |

Emitted after the RPC outcome is known. Not emitted for task/event, and not emitted if the request was not successfully published.

## Server-side receive and dispatch

### `RequestReceivedEvent` — `on_request_received`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `mode` | `"rpc" \| "task" \| "event"` | Request mode |
| `queue` | `str` | Receiving queue |
| `request` | `Request` | Deserialized request |

Emitted after successful deserialization, before dispatch.

### `BeforeHandlerEvent` — `before_handler`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `mode` | `"rpc" \| "task" \| "event"` | Mode |
| `queue` | `str` | Queue |
| `request` | `Request` | Request |
| `handler` | `HandlerInfo` | Target handler metadata |
| `topic` | `str \| None` | Event topic for subscriber invocation; otherwise `None` |

Emitted before handler execution (after validation). For events, emitted per subscriber.

### `AfterHandlerEvent` — `after_handler`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `mode` | `"rpc" \| "task" \| "event"` | Mode |
| `queue` | `str` | Queue |
| `request` | `Request` | Request |
| `handler` | `HandlerInfo` | Handler metadata |
| `topic` | `str \| None` | Event topic or `None` |
| `result` | `Any` | Handler result (RPC); typically `None` for task/event |

Emitted on successful handler completion.

### `HandlerErrorEvent` — `on_handler_error`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `mode` | `"rpc" \| "task" \| "event"` | Mode |
| `queue` | `str` | Queue |
| `request` | `Request` | Request |
| `handler` | `HandlerInfo` | Handler metadata |
| `topic` | `str \| None` | Event topic or `None` |
| `error` | `BaseException \| None` | Raised exception |
| `error_code` | `str \| None` | Error code (e.g. RPC/internal) |
| `error_message` | `str \| None` | Error message |
| `outcome` | `"error" \| "timeout"` | Failure kind |

Emitted when a handler raises or times out.

### `MessageDroppedEvent` — `on_message_dropped`

| Field | Type | Description |
|---|---|---|
| `app` | `str` | Application name |
| `mode` | `"rpc" \| "task" \| "event" \| None` | Mode when known |
| `queue` | `str \| None` | Queue when known |
| `reason` | `DropReason` | Drop reason |
| `request` | `Request \| None` | Request when available |
| `handler` | `HandlerInfo \| None` | Handler when applicable |
| `topic` | `str \| None` | Event topic when applicable |
| `error` | `BaseException \| None` | Associated error |
| `details` | `Any` | Additional details (e.g. validation errors) |

**`DropReason`:** `invalid_request`, `handler_not_found`, `validation_failed`, `no_subscribers`, `decode_error`.

## Registration helpers

- `AppHooks`: base class with `on_*`/`before_handler`/`after_handler` methods to override
- `HookRegistry` (`app.hooks`): `on_consume_start`, `on_consume_stop`, `on_request_published`, `on_response_received`, `on_request_received`, `before_handler`, `after_handler`, `on_handler_error`, `on_message_dropped` hook points (`subscribe`, `unsubscribe`, decorator call)
- `app.add_hooks(observer)` / `app.remove_hooks(observer)`
