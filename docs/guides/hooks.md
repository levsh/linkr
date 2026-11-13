# Hooks

Hooks provide an observer mechanism for app-level events: consume lifecycle, client publish/response, server receive/dispatch, handler execution, and dropped messages. Hooks do not participate in the `call_next` chain and must not be used to alter routing or results.

## Registering hooks

```python
from linkr import AfterHandlerEvent, App, AppHooks, HandlerErrorEvent, LocalBackend

app = App("orders", backend=LocalBackend())

class MyHooks(AppHooks):
    async def after_handler(self, event: AfterHandlerEvent) -> None:
        print(event.handler.name, event.result)

app.add_hooks(MyHooks())
```

Ways to subscribe:

- `app.add_hooks(observer)` / `app.remove_hooks(observer)` — register an `AppHooks` instance (overridden methods are subscribed).
- `app.hooks.<event>.subscribe(listener)` / `.unsubscribe(listener)` — subscribe a function to a specific event.
- `@app.hook.<event>` or `@app.hooks.<event>` — decorator subscription.

```python
@app.hooks.on_handler_error
async def on_error(event: HandlerErrorEvent) -> None:
    ...
```

Listeners may be sync or async. Multiple listeners per event are supported (fan-out, registration order). Listener exceptions are caught and logged so they do not break message processing.

## Client-side events

### `RequestPublishedEvent` (`on_request_published`)

Emitted when a client request is published/sent to the backend (publish phase completed).

| Field | Type | Description |
|---|---|---|
| `app` | `str` | App name |
| `request` | `Request` | Full request (`id`, `type`, `data`, `headers`) |
| `outcome` | `"ok" \| "error"` | Publish succeeded or failed |
| `error` | `BaseException \| None` | Error if publish failed |

- RPC: emitted after the backend returns a response future (send/registration done), before awaiting the response.
- Task/Event: emitted after successful `backend.send/emit`.
- If publishing fails before completion, emitted with `outcome="error"`.

### `ResponseReceivedEvent` (`on_response_received`, RPC only)

Emitted after an RPC response outcome is resolved.

| Field | Type | Description |
|---|---|---|
| `app` | `str` | App name |
| `request` | `Request` | Full request |
| `response` | `Any` | Result data on `ok`, else `None` |
| `outcome` | `"ok" \| "error" \| "timeout"` | Call outcome |
| `error` | `BaseException \| None` | `RPCError`/exception on failure or timeout |

Not emitted for tasks/events. Not emitted if the request was never successfully published.

## Server-side events

### `RequestReceivedEvent` (`on_request_received`)

Emitted after an incoming message is successfully deserialized to a `Request`.

Fields: `app`, `mode`, `queue`, `request`.

### `BeforeHandlerEvent` (`before_handler`)

Emitted immediately before executing a resolved handler (after validation passes).

Fields: `app`, `mode`, `queue`, `request`, `handler` (`HandlerInfo`), `topic` (event subscriber topic/name, else `None`).

For events, emitted per matching subscriber.

### `AfterHandlerEvent` (`after_handler`)

Emitted after a handler completes successfully.

Fields: `app`, `mode`, `queue`, `request`, `handler`, `topic`, `result`.

- RPC: `result` is the handler return value.
- Task/Event: `result` is typically `None`/unused.

### `HandlerErrorEvent` (`on_handler_error`)

Emitted when handler execution fails or times out.

Fields: `app`, `mode`, `queue`, `request`, `handler`, `topic`, `error`, `error_code`, `error_message`, `outcome` (`"error" | "timeout"`).

### `MessageDroppedEvent` (`on_message_dropped`)

Emitted when a message is not handled.

| Reason | Meaning |
|---|---|
| `decode_error` | Incoming message could not be deserialized/handled at receive stage |
| `invalid_request` | Missing/invalid method or task name |
| `handler_not_found` | No registered RPC/task handler for the target |
| `validation_failed` | Argument validation failed |
| `no_subscribers` | No event subscribers matched the topic |

Fields: `app`, `mode`, `queue`, `reason`, `request`, `handler`, `topic`, `error`, `details`.

## Consume lifecycle events

### `ConsumeStartEvent` (`on_consume_start`)

Emitted when consuming starts for a queue.

Fields: `app`, `mode`, `queue`, `topics` (tuple of patterns for event mode, else empty).

### `ConsumeStopEvent` (`on_consume_stop`)

Emitted when consuming stops or a subscription is removed.

| Reason | Meaning |
|---|---|
| `all` | All consumers of the mode stopped |
| `queue_drained` | Queue consumer stopped because no handlers/subscriptions remain |
| `topic_unsubscribed` | Only a specific event topic subscription was removed; consumer remains active |

Fields: `app`, `mode`, `queue`, `topic`, `reason`.

## Example: metrics and tracing hooks

```python
from linkr import (
    AfterHandlerEvent,
    AppHooks,
    HandlerErrorEvent,
    MessageDroppedEvent,
    RequestPublishedEvent,
    ResponseReceivedEvent,
)

class MetricsHooks(AppHooks):
    async def on_request_published(self, event: RequestPublishedEvent) -> None:
        metrics.increment("published", event.request.type, event.outcome)

    async def on_response_received(self, event: ResponseReceivedEvent) -> None:
        metrics.increment("rpc_response", event.outcome)

    async def after_handler(self, event: AfterHandlerEvent) -> None:
        metrics.increment("handler_ok", event.mode, event.handler.name)

    async def on_handler_error(self, event: HandlerErrorEvent) -> None:
        metrics.increment("handler_error", event.mode, event.outcome)

    async def on_message_dropped(self, event: MessageDroppedEvent) -> None:
        metrics.increment("dropped", event.mode, event.reason)

app.add_hooks(MetricsHooks())
```

See [Hooks events reference](../reference/hooks-events.md) for the full event catalog.
