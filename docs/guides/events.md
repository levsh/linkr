# Events

Events implement publish/subscribe: an emitted event is delivered to all matching subscribers (fan-out). Event handlers do not return responses.

## Subscribing

```python
from linkr import App, LocalBackend

app = App("billing", backend=LocalBackend())

@app.subscribe("user.created")
def on_user_created(user_id: int) -> None:
    ...

@app.subscribe("user.*")
def on_any_user_event(user_id: int | None = None, **event: object) -> None:
    ...
```

- `@app.subscribe(topic_pattern, **options)` registers a subscriber.
- Each app instance receives events on its own event queue (`<app>.events.<instance_id>`), so multiple instances can all receive the same event.
- Multiple subscribers in the same app for the same topic are all invoked.
- `options` can include `validate_types=True|False`.

## Emitting events

```python
await app.consume()

await app.emit("user.created", {"user_id": 7})
```

Signature:

```python
await app.emit(
    topic: str,
    event: dict[str, Any],
    *,
    request_id: UUID | None = None,
    ttl: float | None = None,
    serializer: str | None = None,
    context: dict[str, Any] | None = None,
) -> None
```

- `topic` is the concrete topic string to publish.
- `event` is a dictionary passed to subscribers as **keyword arguments**.
- Returns `None`.

## Topic patterns and wildcards

Subscriptions use topic patterns:

- `*` — matches exactly one segment (e.g. `user.*` matches `user.created`, not `user.profile.updated`)
- `#` — matches zero or more segments (e.g. `user.#` matches `user.created` and `user.profile.updated`)

Patterns are matched against the full topic.

```python
@app.subscribe("order.#")
def on_order(topic_data: dict | None = None, **event): ...

@app.subscribe("audit.*.created")
def on_created(**event): ...
```

## Delivery and errors

- **Fan-out:** every subscribed instance with a matching pattern receives the event.
- **No response:** subscribers cannot return a value to the emitter.
- **No matching subscribers:** the event is not delivered to handlers (server-side drop/no-op behavior may be logged; hooks can observe this).
- **Validation/exceptions:** argument validation failures and handler exceptions are logged and do not produce responses to the emitter. When multiple subscribers are invoked, failures are isolated per subscriber.

## Observability

Use [hooks](hooks.md) for monitoring:

- Client: `on_request_published` (emit sent)
- Server: `on_request_received`, `before_handler`/`after_handler`/`on_handler_error` (per subscriber), `on_message_dropped` with reason `no_subscribers` or `validation_failed`

## Example: fan-out to multiple instances

```python
import asyncio
from linkr import App, LocalBackend

backend = LocalBackend()
billing_a = App("billing", backend=backend)
billing_b = App("billing", backend=backend)
producer = App("producer", backend=backend)

seen_a, seen_b = [], []

@billing_a.subscribe("user.created")
def a(user_id: int): seen_a.append(user_id)

@billing_b.subscribe("user.created")
def b(user_id: int): seen_b.append(user_id)

async def main():
    async with billing_a, billing_b, producer:
        await billing_a.consume()
        await billing_b.consume()
        await producer.emit("user.created", {"user_id": 1})
        await asyncio.sleep(0)

asyncio.run(main())
```
