# Events

Events are published to a topic and delivered to matching subscribers on buses. Publishing is done via `app.client.emit(...)`; subscriptions and consuming are managed per `Bus`.

## Subscribing

```python
from linkr import App, LocalBackend

app = App("billing", backend=LocalBackend())
bus = app.bus()

@bus.subscribe("user.created")
def on_user_created(user_id: int) -> None:
    ...

@bus.subscribe("user.*")
def on_any_user_event(user_id: int) -> None:
    ...
```

- `@bus.subscribe(topic_pattern, **options)` registers a subscriber on this bus. It must be used while the bus is not consuming.
- `await bus.add_subscriber(topic_pattern, handler, **options)` registers a subscriber at runtime.
  - If the bus is not consuming, it is applied on the next `consume()`.
  - If the bus is consuming and the topic pattern is already bound, the subscriber becomes effective for subsequent matching events.
  - If the bus is consuming and this is a new topic pattern, the backend binds it dynamically (no `stop_consume()`/`consume()`), when the backend supports dynamic event binding; events published before binding completes are not delivered.
- `await bus.remove_subscriber(topic_pattern, handler=None)` removes a specific subscriber or the whole topic pattern.
  - Removing the last handler of a pattern while consuming unbinds the topic from the active event queue (when bound).
  - Idle changes affect the next `consume()`.
- Each bus has its **own event queue**; subscriptions on a bus are consumed only by that bus.
- Supported handler options (see [Configuration](../reference/configuration.md)): e.g. `validate_types`.
- The event payload is passed to handlers as keyword arguments.
- Sync and async handlers are supported.

## Emitting

```python
app = App("billing", backend=LocalBackend())
bus = app.bus()

@bus.subscribe("user.created")
def on_user_created(user_id: int) -> None:
    ...

await app.init()
await bus.consume()
await app.client.emit("user.created", {"user_id": 1})
```

Signature: `app.client.emit(topic, event: dict, *, request_id=None, client_timeout=None, ttl=None, serializer=None, context=None)`.

- `topic` is the concrete topic string to publish.
- `event` must be a `dict` of keyword arguments delivered to matching subscribers.
- `emit` returns `None`.

## Matching and delivery

- **Fan-out:** all buses with subscriptions whose patterns match the emitted topic receive the event (on their own event queues).
- **Multiple subscribers in one bus:** if multiple handlers in the same bus match, all of them are invoked.
- Wildcards:
  - `*` matches exactly one segment (e.g. `user.*` matches `user.created`).
  - `#` matches zero or more segments (e.g. `user.#` matches `user.created`, `user.profile.updated`).

## Errors and validation

- Subscriber exceptions are logged; they do not produce a response to the publisher.
- Validation failures (when `validate_types` is enabled) prevent that subscriber invocation and are logged/dropped according to hooks.

## Consuming

```python
bus = app.bus()

await bus.consume()       # start consuming for this bus
await bus.stop_consume()  # pause this bus (resumable)
```

Consuming is controlled per bus. Stopping event consuming on a bus pauses its subscriptions without removing them. While consuming, use `add_subscriber()` / `remove_subscriber()` for runtime changes.
