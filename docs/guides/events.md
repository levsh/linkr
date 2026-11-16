# Events

Events are published to a topic and delivered to matching subscribers on servers. Publishing is done via `app.client.emit(...)`; subscriptions and consuming are managed per `Server`.

## Subscribing

```python
from linkr import App, LocalBackend

app = App("billing", backend=LocalBackend())
server = app.server()

@server.subscribe("user.created")
def on_user_created(user_id: int) -> None:
    ...

@server.subscribe("user.*")
def on_any_user_event(user_id: int) -> None:
    ...
```

- `@server.subscribe(topic_pattern, **options)` registers a subscriber on this server.
- Each server has its **own event queue**; subscriptions on a server are consumed only by that server.
- Supported handler options (see [Configuration](../reference/configuration.md)): e.g. `validate_types`.
- The event payload is passed to handlers as keyword arguments.
- Sync and async handlers are supported.

## Emitting

```python
app = App("billing", backend=LocalBackend())
server = app.server()

@server.subscribe("user.created")
def on_user_created(user_id: int) -> None:
    ...

await app.init()
await server.consume(mode="event")
await app.client.emit("user.created", {"user_id": 1})
```

Signature: `app.client.emit(topic, event: dict, *, ttl=None, serializer=None, request_id=None, context=None)`.

- `topic` is the concrete topic string to publish.
- `event` must be a `dict` of keyword arguments delivered to matching subscribers.
- `emit` returns `None`.

## Matching and delivery

- **Fan-out:** all servers with subscriptions whose patterns match the emitted topic receive the event (on their own event queues).
- **Multiple subscribers in one server:** if multiple handlers in the same server match, all of them are invoked.
- Wildcards:
  - `*` matches exactly one segment (e.g. `user.*` matches `user.created`).
  - `#` matches zero or more segments (e.g. `user.#` matches `user.created`, `user.profile.updated`).

## Errors and validation

- Subscriber exceptions are logged; they do not produce a response to the publisher.
- Validation failures (when `validate_types` is enabled) prevent that subscriber invocation and are logged/dropped according to hooks.

## Consuming

```python
server = app.server()

await server.consume(mode="event")      # start event consuming for this server
await server.stop_consume(mode="event") # pause event consuming (resumable)
```

Consuming is controlled per server and mode. Stopping event consuming on a server pauses its subscriptions without removing them.
