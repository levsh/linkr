# Local backend

`LocalBackend` is an in-process backend that requires no external broker. It runs handlers within the current process and is useful for trying `linkr` locally.

```python
from linkr import App, LocalBackend

backend = LocalBackend()
app = App("orders", backend=backend)
```

## Characteristics

- **Transport:** in-process async delivery (no network, no persistence).
- **RPC:** competing consumers on a queue (round-robin); `backend.request(...)` schedules handler execution and returns a `Future` completed with the handler response.
- **Tasks:** competing consumers on a queue (round-robin). If no task consumer is registered, the task is dropped with a warning.
- **Events:** fan-out to every registered event consumer whose topic pattern matches. Events are dispatched concurrently and each subscriber receives a copy of the message.
- **State:** consumer registries are class-level (process-wide). Use `LocalBackend.reset()` in tests to clear state.
- **Queue options:** `consume_rpc` / `consume_task` accept backend-specific queue options for API compatibility but ignore them (in-process transport has no broker queue/QoS settings). Event consumers are not configured via app-level options.

## Key API

```python
LocalBackend(name: str | None = None)
```

- `consume_rpc(queue, handler, *, options=None)` / `consume_task(queue, handler, *, options=None)` register competing consumers. `options` is ignored.
- `consume_event(queue, topics, handler)` registers an event consumer for topic patterns on a queue.
- `stop_consume_rpc(queue=None)`, `stop_consume_tasks(queue=None)`, `stop_consume_events(queue=None, topic=None)` stop/unregister consumers owned by the backend instance.
- `request(queue, message, *, context=None) -> asyncio.Future[Message]` returns after publish/registration.
  - `send(queue, message, *, context=None)` delivers to one task consumer (or warns if none).
  - `emit(topic, message, *, context=None)` delivers to all matching subscribers.
- `LocalBackend.reset()` clears process-wide consumer registries and round-robin state.

## Usage

```python
from linkr import App, LocalBackend

async def main() -> None:
    backend = LocalBackend()
    app = App("demo", backend=backend)

    @app.method("ping")
    def ping() -> str:
        return "pong"

    async with app:
        await app.consume()
        assert await app.call("demo.ping", (), {}) == "pong"
```

## Notes

- Call `LocalBackend.reset()` when you need to clear process-wide consumer registries and round-robin state between runs.
- Behavior such as delivery without consumers and error propagation is local/in-process and differs from broker backends; validate critical semantics against the target production backend.
