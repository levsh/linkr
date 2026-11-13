# Tasks

Tasks are fire-and-forget jobs: one consumer handles each task, and the caller does not receive a result.

## Registering tasks

```python
from linkr import App, LocalBackend

app = App("worker", backend=LocalBackend())

@app.task("email.send")
async def send_email(user_id: int, template: str = "welcome") -> None:
    ...
```

- `@app.task(name, **options)` registers a task handler.
- Dotted names control queue routing (e.g. `email.send` → queue `<app>.email`, task name `send`).
- `options` can include `validate_types=True|False`.

## Sending tasks

```python
await app.consume()

await app.send("worker.email.send", (), {"user_id": 42})
await app.send("worker.email.send", (42,), {"template": "welcome"})
```

Signature:

```python
await app.send(
    task: str,
    args: tuple[Any, ...],
    kwds: dict[str, Any],
    *,
    request_id: UUID | None = None,
    timeout: float | None = None,
    ttl: float | None = None,
    serializer: str | None = None,
    context: dict[str, Any] | None = None,
) -> None
```

- Returns `None`. Completion of `send` means the client-side publish completed (subject to backend semantics), not that the task ran successfully.
- `task` should be the fully qualified path targeting the consumer queue.

## Delivery semantics

- **Competing consumers:** if multiple consumers listen on the same task queue, each task is delivered to exactly one of them.
- **No caller response:** task handlers do not return a value to the sender.
- **Failures:** handler exceptions and execution timeouts are logged; argument validation failures are logged and the handler is not invoked. No error is propagated to the sender.
- **No consumer available:** behavior depends on the backend. For example, `LocalBackend` logs a warning when no task consumer is registered for the queue; broker-backed backends may queue messages until a consumer appears (per broker/queue configuration).

## Observability

Because tasks have no response channel, use [hooks](hooks.md) and logging for visibility:

- Client: `on_request_published`
- Server: `on_request_received`, `before_handler`, `after_handler`, `on_handler_error`, `on_message_dropped`

## Example: competing workers

```python
import asyncio
from linkr import App, LocalBackend

backend = LocalBackend()
worker_a = App("worker", backend=backend)
worker_b = App("worker", backend=backend)
client = App("client", backend=backend)

@worker_a.task("job.run")
def run_a(n: int) -> None:
    print("A", n)

@worker_b.task("job.run")
def run_b(n: int) -> None:
    print("B", n)

async def main() -> None:
    async with worker_a, worker_b, client:
        await worker_a.consume()
        await worker_b.consume()
        for i in range(4):
            await client.send("worker.job.run", (i,), {})

asyncio.run(main())
```

(Distribution across workers depends on backend/consumer selection.)
