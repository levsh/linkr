# LocalBackend

`LocalBackend` is an in-process backend intended for development, testing, and examples. It requires no external broker and runs handlers within the same process event loop.

## Usage

```python
from linkr import App, LocalBackend

backend = LocalBackend()
app = App("app", backend=backend)
```

You can also construct and pass it inline: `App("app", backend=LocalBackend())`.

## Characteristics

- **No external dependencies/services:** everything runs in-memory in the current process.
- **RPC:** single consumer per logical queue (the registered handler); request/response correlated via futures.
- **Tasks:** competing consumers per task queue — when multiple consumers are registered for the same queue, tasks are distributed round-robin.
- **Events:** fan-out to all registered consumers whose topic patterns match; each server instance has its own event queue/consumer.
- **Stop semantics:** supports `stop_consume_rpc(queue=None)`, `stop_consume_tasks(queue=None)`, and `stop_consume_events(queue=None, topic=None)` for per-queue/topic control (used by `Server.stop_consume(...)`).
- **Reset:** `LocalBackend.reset()` clears global in-process registries (useful in tests).

## Limitations

- Not suitable for multi-process or distributed deployments.
- No persistence, durability, or broker-level guarantees.
- Delivery/ordering behavior is in-process and simpler than broker backends.

## Example

```python
import asyncio

from linkr import App, LocalBackend


async def main() -> None:
    backend = LocalBackend()

    async with App("server", backend=backend) as server_app:
        srv = server_app.server()

        @srv.method("ping")
        def ping() -> str:
            return "pong"

        await srv.consume(mode="rpc")

        async with App("client", backend=backend) as client_app:
            print(await client_app.client.call("server.ping", (), {}))


asyncio.run(main())
```
