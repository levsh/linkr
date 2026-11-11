# Local backend

`LocalBackend` is an in-process backend intended for development and tests. It routes messages directly between `App` instances in the same process.

```python
from linkr import App, LocalBackend

backend = LocalBackend()
app = App("orders", backend=backend)
```

## Delivery semantics

- **RPC (`consume_rpc` / `request`)**: competing consumers per queue. A request is delivered to one consumer selected for the queue; the call returns the handler's response. If no RPC consumer is registered, `request` fails.
- **Task (`consume_task` / `send`)**: competing consumers per queue. Each task is delivered to exactly one consumer. If no task consumer is registered, the task is not handled (a warning is logged).
- **Event (`consume_event` / `emit`)**: fan-out. Events are delivered to event consumers whose topic patterns match the emitted topic.

## Lifecycle and stop behavior

- `init()` / `close()` manage this backend instance's consumers.
- `consume_rpc(queue, handler, *, options=None)` / `consume_task(queue, handler, *, options=None)` / `consume_event(queue, topics, handler, *, options=None)` accept backend-specific queue options for API compatibility but ignore them (in-process transport has no broker queue/QoS settings).
- `stop_consume_rpc(queue=None)` / `stop_consume_tasks(queue=None)` stop consumers for all queues of the mode or a specific queue.
- `stop_consume_events(queue=None, topic=None)`:
  - `queue=None` — stop all event consumers for this instance
  - `queue` set, `topic=None` — stop the specified event queue consumer
  - `queue` and `topic` set — remove only that topic subscription/pattern from the queue

`App` is responsible for deciding when to call these (e.g. only stopping a queue when no handlers remain).

## Testing notes

- **Process-wide state:** consumer registries and round-robin state are shared at the class/process level. Tests that use `LocalBackend` directly should ensure isolation when needed.
- **`reset()`:** `LocalBackend.reset()` clears all local routing/consumer state (useful in test fixtures):

```python
from linkr import LocalBackend

LocalBackend.reset()
```

- **Behavior differs from brokers:** in-process execution does not model network partitions, broker persistence, redelivery, or cross-process routing. Validate production semantics with your target backend (e.g. `RmqBackend`).

## When to use

- Unit/integration tests of application logic
- Local development without external infrastructure
- Examples and quickstarts
