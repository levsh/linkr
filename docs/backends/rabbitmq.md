# RabbitMQ backend

`RmqBackend` provides broker-backed delivery using RabbitMQ via `aio-pika`.

## Installation

```bash
pip install "linkr[rabbitmq]"
```

```python
from linkr import App
from linkr.backends.rmq import RmqBackend

backend = RmqBackend(host="localhost", port=5672)
app = App("orders", backend=backend)
```

## Configuration

`RmqBackend` parameters (from implementation):

| Parameter | Default | Description |
|---|---|---|
| `host` | `"localhost"` | RabbitMQ host |
| `port` | `5672` | AMQP port |
| `login` | `"guest"` | Username (used with `PLAIN` auth) |
| `password` | `"guest"` | Password (used with `PLAIN` auth) |
| `virtualhost` | `"/"` | Virtual host |
| `ssl_context` | `None` | Optional `ssl.SSLContext` for TLS connections |
| `auth_mechanism` | `"PLAIN"` | Authentication mechanism (e.g. `PLAIN`, `EXTERNAL`) |
| `connection_name` | `None` | Optional AMQP client connection name |
| `rpc_exchange` | `"linkr.rpc"` | RPC exchange name |
| `task_exchange` | `"linkr.tasks"` | Task exchange name |
| `event_exchange` | `"linkr.events"` | Event exchange name |
| `exchange_durable` | `True` | Declare exchanges as durable |
| `queue_durable` | `True` | Default durability for RPC/task queues (unless overridden per queue) |
| `prefetch_count` | `10` | Default consumer prefetch (QoS) on the main channel |
| `heartbeat` | `30` | Connection heartbeat (seconds) |
| `blocked_connection_timeout` | `30` | Blocked connection timeout (seconds) |
| `results_queue_prefix` | `None` (`"linkr.res."`) | Prefix for the exclusive RPC callback queue name |

## Topology and routing

On `init()`, the backend connects and declares exchanges:

- **RPC:** direct exchange `rpc_exchange` (default `linkr.rpc`), queue per logical queue `linkr.rpc.<queue>`
- **Tasks:** direct exchange `task_exchange` (default `linkr.tasks`), queue `linkr.task.<queue>`
- **Events:** topic exchange `event_exchange` (default `linkr.events`), queue per app instance `linkr.event.<queue>` bound with topic patterns

RPC uses a callback/reply mechanism (`correlation_id`, reply queue) to deliver responses; `request(...)` returns a `Future` once the request is published and waiting is registered.

## Consume and stop

- `consume_rpc(queue, handler, *, options=None)` / `consume_task(queue, handler, *, options=None)` / `consume_event(queue, topics, handler, *, options=None)` register consumers and bindings as required. If `options` includes an `RmqQueueOptions` instance, its queue/consumer settings are applied (other option types are ignored).
- `stop_consume_rpc(queue=None)` / `stop_consume_tasks(queue=None)` cancel consumer(s) for the mode/queue (and close dedicated per-queue channels when used).
- `stop_consume_events(queue=None, topic=None)` cancels event consumer(s) or, when `topic` is provided, unbinds only that routing key/topic from the queue (without stopping the consumer).

## Per-queue options (`RmqQueueOptions`)

Use `app.router(...)` to configure a queue before `consume_*`. `RmqBackend` applies the `RmqQueueOptions` object from the provided options list.

```python
from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions

backend = RmqBackend()
app = App("orders", backend=backend)

# default rpc queue ("orders")
rpc = app.router("rpc", options=[RmqQueueOptions(prefetch_count=5, durable=True)])

@rpc("ping")
def ping() -> str:
    return "pong"

# named task queue ("orders.email"), shared by handlers on that queue
email = app.router("task", path="email", options=[
    RmqQueueOptions(prefetch_count=50, arguments={"x-max-priority": 10}),
])

@email("send")
async def send(user_id: int) -> None:
    ...
```

`RmqQueueOptions` fields:

| Field | Default | Effect |
|---|---|---|
| `durable` | `None` | Queue durability (`None` → rpc/task use `queue_durable`; event defaults to `False`) |
| `exclusive` | `None` | Exclusive queue (`None` → `False` for rpc/task, `True` for event) |
| `auto_delete` | `None` | Auto-delete queue (`None` → `False`) |
| `passive` | `None` | Passive declare when set |
| `arguments` | `None` | AMQP queue arguments (e.g. `x-max-length`, `x-dead-letter-exchange`, `x-queue-type`, `x-max-priority`) |
| `prefetch_count` | `None` | Per-queue consumer QoS; when set, consume uses a dedicated channel |
| `extra` | `{}` | Reserved for backend-specific extensions |

`None` means "do not override" the backend/mode default. Options from other backends in the same list are ignored by `RmqBackend`.

## Usage example

```python
import asyncio

from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions


async def main() -> None:
    backend = RmqBackend(host="localhost", port=5672)
    app = App("orders", backend=backend)

    rpc = app.router("rpc", options=[RmqQueueOptions(prefetch_count=5)])

    @rpc("ping")
    def ping() -> str:
        return "pong"

    async with app:
        await app.consume()
        # client in another process/app can call "orders.ping"


asyncio.run(main())
```

## Operational notes

- **External dependency:** requires a reachable RabbitMQ broker and appropriate permissions/vhost configuration.
- **Durability and guarantees:** controlled by broker/exchange/queue settings and application patterns (ack/retry/idempotency are transport- and design-dependent). Validate behavior for your deployment.
- **Queue declaration compatibility:** changing declaration parameters of an existing broker queue (e.g. durability, arguments, `x-queue-type`) can cause a broker precondition error; keep configuration consistent or use a new queue name.
- **Event queues:** instance-specific event queues default to exclusive/non-durable. Overriding event queue durability/exclusivity is explicit via router options; plan lifecycle/cleanup according to your environment.
- **Prefetch and concurrency:** `prefetch_count` affects delivery/throughput for competing consumers. Per-queue overrides use a dedicated consumer channel; publishing remains on the main channel.
- **Error handling:** task/event handler failures are handled at the app level (logged/hooks) and do not produce caller responses; broker-level redelivery/DLQ behavior depends on broker/backend semantics and configuration.
