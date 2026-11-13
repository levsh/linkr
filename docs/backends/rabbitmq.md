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
| `exchange` | `"linkr"` | Exchange name (str for single exchange shared by all modes, or `dict[Mode, str]` for per-mode exchanges) |
| `exchange_durable` | `True` | Declare exchanges as durable (bool for all modes, or `dict[Mode, bool]` for per-mode) |
| `queue_options` | `None` | Default `RmqQueueOptions` for RPC/task queues (single object for both, or `dict["rpc"/"task", RmqQueueOptions]` for per-mode) |
| `heartbeat` | `30` | Connection heartbeat (seconds) |
| `blocked_connection_timeout` | `30` | Blocked connection timeout (seconds) |
| `results_queue_prefix` | `None` (`"linkr.res."`) | Prefix for the exclusive RPC callback queue name |

## Topology and routing

On `init()`, the backend connects and declares exchanges:

- **Single exchange (default):** `exchange="linkr"` (str) — one topic exchange named `linkr` shared by RPC, tasks, and events. Queue per logical queue: `linkr.rpc.<queue>`, `linkr.task.<queue>`, `linkr.event.<queue>`.
- **Per-mode exchanges:** `exchange={"rpc": "...", "task": "...", "event": "..."}` — separate exchanges for each mode. RPC and tasks use DIRECT type; events use TOPIC type. All three mode keys are required.
- **Durability:** `exchange_durable` applies to all modes (bool) or per-mode (dict with all three keys required).

RPC uses a callback/reply mechanism (`correlation_id`, reply queue) to deliver responses; `request(...)` returns a `Future` once the request is published and waiting is registered.

## Consume and stop

- `consume_rpc(queue, handler, *, options=None)` / `consume_task(queue, handler, *, options=None)` register consumers and bindings as required. If `options` includes an `RmqQueueOptions` instance, its queue/consumer settings are applied to that named RPC/task queue (other option types are ignored). If no per-queue options are provided, `queue_options` from `RmqBackend(...)` are used as backend defaults (single object for both rpc/task, or per-mode dict).
- `consume_event(queue, topics, handler)` registers the instance event consumer and bindings using backend-static configuration (not configured via `App`).
- `stop_consume_rpc(queue=None)` / `stop_consume_tasks(queue=None)` cancel consumer(s) for the mode/queue (and close dedicated per-queue channels when used).
- `stop_consume_events(queue=None, topic=None)` cancels event consumer(s) or, when `topic` is provided, unbinds only that routing key/topic from the queue (without stopping the consumer).

## Per-queue options (`RmqQueueOptions`) for named RPC/task queues

Use `app.rpc_router(...)` / `app.task_router(...)` to configure a **named** RPC/task queue before `consume_*`. `RmqBackend` applies the `RmqQueueOptions` object from the provided options list. `path` is required and targets queue `app.name.<path>`.

The default app queue (`app.name`) and event queues are not configured via routers/options from `App`; they use backend-static defaults.

```python
from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions

backend = RmqBackend()
app = App("orders", backend=backend)

# named RPC queue ("orders.serviceA")
rpc = app.rpc_router("serviceA", options=[RmqQueueOptions(prefetch_count=5, durable=True)])

@rpc("ping")
def ping() -> str:
    return "pong"

# named task queue ("orders.email"), shared by handlers on that queue
email = app.task_router("email", options=[
    RmqQueueOptions(prefetch_count=50, arguments={"x-max-priority": 10}),
])

@email("send")
async def send(user_id: int) -> None:
    ...
```

**Priority (highest first):** per-queue router options > backend `queue_options` defaults (single object or per-mode dict) > built-in mode defaults (durable for rpc/task, non-durable for event).

`RmqQueueOptions` fields:

| Field | Default | Effect |
|---|---|---|
| `durable` | `None` | Queue durability (`None` → backend/mode default, durable for rpc/task) |
| `exclusive` | `None` | Exclusive queue (`None` → `False` for rpc/task) |
| `auto_delete` | `None` | Auto-delete queue (`None` → `False` for rpc/task) |
| `passive` | `None` | Passive declare when set |
| `arguments` | `None` | AMQP queue arguments (e.g. `x-max-length`, `x-dead-letter-exchange`, `x-queue-type`, `x-max-priority`) |
| `prefetch_count` | `None` | Per-queue consumer QoS; when set, consume uses a dedicated channel |
| `extra` | `{}` | Reserved for backend-specific extensions |

`None` means "do not override" the backend/mode default. Per-queue router options override `queue_options` backend defaults for that queue.

## Usage example

```python
import asyncio

from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions


async def main() -> None:
    backend = RmqBackend(host="localhost", port=5672)
    app = App("orders", backend=backend)

    rpc = app.rpc_router("serviceA", options=[RmqQueueOptions(prefetch_count=5)])

    @rpc("ping")
    def ping() -> str:
        return "pong"

    async with app:
        await app.consume()
        # client in another process/app can call "orders.serviceA.ping"


asyncio.run(main())
```

## Operational notes

- **External dependency:** requires a reachable RabbitMQ broker and appropriate permissions/vhost configuration.
- **Durability and guarantees:** controlled by broker/exchange/queue settings and application patterns (ack/retry/idempotency are transport- and design-dependent). Validate behavior for your deployment.
- **Queue declaration compatibility:** changing declaration parameters of an existing broker queue (e.g. durability, arguments, `x-queue-type`) can cause a broker precondition error; keep configuration consistent or use a new queue name.
- **Event queues:** instance-specific event queues are configured statically by the backend (default exclusive/non-durable for the instance queue). App-level router/options do not configure event queues.
- **Prefetch and concurrency:** `prefetch_count` affects delivery/throughput for competing consumers. Per-queue overrides for named RPC/task queues use a dedicated consumer channel; publishing remains on the main channel.
- **Error handling:** task/event handler failures are handled at the app level (logged/hooks) and do not produce caller responses; broker-level redelivery/DLQ behavior depends on broker/backend semantics and configuration.
