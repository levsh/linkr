# RabbitMQ backend (`RmqBackend`)

`RmqBackend` is a RabbitMQ transport for linkr built on `aio-pika`.

## Installation

Install the RabbitMQ extra:

```bash
pip install "linkr[rabbitmq]"
```

## Usage

```python
from linkr import App
from linkr.backends.rmq import RmqBackend

backend = RmqBackend(
    host="localhost",
    port=5672,
    login="guest",
    password="guest",
    virtualhost="/",
    exchange="linkr",  # single exchange name or per-mode mapping
)

app = App("orders", backend=backend)
rpc = app.rpc()

@rpc.method("ping")
def ping() -> str:
    return "pong"

await app.init()
await rpc.consume()
```

## Configuration

`RmqBackend(...)` parameters:

- `host` (default `"localhost"`), `port` (default `5672`)
- `login`, `password`, `virtualhost`
- `ssl_context`: optional `ssl.SSLContext` for TLS connections
- `auth_mechanism` (default `"PLAIN"`)
- `connection_name`: optional AMQP client connection name
- `exchange` (default `"linkr"`):
  - **Single exchange (default):** `exchange="linkr"` (str) — one topic exchange named `linkr` shared by RPC, tasks, and events. Queues per logical queue: `linkr.rpc.<queue>`, `linkr.tasks.<queue>`, `linkr.events.<queue>`.
  - **Per-mode exchanges:** `{"rpc": "app.rpc", "task": "app.tasks", "event": "app.events"}` (all keys required) — separate exchange per mode. Exchange types: `rpc=direct`, `task=direct`, `event=topic`. Physical queue names use the same `linkr.<mode>.<queue>` scheme.
- `exchange_durable` (default `True`): `bool` applied to all exchanges, or per-mode `{"rpc": ..., "task": ..., "event": ...}`.
- `queue_name_template` (default `None`): mode-to-format mapping or callable `(queue, mode) -> name` for physical queue names (default legacy: `linkr.{queue}.rpc`, `linkr.{queue}.tasks`, `linkr.{queue}.events`; `rpc` and `task` names must differ).
- `heartbeat` (default `30`): AMQP heartbeat in seconds.
- `blocked_connection_timeout` (default `30`): AMQP blocked-connection timeout in seconds.
- `results_queue_prefix` (default `"linkr.res."`): prefix for the per-instance RPC callback (reply) queue name.

## Queue topology

- Logical queues come from server scopes and event queues (e.g. `app`, `app.group`, `app.<instance_id>` or `app.group.<instance_id>` for events).
- Physical broker queues are namespaced: `linkr.rpc.<queue>`, `linkr.tasks.<queue>`, `linkr.events.<queue>`.
- Bind/publish routing keys use the logical queue/topic names as expected by linkr.
- RPC replies are delivered to a per-backend-instance exclusive callback queue (`<results_queue_prefix><id>`), used as `reply_to` with `correlation_id` matching.

## Consuming and stopping

Per-server consuming is managed by `server.consume()` / `server.stop_consume()` on `RPC`/`Worker`/`Bus` instances, which delegate to backend queue-level operations:

- `consume_rpc(queue, handler, *, options=None)`
- `consume_task(queue, handler, *, options=None)`
- `consume_event(queue, topics, handler, *, options=None)`
- `stop_consume_rpc(queue=None)`
- `stop_consume_tasks(queue=None)`
- `stop_consume_events(queue=None, topic=None)`

`stop_consume_events(queue, topic)` removes a topic binding/subscription without stopping the consumer when other bindings remain (subject to backend state). Passing `queue=None` stops all consumers of that mode for this backend instance.

## Healthcheck

`RmqBackend.healthcheck()` returns:

- `HealthStatus(ready=False, health=None)` when the backend is not initialized or is closed.
- `HealthStatus(ready=True, health=False)` when the AMQP connection/channel is missing or closed, or when a health probe fails.
- `HealthStatus(ready=True, health=True)` when the connection/channel is alive and either there are no active queues (no consumers currently registered on this backend instance) or every active queue has at least one broker consumer.

Active-queue consumer presence is verified against the broker via passive queue declarations and is performed in parallel for active queues. If broker checks fail or report no consumers for any active queue, health is `False`. The callback/reply queue is not treated as an application consumer queue by this check.

## Per-server queue options (`RmqQueueOptions`)

Configure broker/consumer options per server when creating it:

```python
from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions

app = App("orders", backend=RmqBackend())
rpc = app.rpc("serviceA", options=[RmqQueueOptions(prefetch_count=5, durable=True)])
email = app.worker(
    "email",
    options=[RmqQueueOptions(
        prefetch_count=50,
        arguments={"x-max-length": 10_000, "x-dead-letter-exchange": "dlx"},
    )],
)
bus = app.bus(options=[RmqQueueOptions(prefetch_count=20)])
```

- `options` apply to that mode server's consume/queue.
- `RmqBackend` selects the `RmqQueueOptions` object from the provided options and applies it when consuming.
- If no per-server option is provided, backend/mode defaults apply for declaration/QoS behavior.

`RmqQueueOptions` fields:

- `durable`, `exclusive`, `auto_delete`, `passive` — queue declare flags (backend/mode defaults apply when `None`).
- `arguments` — AMQP queue arguments (e.g. `x-max-length`, `x-dead-letter-exchange`, `x-queue-type`).
- `prefetch_count` — consumer QoS prefetch for that queue/consumer (a dedicated channel may be used for per-queue overrides).
- `extra` — reserved for future use.

Notes:

- Queue declare parameters must remain compatible with existing broker queues (changing them may raise precondition errors); use a new server/queue path when topology must differ.
- Bus event queues are per-bus instance queues (separate from RPC/task queues of the same app).

## Example

```python
import asyncio

from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions


async def main() -> None:
    app = App(
        "orders",
        backend=RmqBackend(host="localhost", port=5672),
    )

    rpc = app.rpc("serviceA", options=[RmqQueueOptions(prefetch_count=5)])
    bus = app.bus()

    @rpc.method("ping")
    def ping() -> str:
        return "pong"

    @bus.subscribe("user.created")
    def on_user_created(user_id: int) -> None:
        print("user", user_id)

    async with app:
        await rpc.consume()
        await bus.consume()
        print(await app.client.call("orders.serviceA.ping", (), {}))
        await app.client.emit("user.created", {"user_id": 1})
        await asyncio.sleep(0.1)


asyncio.run(main())
```

## Notes

- Requires a running RabbitMQ broker and appropriate permissions/vhost access.
- Delivery guarantees and ordering depend on broker configuration and messaging patterns.
- RPC uses a per-backend-instance callback queue for replies; tasks/events are published to exchanges/queues as configured.
- `app.close()` stops server consumers and closes backend connections; pending RPC futures are failed on backend close.
