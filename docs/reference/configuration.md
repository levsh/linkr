# Configuration

## `App`

```python
App(
    name: str,
    *,
    backend: Backend,
    timeout: float | None = None,
    ttl: float | None = None,
    rttl: float | None = None,
    serializer: Serializer | list[Serializer] | None = None,
    validate_types: bool = False,
)
```

| Parameter | Default | Description |
|---|---|---|
| `name` | required | Application name; used as default queue prefix and in event queue naming |
| `backend` | required | `Backend` implementation (`LocalBackend`, `RmqBackend`, custom) |
| `timeout` | `None` (RPC default `3600.0`) | Default RPC timeout (seconds) for outgoing requests; sent in request headers |
| `ttl` | `None` (RPC default `3600.0`) | Default message TTL (seconds) |
| `rttl` | `None` | Default response TTL (seconds, RPC) |
| `serializer` | `JsonSerializer()` | Single serializer or list; first is default for outgoing, list enables server auto-detection |
| `validate_types` | `False` | Default argument type validation for handlers (overridable per handler) |

**Constants:** `RPC_TIMEOUT = 3600.0`, `RPC_TTL = 3600.0`. For RPC, the client waits for a response up to `timeout + ttl` seconds.

## Router and per-queue options

Use `app.router(...)` to configure transport/backend options for a queue and to register handlers on it. Options are backend-specific and opaque to `App`: the active backend selects option types it understands and ignores the rest.

```python
router = app.router(
    mode: Literal["rpc", "task", "event"],
    path: str | None = None,
    *,
    options: QueueOptions | Sequence[QueueOptions] | None = None,
) -> Router
```

- **Target queue** (configuration unit is `(mode, queue)`):
  - `mode in {"rpc","task"}`, `path=None` → default app queue `app.name`
  - `mode in {"rpc","task"}`, `path="svc.group"` → named queue `f"{app.name}.{path}"`
  - `mode == "event"` → instance event queue `f"{app.name}.events.{instance_id}"`; `path` must be `None`
- **Options:** a single `QueueOptions` instance, a sequence of them, or `None`.
  - Pass a list to provide options for multiple backends; each backend picks its own type.
  - Each element must be a `QueueOptions` instance; duplicate option types in one list are rejected.
  - `options=None` leaves existing queue configuration unchanged (useful to obtain a handle).
- **Rules:** declare before `consume_*` for that queue. A queue cannot be (re)configured while it is consuming. One queue has one option set: re-declaring with different options raises `ValueError`; an identical declaration is idempotent.
- **`Router` handle:** `@router("name", **handler_options)` registers a handler on the router queue.
  - rpc/task: `name` must be a non-empty leaf (no `.`); full handler name becomes `<path>.<name>` or `<name>` for the default queue.
  - event: `name` is a topic pattern (equivalent to `app.subscribe`).
  - `handler_options` (e.g. `validate_types`) are handler options, not transport options.
- Handlers registered via `@app.method/task/subscribe` with full names use the configured options of their resolved queue as well.

```python
from linkr import App, QueueOptions
from linkr.backends.rmq import RmqQueueOptions

app = App("orders", backend=backend)

# default rpc queue ("orders")
rpc = app.router("rpc", options=[RmqQueueOptions(prefetch_count=5, durable=True)])

@rpc("ping")
def ping() -> str:
    return "pong"

# named task queue ("orders.email")
email = app.router("task", path="email", options=[RmqQueueOptions(prefetch_count=50)])

@email("send")
async def send(user_id: int) -> None:
    ...

# event instance queue
app.router("event", options=[RmqQueueOptions(prefetch_count=20)])
```

`QueueOptions` is a marker base in `linkr.models` (exported as `linkr.QueueOptions`). Backends define concrete option types (e.g. `RmqQueueOptions`).

## Client call parameters

`call(method, args, kwds, *, request_id=None, timeout=None, ttl=None, rttl=None, serializer=None, context=None)`  
`send(task, args, kwds, *, request_id=None, timeout=None, ttl=None, serializer=None, context=None)`  
`emit(topic, event, *, request_id=None, ttl=None, serializer=None, context=None)`

- Per-call `timeout`/`ttl`/`rttl` override app defaults where applicable.
- `serializer` selects serializer by `name` when multiple are configured.
- `context` is forwarded to middleware/backend.

## Handler options

Decorators accept `**options`, commonly:

- `validate_types: bool` — enable/disable argument validation for that handler.

```python
@app.method("add", validate_types=True)
def add(x: int, y: int) -> int: ...
```

## `LocalBackend`

```python
LocalBackend(name: str | None = None)
```

- In-process routing; `name` kept for compatibility.
- Class-level state shared in process; `LocalBackend.reset()` clears consumers/state.
- Accepts and ignores per-queue router `options`.

## `RmqBackend`

```python
RmqBackend(
    host: str = "localhost",
    port: int = 5672,
    login: str = "guest",
    password: str = "guest",
    virtualhost: str = "/",
    *,
    ssl_context: ssl.SSLContext | None = None,
    auth_mechanism: str = "PLAIN",
    connection_name: str | None = None,
    rpc_exchange: str = "linkr.rpc",
    task_exchange: str = "linkr.tasks",
    event_exchange: str = "linkr.events",
    exchange_durable: bool = True,
    queue_durable: bool = True,
    prefetch_count: int = 10,
    heartbeat: int = 30,
    blocked_connection_timeout: int = 30,
    results_queue_prefix: str | None = None,
)
```

- Requires `aio-pika` (`pip install "linkr[rabbitmq]"`).
- `login`/`password` are used for `PLAIN` auth; `ssl_context` enables TLS; `connection_name` sets AMQP client properties when provided.
- Declares direct exchanges for RPC/tasks and topic exchange for events; physical queues are `linkr.rpc.<queue>`, `linkr.task.<queue>`, `linkr.event.<queue>`.
- `prefetch_count` is the default consumer QoS on the main channel. `heartbeat` and `blocked_connection_timeout` are connection settings. `results_queue_prefix` (default `linkr.res.`) prefixes the exclusive RPC callback queue.

### `RmqQueueOptions`

Per-queue/consumer options selected by `RmqBackend` from `app.router(..., options=[...])`:

```python
from linkr.backends.rmq import RmqQueueOptions

RmqQueueOptions(
    durable: bool | None = None,
    exclusive: bool | None = None,
    auto_delete: bool | None = None,
    passive: bool | None = None,
    arguments: dict[str, Any] | None = None,
    prefetch_count: int | None = None,
    extra: dict[str, Any] = {},
)
```

- `None` fields keep backend/mode defaults. Defaults when not overridden: rpc/task queues use `queue_durable` with non-exclusive/non-auto-delete; event instance queues default to `durable=False`, `exclusive=True`, `auto_delete=False`.
- `arguments` is passed to queue declaration (e.g. `x-max-length`, `x-dead-letter-exchange`, `x-queue-type`, `x-max-priority`).
- `prefetch_count` overrides consumer QoS for that queue by consuming on a dedicated channel (publishing still uses the main channel). Without override, the backend default `prefetch_count` applies.

## Middleware

- `GzipMiddleware(min_size: int = 1024)` — compress payloads >= `min_size` bytes and set `content_encoding`.

## Serialization

- `JsonSerializer()` — name `linkr1.0`
- `JsonRpcSerializer()` — name `jsonrpc2.0`, RPC-only, extensible `ERROR_CODE_MAP`

## Notes

- Event queue per app instance: `<app_name>.events.<instance_id>`.
- Routing/queue derivation from names: see [Routing and naming](../concepts/routing-and-naming.md). Per-queue options are applied when `consume_*` starts for the queue. Re-declaring an existing broker queue with incompatible parameters may fail with a broker precondition error.
