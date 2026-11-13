# Configuration

## `App`

```python
App(
    name: str,
    *,
    backend: Backend,
    ttl: float | None = None,
    rpc_timeout: float | None = None,
    rpc_ttl: float | None = None,
    rpc_rttl: float | None = None,
    task_timeout: float | None = None,
    task_ttl: float | None = None,
    event_ttl: float | None = None,
    serializer: Serializer | list[Serializer] | None = None,
    validate_types: bool = False,
)
```

| Parameter | Default | Description |
|---|---|---|
| `name` | required | Application name; used as default queue prefix and in event queue naming |
| `backend` | required | `Backend` implementation (`LocalBackend`, `RmqBackend`, custom) |
| `ttl` | `None` | Accepted for API symmetry; currently not used to derive per-mode defaults |
| `rpc_timeout` | `None` (RPC default `300.0`) | Default RPC timeout (seconds) for outgoing requests; sent in request headers |
| `rpc_ttl` | `None` (RPC default `300.0`) | Default RPC message TTL (seconds) |
| `rpc_rttl` | `None` (RPC default `300.0`) | Default response TTL (seconds, RPC) |
| `task_timeout` | `None` | Accepted but not used; effective task timeout falls back to `TASK_TIMEOUT` (`None`) |
| `task_ttl` | `None` | Accepted but not used; effective task TTL falls back to `TASK_TTL` (`3600.0`) |
| `event_ttl` | `None` | Accepted but not used; effective event TTL falls back to `EVENT_TTL` (`3600.0`) |
| `serializer` | `JsonSerializer()` | Single serializer or list; first is default for outgoing, list enables server auto-detection |
| `validate_types` | `False` | Default argument type validation for handlers (overridable per handler) |

**Constants:** `RPC_TIMEOUT = 300.0`, `RPC_TTL = 300.0`, `RPC_RTTL = 300.0`, `TASK_TIMEOUT = None`, `TASK_TTL = 3600.0`, `EVENT_TTL = 3600.0`. For RPC, the client waits for a response up to `timeout + ttl` seconds.

## Named RPC/task queue routers and options

Use `app.rpc_router(...)` / `app.task_router(...)` to configure transport/backend
options for a **named** RPC or task queue and to register handlers on it.
Options are backend-specific and opaque to `App`: the active backend selects
option types it understands and ignores the rest.

Routers do not apply to the default app queue (`app.name`) and do not apply to
event queues. Default and event queue options are static backend configuration.

```python
rpc_router = app.rpc_router(
    path: str,
    *,
    options: QueueOptions | Sequence[QueueOptions] | None = None,
) -> RPCRouter

task_router = app.task_router(
    path: str,
    *,
    options: QueueOptions | Sequence[QueueOptions] | None = None,
) -> TaskRouter
```

- **Target queue:** configuration unit is `(mode, queue)` where queue is
  `f"{app.name}.{path}"`.
  - `path` is required and must be a non-empty string.
  - The default queue (`app.name`) and the per-instance event queue are not
    configurable via `App`; backends apply their own defaults.
- **Options:** a single `QueueOptions` instance, a sequence of them, or `None`.
  - Pass a list to provide options for multiple backends; each backend picks its
    own type.
  - Each element must be a `QueueOptions` instance; duplicate option types in one
    list are rejected.
  - `options=None` leaves existing queue configuration unchanged (useful to
    obtain a handle).
- **Rules:** declare before `consume_*` for that queue. A queue cannot be
  (re)configured while it is consuming. One queue has one option set:
  re-declaring with different options raises `ValueError`; an identical
  declaration is idempotent.
- **`RPCRouter` / `TaskRouter` handle:** `@router("name", **handler_options)`
  registers a leaf handler on the router queue.
  - `name` must be a non-empty leaf (no `.`); full handler name becomes
    `<path>.<name>` on queue `app.name.<path>`.
  - `handler_options` (e.g. `validate_types`) are handler options, not transport
    options.
- Handlers registered via `@app.method/task` with full names use the configured
  options of their resolved queue as well.

```python
from linkr import App, QueueOptions
from linkr.backends.rmq import RmqQueueOptions

app = App("orders", backend=backend)

# named RPC queue ("orders.serviceA")
rpc = app.rpc_router("serviceA", options=[RmqQueueOptions(prefetch_count=5, durable=True)])

@rpc("ping")
def ping() -> str:
    return "pong"

# named task queue ("orders.email")
email = app.task_router("email", options=[RmqQueueOptions(prefetch_count=50)])

@email("send")
async def send(user_id: int) -> None:
    ...
```

`QueueOptions` is a marker base in `linkr.models` (exported as
`linkr.QueueOptions`). Backends define concrete option types (e.g.
`RmqQueueOptions`).

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
- Accepts and ignores per-queue RPC/task router `options`; event queue behavior is in-process/backend-defined.

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
    exchange: str | dict[Mode, str] = "linkr",
    exchange_durable: bool | dict[Mode, bool] = True,
    queue_options: RmqQueueOptions | dict[Literal["rpc", "task"], RmqQueueOptions] | None = None,
    heartbeat: int = 30,
    blocked_connection_timeout: int = 30,
    results_queue_prefix: str | None = None,
)
```

- Requires `aio-pika` (`pip install "linkr[rabbitmq]"`).
- `login`/`password` are used for `PLAIN` auth; `ssl_context` enables TLS; `connection_name` sets AMQP client properties when provided.
- **Exchange configuration:**
  - Single exchange (default): `exchange="linkr"` (str) — one TOPIC exchange named `linkr` shared by RPC, tasks, and events.
  - Per-mode exchanges: `exchange={"rpc": "...", "task": "...", "event": "..."}` — separate exchanges for each mode. RPC and tasks use DIRECT type; events use TOPIC type. All three mode keys are required.
- **Durability:** `exchange_durable` applies to all modes (bool) or per-mode (dict with all three keys required).
- **Queue options defaults:** `queue_options` provides default `RmqQueueOptions` for RPC/task queues when no per-queue router options are provided. Accepts a single object (applies to both rpc and task) or a dict with "rpc" and "task" keys (both required).
- `heartbeat` and `blocked_connection_timeout` are connection settings. `results_queue_prefix` (default `linkr.res.`) prefixes the exclusive RPC callback queue.
- Event queue/consumer settings are static backend configuration (not configured via `App` routers/options).

### `RmqQueueOptions`

Per-queue/consumer options selected by `RmqBackend` from
`app.rpc_router(..., options=[...])` / `app.task_router(..., options=[...])`
for named RPC/task queues:

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

- `None` fields keep backend/mode defaults. RPC/task queue defaults are durable, non-exclusive, non-auto-delete unless overridden by `queue_options` on `RmqBackend` or by per-queue router options.
- `arguments` is passed to queue declaration (e.g. `x-max-length`,
  `x-dead-letter-exchange`, `x-queue-type`, `x-max-priority`).
- `prefetch_count` overrides consumer QoS for that queue by consuming on a
  dedicated channel (publishing still uses the main channel). If not set in
  per-queue or backend-level options, no explicit QoS is set (broker default
  applies).
- `queue_options` on `RmqBackend` provides backend-level defaults (single `RmqQueueOptions` for both rpc/task, or a dict with "rpc" and "task" keys); per-queue router options override them for that queue.

## Middleware

- `GzipMiddleware(min_size: int = 1024)` — compress payloads >= `min_size` bytes and set `content_encoding`.

## Serialization

- `JsonSerializer()` — name `linkr1.0`
- `JsonRpcSerializer()` — name `jsonrpc2.0`, RPC-only, extensible `ERROR_CODE_MAP`

## Notes

- Event queue per app instance: `<app_name>.<instance_id>` (logical queue name; RabbitMQ maps it to `linkr.event.<app_name>.<instance_id>`).
- Routing/queue derivation from names: see [Routing and naming](../concepts/routing-and-naming.md). Per-queue options for named RPC/task queues are applied when `consume_*` starts for the queue. Re-declaring an existing broker queue with incompatible parameters may fail with a broker precondition error.
