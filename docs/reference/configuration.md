# Configuration

## `App`

```python
App(
    name: str,
    *,
    backend: Backend,
    serializer: Serializer | list[Serializer] | None = None,
    middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    rpc_client_timeout: float | None = RPC_CLIENT_TIMEOUT,
    rpc_server_timeout: float | None = RPC_SERVER_TIMEOUT,
    rpc_ttl: float | None = RPC_TTL,
    rpc_rttl: float | None = RPC_RTTL,
    task_client_timeout: float | None = TASK_CLIENT_TIMEOUT,
    task_server_timeout: float | None = TASK_SERVER_TIMEOUT,
    task_ttl: float | None = TASK_TTL,
    event_client_timeout: float | None = EVENT_CLIENT_TIMEOUT,
    event_ttl: float | None = EVENT_TTL,
    validate_types: bool = False,
)
```

| Parameter | Default | Description |
|---|---|---|
| `name` | required | Application name; used as queue prefix and in server/event queue naming |
| `backend` | required | `Backend` implementation (`LocalBackend`, `RmqBackend`, custom) |
| `serializer` | `JsonSerializer()` | Single serializer or list; first is default for outgoing, list enables auto-detection |
| `middlewares` | `None` | List of `AppMiddleware`/`WireMiddleware` instances; initialized/closed with the app and applied across its servers |
| `rpc_client_timeout` | `RPC_CLIENT_TIMEOUT` (`60.0`) | Default RPC client wait timeout (seconds) for waiting for a response |
| `rpc_server_timeout` | `RPC_SERVER_TIMEOUT` (`60.0`) | Default RPC handler execution timeout (seconds) |
| `rpc_ttl` | `RPC_TTL` (`60.0`) | Default RPC request time-to-live in transport (seconds) |
| `rpc_rttl` | `RPC_RTTL` (`60.0`) | Default RPC response time-to-live in transport (seconds) |
| `task_client_timeout` | `TASK_CLIENT_TIMEOUT` (`30.0`) | Default task client wait timeout (seconds) for publish/send |
| `task_server_timeout` | `TASK_SERVER_TIMEOUT` (`3600.0`) | Default task handler execution timeout (seconds) |
| `task_ttl` | `TASK_TTL` (`3600.0`) | Default task message time-to-live in transport (seconds) |
| `event_client_timeout` | `EVENT_CLIENT_TIMEOUT` (`30.0`) | Default event client wait timeout (seconds) for publish/emit |
| `event_ttl` | `EVENT_TTL` (`3600.0`) | Default event message time-to-live in transport (seconds) |
| `validate_types` | `False` | Default argument type validation for handlers (overridable per handler/server) |

**Class constants:** `RPC_CLIENT_TIMEOUT = 60.0`, `RPC_SERVER_TIMEOUT = 60.0`, `RPC_TTL = 60.0`, `RPC_RTTL = 60.0`, `TASK_CLIENT_TIMEOUT = 30.0`, `TASK_SERVER_TIMEOUT = 3600.0`, `TASK_TTL = 3600.0`, `EVENT_CLIENT_TIMEOUT = 30.0`, `EVENT_TTL = 3600.0`.

**Timeout semantics:**

- `client_timeout` — local client wait budget (response for RPC, publish for task/event). Not sent in request headers.
- `server_timeout` — handler execution limit on the server (`rpc`/`task` only), sent as `headers["server_timeout"]`.
- `ttl` / `rttl` — transport message/response lifetimes (e.g. broker expiration). Independent from client/server timeouts.
- Negative `client_timeout` / `server_timeout` values are normalized to `0` (`max(value, 0)`). `None` means no limit for that dimension (or fall back to default where applicable).
- There is no server execution timeout for events.

## Mode servers and queue options

Servers are created and configured via mode-specific factories:

```python
rpc = app.rpc(path: str | None = None, *, options: QueueOptions | Sequence[QueueOptions] | None = None, validate_types: bool | None = None) -> RPC
worker = app.worker(path: str | None = None, *, options: QueueOptions | Sequence[QueueOptions] | None = None, validate_types: bool | None = None) -> Worker
bus = app.bus(path: str | None = None, *, options: QueueOptions | Sequence[QueueOptions] | None = None, validate_types: bool | None = None) -> Bus
```

- **Scope/queue:** `path=None` → root server, base queue `app.name`; `path="group"` → queue `app.name.group`.
- **Handler registration:**
  - `RPC`: `@rpc.method(name, **opts)` for RPC handlers
  - `Worker`: `@worker.task(name, **opts)` for task handlers
  - `Bus`: `@bus.subscribe(topic, **opts)` for event subscribers
  Method/task `name` is a local leaf (must not contain `.`). Common option: `validate_types` (handler > server > app).
- **Consume control:** `server.consume()` / `server.stop_consume()` (pause/resume, per server).
- **Options:** backend-specific and opaque to `App`; a `QueueOptions` instance, a sequence, or `None`. The active backend selects option types it understands. Repeated creation for the same mode/path returns the same server if options and `validate_types` match (conflicting configuration raises `ValueError`).
- **Validation default:** `validate_types` sets the server-level default for argument validation (`None` inherits the app default).
- **Events:** each `Bus` has its own event queue (`<queue_base>.<app.instance_id>`); `options` apply to that bus when supported by the backend.

Example:

```python
from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions

app = App("orders", backend=RmqBackend())
rpc = app.rpc("serviceA", options=[RmqQueueOptions(prefetch_count=5, durable=True)])
bus = app.bus(options=[RmqQueueOptions(prefetch_count=10)])

@rpc.method("ping")
def ping() -> str:
    return "pong"
```

## Client call parameters

`call(method, args, kwds, *, request_id=None, client_timeout=None, server_timeout=None, ttl=None, rttl=None, serializer=None, context=None)`  
`send(task, args, kwds, *, request_id=None, client_timeout=None, server_timeout=None, ttl=None, serializer=None, context=None)`  
`emit(topic, event, *, request_id=None, client_timeout=None, ttl=None, serializer=None, context=None)`

- `client_timeout`: client wait budget (RPC response wait, or publish wait for task/event).
- `server_timeout`: handler execution limit (`call`/`send` only; sent in headers). Not accepted by `emit`.
- `ttl` / `rttl`: transport lifetimes (per-call overrides of app defaults where applicable).
- Negative `client_timeout` / `server_timeout` are normalized to `0`.
- `serializer` selects serializer by `name` when multiple are configured.
- `context` is forwarded to middleware/backend.

## Handler options

Server decorators accept `**options`, commonly:

- `validate_types: bool` — enable/disable argument validation for that handler.

```python
rpc = app.rpc()

@rpc.method("add", validate_types=True)
def add(x: int, y: int) -> int: ...
```

## `LocalBackend`

```python
LocalBackend(name: str | None = None)
```

- In-process routing; `LocalBackend.reset()` clears shared in-process state.
- Ignores transport queue options.

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
    queue_name_template: dict[Mode, str] | Callable[[str, Mode], str] | None = None,
    heartbeat: int = 30,
    blocked_connection_timeout: int = 30,
    results_queue_prefix: str | None = None,
)
```

- Requires `aio-pika` (`pip install "linkr[rabbitmq]"`).
- **Exchange:** single string (one exchange for all modes) or per-mode dict `{"rpc","task","event"}`. Types: rpc/task `DIRECT` in per-mode config, event `TOPIC`.
- **Queue options:** per-server `RmqQueueOptions` passed to `app.rpc/worker/bus(..., options=...)` override defaults for that consume; if omitted, backend/mode defaults apply.
- **Queue names:** `queue_name_template` can customize physical RMQ queue names per mode (defaults to legacy `linkr.{queue}.rpc|tasks|events`).
- Per-bus event queues are distinct queues scoped to each bus instance.

### `RmqQueueOptions`

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

- `None` fields keep backend/mode defaults.
- `arguments` passed to queue declaration (e.g. `x-max-length`, `x-dead-letter-exchange`).
- `prefetch_count` sets consumer QoS (dedicated channel may be used for overrides).

## Middleware

- `GzipMiddleware(min_size: int = 1024)` — compress payloads >= `min_size` bytes and set `content_encoding`.

## Serialization

- `JsonSerializer()` — name `linkr1.0`
- `JsonRpcSerializer()` — name `jsonrpc2.0`, RPC-only, extensible `ERROR_CODE_MAP`

## Notes

- Bus event queue naming is per bus scope and app instance (logical queue mapped by backend, e.g. RabbitMQ `linkr.events.<queue>`).
- Routing/queue derivation from names: see [Routing and naming](../concepts/routing-and-naming.md).
