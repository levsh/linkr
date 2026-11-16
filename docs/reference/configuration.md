# Configuration

## `App`

```python
App(
    name: str,
    *,
    backend: Backend,
    serializer: Serializer | list[Serializer] | None = None,
    middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    rpc_timeout: float = RPC_TIMEOUT,
    rpc_ttl: float = RPC_TTL,
    rpc_rttl: float = RPC_RTTL,
    task_timeout: float = TASK_TIMEOUT,
    task_ttl: float = TASK_TTL,
    event_ttl: float = EVENT_TTL,
    validate_types: bool = False,
)
```

| Parameter | Default | Description |
|---|---|---|
| `name` | required | Application name; used as queue prefix and in server/event queue naming |
| `backend` | required | `Backend` implementation (`LocalBackend`, `RmqBackend`, custom) |
| `serializer` | `JsonSerializer()` | Single serializer or list; first is default for outgoing, list enables auto-detection |
| `middlewares` | `None` | List of `AppMiddleware`/`WireMiddleware` instances; initialized/closed with the app and applied across its servers |
| `rpc_timeout` | `RPC_TIMEOUT` (`60.0`) | Default RPC handler execution timeout (seconds) |
| `rpc_ttl` | `RPC_TTL` (`60.0`) | Default RPC request time-to-live (seconds) |
| `rpc_rttl` | `RPC_RTTL` (`600.0`) | Default RPC response time-to-live (seconds) |
| `task_timeout` | `TASK_TIMEOUT` (`3600.0`) | Default task handler execution timeout (seconds) |
| `task_ttl` | `TASK_TTL` (`3600.0`) | Default task time-to-live (seconds) |
| `event_ttl` | `EVENT_TTL` (`3600.0`) | Default event time-to-live (seconds) |
| `validate_types` | `False` | Default argument type validation for handlers (overridable per handler) |

**Class constants:** `RPC_TIMEOUT = 60.0`, `RPC_TTL = 60.0`, `RPC_RTTL = 600.0`, `TASK_TIMEOUT = 3600.0`, `TASK_TTL = 3600.0`, `EVENT_TTL = 3600.0`. For RPC, the client waits for a response up to `timeout + ttl` seconds.

## `Server` and per-server queue options

Servers are created and configured via `App.server(...)`:

```python
server = app.server(
    path: str | None = None,
    *,
    rpc_options: QueueOptions | Sequence[QueueOptions] | None = None,
    task_options: QueueOptions | Sequence[QueueOptions] | None = None,
    event_options: QueueOptions | Sequence[QueueOptions] | None = None,
) -> Server
```

- **Scope/queue:** `path=None` → root server, base queue `app.name`; `path="group"` → queue `app.name.group`.
- **Handler registration (on server):** `@server.method(name, **opts)`, `@server.task(name, **opts)`, `@server.subscribe(topic, **opts)`. Method/task `name` is a local leaf (must not contain `.`). Common option: `validate_types`.
- **Consume control:** `server.consume(mode=None|"rpc"|"task"|"event")`, `server.stop_consume(mode=None|...)` (pause/resume, per server and mode).
- **Options:** backend-specific and opaque to `App`; a `QueueOptions` instance, a sequence, or `None`. The active backend selects option types it understands. Repeated `app.server` for the same path returns the same server if options match (conflicting options raise `ValueError`).
- **Events:** each server has its own event queue; `event_options` apply to that server's event consumer/queue when supported by the backend.

Example:

```python
from linkr import App
from linkr.backends.rmq import RmqBackend, RmqQueueOptions

app = App("orders", backend=RmqBackend())
rpc = app.server("serviceA", rpc_options=[RmqQueueOptions(prefetch_count=5, durable=True)])
events = app.server(event_options=[RmqQueueOptions(prefetch_count=10)])

@rpc.method("ping")
def ping() -> str:
    return "pong"
```

## Client call parameters

`call(method, args, kwds, *, request_id=None, timeout=None, ttl=None, rttl=None, serializer=None, context=None)`  
`send(task, args, kwds, *, request_id=None, timeout=None, ttl=None, serializer=None, context=None)`  
`emit(topic, event, *, request_id=None, ttl=None, serializer=None, context=None)`

- Per-call `timeout`/`ttl`/`rttl` override app defaults where applicable.
- `serializer` selects serializer by `name` when multiple are configured.
- `context` is forwarded to middleware/backend.

## Handler options

Server decorators accept `**options`, commonly:

- `validate_types: bool` — enable/disable argument validation for that handler.

```python
server = app.server()

@server.method("add", validate_types=True)
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
    queue_options: RmqQueueOptions | dict[Literal["rpc", "task", "event"], RmqQueueOptions] | None = None,
    heartbeat: int = 30,
    blocked_connection_timeout: int = 30,
    results_queue_prefix: str | None = None,
)
```

- Requires `aio-pika` (`pip install "linkr[rabbitmq]"`).
- **Exchange:** single string (one exchange for all modes) or per-mode dict `{"rpc","task","event"}`. Types: rpc/task `DIRECT` in per-mode config, event `TOPIC`.
- **Queue option defaults:** `queue_options` may be a single `RmqQueueOptions` (all modes) or a per-mode dict for `rpc`/`task`/`event`; per-server options override defaults for that consume.
- Per-server event queues are distinct queues scoped to each server instance.

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

- Server event queue naming is per server scope and app instance (logical queue mapped by backend, e.g. RabbitMQ `linkr.events.<queue>`).
- Routing/queue derivation from names: see [Routing and naming](../concepts/routing-and-naming.md).
