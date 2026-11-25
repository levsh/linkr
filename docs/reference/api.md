# API reference

Public API exported from `linkr` (see `src/linkr/__init__.py`) and key classes.

## Top-level exports

```python
from linkr import (
    App, RPC, Worker, Bus, get_current_request, Depends,
    Backend, LocalBackend, Mode, QueueOptions,
    Message, Request, Response, RequestType, ResponseType, HandlerInfo, ErrorInfo, HealthStatus,
    Serializer, JsonSerializer, JsonRpcSerializer,
    AppMiddleware, BaseMiddleware, WireMiddleware,
    RPCError, ErrorCode, LinkrError, RequestError,
    AppHooks, HookRegistry, HookPoint,
    ConsumeStartEvent, ConsumeStopEvent,
    RequestPublishedEvent, ResponseReceivedEvent, RequestReceivedEvent,
    BeforeHandlerEvent, AfterHandlerEvent, HandlerErrorEvent, MessageDroppedEvent,
)
```

`RmqBackend` and `RmqQueueOptions` are available from `linkr.backends.rmq` (requires `aio-pika`).

## `App`

`App(name, *, backend, serializer=None, middlewares=None, rpc_client_timeout=RPC_CLIENT_TIMEOUT, rpc_server_timeout=RPC_SERVER_TIMEOUT, rpc_ttl=RPC_TTL, rpc_rttl=RPC_RTTL, task_client_timeout=TASK_CLIENT_TIMEOUT, task_server_timeout=TASK_SERVER_TIMEOUT, task_ttl=TASK_TTL, event_client_timeout=EVENT_CLIENT_TIMEOUT, event_ttl=EVENT_TTL, validate_types=False)`

Application infrastructure, lifecycle, hooks/middleware, and client API. Handler registration and consuming are performed on mode servers.

- **Lifecycle:** `init()`, `close(timeout=None)`, `healthcheck(fast=None)`, async context manager
  - `init()` initializes backend and middleware.
  - `close(timeout=None)` stops consuming on all servers, closes middleware and backend (the `timeout` argument is reserved for future graceful-shutdown control and is currently unused).
  - `healthcheck(fast: bool | None = None) -> HealthStatus` returns application readiness/health. If the app is closed, returns `ready=False, health=None` without `details`. Otherwise it delegates to backend `healthcheck(fast=fast)`; if the backend is not ready, returns `ready=False, health=None` preserving backend `details` when present; if ready, returns a new status with backend `health/details`. Backend exceptions propagate. The status includes read-only public `details`: `connection` is `"ok"|"error"`; `queues` is `"ok"` or a tuple of problematic logical queues with `{"mode","queue","reason"}` where `reason` is `"no_consumer"` or `"unavailable"`. `fast=True` checks connection/transport only; `fast=False` or `fast=None` performs full health checks, including queue/consumer checks where supported. If a check is not requested or a fail-fast connection issue is reported, the corresponding key is omitted from `details`. `App.healthcheck()` never returns the backend instance and does not expose backend-internal transport details.
- **Servers:**
  - `rpc(path: str | None = None, *, options=None, validate_types=None) -> RPC`
  - `worker(path: str | None = None, *, options=None, validate_types=None) -> Worker`
  - `bus(path: str | None = None, *, options=None, validate_types=None) -> Bus`
  - `path=None` → root scope (base queue `app.name`); `path="group"` → `app.name.group`.
  - Same mode/path returns the same server when options match; conflicting options or `validate_types` raise `ValueError`.
  - `options`: `QueueOptions`, a sequence of `QueueOptions`, or `None` (backend-specific, opaque to `App`).
  - `validate_types`: server-level default for argument validation (`True`/`False`/`None` to inherit app default); handler-level `validate_types` overrides server, which overrides app.
- **Client:** 
  - `call(method, args, kwds, *, request_id=None, client_timeout=None, server_timeout=None, ttl=None, rttl=None, serializer=None, context=None) -> Any`
  - `send(task, args, kwds, *, request_id=None, client_timeout=None, server_timeout=None, ttl=None, serializer=None, context=None) -> None`
  - `emit(topic, event, *, request_id=None, client_timeout=None, ttl=None, serializer=None, context=None) -> None`
- **Middleware/Hooks/Errors:** `middlewares` (constructor, list of `AppMiddleware`/`WireMiddleware`), `hooks` / `hook`, `add_hooks(observer)`, `remove_hooks(observer)`, `exception_handler(exc_class)`
- **Properties:** `name`, `backend`, `hooks`, `servers`, `methods`, `tasks`, `subscriptions`, `is_closed`

Built-in defaults (class constants):

- `RPC_CLIENT_TIMEOUT = 60.0` (seconds)
- `RPC_SERVER_TIMEOUT = 60.0` (seconds)
- `RPC_TTL = 60.0` (seconds)
- `RPC_RTTL = 60.0` (seconds)
- `TASK_CLIENT_TIMEOUT = 30.0` (seconds)
- `TASK_SERVER_TIMEOUT = 3600.0` (seconds)
- `TASK_TTL = 3600.0` (seconds)
- `EVENT_CLIENT_TIMEOUT = 30.0` (seconds)
- `EVENT_TTL = 3600.0` (seconds)

Negative `client_timeout` / `server_timeout` values are normalized to `0`. There is no server execution timeout for events.

There are no global `app.consume()` / `app.stop_consume()` methods; consuming is managed per mode server.

## Mode servers

Created via `app.rpc(...)`, `app.worker(...)`, `app.bus(...)`. Each server is bound to an app and queue scope.

- **Common:** `app`, `path` (`None` for root), `queue_base` (`app.name` or `app.name.<path>`), `validate_types` (server default), `consume()`, `stop_consume()`
  - `consume()` starts consuming for the server (idempotent).
  - `stop_consume()` pauses consuming (idempotent); registrations/subscriptions remain and can be resumed with `consume()`.
- **`RPC`:**
  - `method(name, **options)` — register RPC handler on this server's queue (`<queue_base>.<name>`)
  - `methods` — registered RPC handlers (by leaf name)
- **`Worker`:**
  - `task(name, **options)` — register task handler on this server's queue
  - `tasks` — registered task handlers (by leaf name)
- **`Bus`:**
  - `subscribe(topic, **options)` — register event subscriber (topic pattern) on this bus's event queue
  - `subscriptions` — registered subscriptions grouped by topic
  - `event_queue` — active event queue name when consuming
  - Each bus has its own event queue (`<queue_base>.<app.instance_id>` pattern).

Method/task `name` must be a local leaf (no `.`). Common handler option: `validate_types` (precedence: handler > server > app).

## Backends

- `Backend` (ABC): `init`, `close`, `healthcheck(fast: bool | None = None) -> HealthStatus`, `consume_rpc(queue, handler, *, options=None)`, `consume_task(queue, handler, *, options=None)`, `consume_event(queue, topics, handler, *, options=None)`, `stop_consume_rpc(queue=None)`, `stop_consume_tasks(queue=None)`, `stop_consume_events(queue=None, topic=None)`, `request(queue, message, *, context=None) -> asyncio.Future[Message]`, `send(queue, message, *, context=None)`, `emit(topic, message, *, context=None)`
- `LocalBackend(name=None)`: in-process backend, `reset()` class method; ignores transport queue options. `healthcheck(fast=None)` reports `connection="error"` when not ready; otherwise `connection="ok"` and includes `queues="ok"` when `fast` is `False` or `None`.
- `RmqBackend(...)` (`linkr.backends.rmq`): RabbitMQ backend (`aio-pika`); see `RmqQueueOptions`. `healthcheck(fast=None)` reports `connection/queues` status. `fast=True` checks connection/transport only; `fast=False` or `fast=None` performs full checks, including queue/consumer checks. When checked, `queues` contains only problematic logical queues with `reason="no_consumer"` or `reason="unavailable"`. No backend-internal transport/probe/exception details are exposed.

## Models and queue options

- `Request(id, type, data, headers)` — frozen; `type`: `rpc|task|event`
- `Response(id, type, data, headers)` — frozen; `type`: `result|error`
- `Message(data: bytes, headers: dict)` — wire envelope
- `ErrorInfo(error_code, error_message, error_details)`
- `HandlerInfo` — registered handler metadata
- `HealthStatus(ready: bool, health: bool | None, details: Mapping[str, Any] | None = None)` — frozen readiness/health status; `health` is `None` when `ready` is `False`; `details` is normalized to a read-only mapping with public `connection/queues` diagnostics
- `QueueOptions` — marker base for backend-specific queue/consumer options
- `RmqQueueOptions(...)` (`linkr.backends.rmq`) — RabbitMQ queue/consumer options (`durable`, `exclusive`, `auto_delete`, `passive`, `arguments`, `prefetch_count`, `ack_mode`, `extra`). `ack_mode="after_reply"` (default) acknowledges after successful processing and, for RPC, after successful reply publish; for task/event it is equivalent to `after_handler`. `ack_mode="after_handler"` acknowledges after successful handler execution. `ack_mode="before_handler"` acknowledges before handler execution.

## Serialization

- `Serializer` (ABC): `name`, `dumps_request/loads_request`, `dumps_response/loads_response`
- `JsonSerializer` — `linkr1.0`
- `JsonRpcSerializer` — `jsonrpc2.0`, RPC-only, `ERROR_CODE_MAP`

## Middleware

- `BaseMiddleware`: `init()`, `close()`
- `AppMiddleware`: `dispatch_client(call_next, request, *, context) -> Response|None`, `dispatch_server(call_next, request) -> Response|None`
- `WireMiddleware`: `dispatch_client(call_next, request, request_message, *, context) -> Message|None`, `dispatch_server(call_next, request_message) -> (Message,Response)|(None,None)`
- `linkr.middleware.gzip.GzipMiddleware(min_size=1024)`

## DI and errors

- `Depends[T, factory]` — dependency marker for handler annotations
- `get_current_request() -> Request`
- `RPCError(error_code, error_message, error_details)`
- `ErrorCode`: `INTERNAL_ERROR`, `METHOD_NOT_FOUND`, `TIMEOUT`, `VALIDATION_ERROR`

## Hooks

- `AppHooks` (observer base), `HookRegistry` (`app.hooks`), `HookPoint`
- Events: `ConsumeStartEvent`, `ConsumeStopEvent`, `RequestPublishedEvent`, `ResponseReceivedEvent`, `RequestReceivedEvent`, `BeforeHandlerEvent`, `AfterHandlerEvent`, `HandlerErrorEvent`, `MessageDroppedEvent`

See [Configuration](configuration.md) and [Hooks events](hooks-events.md) for detailed fields and parameters.

## Contracts / introspection

- `describe_app(app, *, include=None, include_internal=False) -> AppContract` (`linkr.contracts`) — contract/spec (`contract: "linkr/1.0-alpha"`, alpha) describing registered RPC methods, tasks, and event subscriptions across servers.
- `describe_app_json(app, *, indent=2, **kwargs) -> str` (`linkr.contracts`) — JSON representation of the contract.
- `describe_app_html(app, *, title=None, **describe_kwargs) -> str` (`linkr.contracts`) — self-contained HTML contract view.
- `expose_describe(app, name="describe", server: RPC | None = None, **handler_options)` (`linkr.contracts`) — registers an RPC handler on an RPC server that returns `describe_app(app).to_dict()`.
- `AppContract` (`linkr.contracts`) — `to_dict()`, `to_json()`, `to_html()`.
- `ContractBuilder(...).build()`, `HtmlContractRenderer(...)` (`linkr.contracts`).
- `App.subscriptions` / `App.methods` / `App.tasks` — aggregated views across servers.

Contract entries include fully qualified call/send targets, queue names, middleware `features`, input/output schema fragments (when available), and `validate_types` flags. Event subscriber entries also include `features`. `Depends[...]` parameters are excluded from public input contracts.
