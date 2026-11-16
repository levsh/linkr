# API reference

Public API exported from `linkr` (see `src/linkr/__init__.py`) and key classes.

## Top-level exports

```python
from linkr import (
    App, Server, get_current_request, Depends,
    Backend, LocalBackend, Mode, QueueOptions,
    Message, Request, Response, RequestType, ResponseType, HandlerInfo, ErrorInfo,
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

`App(name, *, backend, serializer=None, middlewares=None, rpc_timeout=RPC_TIMEOUT, rpc_ttl=RPC_TTL, rpc_rttl=RPC_RTTL, task_timeout=TASK_TIMEOUT, task_ttl=TASK_TTL, event_ttl=EVENT_TTL, validate_types=False)`

Application infrastructure, lifecycle, hooks/middleware, and client API. Handler registration and consuming are performed on `Server` instances.

- **Lifecycle:** `init()`, `close(timeout=None)`, async context manager
  - `init()` initializes backend and middleware.
  - `close(timeout=None)` stops consuming on all servers, closes middleware and backend (the `timeout` argument is reserved for future graceful-shutdown control and is currently unused).
- **Servers:** `server(path: str | None = None, *, rpc_options=None, task_options=None, event_options=None) -> Server`
  - Returns a server scope for `path` (`None` = root, base queue `app.name`; `"group"` = `app.name.group`).
  - Same `path` returns the same server when options match; conflicting options raise `ValueError`.
  - `*_options`: `QueueOptions`, a sequence of `QueueOptions`, or `None` (backend-specific, opaque to `App`).
- **Client:** 
  - `call(method, args, kwds, *, request_id=None, timeout=None, ttl=None, rttl=None, serializer=None, context=None) -> Any`
  - `send(task, args, kwds, *, request_id=None, timeout=None, ttl=None, serializer=None, context=None) -> None`
  - `emit(topic, event, *, request_id=None, ttl=None, serializer=None, context=None) -> None`
- **Middleware/Hooks/Errors:** `middlewares` (constructor, list of `AppMiddleware`/`WireMiddleware`), `hooks` / `hook`, `add_hooks(observer)`, `remove_hooks(observer)`, `exception_handler(exc_class)`
- **Contracts/Introspection:** `describe(*, include=None, include_internal=False) -> AppContract`, `describe_json(*, indent=2, **kwargs) -> str`, `describe_html(*, title=None, **describe_kwargs) -> str`, `expose_describe(name="describe", server=None, **handler_options)`
- **Properties:** `name`, `backend`, `hooks`, `servers`, `methods`, `tasks`, `subscriptions`

Built-in defaults (class constants):

- `RPC_TIMEOUT = 60.0` (seconds)
- `RPC_TTL = 60.0` (seconds)
- `RPC_RTTL = 600.0` (seconds)
- `TASK_TIMEOUT = 3600.0` (seconds)
- `TASK_TTL = 3600.0` (seconds)
- `EVENT_TTL = 3600.0` (seconds)

For RPC calls the client waits for a response up to `timeout + ttl` seconds.

There are no global `app.consume()` / `app.stop_consume()` methods; consuming is managed per `Server`.

## `Server`

Created via `app.server(...)`. A server is bound to an app and queue scope.

- **Identity:** `app`, `path` (`None` for root), `queue_base` (`app.name` or `app.name.<path>`), `event_queue` (when event consuming is active)
- **Registration (local leaf names, no `.` in method/task name):**
  - `method(name, **options)` — register RPC handler on this server's queue (`<queue_base>.<name>`)
  - `task(name, **options)` — register task handler on this server's queue
  - `subscribe(topic, **options)` — register event subscriber (topic pattern) on this server's event queue
  - Common handler option: `validate_types`
- **Consuming:** `consume(mode=None)` / `stop_consume(mode=None)` where `mode` is `None | "rpc" | "task" | "event"`
  - `consume` starts consuming for the specified/all applicable modes (idempotent).
  - `stop_consume` pauses consuming for the specified/all modes (idempotent); registrations/subscriptions remain and can be resumed with `consume`.
  - Each server has its own event queue; event consuming/stopping is scoped to the server.
- **Introspection:** `methods`, `tasks`, `subscriptions` (handlers registered on this server)

## Backends

- `Backend` (ABC): `init`, `close`, `consume_rpc(queue, handler, *, options=None)`, `consume_task(queue, handler, *, options=None)`, `consume_event(queue, topics, handler, *, options=None)`, `stop_consume_rpc(queue=None)`, `stop_consume_tasks(queue=None)`, `stop_consume_events(queue=None, topic=None)`, `request(queue, message, *, context=None) -> asyncio.Future[Message]`, `send(queue, message, *, context=None)`, `emit(topic, message, *, context=None)`
- `LocalBackend(name=None)`: in-process backend, `reset()` class method; ignores transport queue options
- `RmqBackend(...)` (`linkr.backends.rmq`): RabbitMQ backend (`aio-pika`); see `RmqQueueOptions`

## Models and queue options

- `Request(id, type, data, headers)` — frozen; `type`: `rpc|task|event`
- `Response(id, type, data, headers)` — frozen; `type`: `result|error`
- `Message(data: bytes, headers: dict)` — wire envelope
- `ErrorInfo(error_code, error_message, error_details)`
- `HandlerInfo` — registered handler metadata
- `QueueOptions` — marker base for backend-specific queue/consumer options
- `RmqQueueOptions(...)` (`linkr.backends.rmq`) — RabbitMQ queue/consumer options (`durable`, `exclusive`, `auto_delete`, `passive`, `arguments`, `prefetch_count`, `extra`)

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

- `App.describe(*, include=None, include_internal=False) -> AppContract` — contract/spec (`contract: "linkr/1.0-alpha"`, alpha) describing registered RPC methods, tasks, and event subscriptions across servers.
- `AppContract` (`linkr.contracts`) — `to_dict()`, `to_json()`, `to_html()`.
- `ContractBuilder(...).build()`, `HtmlContractRenderer(...)`, `render_contract_html(...)`.
- `App.describe_json(...)`, `App.describe_html(...)`.
- `App.expose_describe(name="describe", server=None, **handler_options)` — registers an RPC handler on a server that returns `describe().to_dict()`.
- `App.subscriptions` / `App.methods` / `App.tasks` — aggregated views across servers.

Contract entries include fully qualified call/send targets, queue names, input/output schema fragments (when available), and `validate_types` flags. `Depends[...]` parameters are excluded from public input contracts.
