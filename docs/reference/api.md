# API reference

Public API exported from `linkr` (see `src/linkr/__init__.py`) and key classes.

## Top-level exports

```python
from linkr import (
    App, RPCRouter, TaskRouter, get_current_request, Depends,
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

`App(name, *, backend, ttl=None, rpc_timeout=None, rpc_ttl=None, rpc_rttl=None, task_timeout=None, task_ttl=None, event_ttl=None, serializer=None, validate_types=False)`

- **Registration:** `method(name, **options)`, `task(name, **options)`, `subscribe(topic, **options)`, `exception_handler(exc_class)`
- **Queue/router configuration (named RPC/task queues):**
  - `rpc_router(path: str, *, options=None) -> RPCRouter`
  - `task_router(path: str, *, options=None) -> TaskRouter`
  - `path` is required, non-empty, and targets queue `app.name.<path>`.
  - `options`: `QueueOptions`, a sequence of `QueueOptions`, or `None` (backend-specific, opaque to `App`).
  - Default app queue (`app.name`) and event queues are not configured via routers; they use backend-static defaults.
- **Lifecycle:** `init()`, `close(timeout=None)`, async context manager
- **Consume:** `consume()`, `consume_rpc()`, `consume_tasks()`, `consume_events()`
- **Stop:** `stop_consume()`, `stop_consume_rpc(method=None)`, `stop_consume_tasks(task=None)`, `stop_consume_events(topic=None)`
- **Client:** `call(method, args, kwds, *, request_id=None, timeout=None, ttl=None, rttl=None, serializer=None, context=None) -> Any`
  `send(task, args, kwds, *, request_id=None, timeout=None, ttl=None, serializer=None, context=None) -> None`
  `emit(topic, event, *, request_id=None, ttl=None, serializer=None, context=None) -> None`
- **Middleware/Hooks:** `add_middleware(mw)`, `hooks` / `hook`, `add_hooks(observer)`, `remove_hooks(observer)`
- **Contracts/Introspection:** `describe(*, include=None, include_internal=False) -> AppContract`, `describe_json(*, indent=2, **kwargs) -> str`, `describe_html(*, title=None, **describe_kwargs) -> str`, `expose_describe(name="linkr.describe", **handler_options)`
- **Properties:** `name`, `methods`, `tasks`, `subscriptions`

Built-in defaults (used when the corresponding constructor argument is `None`):

* `RPC_TIMEOUT = 300.0` (seconds)
* `RPC_TTL = 300.0` (seconds)
* `RPC_RTTL = 300.0` (seconds)
* `TASK_TIMEOUT = None`
* `TASK_TTL = 3600.0` (seconds)
* `EVENT_TTL = 3600.0` (seconds)

For RPC calls the client waits up to `timeout + ttl` seconds for a response.

Note: the constructor accepts `ttl`, `task_timeout`, `task_ttl`, and `event_ttl`
for API symmetry, but they are currently not used to derive per-mode defaults;
effective values fall back to the corresponding `rpc_*`/built-in defaults.

## `RPCRouter` / `TaskRouter`

Returned by `App.rpc_router(...)` / `App.task_router(...)`; bound to a named `(mode, queue)`.

- **Registration:** `router(name, **options)` — callable decorator
  - `name` must be a non-empty leaf (no `.`); registers as `<path>.<name>` on queue `app.name.<path>`.
  - `handler_options` (e.g. `validate_types`) are handler options, not transport options.
- **Properties:** `mode` (`"rpc"` or `"task"`), `path`

## Backends

- `Backend` (ABC): `init`, `close`, `consume_rpc(queue, handler, *, options=None)`, `consume_task(queue, handler, *, options=None)`, `consume_event(queue, topics, handler)`, `stop_consume_rpc/tasks/events`, `request -> asyncio.Future[Message]`, `send`, `emit`, deprecated `stop_consume(mode=None)`
- `LocalBackend(name=None)`: in-process backend, `reset()` class method; accepts and ignores RPC/task queue `options`
- `RmqBackend(...)`: RabbitMQ backend (`aio-pika`); see `RmqQueueOptions`

Event queue/consumer configuration is backend-static (not configured via `App`).

## Models and queue options

- `Request(id, type, data, headers)` — frozen; `type`: `rpc|task|event`
- `Response(id, type, data, headers)` — frozen; `type`: `result|error`
- `Message(data: bytes, headers: dict)` — wire envelope
- `ErrorInfo(error_code, error_message, error_details)`
- `HandlerInfo` — registered handler metadata
- `QueueOptions` — marker base for backend-specific queue/consumer options (core is transport-agnostic)
- `RmqQueueOptions(...)` (`linkr.backends.rmq`) — RabbitMQ queue/consumer options for named RPC/task queues (`durable`, `exclusive`, `auto_delete`, `passive`, `arguments`, `prefetch_count`, `extra`; `None` = backend/mode default)

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

- `App.describe(*, include=None, include_internal=False) -> AppContract` — builds a contract/spec model (`contract: "linkr/1.0-alpha"`, alpha) describing registered RPC methods, tasks, and event subscriptions. `include` can limit sections to any of `{"methods", "tasks", "events"}`.
- `AppContract` (`linkr.contracts`) — Pydantic model of the contract (`methods`, `tasks`, `events`, `components`). Use `to_dict()` for a JSON-compatible wire dictionary, `to_json()` for a JSON string, and `to_html()` for a self-contained, human-readable HTML page.
- `RpcMethodContract` / `TaskMethodContract` (`linkr.contracts`) — discriminated-by-`kind` method models (`rpc` → `call`+`output.schema`, `task` → `send`+`output: null`); `MethodContract` is their discriminated union.
- `ContractBuilder(app, *, include=None, include_internal=False).build() -> AppContract` — assembles the contract model from an app.
- `HtmlContractRenderer(contract, *, title=None).render() -> str` and `render_contract_html(contract, *, title=None) -> str` (`linkr.contracts`) — render a contract model/dictionary as self-contained HTML (no external dependencies).
- `App.describe_json(*, indent=2, **kwargs) -> str` — JSON string form of the contract.
- `App.describe_html(*, title=None, **describe_kwargs) -> str` — HTML form of the contract (accepts the same filtering kwargs as `describe()`).
- `App.expose_describe(name="linkr.describe", **handler_options)` — registers an RPC handler (opt-in) that returns `describe().to_dict()`.
- `App.subscriptions` — registered event subscriptions grouped by topic pattern.

Contract entries include fully qualified call/send targets, queue names, docstrings, input parameter metadata, JSON Schema fragments for `args`/`kwds`/payload/return (when available), and `validate_types` flags. `Depends[...]` parameters are excluded from public input contracts.
