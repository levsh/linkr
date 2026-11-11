# API reference

Public API exported from `linkr` (see `src/linkr/__init__.py`) and key classes.

## Top-level exports

```python
from linkr import (
    App, Router, get_current_request, Depends,
    Backend, LocalBackend, Mode, QueueOptions,
    Message, Request, Response, RequestType, ResponseType, HandlerInfo, ErrorInfo,
    Serializer, JsonSerializer, JsonRpcSerializer,
    AppMiddleware, BaseMiddleware, WireMiddleware,
    RPCError, ErrorCode,
    AppHooks, HookRegistry, HookPoint,
    ConsumeStartEvent, ConsumeStopEvent,
    RequestPublishedEvent, ResponseReceivedEvent, RequestReceivedEvent,
    BeforeHandlerEvent, AfterHandlerEvent, HandlerErrorEvent, MessageDroppedEvent,
)
```

`RmqBackend` and `RmqQueueOptions` are available from `linkr.backends.rmq` (requires `aio-pika`).

## `App`

`App(name, *, backend, timeout=None, ttl=None, rttl=None, serializer=None, validate_types=False)`

- **Registration:** `method(name, **options)`, `task(name, **options)`, `subscribe(topic, **options)`, `exception_handler(exc_class)`
- **Queue/router configuration:** `router(mode, path=None, *, options=None) -> Router`
  - `mode`: `"rpc" | "task" | "event"`
  - `path`: `None` targets the default app queue for rpc/task (`app.name`) or the instance event queue for event; a string targets a named rpc/task queue (`app.name.<path>`). Event routers must not specify `path`.
  - `options`: `QueueOptions`, a sequence of `QueueOptions`, or `None` (backend-specific, opaque to `App`)
- **Lifecycle:** `init()`, `close(timeout=None)`, async context manager
- **Consume:** `consume()`, `consume_rpc()`, `consume_tasks()`, `consume_events()`
- **Stop:** `stop_consume()`, `stop_consume_rpc(method=None)`, `stop_consume_tasks(task=None)`, `stop_consume_events(topic=None)`
- **Client:** `call(method, args, kwds, *, request_id=None, timeout=None, ttl=None, rttl=None, serializer=None, context=None) -> Any`
  `send(task, args, kwds, *, request_id=None, timeout=None, ttl=None, serializer=None, context=None) -> None`
  `emit(topic, event, *, request_id=None, ttl=None, serializer=None, context=None) -> None`
- **Middleware/Hooks:** `add_middleware(mw)`, `hooks` / `hook`, `add_hooks(observer)`, `remove_hooks(observer)`
- **Properties:** `name`, `methods`, `tasks`

## `Router`

Returned by `App.router(...)`; bound to a `(mode, queue)`.

- **Registration:** `router(name, **options)` — callable decorator
  - rpc/task: `name` must be a non-empty leaf (no `.`); registers as `<path>.<name>` or `<name>` on the router queue
  - event: `name` is a topic pattern (equivalent to `app.subscribe`)
- **Properties:** `mode`, `path`

## Backends

- `Backend` (ABC): `init`, `close`, `consume_rpc(queue, handler, *, options=None)`, `consume_task(queue, handler, *, options=None)`, `consume_event(queue, topics, handler, *, options=None)`, `stop_consume_rpc/tasks/events`, `request -> asyncio.Future[Message]`, `send`, `emit`, deprecated `stop_consume(mode=None)`
- `LocalBackend(name=None)`: in-process backend, `reset()` class method; accepts and ignores queue `options`
- `RmqBackend(...)`: RabbitMQ backend (`aio-pika`); see `RmqQueueOptions`

## Models and queue options

- `Request(id, type, data, headers)` — frozen; `type`: `rpc|task|event`
- `Response(id, type, data, headers)` — frozen; `type`: `result|error`
- `Message(data: bytes, headers: dict)` — wire envelope
- `ErrorInfo(error_code, error_message, error_details)`
- `HandlerInfo` — registered handler metadata
- `QueueOptions` — marker base for backend-specific queue/consumer options (core is transport-agnostic)
- `RmqQueueOptions(...)` (`linkr.backends.rmq`) — RabbitMQ queue/consumer options (`durable`, `exclusive`, `auto_delete`, `passive`, `arguments`, `prefetch_count`, `extra`; `None` = backend/mode default)

## Serialization

- `Serializer` (ABC): `name`, `dumps_request/loads_request`, `dumps_response/loads_response`
- `JsonSerializer` — `linkr1.0`
- `JsonRpcSerializer` — `jsonrpc2.0`, RPC-only, `ERROR_CODE_MAP`

## Middleware

- `BaseMiddleware`: `init()`, `close()`
- `AppMiddleware`: `dispatch_client(call_next, request, *, kwds) -> Response|None`, `dispatch_server(call_next, request, request_message) -> Response|None`
- `WireMiddleware`: `dispatch_client(call_next, request_message, request, *, kwds) -> Message|None`, `dispatch_server(call_next, request_message) -> (Message,Response)|(None,None)`
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
