from __future__ import annotations

import asyncio
import inspect
import logging
import re
from collections.abc import Callable, Coroutine, Sequence
from contextvars import ContextVar
from functools import wraps
from typing import Annotated, Any, Literal, get_args, get_origin, get_type_hints
from uuid import UUID, uuid4

from pydantic import TypeAdapter, ValidationError

from .backends import Backend, Mode
from .di import Depends
from .exceptions import ErrorCode, RPCError
from .hooks import (
    AfterHandlerEvent,
    AppHooks,
    BeforeHandlerEvent,
    ConsumeStartEvent,
    ConsumeStopEvent,
    HandlerErrorEvent,
    HookRegistry,
    MessageDroppedEvent,
    RequestPublishedEvent,
    RequestReceivedEvent,
    ResponseReceivedEvent,
)
from .middleware import AppMiddleware, WireMiddleware
from .models import ErrorInfo, HandlerInfo, Message, QueueOptions, Request, RequestType, Response
from .serializer import JsonSerializer, Serializer
from .utils import topic_to_regex

logger = logging.getLogger("linkr")


class Router:
    """Handle for configuring and registering handlers on a specific queue."""

    def __init__(self, app: App, mode: Mode, path: str | None) -> None:
        self._app = app
        self._mode = mode
        self._path = path

    @property
    def mode(self) -> Mode:
        return self._mode

    @property
    def path(self) -> str | None:
        return self._path

    def _full_name(self, name: str) -> str:
        if self._mode == "event":
            return name
        if not name:
            raise ValueError("handler name must not be empty")
        if "." in name:
            raise ValueError("router handler name must not contain '.'; use router path for queue grouping")
        if self._path is None:
            return name
        return f"{self._path}.{name}"

    def __call__(
        self,
        name: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a handler on this router's queue."""
        full_name = self._full_name(name)
        if self._mode == "rpc":
            return self._app.method(full_name, **options)
        if self._mode == "task":
            return self._app.task(full_name, **options)
        return self._app.subscribe(full_name, **options)



_current_request: ContextVar[Request | None] = ContextVar("linkr_current_request", default=None)


def get_current_request() -> Request:
    """
    Get the current RPC request being dispatched in this async context.
    """
    req = _current_request.get()
    if req is None:
        raise RuntimeError("no active RPC request in current context")
    return req


class App:
    """
    Main application: register RPC/task/event handlers, send messages and
    manage middleware.

    Delivery modes::

        @app.method("foo")             # one queue, one consumer, reply
        @app.method("service.foo")     # dedicated queue service, reply
        @app.task("email.send")        # task queue, one consumer, no reply
        @app.subscribe("user.created") # every subscriber, no reply
    """

    RPC_TIMEOUT = 3600.0
    RPC_TTL = 3600.0

    def __init__(
        self,
        name: str,
        *,
        backend: Backend,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: Serializer | list[Serializer] | None = None,
        validate_types: bool = False,
    ) -> None:
        """
        Create a new application instance.

        Args:
            name: Application name used as the default queue prefix.
            backend: Backend instance for message delivery (``LocalBackend``
                or ``RmqBackend``).
            timeout: Default RPC/client timeout in seconds. Applied to the
                ``timeout`` header of outgoing RPC requests. Defaults to
                :attr:`RPC_TIMEOUT` (3600s) if not set.
            ttl: Default message TTL in seconds. Applied to the ``ttl``
                header of outgoing RPC/task/event requests. Defaults to
                :attr:`RPC_TTL` (3600s) if not set.
            rttl: Default response TTL in seconds. Applied to the ``rttl``
                header of outgoing RPC requests.
            serializer: Single serializer or list of serializers. The first
                entry is used for outgoing calls; the list enables
                auto-detection on the server. Defaults to ``JsonSerializer``.
            validate_types: Enable pydantic type validation for all handlers
                by default. Can be overridden per-handler via
                ``validate_types=True/False`` in ``method``/``task``/
                ``subscribe`` options.
        """
        self._name = name
        self._backend = backend
        self._timeout = timeout
        self._ttl = ttl
        self._rttl = rttl
        self._validate_types = validate_types
        self._closed = False

        self._rpc_handlers: dict[tuple[str, str], HandlerInfo] = {}
        self._task_handlers: dict[tuple[str, str], HandlerInfo] = {}
        self._subscribers: dict[re.Pattern[str], list[HandlerInfo]] = {}
        self._subscription_patterns: set[str] = set()

        self._consumed_rpc: set[str] = set()
        self._consumed_tasks: set[str] = set()
        self._event_queue: str | None = None
        self._queue_options: dict[tuple[Mode, str], tuple[QueueOptions, ...]] = {}

        self._instance_id = uuid4().hex
        self._app_mw: list[AppMiddleware] = []
        self._wire_mw: list[WireMiddleware] = []
        self._exception_handlers: dict[type[Exception], Callable[[Exception], Any]] = {}
        self._serializers: dict[str, Serializer] = {}
        self._hooks = HookRegistry()

        serializers: list[Serializer]
        if serializer is None:
            serializers = [JsonSerializer()]
        elif isinstance(serializer, Serializer):
            serializers = [serializer]
        else:
            serializers = list(serializer)

        if not serializers:
            raise ValueError("at least one serializer is required")

        self._serializer_list = serializers
        self._default_serializer = serializers[0]
        for ser in serializers:
            self._serializers[ser.name] = ser

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Application name used as the default queue prefix."""
        return self._name

    @property
    def methods(self) -> dict[str, HandlerInfo]:
        """All registered RPC handlers keyed by method name."""
        return {method: info for (_, method), info in self._rpc_handlers.items()}

    @property
    def tasks(self) -> dict[str, HandlerInfo]:
        """All registered tasks keyed by name."""
        return {task: info for (_, task), info in self._task_handlers.items()}

    @property
    def hooks(self) -> HookRegistry:
        """Hook subscription registry for this app instance."""
        return self._hooks

    @property
    def hook(self) -> HookRegistry:
        """Alias for :attr:`hooks` (for decorator-style subscriptions)."""
        return self._hooks

    def add_hooks(self, hooks: AppHooks) -> None:
        """Register an :class:`AppHooks` observer."""
        self._hooks.add_hooks(hooks)

    def remove_hooks(self, hooks: AppHooks) -> None:
        """Unregister an :class:`AppHooks` observer."""
        self._hooks.remove_hooks(hooks)

    # ------------------------------------------------------------------
    # Routing helpers
    # ------------------------------------------------------------------

    def _registration_queue(self, name: str) -> str:
        if "." in name:
            return f"{self._name}.{name.rsplit('.', 1)[0]}"
        return self._name

    def _resolve_queue(self, name: str) -> str:
        if "." in name:
            return name.rsplit(".", 1)[0]
        return self._name

    def _event_queue_name(self) -> str:
        return f"{self._name}.events.{self._instance_id}"

    # ------------------------------------------------------------------
    # Routing / queue configuration
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_queue_options(
        options: QueueOptions | Sequence[QueueOptions] | None,
    ) -> tuple[QueueOptions, ...] | None:
        if options is None:
            return None

        if isinstance(options, QueueOptions):
            normalized: tuple[QueueOptions, ...] = (options,)
        elif isinstance(options, Sequence) and not isinstance(options, (str, bytes)):
            normalized = tuple(options)
        else:
            raise TypeError("options must be a QueueOptions instance, a sequence of QueueOptions, or None")

        for option in normalized:
            if not isinstance(option, QueueOptions):
                raise TypeError("each option must be an instance of QueueOptions")

        seen: dict[type[QueueOptions], int] = {}
        for option in normalized:
            opt_type = type(option)
            if opt_type in seen:
                raise ValueError(f"duplicate queue options for backend type: {opt_type.__name__}")
            seen[opt_type] = 1

        return normalized

    def _queue_key(self, mode: Mode, path: str | None) -> tuple[Mode, str]:
        if mode == "event":
            if path is not None:
                raise ValueError("event router does not accept a path")
            return mode, self._event_queue_name()

        if path is None:
            return mode, self._name

        if path == "":
            raise ValueError("queue path must not be empty")

        return mode, f"{self._name}.{path}"

    def _is_queue_consuming(self, mode: Mode, queue: str) -> bool:
        if mode == "rpc":
            return queue in self._consumed_rpc
        if mode == "task":
            return queue in self._consumed_tasks
        return self._event_queue == queue

    def _configure_queue(
        self,
        mode: Mode,
        path: str | None,
        options: QueueOptions | Sequence[QueueOptions] | None,
    ) -> tuple[Mode, str]:
        key = self._queue_key(mode, path)
        normalized = self._normalize_queue_options(options)
        if normalized is None:
            return key

        if self._is_queue_consuming(*key):
            raise RuntimeError(f"cannot configure queue while consuming: {mode} {key[1]!r}")

        existing = self._queue_options.get(key)
        if existing is None:
            self._queue_options[key] = normalized
        elif existing != normalized:
            raise ValueError(f"queue options already configured for {mode} queue {key[1]!r}")
        return key

    def router(
        self,
        mode: Mode,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
    ) -> Router:
        """Configure and/or get a router handle for a queue.

        ``path=None`` targets the default queue (``app.name``) for rpc/task,
        and the instance event queue for ``mode='event'``.
        """
        if mode not in ("rpc", "task", "event"):
            raise ValueError(f"invalid router mode: {mode!r}")
        self._configure_queue(mode, path, options)
        return Router(self, mode, path)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Open backend and initialise all middleware."""
        if self._closed:
            raise RuntimeError("App is closed")

        await self._backend.init()

        for app_mw in self._app_mw:
            await app_mw.init()

        for wire_mw in self._wire_mw:
            await wire_mw.init()

    async def close(self, timeout: float | None = None) -> None:
        """Shut down the application."""
        await self.stop_consume()

        for app_mw in reversed(self._app_mw):
            await app_mw.close()

        for wire_mw in reversed(self._wire_mw):
            await wire_mw.close()

        await self._backend.close(timeout=timeout)
        self._closed = True

    async def __aenter__(self) -> App:
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def add_middleware(self, mw: AppMiddleware | WireMiddleware) -> None:
        """Register app-level or wire-level middleware."""
        if isinstance(mw, AppMiddleware):
            self._app_mw.append(mw)
        else:
            self._wire_mw.append(mw)

    def method(
        self,
        name: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register an RPC handler."""
        return self._register("rpc", name, options)

    def task(
        self,
        name: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Register a task handler. Tasks never return a response."""
        return self._register("task", name, options)

    def subscribe(
        self,
        topic: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Register an event subscriber.

        Every app instance gets its own queue, so all subscribers receive
        every event matching *topic*. ``*`` and ``#`` wildcards are supported.
        """
        pattern = topic_to_regex(topic)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            @wraps(fn)
            def wrapper(*args: Any, **kwds: Any) -> Any:
                return fn(*args, **kwds)

            info = HandlerInfo(
                name=topic,
                request_type="event",
                fn=wrapper,
                signature=str(inspect.signature(fn)),
                options=options,
                dep_types=self._dep_types(fn),
                queue=self._event_queue_name(),
            )
            self._subscribers.setdefault(pattern, []).append(info)
            self._subscription_patterns.add(topic)
            return wrapper

        return decorator

    def _register(
        self,
        mode: Literal["rpc", "task"],
        name: str,
        options: dict[str, Any],
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        queue = self._registration_queue(name)
        method = name.rsplit(".", 1)[-1]
        registry = self._rpc_handlers if mode == "rpc" else self._task_handlers
        key = (queue, method)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            if key in registry:
                raise ValueError(f"{mode} handler already registered for {queue!r}: {name!r}")

            @wraps(fn)
            def wrapper(*args: Any, **kwds: Any) -> Any:
                return fn(*args, **kwds)

            registry[key] = HandlerInfo(
                name=name,
                request_type=mode,
                fn=wrapper,
                signature=str(inspect.signature(fn)),
                options=options,
                dep_types=self._dep_types(fn),
                queue=queue,
            )
            return wrapper

        return decorator

    @staticmethod
    def _dep_types(fn: Callable[..., Any]) -> dict[str, tuple[type, Callable[[], Any] | None]]:
        dep_types: dict[str, tuple[type, Callable[[], Any] | None]] = {}
        hints = get_type_hints(fn, include_extras=True)
        for pname, ann in hints.items():
            if pname == "return":
                continue
            if get_origin(ann) is not Annotated:
                continue
            args = get_args(ann)
            if not args:
                continue
            type_ = args[0]
            for meta in args[1:]:
                if isinstance(meta, Depends):
                    dep_types[pname] = (type_, meta.factory)
                    break
        return dep_types

    def exception_handler(
        self,
        exc_class: type[Exception],
    ) -> Callable[[Callable[[Exception], Any]], Callable[[Exception], Any]]:
        """Register a custom exception handler for *exc_class*."""

        def decorator(fn: Callable[[Exception], Any]) -> Callable[[Exception], Any]:
            self._exception_handlers[exc_class] = fn
            return fn

        return decorator

    async def consume_rpc(self) -> None:
        """Start listening for incoming RPC messages on all registered queues."""
        if self._closed:
            raise RuntimeError("App is closed")

        rpc_queues = {queue for queue, _ in self._rpc_handlers}
        if self._rpc_handlers:
            rpc_queues.add(self._name)

        for queue in sorted(rpc_queues):
            if queue in self._consumed_rpc:
                continue
            await self._consume_backend("rpc", queue, self._make_handler("rpc", queue))
            self._consumed_rpc.add(queue)
            await self._hooks.emit(
                "on_consume_start",
                ConsumeStartEvent(app=self._name, mode="rpc", queue=queue, topics=()),
            )

    async def consume_tasks(self) -> None:
        """Start listening for incoming task messages on all registered queues."""
        if self._closed:
            raise RuntimeError("App is closed")

        for queue in sorted({queue for queue, _ in self._task_handlers}):
            if queue in self._consumed_tasks:
                continue
            await self._consume_backend("task", queue, self._make_handler("task", queue))
            self._consumed_tasks.add(queue)
            await self._hooks.emit(
                "on_consume_start",
                ConsumeStartEvent(app=self._name, mode="task", queue=queue, topics=()),
            )

    async def consume_events(self) -> None:
        """Start listening for incoming event messages on this instance's queue."""
        if self._closed:
            raise RuntimeError("App is closed")

        if self._subscription_patterns and self._event_queue is None:
            queue = self._event_queue_name()
            topics = tuple(sorted(self._subscription_patterns))
            await self._consume_backend("event", queue, self._make_handler("event", queue), topics=list(topics))
            self._event_queue = queue
            await self._hooks.emit(
                "on_consume_start",
                ConsumeStartEvent(app=self._name, mode="event", queue=queue, topics=topics),
            )

    async def _consume_backend(
        self,
        mode: Mode,
        queue: str,
        handler: Callable[[Message], Coroutine[Any, Any, Message | None]],
        *,
        topics: list[str] | None = None,
    ) -> None:
        options = self._queue_options.get((mode, queue))
        method: Any
        args: tuple[Any, ...]
        if mode == "rpc":
            method = self._backend.consume_rpc
            args = (queue, handler)
        elif mode == "task":
            method = self._backend.consume_task
            args = (queue, handler)
        else:
            method = self._backend.consume_event
            args = (queue, topics or [], handler)

        try:
            params: Any = inspect.signature(method).parameters
        except (TypeError, ValueError):
            params = {}

        if "options" in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
            await method(*args, options=options)
        else:
            if options is not None:
                logger.warning(
                    "Backend %s.consume_%s does not accept queue options; options ignored for queue %s",
                    type(self._backend).__name__,
                    "events" if mode == "event" else mode,
                    queue,
                )
            await method(*args)

    async def consume(self) -> None:
        """Start listening for incoming RPC, task and event messages."""
        await self.consume_rpc()
        await self.consume_tasks()
        await self.consume_events()

    async def stop_consume_rpc(self, method: str | None = None) -> None:
        """
        Stop consuming RPC messages.

        Args:
            method: When ``None``, stop all RPC consumers. Otherwise stop
                consuming the specified registered RPC method. If other RPC
                handlers still share the same queue, the queue consumer remains
                active.
        """
        if method is None:
            stopped = bool(self._consumed_rpc)
            if stopped:
                await self._backend.stop_consume_rpc()
            self._consumed_rpc.clear()
            if stopped:
                await self._hooks.emit(
                    "on_consume_stop",
                    ConsumeStopEvent(app=self._name, mode="rpc", queue=None, topic=None, reason="all"),
                )
            return

        queue = self._registration_queue(method)
        key = (queue, method.rsplit(".", 1)[-1])
        if key not in self._rpc_handlers:
            raise ValueError(f"RPC handler not registered: {method!r}")

        del self._rpc_handlers[key]

        if queue in self._consumed_rpc and not any(q == queue for q, _ in self._rpc_handlers):
            await self._backend.stop_consume_rpc(queue)
            self._consumed_rpc.discard(queue)
            await self._hooks.emit(
                "on_consume_stop",
                ConsumeStopEvent(app=self._name, mode="rpc", queue=queue, topic=None, reason="queue_drained"),
            )

    async def stop_consume_tasks(self, task: str | None = None) -> None:
        """
        Stop consuming task messages.

        Args:
            task: When ``None``, stop all task consumers. Otherwise stop
                consuming the specified registered task. If other task
                handlers still share the same queue, the queue consumer remains
                active.
        """
        if task is None:
            stopped = bool(self._consumed_tasks)
            if stopped:
                await self._backend.stop_consume_tasks()
            self._consumed_tasks.clear()
            if stopped:
                await self._hooks.emit(
                    "on_consume_stop",
                    ConsumeStopEvent(app=self._name, mode="task", queue=None, topic=None, reason="all"),
                )
            return

        queue = self._registration_queue(task)
        key = (queue, task.rsplit(".", 1)[-1])
        if key not in self._task_handlers:
            raise ValueError(f"Task handler not registered: {task!r}")

        del self._task_handlers[key]

        if queue in self._consumed_tasks and not any(q == queue for q, _ in self._task_handlers):
            await self._backend.stop_consume_tasks(queue)
            self._consumed_tasks.discard(queue)
            await self._hooks.emit(
                "on_consume_stop",
                ConsumeStopEvent(app=self._name, mode="task", queue=queue, topic=None, reason="queue_drained"),
            )

    async def stop_consume_events(self, topic: str | None = None) -> None:
        """
        Stop consuming event messages.

        Args:
            topic: When ``None``, stop all event consumers and remove all
                subscriptions. Otherwise stop consuming the specified topic
                pattern. If other topic subscriptions remain, only that topic
                subscription is removed.
        """
        if topic is None:
            if self._event_queue is not None:
                queue = self._event_queue
                await self._backend.stop_consume_events()
                self._event_queue = None
                self._subscribers.clear()
                self._subscription_patterns.clear()
                await self._hooks.emit(
                    "on_consume_stop",
                    ConsumeStopEvent(app=self._name, mode="event", queue=queue, topic=None, reason="all"),
                )
            else:
                self._subscribers.clear()
                self._subscription_patterns.clear()
            return

        if topic not in self._subscription_patterns:
            raise ValueError(f"Event subscription not registered: {topic!r}")

        pattern = topic_to_regex(topic)
        self._subscribers.pop(pattern, None)
        self._subscription_patterns.discard(topic)

        if self._event_queue is None:
            return

        queue = self._event_queue

        if not self._subscription_patterns:
            await self._backend.stop_consume_events(queue)
            self._event_queue = None
            await self._hooks.emit(
                "on_consume_stop",
                ConsumeStopEvent(app=self._name, mode="event", queue=queue, topic=topic, reason="queue_drained"),
            )
            return

        await self._backend.stop_consume_events(queue, topic)
        await self._hooks.emit(
            "on_consume_stop",
            ConsumeStopEvent(app=self._name, mode="event", queue=queue, topic=topic, reason="topic_unsubscribed"),
        )
        self._event_queue = queue

    async def stop_consume(self) -> None:
        """Stop listening for incoming messages."""
        await self.stop_consume_rpc()
        await self.stop_consume_tasks()
        await self.stop_consume_events()

    def _make_handler(
        self,
        request_type: RequestType,
        queue: str,
    ) -> Callable[[Message], Coroutine[Any, Any, Message | None]]:
        async def handler(message: Message) -> Message | None:
            return await self._handle(request_type, queue, message)

        return handler

    # ------------------------------------------------------------------
    # Client API
    # ------------------------------------------------------------------

    async def call(
        self,
        method: str,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        *,
        request_id: UUID | None = None,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Any:
        """
        Call an RPC method and wait for the response.

        Args:
            method: Fully qualified method name (``app.group.method`` or plain
                ``method`` for the default queue).
            args: Positional arguments passed to the handler.
            kwds: Keyword arguments passed to the handler.
            request_id: Optional request identifier for tracing.
            timeout: Client-side timeout in seconds.
            ttl: Message TTL in seconds.
            rttl: Response TTL in seconds.
            serializer: Serializer name to use for this call.
            context: Additional context forwarded to middleware.

        Returns:
            The deserialized response data from the handler.
        """
        request = self._build_rpc_request(
            method,
            args,
            kwds,
            request_id=request_id,
            timeout=timeout,
            ttl=ttl,
            rttl=rttl,
        )
        return await self._invoke_rpc_request(request, serializer=serializer, context=context)

    async def send(
        self,
        task: str,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        *,
        request_id: UUID | None = None,
        timeout: float | None = None,
        ttl: float | None = None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """
        Send a task for fire-and-forget execution.

        Args:
            task: Fully qualified task name.
            args: Positional arguments passed to the handler.
            kwds: Keyword arguments passed to the handler.
            request_id: Optional request identifier for tracing.
            timeout: Client-side timeout in seconds.
            ttl: Message TTL in seconds.
            serializer: Serializer name to use for this call.
            context: Additional context forwarded to middleware.
        """
        request = self._build_task_request(
            task,
            args,
            kwds,
            request_id=request_id,
            timeout=timeout,
            ttl=ttl,
        )
        return await self._invoke_task_request(request, serializer=serializer, context=context)

    async def emit(
        self,
        topic: str,
        event: dict[str, Any],
        *,
        request_id: UUID | None = None,
        ttl: float | None = None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        """
        Emit an event to all matching subscribers.

        Args:
            topic: Event topic pattern.
            event: Event payload as a dict of keyword arguments.
            request_id: Optional request identifier for tracing.
            ttl: Message TTL in seconds.
            serializer: Serializer name to use for this call.
            context: Additional context forwarded to middleware.
        """
        request = self._build_event_request(
            topic,
            event,
            request_id=request_id,
            ttl=ttl,
        )
        return await self._invoke_event_request(request, serializer=serializer, context=context)

    # ------------------------------------------------------------------
    # Client pipeline
    # ------------------------------------------------------------------

    def _build_rpc_request(
        self,
        method: str,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        *,
        request_id: UUID | None = None,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
    ) -> Request:
        queue = self._resolve_queue(method)

        headers: dict[str, Any] = {"queue": queue}

        # RPC: timeout/ttl always have a value (default RPC_TIMEOUT / RPC_TTL)
        timeout = timeout if timeout is not None else (self._timeout if self._timeout is not None else self.RPC_TIMEOUT)
        ttl = ttl if ttl is not None else (self._ttl if self._ttl is not None else self.RPC_TTL)
        rttl = rttl if rttl is not None else self._rttl

        if timeout is not None:
            headers["timeout"] = timeout

        if rttl is not None:
            headers["rttl"] = rttl

        if ttl is not None:
            headers["ttl"] = ttl

        return Request(
            id=request_id or uuid4(),
            type="rpc",
            data={
                "method": method.rsplit(".", 1)[-1],
                "args": args,
                "kwds": kwds,
            },
            headers=headers,
        )

    def _build_task_request(
        self,
        task: str,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        *,
        request_id: UUID | None = None,
        timeout: float | None = None,
        ttl: float | None = None,
    ) -> Request:
        queue = self._resolve_queue(task)

        headers: dict[str, Any] = {"queue": queue}

        ttl = ttl if ttl is not None else self._ttl
        if ttl is None and timeout is not None:
            ttl = timeout

        if timeout is not None:
            headers["timeout"] = timeout

        if ttl is not None:
            headers["ttl"] = ttl

        return Request(
            id=request_id or uuid4(),
            type="task",
            data={
                "task": task.rsplit(".", 1)[-1],
                "args": args,
                "kwds": kwds,
            },
            headers=headers,
        )

    def _build_event_request(
        self,
        topic: str,
        event: dict[str, Any],
        *,
        request_id: UUID | None = None,
        ttl: float | None = None,
    ) -> Request:
        queue = self._event_queue_name()

        headers: dict[str, Any] = {"queue": queue}

        ttl = ttl if ttl is not None else self._ttl

        if ttl is not None:
            headers["ttl"] = ttl

        return Request(
            id=request_id or uuid4(),
            type="event",
            data={
                "topic": topic,
                "event": event,
            },
            headers=headers,
        )

    async def _invoke_rpc_request(
        self,
        request: Request,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Any:
        if self._closed:
            raise RuntimeError("App is closed")

        _serializer = self._get_serializer(serializer)
        _context = context or {}

        client_timeout = float(request.headers["timeout"]) + float(request.headers["ttl"])

        published = False
        response_outcome: Literal["ok", "error", "timeout"] = "ok"
        response_error: BaseException | None = None
        response: Response | None = None
        result: Any = None

        async def core() -> Response | None:
            nonlocal published
            req_message = _serializer.dumps_request(request)

            async def backend_request() -> Message | None:
                nonlocal published
                future = await self._backend.request(request.headers["queue"], req_message, kwds=_context)
                published = True
                await self._hooks.emit(
                    "on_request_published",
                    RequestPublishedEvent(app=self._name, request=request, outcome="ok", error=None),
                )
                res_message = await future
                return res_message

            wire_handler = backend_request
            for mw in reversed(self._wire_mw):
                wire_handler = self._wrap_wire_client(mw, wire_handler, req_message, request, _context)

            res_message = await wire_handler()
            if res_message is None:
                return None
            return _serializer.loads_response(res_message)

        handler = core
        for mw in reversed(self._app_mw):
            handler = self._wrap_app_client(mw, handler, request, _context)

        try:
            try:
                response = await asyncio.wait_for(handler(), timeout=client_timeout)
            except TimeoutError as exc:
                response_outcome = "timeout"
                response_error = RPCError(
                    error_code=ErrorCode.TIMEOUT,
                    error_message="RPC call timed out waiting for response",
                )
                raise response_error from exc

            if response is None:
                result = None
            elif response.type == "error" or (isinstance(response.data, dict) and "error_code" in response.data):
                info = ErrorInfo.model_validate(response.data)
                response_error = RPCError(
                    error_code=info.error_code,
                    error_message=info.error_message,
                    error_details=info.error_details,
                )
                response_outcome = "timeout" if info.error_code == ErrorCode.TIMEOUT else "error"
                raise response_error
            else:
                result = response.data
            await self._hooks.emit(
                "on_response_received",
                ResponseReceivedEvent(
                    app=self._name,
                    request=request,
                    response=result,
                    outcome="ok",
                    error=None,
                ),
            )
            return result
        except BaseException as exc:
            if not published:
                await self._hooks.emit(
                    "on_request_published",
                    RequestPublishedEvent(app=self._name, request=request, outcome="error", error=exc),
                )
            else:
                if response_error is None:
                    response_outcome = "error"
                    response_error = exc
                await self._hooks.emit(
                    "on_response_received",
                    ResponseReceivedEvent(
                        app=self._name,
                        request=request,
                        response=None,
                        outcome=response_outcome,
                        error=response_error,
                    ),
                )
            raise

    async def _invoke_task_request(
        self,
        request: Request,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("App is closed")

        _serializer = self._get_serializer(serializer)
        _context = context or {}
        published = False

        async def core() -> None:
            nonlocal published
            req_message = _serializer.dumps_request(request)

            async def backend_send() -> Message | None:
                nonlocal published
                await self._backend.send(request.headers["queue"], req_message, kwds=_context)
                published = True
                await self._hooks.emit(
                    "on_request_published",
                    RequestPublishedEvent(app=self._name, request=request, outcome="ok", error=None),
                )
                return None

            wire_handler = backend_send
            for mw in reversed(self._wire_mw):
                wire_handler = self._wrap_wire_client(mw, wire_handler, req_message, request, _context)

            await wire_handler()

        handler = core
        for mw in reversed(self._app_mw):
            handler = self._wrap_app_client(mw, handler, request, _context)

        try:
            await handler()
        except BaseException as exc:
            if not published:
                await self._hooks.emit(
                    "on_request_published",
                    RequestPublishedEvent(app=self._name, request=request, outcome="error", error=exc),
                )
            raise

    async def _invoke_event_request(
        self,
        request: Request,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("App is closed")

        _serializer = self._get_serializer(serializer)
        _context = context or {}
        published = False

        async def core() -> None:
            nonlocal published
            req_message = _serializer.dumps_request(request)

            async def backend_emit() -> Message | None:
                nonlocal published
                await self._backend.emit(request.data["topic"], req_message, kwds=_context)
                published = True
                await self._hooks.emit(
                    "on_request_published",
                    RequestPublishedEvent(app=self._name, request=request, outcome="ok", error=None),
                )
                return None

            wire_handler = backend_emit
            for mw in reversed(self._wire_mw):
                wire_handler = self._wrap_wire_client(mw, wire_handler, req_message, request, _context)

            await wire_handler()

        handler = core
        for mw in reversed(self._app_mw):
            handler = self._wrap_app_client(mw, handler, request, _context)

        try:
            await handler()
        except BaseException as exc:
            if not published:
                await self._hooks.emit(
                    "on_request_published",
                    RequestPublishedEvent(app=self._name, request=request, outcome="error", error=exc),
                )
            raise

    # ------------------------------------------------------------------
    # Server pipeline
    # ------------------------------------------------------------------

    async def _handle(self, request_type: RequestType, queue: str, message: Message) -> Message | None:
        _serializer = self._default_serializer
        try:
            _serializer = self._detect_serializer(message)

            async def core() -> tuple[Message, Response] | tuple[None, None]:
                request = _serializer.loads_request(message)
                await self._hooks.emit(
                    "on_request_received",
                    RequestReceivedEvent(app=self._name, mode=request.type, queue=queue, request=request),
                )

                async def dispatch_core() -> Response | None:
                    return await self._dispatch_request(request)

                app_handler: Callable[[], Coroutine[Any, Any, Response | None]] = dispatch_core
                for amw in reversed(self._app_mw):
                    app_handler = self._wrap_app_server(amw, app_handler, request, message)

                response = await app_handler()
                if response is None or request_type != "rpc":
                    return None, None

                rttl = request.headers.get("rttl")
                if rttl is not None:
                    response = response.model_copy(update={"headers": {**response.headers, "rttl": rttl}})

                response_message = _serializer.dumps_response(response)
                return response_message, response

            wire_handler: Callable[
                [],
                Coroutine[Any, Any, tuple[Message, Response] | tuple[None, None]],
            ] = core

            for mw in reversed(self._wire_mw):
                wire_handler = self._wrap_wire_server(mw, wire_handler, message)

            result = await wire_handler()
            if result is None or result[0] is None:
                return None

            response_message, _ = result
            return response_message

        except Exception as e:
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode=request_type,
                    queue=queue,
                    reason="decode_error",
                    request=None,
                    handler=None,
                    topic=None,
                    error=e,
                    details=None,
                ),
            )
            if request_type != "rpc":
                logger.exception("Failed to process %s message from queue %s", request_type, queue)
                return None
            return self._build_error_response(e, message, _serializer)

    async def _dispatch_request(self, request: Request) -> Response | None:
        if request.type == "rpc":
            return await self._dispatch_rpc_request(request)
        if request.type == "task":
            await self._dispatch_task_request(request)
            return None
        await self._dispatch_event_request(request)
        return None

    async def _dispatch_rpc_request(self, request: Request) -> Response | None:
        queue = request.headers.get("queue", self._name)
        method = request.data.get("method")

        if not isinstance(method, str):
            response = Response(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.VALIDATION_ERROR,
                    error_message="Request does not contain a method name",
                ),
            )
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="rpc",
                    queue=str(queue),
                    reason="invalid_request",
                    request=request,
                    handler=None,
                    topic=None,
                    error=None,
                    details=response.data,
                ),
            )
            return response

        info = self._rpc_handlers.get((queue, method))

        if info is None:
            response = Response(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.METHOD_NOT_FOUND,
                    error_message=f"No handler registered for method: {method}",
                ),
            )
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="rpc",
                    queue=str(queue),
                    reason="handler_not_found",
                    request=request,
                    handler=None,
                    topic=None,
                    error=None,
                    details=response.data,
                ),
            )
            return response

        error_response = self._validate_args(info, request)
        if error_response is not None:
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="rpc",
                    queue=str(queue),
                    reason="validation_failed",
                    request=request,
                    handler=info,
                    topic=None,
                    error=None,
                    details=error_response.data,
                ),
            )
            return error_response

        await self._hooks.emit(
            "before_handler",
            BeforeHandlerEvent(
                app=self._name,
                mode="rpc",
                queue=str(queue),
                request=request,
                handler=info,
                topic=None,
            ),
        )
        try:
            result = await self._run_handler(info, request)
        except TimeoutError as exc:
            response = Response(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.TIMEOUT,
                    error_message="Handler execution timed out",
                ),
            )
            await self._hooks.emit(
                "on_handler_error",
                HandlerErrorEvent(
                    app=self._name,
                    mode="rpc",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                    error=exc,
                    error_code=ErrorCode.TIMEOUT,
                    error_message="Handler execution timed out",
                    outcome="timeout",
                ),
            )
            return response
        except Exception as exc:
            response = self._error_response(request, exc)
            error_info = response.data if isinstance(response.data, ErrorInfo) else ErrorInfo()
            await self._hooks.emit(
                "on_handler_error",
                HandlerErrorEvent(
                    app=self._name,
                    mode="rpc",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                    error=exc,
                    error_code=error_info.error_code,
                    error_message=error_info.error_message,
                    outcome="error",
                ),
            )
            return response

        await self._hooks.emit(
            "after_handler",
            AfterHandlerEvent(
                app=self._name,
                mode="rpc",
                queue=str(queue),
                request=request,
                handler=info,
                topic=None,
                result=result,
            ),
        )
        return Response(id=request.id, type="result", data=result)

    async def _dispatch_task_request(self, request: Request) -> None:
        queue = request.headers.get("queue", self._name)
        task = request.data.get("task")

        if not isinstance(task, str):
            logger.warning("Task request does not contain a task name")
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="task",
                    queue=str(queue),
                    reason="invalid_request",
                    request=request,
                    handler=None,
                    topic=None,
                    error=None,
                    details=None,
                ),
            )
            return None

        info = self._task_handlers.get((queue, task))
        if info is None:
            logger.warning("No handler registered for task: %s", task)
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="task",
                    queue=str(queue),
                    reason="handler_not_found",
                    request=request,
                    handler=None,
                    topic=None,
                    error=None,
                    details={"task": task},
                ),
            )
            return None

        error_response = self._validate_args(info, request)
        if error_response is not None:
            logger.warning(
                "Invalid arguments for task %s: %s",
                task,
                error_response.data,
            )
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="task",
                    queue=str(queue),
                    reason="validation_failed",
                    request=request,
                    handler=info,
                    topic=None,
                    error=None,
                    details=error_response.data,
                ),
            )
            return None

        await self._hooks.emit(
            "before_handler",
            BeforeHandlerEvent(
                app=self._name,
                mode="task",
                queue=str(queue),
                request=request,
                handler=info,
                topic=None,
            ),
        )
        try:
            result = await self._run_handler(info, request)
        except TimeoutError as exc:
            logger.warning("Task execution timed out: %s", task)
            await self._hooks.emit(
                "on_handler_error",
                HandlerErrorEvent(
                    app=self._name,
                    mode="task",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                    error=exc,
                    error_code=ErrorCode.TIMEOUT,
                    error_message="Handler execution timed out",
                    outcome="timeout",
                ),
            )
            return None
        except Exception as exc:
            logger.exception("Handler failed for task: %s", task)
            await self._hooks.emit(
                "on_handler_error",
                HandlerErrorEvent(
                    app=self._name,
                    mode="task",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                    error=exc,
                    error_code=ErrorCode.INTERNAL_ERROR,
                    error_message=str(exc),
                    outcome="error",
                ),
            )
            return None

        await self._hooks.emit(
            "after_handler",
            AfterHandlerEvent(
                app=self._name,
                mode="task",
                queue=str(queue),
                request=request,
                handler=info,
                topic=None,
                result=result,
            ),
        )
        return None

    async def _dispatch_event_request(self, request: Request) -> None:
        queue = request.headers.get("queue", self._name)
        topic = request.data["topic"]
        subscribers = self._find_subscribers(topic)
        if not subscribers:
            logger.debug("No event subscribers for topic: %s", topic)
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="event",
                    queue=str(queue),
                    reason="no_subscribers",
                    request=request,
                    handler=None,
                    topic=str(topic),
                    error=None,
                    details=None,
                ),
            )
            return None

        await asyncio.gather(*(self._exec_subscriber(subscriber, request) for subscriber in subscribers))
        return None

    async def _exec_subscriber(self, info: HandlerInfo, request: Request) -> None:
        queue = request.headers.get("queue", self._name)
        topic = str(request.data.get("topic"))
        error_response = self._validate_args(info, request)
        if error_response is not None:
            logger.warning(
                "Invalid event arguments for %s: %s",
                info.name,
                error_response.data,
            )
            await self._hooks.emit(
                "on_message_dropped",
                MessageDroppedEvent(
                    app=self._name,
                    mode="event",
                    queue=str(queue),
                    reason="validation_failed",
                    request=request,
                    handler=info,
                    topic=topic,
                    error=None,
                    details=error_response.data,
                ),
            )
            return

        await self._hooks.emit(
            "before_handler",
            BeforeHandlerEvent(
                app=self._name,
                mode="event",
                queue=str(queue),
                request=request,
                handler=info,
                topic=topic,
            ),
        )
        try:
            result = await self._run_handler(info, request)
        except Exception as exc:
            logger.exception("Subscriber handler failed for %s", info.name)
            await self._hooks.emit(
                "on_handler_error",
                HandlerErrorEvent(
                    app=self._name,
                    mode="event",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=topic,
                    error=exc,
                    error_code=ErrorCode.INTERNAL_ERROR,
                    error_message=str(exc),
                    outcome="error",
                ),
            )
            return

        await self._hooks.emit(
            "after_handler",
            AfterHandlerEvent(
                app=self._name,
                mode="event",
                queue=str(queue),
                request=request,
                handler=info,
                topic=topic,
                result=result,
            ),
        )

    async def _run_handler(self, info: HandlerInfo, request: Request) -> Any:
        token_req = _current_request.set(request)
        try:
            deps = self._resolve_deps(info)
            if info.request_type == "event":
                args: tuple[Any, ...] = ()
                kwds = {**deps, **request.data.get("event", {})}
            else:
                args = tuple(request.data.get("args", ()))
                kwds = {**deps, **request.data.get("kwds", {})}
            result = info.fn(*args, **kwds)
            if inspect.isawaitable(result):
                timeout = request.headers.get("timeout")
                if info.request_type in ("rpc", "task") and timeout is not None:
                    result = await asyncio.wait_for(result, timeout=float(timeout))
                else:
                    result = await result
            return result
        finally:
            _current_request.reset(token_req)

    def _resolve_deps(self, info: HandlerInfo) -> dict[str, Any]:
        deps: dict[str, Any] = {}
        for name, (type_, factory) in info.dep_types.items():
            if factory is None:
                raise RuntimeError(f"Depends[{type_.__name__}] requires inline factory")
            deps[name] = factory()
        return deps

    def _get_serializer(self, name: str | None) -> Serializer:
        if name is None:
            return self._default_serializer
        try:
            return self._serializers[name]
        except KeyError as exc:
            raise RPCError(
                error_code=ErrorCode.VALIDATION_ERROR,
                error_message=f"unknown serializer: {name}",
            ) from exc

    def _detect_serializer(self, message: Message) -> Serializer:
        ser_name = message.headers.get("serializer")
        if ser_name is not None:
            return self._get_serializer(str(ser_name))

        for candidate in self._serializer_list:
            try:
                candidate.loads_request(message)
                return candidate
            except Exception:
                continue

        raise RPCError(
            error_code=ErrorCode.VALIDATION_ERROR,
            error_message="cannot detect serializer for incoming message",
        )

    def _validate_args(self, info: HandlerInfo, request: Request) -> Response | None:
        validate = info.options.get("validate_types")
        if validate is None:
            validate = self._validate_types
        if not validate:
            return None

        fn = inspect.unwrap(info.fn)
        hints = get_type_hints(fn, localns={"Depends": Depends})
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())

        arg_map: dict[str, Any] = {}
        if request.type == "event":
            arg_map.update(request.data.get("event", {}))
        else:
            for i, arg in enumerate(request.data.get("args", ())):
                if i < len(params):
                    arg_map[params[i].name] = arg
            arg_map.update(request.data.get("kwds", {}))

        errors: list[str] = []
        for pname, value in arg_map.items():
            hint = hints.get(pname)
            if hint is None or pname == "return":
                continue
            if get_origin(hint) is Depends or hint is Any:
                continue

            try:
                TypeAdapter(hint).validate_python(value)
            except ValidationError as exc:
                errors.append(f"{pname}: {exc.errors(include_input=False, include_url=False)}")

        if errors:
            return Response(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.VALIDATION_ERROR,
                    error_message="; ".join(errors),
                ),
            )

        return None

    def _error_response(self, request: Request, exc: Exception) -> Response:
        rpc_err = self._resolve_exception_handler(exc)
        if rpc_err is None and isinstance(exc, RPCError):
            rpc_err = exc

        if rpc_err is not None:
            return Response(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=rpc_err.error_code,
                    error_message=rpc_err.error_message,
                    error_details=rpc_err.error_details,
                ),
            )

        logger.exception(exc)
        return Response(
            id=request.id,
            type="error",
            data=ErrorInfo(
                error_code=ErrorCode.INTERNAL_ERROR,
                error_message=str(exc),
                error_details={"exc_type": type(exc).__name__},
            ),
        )

    def _resolve_exception_handler(self, exc: Exception) -> RPCError | None:
        for cls in type(exc).__mro__:
            handler = self._exception_handlers.get(cls)
            if handler is None:
                continue

            try:
                result = handler(exc)
            except RPCError as rpc_error:
                return rpc_error
            except Exception:
                logger.exception("Exception handler failed for %s", cls.__name__)
                return None

            if isinstance(result, RPCError):
                return result
            if isinstance(result, tuple):
                if len(result) < 2:
                    logger.error("Exception handler for %s returned an invalid tuple", cls.__name__)
                    return None
                code, message, *details = result
                return RPCError(
                    error_code=code,
                    error_message=message,
                    error_details=details[0] if details else None,
                )
            if isinstance(result, dict):
                return RPCError(
                    error_code=result.get("error_code", ErrorCode.INTERNAL_ERROR),
                    error_message=result.get("error_message", str(exc)),
                    error_details=result.get("error_details"),
                )
            if isinstance(result, str):
                return RPCError(error_code=ErrorCode.INTERNAL_ERROR, error_message=result)
            return None

        return None

    def _build_error_response(
        self,
        exc: Exception,
        message: Message,
        serializer: Serializer,
    ) -> Message:
        if isinstance(exc, RPCError):
            error_code = exc.error_code
            error_message = exc.error_message
            details = exc.error_details
        else:
            logger.exception(exc)
            error_code = ErrorCode.INTERNAL_ERROR
            error_message = "Internal server error"
            details = {"exc_type": type(exc).__name__}

        try:
            request = serializer.loads_request(message)
            message_id = request.id
        except Exception:
            message_id = UUID(int=0)

        error_resp = Response(
            id=message_id,
            type="error",
            data=ErrorInfo(error_code=error_code, error_message=error_message, error_details=details),
        )
        return serializer.dumps_response(error_resp)

    # ------------------------------------------------------------------
    # Middleware helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _wrap_app_client(
        mw: AppMiddleware,
        handler: Callable[[], Coroutine[Any, Any, Any]],
        request: Request,
        kwds: dict[str, Any],
    ) -> Callable[[], Coroutine[Any, Any, Any]]:
        async def wrapper() -> Any:
            return await mw.dispatch_client(handler, request, kwds=kwds)

        return wrapper

    @staticmethod
    def _wrap_wire_client(
        mw: WireMiddleware,
        handler: Callable[[], Coroutine[Any, Any, Any]],
        message: Message,
        request: Request,
        kwds: dict[str, Any],
    ) -> Callable[[], Coroutine[Any, Any, Any]]:
        async def wrapper() -> Any:
            return await mw.dispatch_client(handler, message, request, kwds=kwds)

        return wrapper

    @staticmethod
    def _wrap_app_server(
        mw: AppMiddleware,
        handler: Callable[[], Coroutine[Any, Any, Response | None]],
        request: Request,
        message: Message,
    ) -> Callable[[], Coroutine[Any, Any, Response | None]]:
        async def wrapper() -> Response | None:
            return await mw.dispatch_server(handler, request, message)

        return wrapper

    @staticmethod
    def _wrap_wire_server(
        mw: WireMiddleware,
        handler: Callable[[], Coroutine[Any, Any, tuple[Message, Response] | tuple[None, None]]],
        message: Message,
    ) -> Callable[[], Coroutine[Any, Any, tuple[Message, Response] | tuple[None, None]]]:
        async def wrapper() -> tuple[Message, Response] | tuple[None, None]:
            return await mw.dispatch_server(handler, message)

        return wrapper

    # ------------------------------------------------------------------
    # Event topic helpers
    # ------------------------------------------------------------------

    def _find_subscribers(self, topic: str) -> list[HandlerInfo]:
        subscribers: list[HandlerInfo] = []
        for pattern, handlers in self._subscribers.items():
            if pattern.fullmatch(topic):
                subscribers.extend(handlers)
        return subscribers
