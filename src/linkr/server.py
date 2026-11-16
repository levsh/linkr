from __future__ import annotations

import asyncio
import inspect
import logging
import re
from collections.abc import Awaitable, Callable, Coroutine
from contextvars import ContextVar
from functools import wraps
from typing import TYPE_CHECKING, Annotated, Any, Literal, get_args, get_origin, get_type_hints
from uuid import UUID

from pydantic import TypeAdapter, ValidationError

from .di import Depends
from .exceptions import ErrorCode, RequestError, RPCError
from .hooks import (
    AfterHandlerEvent,
    BeforeHandlerEvent,
    ConsumeStartEvent,
    ConsumeStopEvent,
    HandlerErrorEvent,
    MessageDroppedEvent,
    RequestReceivedEvent,
)
from .middleware import apply_middleware, wrap_app_server, wrap_wire_server
from .models import ErrorInfo, HandlerInfo, Message, QueueOptions, Request, RequestType, Response
from .serializer import Serializer
from .utils import topic_to_regex

if TYPE_CHECKING:
    from .app import App

logger = logging.getLogger("linkr")

_current_request: ContextVar[Request | None] = ContextVar("linkr_current_request", default=None)


def get_current_request() -> Request:
    """
    Get the request being dispatched in this async context.

    Raises ``RuntimeError`` if called outside an active handler dispatch context.
    """
    req = _current_request.get()
    if req is None:
        raise RuntimeError("no active request in current context")
    return req


ModeName = Literal["rpc", "task", "event"]


class Server:
    """
    Server scope: handler registration and consume/stop control per mode.

    A server is bound to an application and a queue scope (root or named path).
    """

    def __init__(
        self,
        app: App,
        path: str | None,
        *,
        options: dict[str, tuple[QueueOptions, ...] | None],
    ) -> None:
        """
        Create a server scope.

        Args:
            app: Owning application instance.
            path: Server scope path (``None`` for root).
            options: Normalized per-mode backend queue options.
        """
        self._app = app
        self._path = path
        self._serializer = app.serializers[0]
        self._serializers_map = {s.name: s for s in app.serializers}
        self._app_mw = app.app_middlewares
        self._wire_mw = app.wire_middlewares

        self._queue_base = app.name if path is None else f"{app.name}.{path}"

        self._rpc_options = options.get("rpc")
        self._task_options = options.get("task")
        self._event_options = options.get("event")

        self._rpc_handlers: dict[tuple[str, str], HandlerInfo] = {}
        self._task_handlers: dict[tuple[str, str], HandlerInfo] = {}
        self._subscribers: dict[re.Pattern[str], list[HandlerInfo]] = {}
        self._subscription_patterns: set[str] = set()

        self._rpc_consuming = False
        self._task_consuming = False
        self._event_consuming = False
        self._event_queue: str | None = None

    # ------------------------------------------------------------------
    # Properties / identity
    # ------------------------------------------------------------------

    @property
    def app(self) -> App:
        """Application that owns this server."""
        return self._app

    @property
    def path(self) -> str | None:
        """Server scope path (``None`` for the root scope)."""
        return self._path

    @property
    def queue_base(self) -> str:
        """Base queue name for this server scope."""
        return self._queue_base

    @property
    def event_queue(self) -> str | None:
        """Physical event queue name while event consuming is active."""
        return self._event_queue

    @property
    def methods(self) -> dict[str, HandlerInfo]:
        """RPC handlers registered on this server, keyed by local leaf name."""
        return {leaf: info for (_, leaf), info in self._rpc_handlers.items()}

    @property
    def tasks(self) -> dict[str, HandlerInfo]:
        """Task handlers registered on this server, keyed by local leaf name."""
        return {leaf: info for (_, leaf), info in self._task_handlers.items()}

    @property
    def subscriptions(self) -> dict[str, list[HandlerInfo]]:
        """Event subscriptions registered on this server, grouped by topic pattern."""
        grouped: dict[str, list[HandlerInfo]] = {}
        for handlers in self._subscribers.values():
            for info in handlers:
                grouped.setdefault(info.name, []).append(info)
        return dict(sorted(grouped.items()))

    def options_match(self, options: dict[str, tuple[QueueOptions, ...] | None]) -> bool:
        """
        Return whether normalized per-mode options match this server configuration.

        Args:
            options: Normalized per-mode queue options to compare.

        Returns:
            ``True`` if all mode options match, otherwise ``False``.
        """
        return (
            self._rpc_options == options.get("rpc")
            and self._task_options == options.get("task")
            and self._event_options == options.get("event")
        )

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def _validate_leaf_name(self, name: str) -> str:
        if not isinstance(name, str) or not name:
            raise ValueError("handler name must not be empty")
        if "." in name:
            raise ValueError("handler name must not contain '.'")
        return name

    def method(
        self,
        name: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Register an RPC handler for a local leaf name on this server.

        Args:
            name: Local RPC handler name (leaf, without dots).
            **options: Handler options (e.g. validation settings).

        Returns:
            A decorator that registers the handler function.
        """
        leaf = self._validate_leaf_name(name)
        return self._register("rpc", leaf, options)

    def task(
        self,
        name: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Register a task handler for a local leaf name on this server.

        Args:
            name: Local task handler name (leaf, without dots).
            **options: Handler options (e.g. validation settings).

        Returns:
            A decorator that registers the handler function.
        """
        leaf = self._validate_leaf_name(name)
        return self._register("task", leaf, options)

    def subscribe(
        self,
        topic: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Register an event subscriber for a topic pattern on this server.

        Args:
            topic: Topic pattern to subscribe to.
            **options: Handler options (e.g. validation settings).

        Returns:
            A decorator that registers the subscriber function.
        """
        pattern = topic_to_regex(topic)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            @wraps(fn)
            def wrapper(*args: Any, **kwds: Any) -> Any:
                return fn(*args, **kwds)

            dep_types = self._dep_types(fn)
            validate_args, validation_adapters, validation_param_names = self._build_validation_plan(
                fn, options, dep_types
            )

            info = HandlerInfo(
                name=topic,
                request_type="event",
                fn=wrapper,
                signature=str(inspect.signature(fn)),
                options=options,
                dep_types=dep_types,
                queue=self._event_queue_name(),
                validate_args=validate_args,
                validation_adapters=validation_adapters,
                validation_param_names=validation_param_names,
            )
            self._subscribers.setdefault(pattern, []).append(info)
            self._subscription_patterns.add(topic)
            return wrapper

        return decorator

    def _register(
        self,
        mode: Literal["rpc", "task"],
        leaf: str,
        options: dict[str, Any],
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        queue = self._queue_base
        fq_name = f"{queue}.{leaf}"
        registry = self._rpc_handlers if mode == "rpc" else self._task_handlers
        key = (queue, leaf)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            if key in registry:
                raise ValueError(f"{mode} handler already registered for {queue!r}: {leaf!r}")

            @wraps(fn)
            def wrapper(*args: Any, **kwds: Any) -> Any:
                return fn(*args, **kwds)

            dep_types = self._dep_types(fn)
            validate_args, validation_adapters, validation_param_names = self._build_validation_plan(
                fn, options, dep_types
            )

            registry[key] = HandlerInfo(
                name=fq_name,
                request_type=mode,
                fn=wrapper,
                signature=str(inspect.signature(fn)),
                options=options,
                dep_types=dep_types,
                queue=queue,
                validate_args=validate_args,
                validation_adapters=validation_adapters,
                validation_param_names=validation_param_names,
            )
            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # Validation / deps plans
    # ------------------------------------------------------------------

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

    def _build_validation_plan(
        self,
        fn: Callable[..., Any],
        options: dict[str, Any],
        dep_types: dict[str, tuple[type, Callable[[], Any] | None]],
    ) -> tuple[bool, dict[str, Any], tuple[str, ...]]:
        validate = options.get("validate_types")
        if validate is None:
            validate = self._app.validate_types
        if not validate:
            return False, {}, ()

        try:
            unwrapped_fn = inspect.unwrap(fn)
            sig = inspect.signature(unwrapped_fn)
            param_names = tuple(p.name for p in sig.parameters.values())
            hints = get_type_hints(unwrapped_fn, localns={"Depends": Depends})
        except Exception:
            return False, {}, ()

        adapters: dict[str, Any] = {}
        for pname, hint in hints.items():
            if pname == "return":
                continue
            if pname in dep_types:
                continue
            if hint is Any:
                continue
            if get_origin(hint) is Depends:
                continue
            try:
                adapters[pname] = TypeAdapter(hint)
            except Exception:
                continue

        if not adapters:
            return False, {}, ()

        return True, adapters, param_names

    # ------------------------------------------------------------------
    # Consume / stop
    # ------------------------------------------------------------------

    def _event_queue_name(self) -> str:
        return f"{self._queue_base}.{self._app.instance_id}"

    async def consume(self, mode: ModeName | None = None) -> None:
        """
        Start consuming for the selected mode(s), or all modes when omitted.

        Args:
            mode: Delivery mode to consume (``"rpc"``, ``"task"``, or ``"event"``),
                or ``None`` to consume all registered modes.

        Raises:
            RuntimeError: If the application is closed.
            ValueError: If an unknown mode is provided.
        """
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        modes = ("rpc", "task", "event") if mode is None else (mode,)
        for m in modes:
            if m == "rpc":
                await self._consume_rpc()
            elif m == "task":
                await self._consume_task()
            elif m == "event":
                await self._consume_event()
            else:
                raise ValueError(f"unknown consume mode: {m!r}")

    async def stop_consume(self, mode: ModeName | None = None) -> None:
        """
        Stop consuming for the selected mode(s), or all modes when omitted.

        Args:
            mode: Delivery mode to stop (``"rpc"``, ``"task"``, or ``"event"``),
                or ``None`` to stop all modes.

        Raises:
            ValueError: If an unknown mode is provided.
        """
        modes = ("rpc", "task", "event") if mode is None else (mode,)
        for m in modes:
            if m == "rpc":
                await self._stop_consume_rpc()
            elif m == "task":
                await self._stop_consume_task()
            elif m == "event":
                await self._stop_consume_event()
            else:
                raise ValueError(f"unknown consume mode: {m!r}")

    async def _consume_rpc(self) -> None:
        if not self._rpc_handlers or self._rpc_consuming:
            return
        queue = self._queue_base
        await self._consume_backend("rpc", queue, self._make_handler("rpc", queue))
        self._rpc_consuming = True
        if self._app.hooks.on_consume_start:
            await self._app.hooks.on_consume_start.emit(
                ConsumeStartEvent(app=self._app.name, mode="rpc", queue=queue, topics=())
            )

    async def _consume_task(self) -> None:
        if not self._task_handlers or self._task_consuming:
            return
        queue = self._queue_base
        await self._consume_backend("task", queue, self._make_handler("task", queue))
        self._task_consuming = True
        if self._app.hooks.on_consume_start:
            await self._app.hooks.on_consume_start.emit(
                ConsumeStartEvent(app=self._app.name, mode="task", queue=queue, topics=())
            )

    async def _consume_event(self) -> None:
        if not self._subscription_patterns or self._event_consuming:
            return
        queue = self._event_queue_name()
        topics = tuple(sorted(self._subscription_patterns))
        await self._consume_backend("event", queue, self._make_handler("event", queue), topics=list(topics))
        self._event_queue = queue
        self._event_consuming = True
        if self._app.hooks.on_consume_start:
            await self._app.hooks.on_consume_start.emit(
                ConsumeStartEvent(app=self._app.name, mode="event", queue=queue, topics=topics)
            )

    async def _stop_consume_rpc(self) -> None:
        if not self._rpc_consuming:
            return
        await self._app.backend.stop_consume_rpc(self._queue_base)
        self._rpc_consuming = False
        if self._app.hooks.on_consume_stop:
            await self._app.hooks.on_consume_stop.emit(
                ConsumeStopEvent(
                    app=self._app.name,
                    mode="rpc",
                    queue=self._queue_base,
                    topic=None,
                    reason="stopped",
                )
            )

    async def _stop_consume_task(self) -> None:
        if not self._task_consuming:
            return
        await self._app.backend.stop_consume_tasks(self._queue_base)
        self._task_consuming = False
        if self._app.hooks.on_consume_stop:
            await self._app.hooks.on_consume_stop.emit(
                ConsumeStopEvent(
                    app=self._app.name,
                    mode="task",
                    queue=self._queue_base,
                    topic=None,
                    reason="stopped",
                )
            )

    async def _stop_consume_event(self) -> None:
        if not self._event_consuming or self._event_queue is None:
            self._event_consuming = False
            self._event_queue = None
            return
        queue = self._event_queue
        await self._app.backend.stop_consume_events(queue)
        self._event_consuming = False
        self._event_queue = None
        if self._app.hooks.on_consume_stop:
            await self._app.hooks.on_consume_stop.emit(
                ConsumeStopEvent(
                    app=self._app.name,
                    mode="event",
                    queue=queue,
                    topic=None,
                    reason="stopped",
                )
            )

    async def _consume_backend(
        self,
        mode: ModeName,
        queue: str,
        handler: Callable[[Message], Coroutine[Any, Any, Message | None]],
        *,
        topics: list[str] | None = None,
    ) -> None:
        backend = self._app.backend
        if mode == "event":
            options = self._event_options
            consume_event = backend.consume_event
            try:
                params: Any = inspect.signature(consume_event).parameters
            except (TypeError, ValueError):
                params = {}
            if "options" in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
                await consume_event(queue, topics or [], handler, options=options)
            else:
                if options is not None:
                    logger.warning(
                        "Backend %s.consume_event does not accept queue options; options ignored for queue %s",
                        type(backend).__name__,
                        queue,
                    )
                await consume_event(queue, topics or [], handler)
            return

        options = self._rpc_options if mode == "rpc" else self._task_options
        consume = backend.consume_rpc if mode == "rpc" else backend.consume_task
        args: tuple[Any, ...] = (queue, handler)

        try:
            params = inspect.signature(consume).parameters
        except (TypeError, ValueError):
            params = {}

        if "options" in params or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
            await consume(*args, options=options)
        else:
            if options is not None:
                logger.warning(
                    "Backend %s.consume_%s does not accept queue options; options ignored for queue %s",
                    type(backend).__name__,
                    mode,
                    queue,
                )
            await consume(*args)

    # ------------------------------------------------------------------
    # Handler / dispatch (server-side)
    # ------------------------------------------------------------------

    def _make_handler(
        self,
        request_type: RequestType,
        queue: str,
    ) -> Callable[[Message], Coroutine[Any, Any, Message | None]]:
        async def handler(message: Message) -> Message | None:
            return await self._handle(request_type, queue, message)

        return handler

    async def _handle(self, request_type: RequestType, queue: str, message: Message) -> Message | None:
        try:
            # Fast path: no middleware at all
            if not self._wire_mw and not self._app_mw:
                _serializer, request = self._detect_and_load_serializer(message)

                if self._app.hooks.on_request_received:
                    await self._app.hooks.on_request_received.emit(
                        RequestReceivedEvent(
                            app=self._app.name,
                            mode=request.type,
                            queue=queue,
                            request=request,
                        )
                    )

                response = await self._dispatch_request(request)
                if response is None or request_type != "rpc":
                    return None

                response_message = _serializer.dumps_response(response)

                rttl = request.headers.get("rttl")
                if rttl is not None:
                    response_message.headers["rttl"] = rttl

                return response_message

            # Full pipeline with middleware
            async def core() -> tuple[Message, Response] | tuple[None, None]:
                _serializer, request = self._detect_and_load_serializer(message)

                if self._app.hooks.on_request_received:
                    await self._app.hooks.on_request_received.emit(
                        RequestReceivedEvent(
                            app=self._app.name,
                            mode=request.type,
                            queue=queue,
                            request=request,
                        )
                    )

                async def dispatch_core() -> Response | None:
                    return await self._dispatch_request(request)

                app_handler: Callable[[], Awaitable[Response | None]] = apply_middleware(
                    self._app_mw,
                    dispatch_core,
                    lambda mw, n: wrap_app_server(mw, n, request),
                )

                response = await app_handler()
                if response is None or request_type != "rpc":
                    return None, None

                response_message = _serializer.dumps_response(response)

                rttl = request.headers.get("rttl")
                if rttl is not None:
                    response_message.headers["rttl"] = rttl

                return response_message, response

            wire_handler: Callable[
                [],
                Awaitable[tuple[Message, Response] | tuple[None, None]],
            ] = apply_middleware(
                self._wire_mw,
                core,
                lambda mw, n: wrap_wire_server(mw, n, message),
            )

            result = await wire_handler()
            if result is None or result[0] is None:
                return None

            response_message, _ = result
            return response_message

        except Exception as exc:
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode=request_type,
                        queue=queue,
                        reason="decode_error",
                        request=None,
                        handler=None,
                        topic=None,
                        error=exc,
                        details=None,
                    )
                )
            if request_type != "rpc":
                logger.exception("Failed to process %s message from queue %s", request_type, queue)
                return None
            return self._build_error_response(exc, message, self._serializer)

    async def _dispatch_request(self, request: Request) -> Response | None:
        if request.type == "rpc":
            return await self._dispatch_rpc_request(request)
        if request.type == "task":
            await self._dispatch_task_request(request)
            return None
        await self._dispatch_event_request(request)
        return None

    async def _dispatch_rpc_request(self, request: Request) -> Response | None:
        queue = request.headers.get("queue", self._queue_base)
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
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="rpc",
                        queue=str(queue),
                        reason="invalid_request",
                        request=request,
                        handler=None,
                        topic=None,
                        error=None,
                        details=response.data,
                    )
                )
            return response

        info = self._rpc_handlers.get((str(queue), method))

        if info is None:
            response = Response(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.METHOD_NOT_FOUND,
                    error_message=f"No handler registered for method: {method}",
                ),
            )
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="rpc",
                        queue=str(queue),
                        reason="handler_not_found",
                        request=request,
                        handler=None,
                        topic=None,
                        error=None,
                        details=response.data,
                    )
                )
            return response

        error_response = self._validate_args(info, request)
        if error_response is not None:
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="rpc",
                        queue=str(queue),
                        reason="validation_failed",
                        request=request,
                        handler=info,
                        topic=None,
                        error=None,
                        details=error_response.data,
                    )
                )
            return error_response

        if self._app.hooks.before_handler:
            await self._app.hooks.before_handler.emit(
                BeforeHandlerEvent(
                    app=self._app.name,
                    mode="rpc",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                )
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
            if self._app.hooks.on_handler_error:
                await self._app.hooks.on_handler_error.emit(
                    HandlerErrorEvent(
                        app=self._app.name,
                        mode="rpc",
                        queue=str(queue),
                        request=request,
                        handler=info,
                        topic=None,
                        error=exc,
                        error_code=ErrorCode.TIMEOUT,
                        error_message="Handler execution timed out",
                        outcome="timeout",
                    )
                )
            return response
        except Exception as exc:
            response = self._error_response(request, exc)
            error_info = response.data if isinstance(response.data, ErrorInfo) else ErrorInfo()
            if self._app.hooks.on_handler_error:
                await self._app.hooks.on_handler_error.emit(
                    HandlerErrorEvent(
                        app=self._app.name,
                        mode="rpc",
                        queue=str(queue),
                        request=request,
                        handler=info,
                        topic=None,
                        error=exc,
                        error_code=error_info.error_code,
                        error_message=error_info.error_message,
                        outcome="error",
                    )
                )
            return response

        if self._app.hooks.after_handler:
            await self._app.hooks.after_handler.emit(
                AfterHandlerEvent(
                    app=self._app.name,
                    mode="rpc",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                    result=result,
                )
            )
        return Response(id=request.id, type="result", data=result)

    async def _dispatch_task_request(self, request: Request) -> None:
        queue = request.headers.get("queue", self._queue_base)
        task = request.data.get("task")

        if not isinstance(task, str):
            logger.warning("Task request does not contain a task name")
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="task",
                        queue=str(queue),
                        reason="invalid_request",
                        request=request,
                        handler=None,
                        topic=None,
                        error=None,
                        details=None,
                    )
                )
            return None

        info = self._task_handlers.get((str(queue), task))
        if info is None:
            logger.warning("No handler registered for task: %s", task)
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="task",
                        queue=str(queue),
                        reason="handler_not_found",
                        request=request,
                        handler=None,
                        topic=None,
                        error=None,
                        details={"task": task},
                    )
                )
            return None

        error_response = self._validate_args(info, request)
        if error_response is not None:
            logger.warning(
                "Invalid arguments for task %s: %s",
                task,
                error_response.data,
            )
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="task",
                        queue=str(queue),
                        reason="validation_failed",
                        request=request,
                        handler=info,
                        topic=None,
                        error=None,
                        details=error_response.data,
                    )
                )
            return None

        if self._app.hooks.before_handler:
            await self._app.hooks.before_handler.emit(
                BeforeHandlerEvent(
                    app=self._app.name,
                    mode="task",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                )
            )
        try:
            result = await self._run_handler(info, request)
        except TimeoutError as exc:
            logger.warning("Task execution timed out: %s", task)
            if self._app.hooks.on_handler_error:
                await self._app.hooks.on_handler_error.emit(
                    HandlerErrorEvent(
                        app=self._app.name,
                        mode="task",
                        queue=str(queue),
                        request=request,
                        handler=info,
                        topic=None,
                        error=exc,
                        error_code=ErrorCode.TIMEOUT,
                        error_message="Handler execution timed out",
                        outcome="timeout",
                    )
                )
            return None
        except Exception as exc:
            logger.exception("Handler failed for task: %s", task)
            if self._app.hooks.on_handler_error:
                await self._app.hooks.on_handler_error.emit(
                    HandlerErrorEvent(
                        app=self._app.name,
                        mode="task",
                        queue=str(queue),
                        request=request,
                        handler=info,
                        topic=None,
                        error=exc,
                        error_code=ErrorCode.INTERNAL_ERROR,
                        error_message=str(exc),
                        outcome="error",
                    )
                )
            return None

        if self._app.hooks.after_handler:
            await self._app.hooks.after_handler.emit(
                AfterHandlerEvent(
                    app=self._app.name,
                    mode="task",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=None,
                    result=result,
                )
            )
        return None

    async def _dispatch_event_request(self, request: Request) -> None:
        queue = request.headers.get("queue", self._event_queue or self._queue_base)
        topic = request.data["topic"]
        subscribers = self._find_subscribers(topic)
        if not subscribers:
            logger.debug("No event subscribers for topic: %s", topic)
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="event",
                        queue=str(queue),
                        reason="no_subscribers",
                        request=request,
                        handler=None,
                        topic=str(topic),
                        error=None,
                        details=None,
                    )
                )
            return None

        await asyncio.gather(*(self._exec_subscriber(subscriber, request) for subscriber in subscribers))
        return None

    async def _exec_subscriber(self, info: HandlerInfo, request: Request) -> None:
        queue = request.headers.get("queue", self._event_queue or self._queue_base)
        topic = str(request.data.get("topic"))
        error_response = self._validate_args(info, request)
        if error_response is not None:
            logger.warning(
                "Invalid event arguments for %s: %s",
                info.name,
                error_response.data,
            )
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="event",
                        queue=str(queue),
                        reason="validation_failed",
                        request=request,
                        handler=info,
                        topic=topic,
                        error=None,
                        details=error_response.data,
                    )
                )
            return

        if self._app.hooks.before_handler:
            await self._app.hooks.before_handler.emit(
                BeforeHandlerEvent(
                    app=self._app.name,
                    mode="event",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=topic,
                )
            )
        try:
            result = await self._run_handler(info, request)
        except Exception as exc:
            logger.exception("Subscriber handler failed for %s", info.name)
            if self._app.hooks.on_handler_error:
                await self._app.hooks.on_handler_error.emit(
                    HandlerErrorEvent(
                        app=self._app.name,
                        mode="event",
                        queue=str(queue),
                        request=request,
                        handler=info,
                        topic=topic,
                        error=exc,
                        error_code=ErrorCode.INTERNAL_ERROR,
                        error_message=str(exc),
                        outcome="error",
                    )
                )
            return

        if self._app.hooks.after_handler:
            await self._app.hooks.after_handler.emit(
                AfterHandlerEvent(
                    app=self._app.name,
                    mode="event",
                    queue=str(queue),
                    request=request,
                    handler=info,
                    topic=topic,
                    result=result,
                )
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
                    timeout_value = max(float(timeout), 0)
                    if timeout_value > 0:
                        async with asyncio.timeout(timeout_value):
                            return await result
                    return await result
                return await result
            return result
        finally:
            _current_request.reset(token_req)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_serializer(self, name: str | None) -> Serializer:
        if name is None:
            return self._serializer
        try:
            return self._serializers_map[name]
        except KeyError as exc:
            raise RequestError(error_message=f"unknown serializer: {name}") from exc

    def _detect_and_load_serializer(self, message: Message) -> tuple[Serializer, Request]:
        ser_name = message.headers.get("serializer")
        if ser_name is not None:
            serializer = self._get_serializer(str(ser_name))
            return serializer, serializer.loads_request(message)

        for candidate in self._app.serializers:
            try:
                request = candidate.loads_request(message)
                return candidate, request
            except Exception:
                continue

        raise RequestError(
            error_message="cannot detect serializer for incoming message",
        )

    def _resolve_deps(self, info: HandlerInfo) -> dict[str, Any]:
        deps: dict[str, Any] = {}
        for name, (type_, factory) in info.dep_types.items():
            if factory is None:
                try:
                    deps[name] = type_()
                except Exception as exc:
                    type_name = getattr(type_, "__name__", repr(type_))
                    raise RuntimeError(f"Depends[{type_name}] requires inline factory") from exc
            else:
                deps[name] = factory()
        return deps

    def _validate_args(self, info: HandlerInfo, request: Request) -> Response | None:
        if not info.validate_args or not info.validation_adapters:
            return None

        data = request.data
        adapters = info.validation_adapters
        param_names = info.validation_param_names

        errors: list[str] = []

        if request.type == "event":
            event_data = data.get("event") or {}
            for pname, value in event_data.items():
                adapter = adapters.get(pname)
                if adapter is None:
                    continue
                try:
                    adapter.validate_python(value)
                except ValidationError as exc:
                    errors.append(f"{pname}: {exc.errors(include_input=False, include_url=False)}")
        else:
            args = data.get("args") or ()
            kwds = data.get("kwds") or {}

            for i, arg in enumerate(args):
                if i >= len(param_names):
                    break
                pname = param_names[i]
                if pname in kwds:
                    continue
                adapter = adapters.get(pname)
                if adapter is None:
                    continue
                try:
                    adapter.validate_python(arg)
                except ValidationError as exc:
                    errors.append(f"{pname}: {exc.errors(include_input=False, include_url=False)}")

            for pname, value in kwds.items():
                adapter = adapters.get(pname)
                if adapter is None:
                    continue
                try:
                    adapter.validate_python(value)
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
            handler = self._app.exception_handlers.get(cls)
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

    def _find_subscribers(self, topic: str) -> list[HandlerInfo]:
        subscribers: list[HandlerInfo] = []
        for pattern, handlers in self._subscribers.items():
            if pattern.fullmatch(topic):
                subscribers.extend(handlers)
        return subscribers
