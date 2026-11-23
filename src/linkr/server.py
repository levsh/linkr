from __future__ import annotations

import asyncio
import inspect
import logging
import re
from collections.abc import Callable, Coroutine, Sequence
from contextvars import ContextVar
from functools import wraps
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin, get_type_hints
from uuid import UUID

from pydantic import TypeAdapter, ValidationError

from .di import Depends
from .exceptions import ErrorCode, RegistrationAlreadyExistsError, RegistrationNotFoundError, RequestError, RPCError
from .hooks import (
    AfterHandlerEvent,
    BeforeHandlerEvent,
    ConsumeStartEvent,
    ConsumeStopEvent,
    HandlerErrorEvent,
    MessageDroppedEvent,
    RequestReceivedEvent,
)
from .middleware import AppMiddleware, WireMiddleware, apply_middleware, wrap_app_server, wrap_wire_server
from .models import ErrorInfo, HandlerInfo, Message, QueueOptions, Request, Response
from .serializer import Serializer
from .utils import normalize_queue_options, topic_to_regex

if TYPE_CHECKING:
    from .app import App

logger = logging.getLogger("linkr")

_current_request: ContextVar[Request | None] = ContextVar("linkr_current_request", default=None)


def _unwrap_handler(fn: Callable[..., Any]) -> Callable[..., Any]:
    return getattr(fn, "__wrapped__", fn)


def _same_handler(left: Callable[..., Any], right: Callable[..., Any]) -> bool:
    if left is right:
        return True
    left_orig = _unwrap_handler(left)
    right_orig = _unwrap_handler(right)
    if left_orig is right_orig:
        return True
    left_func = getattr(left_orig, "__func__", None)
    right_func = getattr(right_orig, "__func__", None)
    if left_func is not None and right_func is not None:
        return left_func is right_func and getattr(left_orig, "__self__", None) is getattr(right_orig, "__self__", None)
    return False


def get_current_request() -> Request:
    """Get the request being dispatched in this async context.

    Raises ``RuntimeError`` if called outside an active handler dispatch context.
    """
    req = _current_request.get()
    if req is None:
        raise RuntimeError("no active request in current context")
    return req


class _Server:
    """Base server with common handler processing logic."""

    def __init__(
        self,
        app: App,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
        validate_types: bool | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    ) -> None:
        if path is not None and (not isinstance(path, str) or path == ""):
            raise ValueError("server path must not be empty")

        self._app = app
        self._path = path
        self._queue_base = app.name if path is None else f"{app.name}.{path}"
        self._options = normalize_queue_options(options)
        self._validate_types = validate_types
        self._serializer = app.serializers[0]
        self._serializers_map = {s.name: s for s in app.serializers}
        self._own_middlewares = tuple(middlewares or ())
        own_app_mw = [mw for mw in self._own_middlewares if isinstance(mw, AppMiddleware)]
        own_wire_mw = [mw for mw in self._own_middlewares if isinstance(mw, WireMiddleware)]
        self._app_mw = [*app.app_middlewares, *own_app_mw]
        self._wire_mw = [*app.wire_middlewares, *own_wire_mw]

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def app(self) -> App:
        return self._app

    @property
    def path(self) -> str | None:
        return self._path

    @property
    def queue_base(self) -> str:
        return self._queue_base

    @property
    def validate_types(self) -> bool | None:
        return self._validate_types

    @property
    def options(self) -> tuple[QueueOptions, ...] | None:
        return self._options

    @property
    def own_middlewares(self) -> tuple[AppMiddleware | WireMiddleware, ...]:
        """Middlewares configured on this server (excluding app-level middlewares)."""
        return self._own_middlewares

    @property
    def middlewares(self) -> tuple[AppMiddleware | WireMiddleware, ...]:
        """Middlewares applied to handlers of this server (app + server)."""
        return (*self._app_mw, *self._wire_mw)

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_leaf_name(name: str) -> str:
        if not isinstance(name, str) or not name:
            raise ValueError("handler name must not be empty")
        if "." in name:
            raise ValueError("handler name must not contain '.'")
        return name

    @staticmethod
    def _validate_handler(fn: object) -> None:
        if not inspect.isroutine(fn):
            raise TypeError(f"handler must be a function or method, got {type(fn).__name__!r}")

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
            validate = self._validate_types
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
        raise RequestError(error_message="cannot detect serializer for incoming message")

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
                data=ErrorInfo(error_code=ErrorCode.VALIDATION_ERROR, error_message="; ".join(errors)),
            )
        return None

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
            if inspect.iscoroutinefunction(inspect.unwrap(info.fn)):
                coro = info.fn(*args, **kwds)
                server_timeout = request.headers.get("server_timeout")
                if info.request_type in ("rpc", "task") and server_timeout is not None:
                    wait_timeout = max(float(server_timeout), 0.0)
                    return await asyncio.wait_for(coro, wait_timeout)
                return await coro
            return info.fn(*args, **kwds)
        finally:
            _current_request.reset(token_req)

    # ------------------------------------------------------------------
    # Request handling
    # ------------------------------------------------------------------

    def _make_handler(self, queue: str) -> Callable[[Message], Coroutine[Any, Any, Message | None]]:
        async def handler(message: Message) -> Message | None:
            return await self._handle(queue, message)

        return handler

    async def _handle(self, queue: str, message: Message) -> Message | None:
        if not self._wire_mw and not self._app_mw:
            _serializer, request = self._detect_and_load_serializer(message)
            if self._app.hooks.on_request_received:
                await self._app.hooks.on_request_received.emit(
                    RequestReceivedEvent(app=self._app.name, mode=request.type, queue=queue, request=request)
                )
            response = await self._dispatch_request(request)
            if response is None:
                return None
            response_message = _serializer.dumps_response(response)
            rttl = request.headers.get("rttl")
            if rttl is not None:
                response_message.headers["rttl"] = rttl
            return response_message

        async def core() -> tuple[Message, Response] | tuple[None, None]:
            _serializer, request = self._detect_and_load_serializer(message)
            if self._app.hooks.on_request_received:
                await self._app.hooks.on_request_received.emit(
                    RequestReceivedEvent(app=self._app.name, mode=request.type, queue=queue, request=request)
                )

            async def dispatch_core() -> Response | None:
                return await self._dispatch_request(request)

            app_handler = apply_middleware(
                self._app_mw,
                dispatch_core,
                lambda mw, n: wrap_app_server(mw, n, request),
            )
            response = await app_handler()
            if response is None:
                return None, None
            response_message = _serializer.dumps_response(response)
            rttl = request.headers.get("rttl")
            if rttl is not None:
                response_message.headers["rttl"] = rttl
            return response_message, response

        wire_handler = apply_middleware(
            self._wire_mw,
            core,
            lambda mw, n: wrap_wire_server(mw, n, message),
        )
        result = await wire_handler()
        if result is None or result[0] is None:
            return None
        response_message, _ = result
        return response_message

    # ------------------------------------------------------------------
    # Backend consume helpers
    # ------------------------------------------------------------------

    async def _consume_backend(
        self,
        mode: str,
        queue: str,
        handler: Callable[[Message], Coroutine[Any, Any, Message | None]],
        *,
        topics: list[str] | None = None,
        options: tuple[QueueOptions, ...] | None = None,
    ) -> None:
        backend = self._app.backend
        if mode == "event":
            consume_event = backend.consume_event
            try:
                params = inspect.signature(consume_event).parameters
            except (TypeError, ValueError):
                params = inspect.Signature().parameters
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

        consume = backend.consume_rpc if mode == "rpc" else backend.consume_task
        args: tuple[Any, ...] = (queue, handler)
        try:
            params = inspect.signature(consume).parameters
        except (TypeError, ValueError):
            params = inspect.Signature().parameters
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
    # Abstract interface
    # ------------------------------------------------------------------

    async def _dispatch_request(self, request: Request) -> Response | None:
        raise NotImplementedError

    async def consume(self) -> None:
        raise NotImplementedError

    async def stop_consume(self) -> None:
        raise NotImplementedError


class RPC(_Server):
    """RPC server."""

    def __init__(
        self,
        app: App,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
        validate_types: bool | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    ) -> None:
        super().__init__(
            app,
            path,
            options=options,
            validate_types=validate_types,
            middlewares=middlewares,
        )
        self._handlers: dict[tuple[str, str], HandlerInfo] = {}
        self._consuming = False

    @property
    def methods(self) -> dict[str, HandlerInfo]:
        return {leaf: info for (_, leaf), info in self._handlers.items()}

    def method(self, name: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        if self._consuming:
            raise RuntimeError("cannot register method while consuming; use add_method()")
        leaf = self._validate_leaf_name(name)
        return self._register(leaf, options)

    async def add_method(
        self,
        name: str,
        handler: Callable[..., Any],
        **options: Any,
    ) -> Callable[..., Any]:
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        leaf = self._validate_leaf_name(name)
        return self._register_handler(leaf, handler, options)

    async def remove_method(self, name: str) -> None:
        leaf = self._validate_leaf_name(name)
        key = (self._queue_base, leaf)
        if key not in self._handlers:
            raise RegistrationNotFoundError(
                f"rpc handler not registered: {leaf!r}",
                kind="method",
                name=leaf,
                queue=self._queue_base,
            )
        self._handlers.pop(key)

    def _register(self, leaf: str, options: dict[str, Any]) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return self._register_handler(leaf, fn, options)

        return decorator

    def _register_handler(
        self,
        leaf: str,
        fn: Callable[..., Any],
        options: dict[str, Any],
    ) -> Callable[..., Any]:
        queue = self._queue_base
        fq_name = f"{queue}.{leaf}"
        key = (queue, leaf)

        self._validate_handler(fn)
        existing = self._handlers.get(key)
        if existing is not None:
            if _same_handler(existing.fn, fn) and existing.options == options:
                return existing.fn
            raise RegistrationAlreadyExistsError(
                f"rpc handler already registered for {queue!r}: {leaf!r}",
                kind="method",
                name=leaf,
                queue=queue,
            )

        @wraps(fn)
        def wrapper(*args: Any, **kwds: Any) -> Any:
            return fn(*args, **kwds)

        dep_types = self._dep_types(fn)
        validate_args, validation_adapters, validation_param_names = self._build_validation_plan(fn, options, dep_types)
        self._handlers[key] = HandlerInfo(
            name=fq_name,
            request_type="rpc",
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

    async def consume(self) -> None:
        if self._consuming:
            return
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        queue = self._queue_base
        handler = self._make_handler(queue)
        await self._consume_backend("rpc", queue, handler, options=self._options)
        self._consuming = True
        if self._app.hooks.on_consume_start:
            await self._app.hooks.on_consume_start.emit(
                ConsumeStartEvent(app=self._app.name, mode="rpc", queue=queue, topics=())
            )

    async def stop_consume(self) -> None:
        if not self._consuming:
            return
        await self._app.backend.stop_consume_rpc(self._queue_base)
        self._consuming = False
        if self._app.hooks.on_consume_stop:
            await self._app.hooks.on_consume_stop.emit(
                ConsumeStopEvent(app=self._app.name, mode="rpc", queue=self._queue_base, topic=None, reason="stopped")
            )

    async def _dispatch_request(self, request: Request) -> Response | None:
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

        info = self._handlers.get((str(queue), method))
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
                    app=self._app.name, mode="rpc", queue=str(queue), request=request, handler=info, topic=None
                )
            )
        try:
            result = await self._run_handler(info, request)
        except TimeoutError as exc:
            response = Response(
                id=request.id,
                type="error",
                data=ErrorInfo(error_code=ErrorCode.TIMEOUT, error_message="Handler execution timed out"),
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

    def _build_error_response(self, exc: Exception, message: Message, fallback_serializer: Serializer) -> Message:
        if isinstance(exc, RPCError):
            error_code = exc.error_code
            error_message = exc.error_message
            details = exc.error_details
        else:
            logger.exception(exc)
            error_code = ErrorCode.INTERNAL_ERROR
            error_message = "Internal server error"
            details = {"exc_type": type(exc).__name__}

        best_serializer = fallback_serializer
        message_id = UUID(int=0)

        candidates: list[Serializer] = []
        ser_name = message.headers.get("serializer")
        if ser_name is not None:
            try:
                candidates.append(self._get_serializer(str(ser_name)))
            except Exception:
                pass
        for c in self._app.serializers:
            if c not in candidates:
                candidates.append(c)

        for s in candidates:
            try:
                request = s.loads_request(message)
                message_id = request.id
                best_serializer = s
                break
            except Exception:
                continue

        error_resp = Response(
            id=message_id,
            type="error",
            data=ErrorInfo(error_code=error_code, error_message=error_message, error_details=details),
        )
        return best_serializer.dumps_response(error_resp)

    async def _handle(self, queue: str, message: Message) -> Message | None:
        try:
            return await super()._handle(queue, message)
        except Exception as exc:
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="rpc",
                        queue=queue,
                        reason="decode_error",
                        request=None,
                        handler=None,
                        topic=None,
                        error=exc,
                        details=None,
                    )
                )
            return self._build_error_response(exc, message, self._serializer)


class Worker(_Server):
    """Task server."""

    def __init__(
        self,
        app: App,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
        validate_types: bool | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    ) -> None:
        super().__init__(
            app,
            path,
            options=options,
            validate_types=validate_types,
            middlewares=middlewares,
        )
        self._handlers: dict[tuple[str, str], HandlerInfo] = {}
        self._consuming = False

    @property
    def tasks(self) -> dict[str, HandlerInfo]:
        return {leaf: info for (_, leaf), info in self._handlers.items()}

    def task(self, name: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        if self._consuming:
            raise RuntimeError("cannot register task while consuming; use add_task()")
        leaf = self._validate_leaf_name(name)
        return self._register(leaf, options)

    async def add_task(
        self,
        name: str,
        handler: Callable[..., Any],
        **options: Any,
    ) -> Callable[..., Any]:
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        leaf = self._validate_leaf_name(name)
        return self._register_handler(leaf, handler, options)

    async def remove_task(self, name: str) -> None:
        leaf = self._validate_leaf_name(name)
        key = (self._queue_base, leaf)
        if key not in self._handlers:
            raise RegistrationNotFoundError(
                f"task handler not registered: {leaf!r}",
                kind="task",
                name=leaf,
                queue=self._queue_base,
            )
        self._handlers.pop(key)

    def _register(self, leaf: str, options: dict[str, Any]) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            return self._register_handler(leaf, fn, options)

        return decorator

    def _register_handler(
        self,
        leaf: str,
        fn: Callable[..., Any],
        options: dict[str, Any],
    ) -> Callable[..., Any]:
        queue = self._queue_base
        fq_name = f"{queue}.{leaf}"
        key = (queue, leaf)

        self._validate_handler(fn)
        existing = self._handlers.get(key)
        if existing is not None:
            if _same_handler(existing.fn, fn) and existing.options == options:
                return existing.fn
            raise RegistrationAlreadyExistsError(
                f"task handler already registered for {queue!r}: {leaf!r}",
                kind="task",
                name=leaf,
                queue=queue,
            )

        @wraps(fn)
        def wrapper(*args: Any, **kwds: Any) -> Any:
            return fn(*args, **kwds)

        dep_types = self._dep_types(fn)
        validate_args, validation_adapters, validation_param_names = self._build_validation_plan(fn, options, dep_types)
        self._handlers[key] = HandlerInfo(
            name=fq_name,
            request_type="task",
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

    async def consume(self) -> None:
        if self._consuming:
            return
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        queue = self._queue_base
        handler = self._make_handler(queue)
        await self._consume_backend("task", queue, handler, options=self._options)
        self._consuming = True
        if self._app.hooks.on_consume_start:
            await self._app.hooks.on_consume_start.emit(
                ConsumeStartEvent(app=self._app.name, mode="task", queue=queue, topics=())
            )

    async def stop_consume(self) -> None:
        if not self._consuming:
            return
        await self._app.backend.stop_consume_tasks(self._queue_base)
        self._consuming = False
        if self._app.hooks.on_consume_stop:
            await self._app.hooks.on_consume_stop.emit(
                ConsumeStopEvent(app=self._app.name, mode="task", queue=self._queue_base, topic=None, reason="stopped")
            )

    async def _handle(self, queue: str, message: Message) -> Message | None:
        try:
            return await super()._handle(queue, message)
        except Exception as exc:
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="task",
                        queue=queue,
                        reason="decode_error",
                        request=None,
                        handler=None,
                        topic=None,
                        error=exc,
                        details=None,
                    )
                )
            logger.exception("Failed to handle task message on queue %s", queue)
            return None

    async def _dispatch_request(self, request: Request) -> Response | None:
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

        info = self._handlers.get((str(queue), task))
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
            logger.warning("Invalid arguments for task %s: %s", task, error_response.data)
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
                    app=self._app.name, mode="task", queue=str(queue), request=request, handler=info, topic=None
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


class Bus(_Server):
    """Event server."""

    def __init__(
        self,
        app: App,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
        validate_types: bool | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    ) -> None:
        super().__init__(
            app,
            path,
            options=options,
            validate_types=validate_types,
            middlewares=middlewares,
        )
        self._subscribers_by_topic: dict[str, list[HandlerInfo]] = {}
        self._patterns_by_topic: dict[str, re.Pattern[str]] = {}
        self._subscription_patterns: set[str] = set()
        self._consuming = False
        self._event_queue: str | None = None
        self._bound_patterns: set[str] = set()
        self._subscription_lock = asyncio.Lock()

    @property
    def event_queue(self) -> str | None:
        return self._event_queue

    @property
    def subscriptions(self) -> dict[str, list[HandlerInfo]]:
        grouped: dict[str, list[HandlerInfo]] = {}
        for handlers in self._subscribers_by_topic.values():
            for info in handlers:
                grouped.setdefault(info.name, []).append(info)
        return dict(sorted(grouped.items()))

    def _event_queue_name(self) -> str:
        return f"{self._queue_base}.{self._app.instance_id}"

    def _create_subscriber_info(self, topic: str, fn: Callable[..., Any], options: dict[str, Any]) -> HandlerInfo:
        self._validate_handler(fn)

        @wraps(fn)
        def wrapper(*args: Any, **kwds: Any) -> Any:
            return fn(*args, **kwds)

        dep_types = self._dep_types(fn)
        validate_args, validation_adapters, validation_param_names = self._build_validation_plan(
            fn,
            options,
            dep_types,
        )
        return HandlerInfo(
            name=topic,
            request_type="event",
            fn=wrapper,
            signature=str(inspect.signature(fn)),
            options=options,
            dep_types=dep_types,
            queue=self._event_queue or self._event_queue_name(),
            validate_args=validate_args,
            validation_adapters=validation_adapters,
            validation_param_names=validation_param_names,
        )

    def _register_subscriber_info(self, topic: str, info: HandlerInfo) -> None:
        if topic not in self._patterns_by_topic:
            self._patterns_by_topic[topic] = topic_to_regex(topic)
        self._subscribers_by_topic.setdefault(topic, []).append(info)
        self._subscription_patterns.add(topic)

    def _find_subscriber(self, topic: str, handler: Callable[..., Any]) -> HandlerInfo | None:
        subscribers = self._subscribers_by_topic.get(topic, [])
        for info in subscribers:
            if _same_handler(info.fn, handler):
                return info
        return None

    def subscribe(self, topic: str, **options: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        if self._consuming:
            raise RuntimeError("cannot subscribe while consuming; use add_subscriber()")

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            self._validate_handler(fn)
            existing = self._find_subscriber(topic, fn)
            if existing is not None:
                if existing.options == options:
                    return existing.fn
                raise RegistrationAlreadyExistsError(
                    f"event subscriber already registered for topic: {topic!r}",
                    kind="subscriber",
                    name=topic,
                    queue=self._queue_base,
                )
            info = self._create_subscriber_info(topic, fn, options)
            self._register_subscriber_info(topic, info)
            return info.fn

        return decorator

    async def add_subscriber(
        self,
        topic: str,
        handler: Callable[..., Any],
        **options: Any,
    ) -> Callable[..., Any]:
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        self._validate_handler(handler)
        async with self._subscription_lock:
            existing = self._find_subscriber(topic, handler)
            if existing is not None:
                if existing.options == options:
                    return existing.fn
                raise RegistrationAlreadyExistsError(
                    f"event subscriber already registered for topic: {topic!r}",
                    kind="subscriber",
                    name=topic,
                    queue=self._queue_base,
                )

            info = self._create_subscriber_info(topic, handler, options)
            is_new_pattern = topic not in self._subscription_patterns
            event_queue = self._event_queue
            needs_bind = self._consuming and event_queue is not None and topic not in self._bound_patterns
            if needs_bind and event_queue is not None:
                try:
                    await self._app.backend.bind_event_topics(event_queue, [topic])
                except NotImplementedError as exc:
                    raise RuntimeError(
                        "backend does not support dynamic event binding; stop_consume() and consume() required"
                    ) from exc
            self._register_subscriber_info(topic, info)
            if self._consuming and event_queue is not None and (needs_bind or not is_new_pattern):
                self._bound_patterns.add(topic)
        return info.fn

    async def remove_subscriber(
        self,
        topic: str,
        handler: Callable[..., Any] | None = None,
    ) -> None:
        unbound_queue: str | None = None
        async with self._subscription_lock:
            subscribers = self._subscribers_by_topic.get(topic)
            if not subscribers:
                raise RegistrationNotFoundError(
                    f"event subscriber not registered for topic: {topic!r}",
                    kind="subscriber",
                    name=topic,
                    queue=self._queue_base,
                )

            if handler is None:
                subscribers.clear()
            else:
                index = None
                for i, info in enumerate(subscribers):
                    if _same_handler(info.fn, handler):
                        index = i
                        break
                if index is None:
                    raise RegistrationNotFoundError(
                        f"event subscriber handler not registered for topic: {topic!r}",
                        kind="subscriber",
                        name=topic,
                        queue=self._queue_base,
                    )
                subscribers.pop(index)

            if subscribers:
                return

            self._subscribers_by_topic.pop(topic, None)
            self._patterns_by_topic.pop(topic, None)
            self._subscription_patterns.discard(topic)

            if self._consuming and self._event_queue is not None and topic in self._bound_patterns:
                unbound_queue = self._event_queue
                await self._app.backend.stop_consume_events(self._event_queue, topic)
            self._bound_patterns.discard(topic)

        if unbound_queue is not None and self._app.hooks.on_consume_stop:
            await self._app.hooks.on_consume_stop.emit(
                ConsumeStopEvent(
                    app=self._app.name,
                    mode="event",
                    queue=unbound_queue,
                    topic=topic,
                    reason="topic_unsubscribed",
                )
            )

    async def consume(self) -> None:
        if self._consuming:
            return
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        if not self._subscription_patterns:
            return
        queue = self._event_queue_name()
        topics = tuple(sorted(self._subscription_patterns))
        handler = self._make_handler(queue)
        await self._consume_backend("event", queue, handler, topics=list(topics), options=self._options)
        self._event_queue = queue
        self._bound_patterns = set(topics)
        self._consuming = True
        if self._app.hooks.on_consume_start:
            await self._app.hooks.on_consume_start.emit(
                ConsumeStartEvent(app=self._app.name, mode="event", queue=queue, topics=topics)
            )

    async def stop_consume(self) -> None:
        if not self._consuming or self._event_queue is None:
            self._consuming = False
            self._event_queue = None
            self._bound_patterns.clear()
            return
        queue = self._event_queue
        await self._app.backend.stop_consume_events(queue)
        self._consuming = False
        self._event_queue = None
        self._bound_patterns.clear()
        if self._app.hooks.on_consume_stop:
            await self._app.hooks.on_consume_stop.emit(
                ConsumeStopEvent(app=self._app.name, mode="event", queue=queue, topic=None, reason="stopped")
            )

    async def _handle(self, queue: str, message: Message) -> Message | None:
        try:
            return await super()._handle(queue, message)
        except Exception as exc:
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="event",
                        queue=queue,
                        reason="decode_error",
                        request=None,
                        handler=None,
                        topic=None,
                        error=exc,
                        details=None,
                    )
                )
            logger.exception("Failed to handle event message on queue %s", queue)
            return None

    def _find_subscribers(self, topic: str) -> list[HandlerInfo]:
        subscribers: list[HandlerInfo] = []
        for t, handlers in self._subscribers_by_topic.items():
            pat = self._patterns_by_topic.get(t)
            if pat is not None and pat.fullmatch(topic):
                subscribers.extend(handlers)
        return subscribers

    async def _dispatch_request(self, request: Request) -> Response | None:
        queue = request.headers.get("queue", self._event_queue or self._queue_base)
        topic = request.data.get("topic")
        if not isinstance(topic, str):
            logger.warning("Event request does not contain a topic")
            if self._app.hooks.on_message_dropped:
                await self._app.hooks.on_message_dropped.emit(
                    MessageDroppedEvent(
                        app=self._app.name,
                        mode="event",
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
            logger.warning("Invalid event arguments for %s: %s", info.name, error_response.data)
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
                    app=self._app.name, mode="event", queue=str(queue), request=request, handler=info, topic=topic
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
