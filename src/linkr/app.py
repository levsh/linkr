from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, cast, get_args, get_origin, get_type_hints
from uuid import UUID, uuid4

from pydantic import TypeAdapter, ValidationError

from .di import Depends, DiContainer, _current_request, _request_instances
from .exceptions import ErrorCode, RpcError
from .middleware import AppMiddleware, WireMiddleware
from .models import ErrorInfo, HandlerInfo, RawMessage, Request, Response
from .serializer import JsonSerializer, Serializer
from .transports import Transport

logger = logging.getLogger("linkr")


class Invocation:
    """
    Builder for preparing and executing or publishing a request.

    Returned by :meth:`App.make` to allow deferred or repeated execution
    (via :meth:`invoke` / :meth:`__call__`) or publication (via :meth:`publish`)
    of the same request with optional per-call overrides for timeout, TTL,
    rTTL, and serializer.
    """

    def __init__(self, app: App, method: str, args: tuple[Any, ...], kwds: dict[str, Any]) -> None:
        self._app = app
        self._method = method
        self._args = args
        self._kwds = kwds
        self._id = uuid4()

    @property
    def app(self) -> App:
        """The App instance this call belongs to."""
        return self._app

    @property
    def request(self) -> Request:
        """The prepared Request to send."""
        return Request(
            id=self._id,
            method=self._method,
            args=self._args,
            kwds=self._kwds,
            headers={"queue": self._app.resolve_queue(self._method)},
        )

    async def __call__(
        self,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        **kwds: Any,
    ) -> Any:
        """
        Execute the RPC call.

        Shorthand for ``await self.app.call(self, ...)``.

        Args:
            timeout: Maximum execution time in seconds for the remote handler.
                If the handler does not complete within this window the server
                sends back a TIMEOUT error. For a client-side deadline wrap the
                call in ``asyncio.wait_for(...)``.
            ttl: Message time-to-live in seconds (broker discards expired).
            rttl: Response TTL in seconds.
            serializer: Serializer name to use for this call.
            **kwds: Additional call context.

        Returns:
            The handler's return value.

        Raises:
            RpcError: If the server returned an error response.
            RuntimeError: If the app is closed.
        """
        return await self._app.call(
            self,
            timeout=timeout,
            ttl=ttl,
            rttl=rttl,
            serializer=serializer,
            **kwds,
        )

    async def invoke(
        self,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        **kwds: Any,
    ) -> Any:
        """
        Execute the RPC call.

        Shorthand for ``await self.app.call(self, ...)``.
        Identical to :meth:`__call__`.

        Args:
            timeout: Maximum execution time in seconds for the remote handler.
                If the handler does not complete within this window the server
                sends back a TIMEOUT error. For a client-side deadline wrap the
                call in ``asyncio.wait_for(...)``.
            ttl: Message time-to-live in seconds (broker discards expired).
            rttl: Response TTL in seconds.
            serializer: Serializer name to use for this call.
            **kwds: Additional call context.

        Returns:
            The handler's return value.

        Raises:
            RpcError: If the server returned an error response.
            RuntimeError: If the app is closed.
        """
        return await self._app.call(
            self,
            timeout=timeout,
            ttl=ttl,
            rttl=rttl,
            serializer=serializer,
            **kwds,
        )

    async def publish(
        self,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        **kwds: Any,
    ) -> None:
        """
        Publish the request (fire-and-forget).

        Shorthand for ``await self.app.publish(self, ...)``.
        """
        return await self._app.publish(
            self,
            timeout=timeout,
            ttl=ttl,
            rttl=rttl,
            serializer=serializer,
            **kwds,
        )


class App:
    """
    Main application: register RPC handlers and pub-sub subscribers, send requests, manage middleware.

    Typical usage::

        transport = LocalTransport()
        app = App(transport)

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        result = await app.make("add", 2, 3).invoke()
        await app.close()

    Args:
        transport: Backend used for message exchange (e.g. LocalTransport, RmqTransport).
        timeout: Default execution timeout in seconds for all remote handlers.
            Passed to the server which enforces it; the client does not time out
            the underlying transport call automatically.
        ttl: Default message TTL in seconds.
        rttl: Default response TTL in seconds.
        serializer: Serializer or list of serializers for request/response
            encoding. When a list is given the first entry is the default
            and the server auto-detects the format for incoming requests.
            Defaults to :class:`JsonSerializer`.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: Serializer | list[Serializer] | None = None,
        instance_id: str | None = None,
        validate_types: bool = False,
    ) -> None:
        self._transport = transport
        self._timeout = timeout
        self._ttl = ttl
        self._rttl = rttl
        self._validate_types = validate_types
        self._closed = False
        self._handlers: dict[str, HandlerInfo] = {}
        self._subscribers: dict[str, list[HandlerInfo]] = {}
        self._pending_subscriber_topics: set[str] = set()
        self._subscriber_tasks: set[asyncio.Task[Any]] = set()
        self._instance_id = instance_id or uuid4().hex
        self._app_mw: list[AppMiddleware] = []
        self._wire_mw: list[WireMiddleware] = []
        self._exception_handlers: dict[type[Exception], Callable[[Exception], Any]] = {}
        self.dependencies = DiContainer()
        self._serializers: dict[str | None, Serializer] = {}
        serializers: list[Serializer]
        if serializer is None:
            serializers = [JsonSerializer()]
        elif isinstance(serializer, Serializer):
            serializers = [serializer]
        else:
            serializers = serializer
        for s in serializers:
            self._serializers[s.name] = s
        self._serializers[None] = serializers[0]

    def _get_serializer(self, name: str | None) -> Serializer:
        return self._serializers[name]

    def add_middleware(self, mw: AppMiddleware | WireMiddleware) -> None:
        """
        Register a middleware.

        App-level middleware is applied in registration order
        around request/response processing.
        Wire-level middleware is applied in dispatch order around the
        transport.

        Args:
            mw: Middleware instance. AppMiddleware is added to the app chain
                (deserialized objects); WireMiddleware is added to the wire
                chain (raw bytes).
        """
        if isinstance(mw, AppMiddleware):
            self._app_mw.append(mw)
        else:
            self._wire_mw.append(mw)

    def method(
        self,
        name: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Register an RPC handler via a decorator.

        Args:
            name: Method name used for routing (e.g. ``"add"`` or ``"api/user/get"``).
            **options: Arbitrary metadata stored in :attr:`HandlerInfo.options`.
                Use ``validate_types=True`` to enable Pydantic type validation
                of handler arguments.

        Returns:
            Decorator that wraps the handler function and registers it.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:

            @wraps(fn)
            def wrapper(*args: Any, **kwds: Any) -> Any:
                return fn(*args, **kwds)

            dep_types: dict[str, type] = {}
            hints = get_type_hints(fn, localns={"Depends": Depends})
            for pname, ann in hints.items():
                if pname == "return":
                    continue
                if get_origin(ann) is Depends:
                    dep_types[pname] = get_args(ann)[0]

            sig = str(inspect.signature(fn))
            self._handlers[name] = HandlerInfo(
                name=name,
                fn=wrapper,
                signature=sig,
                options=options,
                dep_types=dep_types,
            )
            return wrapper

        return decorator

    def get_handler(self, name: str) -> HandlerInfo | None:
        """
        Look up a registered handler by method name.

        Args:
            name: The method name used at registration time.

        Returns:
            The handler metadata, or None if no handler is registered under *name*.
        """
        return self._handlers.get(name)

    def subscribe(
        self,
        topic: str,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Register a pub-sub handler for a topic via a decorator.

        Each instance gets its own queue so all subscribers receive every
        event published to *topic*.  Multiple handlers on the same topic
        within one instance are all called concurrently.

        Args:
            topic: Event topic name (e.g. ``"user.created"``).  Wildcard
                patterns (``"users.*"``) are supported on the server side.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:

            @wraps(fn)
            def wrapper(*args: Any, **kwds: Any) -> Any:
                return fn(*args, **kwds)

            sig = str(inspect.signature(fn))
            info = HandlerInfo(
                name=topic,
                fn=wrapper,
                signature=sig,
                options={},
                dep_types={},
            )
            self._subscribers.setdefault(topic, []).append(info)
            self._pending_subscriber_topics.add(topic)

            return wrapper

        return decorator

    def exception_handler(
        self,
        exc_class: type[Exception],
    ) -> Callable[[Callable[[Exception], Any]], Callable[[Exception], Any]]:
        """
        Register a custom exception handler for *exc_class* via a decorator.

        Args:
            exc_class: The exception type to handle (supports inheritance).
        """

        def decorator(fn: Callable[[Exception], Any]) -> Callable[[Exception], Any]:
            self._exception_handlers[exc_class] = fn
            return fn

        return decorator

    @property
    def methods(self) -> dict[str, HandlerInfo]:
        """All registered handlers keyed by method name."""
        return dict(self._handlers)

    def resolve_queue(self, method: str) -> str | None:
        """
        Derive a queue name/prefix from a method name containing slashes.

        Args:
            method: Method name (e.g. ``"api/user/get"``).

        Returns:
            The queue group prefix (e.g. ``"api/user"``), or None.
        """
        method = method.strip("/")
        if "/" in method:
            return method.rsplit("/", 1)[0]
        return None

    def _resolve_deps(self, info: HandlerInfo) -> dict[str, Any]:
        return {name: self.dependencies.resolve(dep_type) for name, dep_type in info.dep_types.items()}

    def _find_subscribers(self, topic: str) -> list[HandlerInfo]:
        results: list[HandlerInfo] = []
        results.extend(self._subscribers.get(topic, []))
        for pattern, handlers in self._subscribers.items():
            if pattern != topic and self._match_topic(pattern, topic):
                results.extend(handlers)
        return results

    @staticmethod
    def _match_topic(pattern: str, topic: str) -> bool:
        p_parts = pattern.split(".")
        t_parts = topic.split(".")
        for p, t in zip(p_parts, t_parts):
            if p == "*":
                continue
            if p == "**":
                return True
            if p != t:
                return False
        return len(p_parts) == len(t_parts)

    def make(
        self,
        method: str,
        *args: Any,
        **kwds: Any,
    ) -> Invocation:
        """
        Create an Invocation for a method with positional/keyword arguments.

        Args:
            method: The registered method name.
            *args: Positional arguments for the handler.
            **kwds: Keyword arguments for the handler.

        Returns:
            An :class:`Invocation` ready to be executed with ``.invoke()`` or ``await``.
        """
        return Invocation(self, method, args, kwds)

    async def init(self) -> None:
        """
        Open transport and initialise all middleware.

        Must be called before :meth:`consume`, :meth:`call`, or :meth:`publish`.
        """
        if self._closed:
            raise RuntimeError("App is closed")
        await self._transport.init()
        for app_mw in self._app_mw:
            await app_mw.init()
        for wire_mw in self._wire_mw:
            await wire_mw.init()

    async def close(self, timeout: float | None = None) -> None:
        """
        Shut down the application.

        Args:
            timeout: Passed to the transport's ``close()``.  See
                :meth:`Transport.close` for details.
        """
        await self.stop_consume()
        for app_mw in reversed(self._app_mw):
            await app_mw.close()
        for wire_mw in reversed(self._wire_mw):
            await wire_mw.close()

        if self._subscriber_tasks:
            _, pending = await asyncio.wait(self._subscriber_tasks, timeout=timeout)
            for task in pending:
                task.cancel()

        await self._transport.close(timeout=timeout)
        self._closed = True

    async def consume(self) -> None:
        """
        Start listening for incoming requests on the transport.

        Registers the internal request handler on the main server queue and
        on any routing-prefix queues derived from method names containing
        ``/`` (e.g. ``"api/user/get"`` creates a queue named
        ``{server_queue_name}.api.user``).

        Raises:
            RuntimeError: If the app has already been closed.
        """
        if self._closed:
            raise RuntimeError("App is closed")

        await self._transport.consume(self._request_handler)

        for name in self._handlers:
            queue = self.resolve_queue(name)
            if queue:
                await self._transport.consume(self._request_handler, queue=queue)

        for topic in self._pending_subscriber_topics:
            queue = f"evt.{topic}.{self._instance_id}"
            await self._transport.consume(self._request_handler, queue=queue)

    async def stop_consume(self) -> None:
        """Stop listening for incoming RPC requests."""
        await self._transport.stop_consume()

    def _prepare_request(
        self,
        invocation: Invocation,
        timeout: float | None,
        ttl: float | None,
        rttl: float | None,
    ) -> Request:
        request = invocation.request
        timeout = timeout if timeout is not None else self._timeout
        ttl = ttl if ttl is not None else self._ttl
        rttl = rttl if rttl is not None else self._rttl

        if ttl is None and timeout is not None:
            ttl = timeout
        if ttl is not None:
            request.headers["ttl"] = ttl
        if timeout is not None:
            request.headers["timeout"] = timeout
        if rttl is not None:
            request.headers["rttl"] = rttl
        return request

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
        raw_request: RawMessage,
        request: Request,
        kwds: dict[str, Any],
    ) -> Callable[[], Coroutine[Any, Any, Any]]:
        async def wrapper() -> Any:
            return await mw.dispatch_client(handler, raw_request, request, kwds=kwds)

        return wrapper

    async def call(
        self,
        invocation: Invocation,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        **kwds: Any,
    ) -> Any:
        """
        Send an RPC request and await the response.

        Runs the full middleware pipeline: app-level dispatch_client,
        serialization, wire-level dispatch_client, transport request,
        wire-level dispatch_client response handling, deserialization,
        app-level dispatch_client response handling.

        Args:
            invocation: The prepared Invocation to send.
            timeout: Maximum execution time in seconds for the remote handler.
                If the handler does not complete within this window the server
                returns a TIMEOUT error. Falls back to the app-level default.
                This is a server-side limit; for a client-side deadline wrap
                the call in ``asyncio.wait_for(...)``.
            ttl: Message time-to-live in seconds (broker discards expired).
                Falls back to the app-level default; if neither is set
                but *timeout* is given, TTL is set to the same value.
            rttl: Response TTL in seconds.
            serializer: Serializer name or None for default.
            **kwds: Additional call context.

        Returns:
            The handler's return value.

        Raises:
            RuntimeError: If the app is closed.
            RpcError: If the server returned an error response
                (error_code, error_message, error_details).
        """
        if self._closed:
            raise RuntimeError("App is closed")

        request = self._prepare_request(invocation, timeout, ttl, rttl)
        ser = self._get_serializer(serializer)

        async def core() -> Response:
            raw_request = ser.dumps_request(request)

            async def transport_call() -> RawMessage | None:
                return await self._transport.request(request, raw_request, kwds=kwds)

            wire_handler = transport_call
            for mw in reversed(self._wire_mw):
                wire_handler = self._wrap_wire_client(mw, wire_handler, raw_request, request, kwds)

            raw_response = await wire_handler()
            if raw_response is None:
                return cast(Response, None)

            return ser.loads_response(raw_response)

        handler = core
        for mw in reversed(self._app_mw):
            handler = self._wrap_app_client(mw, handler, request, kwds)

        response = await handler()

        if response is None or response.data is None:
            return None

        if isinstance(response.data, dict) and "error_code" in response.data:
            info = ErrorInfo.model_validate(response.data)
            raise RpcError(
                error_code=info.error_code,
                error_message=info.error_message,
                error_details=info.error_details,
            )
        return response.data

    async def publish(
        self,
        invocation: Invocation,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        **kwds: Any,
    ) -> None:
        """
        Publish a request or event (fire-and-forget).

        The message is sent but no response is expected. Useful for
        worker tasks or one-way events (pub-sub). The middleware pipeline is
        processed up to transport publish; the response path is skipped.

        Args:
            invocation: The prepared Invocation to publish.
            timeout: Server-side execution timeout (stored in request headers).
                If not set, falls back to the app-level default.
            ttl: Message time-to-live. Falls back to *timeout* if not set.
            rttl: Response TTL (stored in request headers).
            serializer: Serializer name or None for default.
            **kwds: Additional call context.

        Raises:
            RuntimeError: If the app is closed.
        """
        if self._closed:
            raise RuntimeError("App is closed")

        request = self._prepare_request(invocation, timeout, ttl, rttl)
        ser = self._get_serializer(serializer)

        async def core() -> None:
            raw_request = ser.dumps_request(request)

            async def transport_publish() -> RawMessage | None:
                await self._transport.publish(request, raw_request, kwds=kwds)
                return None

            wire_handler = transport_publish
            for mw in reversed(self._wire_mw):
                wire_handler = self._wrap_wire_client(mw, wire_handler, raw_request, request, kwds)

            await wire_handler()

        handler = core
        for mw in reversed(self._app_mw):
            handler = self._wrap_app_client(mw, handler, request, kwds)

        await handler()

    @staticmethod
    def _wrap_app_server(
        mw: AppMiddleware,
        handler: Callable[[], Coroutine[Any, Any, Response | None]],
        request: Request,
    ) -> Callable[[], Coroutine[Any, Any, Response | None]]:
        async def wrapper() -> Response | None:
            return await mw.dispatch_server(handler, request)

        return wrapper

    @staticmethod
    def _wrap_wire_server(
        mw: WireMiddleware,
        handler: Callable[[], Coroutine[Any, Any, tuple[RawMessage, Response] | tuple[None, None]]],
        raw_request: RawMessage,
    ) -> Callable[[], Coroutine[Any, Any, tuple[RawMessage, Response] | tuple[None, None]]]:
        async def wrapper() -> tuple[RawMessage, Response] | tuple[None, None]:
            return await mw.dispatch_server(handler, raw_request)

        return wrapper

    def _build_error_response(
        self,
        exc: Exception,
        raw_request: RawMessage,
        serializer: Serializer,
    ) -> RawMessage:
        if isinstance(exc, RpcError):
            error_code = exc.error_code
            error_message = exc.error_message
            details = exc.error_details
        else:
            logger.exception(exc)
            error_code = ErrorCode.INTERNAL_ERROR
            error_message = "Internal server error"
            details = {"exc_type": type(exc).__name__}

        try:
            request = serializer.loads_request(raw_request)
            req_id = request.id
        except Exception:
            req_id = UUID(int=0)

        error_resp = Response(
            id=req_id,
            type="error",
            data=ErrorInfo(error_code=error_code, error_message=error_message, error_details=details),
        )
        return serializer.dumps_response(error_resp)

    async def _request_handler(
        self,
        raw_request: RawMessage,
    ) -> RawMessage | None:
        ser = self._serializers[None]
        try:
            ser = self._detect_serializer(raw_request)

            async def core() -> tuple[RawMessage, Response] | tuple[None, None]:
                request = ser.loads_request(raw_request)

                async def dispatch_core() -> Response | None:
                    return await self._dispatch(request)

                app_handler: Callable[[], Coroutine[Any, Any, Response | None]] = dispatch_core
                for amw in reversed(self._app_mw):
                    app_handler = self._wrap_app_server(amw, app_handler, request)

                response = await app_handler()

                if response is None:
                    return None, None

                rttl = request.headers.get("rttl")
                if rttl is not None:
                    response.headers["rttl"] = rttl

                raw_msg = ser.dumps_response(response)
                return raw_msg, response

            wire_handler: Callable[
                [],
                Coroutine[Any, Any, tuple[RawMessage, Response] | tuple[None, None]],
            ] = core
            for mw in reversed(self._wire_mw):
                wire_handler = self._wrap_wire_server(mw, wire_handler, raw_request)

            result = await wire_handler()
            if result is None or result[0] is None:
                return None
            raw_response, _ = result
            return raw_response
        except Exception as exc:
            return self._build_error_response(exc, raw_request, ser)

    def _detect_serializer(self, raw_request: RawMessage) -> Serializer:
        ser_name = raw_request.headers.get("serializer")
        if ser_name is not None:
            return self._get_serializer(ser_name)

        for candidate in self._serializers.values():
            if candidate is None:
                continue
            try:
                candidate.loads_request(raw_request)
                return candidate
            except Exception:
                continue

        raise RpcError(
            error_code=ErrorCode.VALIDATION_ERROR,
            error_message="Cannot detect serializer for incoming request",
        )

    def _validate_args(
        self,
        info: HandlerInfo,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        request: Request,
    ) -> Response | None:
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
        for i, arg in enumerate(args):
            if i < len(params):
                arg_map[params[i].name] = arg
        arg_map.update(kwds)

        errors: list[str] = []
        for pname, value in arg_map.items():
            hint = hints.get(pname)
            if hint is None:
                continue
            if pname == "return":
                continue
            if get_origin(hint) is Depends:
                continue
            if hint is Any:
                continue

            try:
                TypeAdapter(hint).validate_python(value)
            except ValidationError as e:
                errors.append(f"{pname}: {e.errors(include_input=False, include_url=False)}")

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

    async def _dispatch(self, request: Request) -> Response | None:
        method = request.method
        info = self._handlers.get(method)
        if info is not None:
            error_response = self._validate_args(info, request.args, request.kwds, request)
            if error_response is not None:
                return error_response

            token_req = _current_request.set(request)
            token_inst = _request_instances.set({})
            try:
                try:
                    deps = self._resolve_deps(info)
                    kwds = {**deps, **request.kwds}
                    result = info.fn(*request.args, **kwds)
                    if asyncio.iscoroutine(result):
                        exec_timeout = request.headers.get("timeout")
                        if exec_timeout is not None:
                            result = await asyncio.wait_for(result, timeout=exec_timeout)
                        else:
                            result = await result
                    return Response(id=request.id, type="result", data=result)
                except TimeoutError:
                    return Response(
                        id=request.id,
                        type="error",
                        data=ErrorInfo(
                            error_code=ErrorCode.TIMEOUT,
                            error_message="Handler execution timed out",
                        ),
                    )
                except Exception as exc:
                    rpc_err = None
                    for cls in type(exc).__mro__:
                        if cls in self._exception_handlers:
                            try:
                                res = self._exception_handlers[cls](exc)
                                if isinstance(res, RpcError):
                                    rpc_err = res
                                elif isinstance(res, tuple):
                                    code, msg, *details = res
                                    rpc_err = RpcError(
                                        error_code=code,
                                        error_message=msg,
                                        error_details=details[0] if details else None,
                                    )
                                elif isinstance(res, dict):
                                    rpc_err = RpcError(
                                        error_code=res.get("error_code", ErrorCode.INTERNAL_ERROR),
                                        error_message=res.get("error_message", str(exc)),
                                        error_details=res.get("error_details"),
                                    )
                                elif isinstance(res, str):
                                    rpc_err = RpcError(error_code=ErrorCode.INTERNAL_ERROR, error_message=res)
                            except RpcError as re:
                                rpc_err = re
                            except Exception as inner_exc:
                                logger.exception(inner_exc)
                            break

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

                    if isinstance(exc, RpcError):
                        return Response(
                            id=request.id,
                            type="error",
                            data=ErrorInfo(
                                error_code=exc.error_code,
                                error_message=exc.error_message,
                                error_details=exc.error_details,
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
            finally:
                _current_request.reset(token_req)
                _request_instances.reset(token_inst)

        subscribers = self._find_subscribers(method)
        if subscribers:
            for sub in subscribers:
                asyncio.create_task(self._exec_subscriber(sub, request))
            return None

        return Response(
            id=request.id,
            type="error",
            data=ErrorInfo(
                error_code=ErrorCode.METHOD_NOT_FOUND,
                error_message=f"No handler registered for method: {method}",
            ),
        )

    async def _exec_subscriber(self, info: HandlerInfo, request: Request) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._subscriber_tasks.add(task)
        try:
            result = info.fn(*request.args, **request.kwds)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.exception("Subscriber handler failed for %s", info.name)
        finally:
            if task is not None:
                self._subscriber_tasks.discard(task)

    async def __aenter__(self) -> App:
        """Enter async context: calls :meth:`init` and returns self."""
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context: calls :meth:`close`."""
        await self.close()
