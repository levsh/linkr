from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, cast, get_args, get_origin, get_type_hints
from uuid import uuid4

from pydantic import TypeAdapter, ValidationError

from .di import Depends, DiContainer
from .exceptions import ErrorCode, RpcError
from .middleware import AppMiddleware, WireMiddleware
from .models import ErrorInfo, HandlerInfo, RawMessage, RpcRequest, RpcResponse
from .serializer import JsonSerializer, Serializer
from .transports import Transport


class RpcCall:
    """
    Builder for executing a prepared RPC request.

    Returned by :meth:`RpcApp.make` to allow deferred or repeated execution
    of the same request with optional per-call overrides for timeout, TTL,
    rTTL, and serializer.
    """

    def __init__(self, app: RpcApp, request: RpcRequest) -> None:
        self._app = app
        self._request = request

    @property
    def app(self) -> RpcApp:
        """The RpcApp instance this call belongs to."""
        return self._app

    @property
    def request(self) -> RpcRequest:
        """The prepared RpcRequest to send."""
        return self._request

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

        Shorthand for ``await self.app.call(self.request, ...)``.

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
        return await self._app.call(self._request, timeout=timeout, ttl=ttl, rttl=rttl, serializer=serializer, **kwds)

    async def call(
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

        Shorthand for ``await self.app.call(self.request, ...)``.
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
        return await self._app.call(self._request, timeout=timeout, ttl=ttl, rttl=rttl, serializer=serializer, **kwds)


class RpcApp:
    """
    Main RPC application: register handlers, send requests, manage middleware.

    Typical usage::

        transport = MockTransport()
        app = RpcApp(transport)

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        result = await app.make("add", 2, 3).call()
        await app.close()

    Args:
        transport: Backend used for message exchange (e.g. MockTransport, RmqTransport).
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
    ) -> None:
        self._transport = transport
        self._timeout = timeout
        self._ttl = ttl
        self._rttl = rttl
        self._closed = False
        self._handlers: dict[str, HandlerInfo] = {}
        self._app_mw: list[AppMiddleware] = []
        self._wire_mw: list[WireMiddleware] = []
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

    @property
    def methods(self) -> dict[str, HandlerInfo]:
        """All registered handlers keyed by method name."""
        return dict(self._handlers)

    def _queue(self, method: str) -> str | None:
        if "/" in method:
            return method.rsplit("/", 1)[0]
        return None

    def _resolve_deps(self, info: HandlerInfo) -> dict[str, Any]:
        return {name: self.dependencies.resolve(dep_type) for name, dep_type in info.dep_types.items()}

    def make(
        self,
        method: str,
        *args: Any,
        **kwds: Any,
    ) -> RpcCall:
        """
        Create an RpcCall for a method with positional/keyword arguments.

        This is a shorthand for building an :class:`RpcRequest` and wrapping
        it in an :class:`RpcCall`.

        Args:
            method: The registered method name.
            *args: Positional arguments for the handler.
            **kwds: Keyword arguments for the handler.

        Returns:
            An :class:`RpcCall` ready to be executed with ``.call()`` or ``await``.
        """
        request = RpcRequest(
            id=uuid4(),
            method=method,
            args=args,
            kwds=kwds,
            headers={"queue": self._queue(method)},
        )
        return RpcCall(self, request)

    async def init(self) -> None:
        """
        Open transport and initialise all middleware.

        Must be called before :meth:`consume`, :meth:`call`, or :meth:`publish`.
        """
        await self._transport.init()
        for amw in self._app_mw:
            await amw.init()
        for wmw in self._wire_mw:
            await wmw.init()

    async def close(self, timeout: float | None = None) -> None:
        """
        Shut down the application.

        Order of shutdown:

        1. Stop consuming requests.
        2. Close app-level middleware (reverse order).
        3. Close wire-level middleware (reverse order).
        4. Close transport.

        Args:
            timeout: Passed to the transport's ``close()``.  See
                :meth:`Transport.close` for details.
        """
        await self.stop_consume()
        for amw in reversed(self._app_mw):
            await amw.close()
        for wmw in reversed(self._wire_mw):
            await wmw.close()
        await self._transport.close(timeout=timeout)
        self._closed = True

    async def consume(self) -> None:
        """
        Start listening for incoming RPC requests on the transport.

        Registers the internal request handler on the main server queue and
        on any routing-prefix queues derived from method names containing
        ``/`` (e.g. ``"api/user/get"`` creates a queue named
        ``{server_queue_name}.api``).

        Raises:
            RuntimeError: If the app has already been closed.
        """
        if self._closed:
            raise RuntimeError("RpcApp is closed")

        await self._transport.consume(self._request_handler)

        for name in self._handlers:
            queue = self._queue(name)
            if queue:
                await self._transport.consume(self._request_handler, queue=queue)

    async def stop_consume(self) -> None:
        """Stop listening for incoming RPC requests."""
        await self._transport.stop_consume()

    async def call(
        self,
        request: RpcRequest,
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
            request: The prepared RPC request to send.
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
            raise RuntimeError("RpcApp is closed")

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

        ser = self._get_serializer(serializer)

        async def core() -> RpcResponse:
            raw_request = ser.dumps_request(request)

            async def transport_call() -> RawMessage | None:
                return await self._transport.request(request, raw_request, kwds=kwds)

            def wrap_client(
                mw: WireMiddleware,
                handler: Callable[[], Coroutine[Any, Any, RawMessage | None]],
            ) -> Callable[[], Coroutine[Any, Any, RawMessage | None]]:
                async def wrapper() -> RawMessage | None:
                    return await mw.dispatch_client(handler, raw_request, request, kwds=kwds)

                return wrapper

            wire_handler: Callable[[], Coroutine[Any, Any, RawMessage | None]] = transport_call
            for mw in reversed(self._wire_mw):
                wire_handler = wrap_client(mw, wire_handler)

            raw_response = await wire_handler()
            if raw_response is None:
                return cast(RpcResponse, None)

            return ser.loads_response(raw_response)

        def wrap_client(
            mw: AppMiddleware,
            handler: Callable[[], Coroutine[Any, Any, RpcResponse]],
        ) -> Callable[[], Coroutine[Any, Any, RpcResponse]]:

            async def wrapper() -> RpcResponse:
                return cast(RpcResponse, await mw.dispatch_client(handler, request, kwds=kwds))

            return wrapper

        handler = core
        for mw in reversed(self._app_mw):
            handler = wrap_client(mw, handler)

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
        return response.data.get("result")

    async def publish(
        self,
        request: RpcRequest,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        **kwds: Any,
    ) -> None:
        """
        Publish an RPC request (fire-and-forget).

        The message is sent but no response is expected. Useful for
        notifications or one-way events. The middleware pipeline is
        processed up to transport publish; the response path is skipped.

        Args:
            request: The prepared RPC request to publish.
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
            raise RuntimeError("RpcApp is closed")

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

        ser = self._get_serializer(serializer)

        async def core() -> None:
            raw_request = ser.dumps_request(request)

            async def transport_publish() -> RawMessage | None:
                await self._transport.publish(request, raw_request, kwds=kwds)
                return None

            def wrap_client(
                mw: WireMiddleware,
                handler: Callable[[], Coroutine[Any, Any, RawMessage | None]],
            ) -> Callable[[], Coroutine[Any, Any, RawMessage | None]]:
                async def wrapper() -> RawMessage | None:
                    return await mw.dispatch_client(handler, raw_request, request, kwds=kwds)

                return wrapper

            wire_handler: Callable[[], Coroutine[Any, Any, RawMessage | None]] = transport_publish
            for mw in reversed(self._wire_mw):
                wire_handler = wrap_client(mw, wire_handler)

            await wire_handler()

        def wrap_client(
            mw: AppMiddleware,
            handler: Callable[[], Coroutine[Any, Any, None]],
        ) -> Callable[[], Coroutine[Any, Any, None]]:

            async def wrapper() -> None:
                await mw.dispatch_client(handler, request, kwds=kwds)

            return wrapper

        handler = core
        for mw in reversed(self._app_mw):
            handler = wrap_client(mw, handler)

        await handler()

    async def _request_handler(
        self,
        raw_request: RawMessage,
    ) -> RawMessage | None:

        async def core() -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
            request = ser.loads_request(raw_request)

            async def dispatch_core() -> RpcResponse | None:
                return await self._dispatch(request)

            def wrap_server(
                mw: AppMiddleware,
                handler: Callable[[], Coroutine[Any, Any, RpcResponse | None]],
            ) -> Callable[[], Coroutine[Any, Any, RpcResponse | None]]:
                async def wrapper() -> RpcResponse | None:
                    return await mw.dispatch_server(handler, request)

                return wrapper

            handler: Callable[[], Coroutine[Any, Any, RpcResponse | None]] = dispatch_core
            for amw in reversed(self._app_mw):
                handler = wrap_server(amw, handler)

            response = await handler()

            if response is None:
                return None, None

            rttl = request.headers.get("rttl")
            if rttl is not None:
                response.headers["rttl"] = rttl

            raw_msg = ser.dumps_response(response)
            return raw_msg, response

        def wrap_server(
            mw: WireMiddleware,
            handler: Callable[[], Coroutine[Any, Any, tuple[RawMessage, RpcResponse] | tuple[None, None]]],
        ) -> Callable[[], Coroutine[Any, Any, tuple[RawMessage, RpcResponse] | tuple[None, None]]]:
            async def wrapper() -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
                return await mw.dispatch_server(handler, raw_request)

            return wrapper

        ser = self._serializers[None]
        try:
            ser = self._detect_serializer(raw_request)

            wire_handler: Callable[[], Coroutine[Any, Any, tuple[RawMessage, RpcResponse] | tuple[None, None]]] = core
            for mw in reversed(self._wire_mw):
                wire_handler = wrap_server(mw, wire_handler)

            result = await wire_handler()
            if result is None or result[0] is None:
                return None
            raw_response, _ = result
            return raw_response
        except Exception as exc:
            if isinstance(exc, RpcError):
                ec = exc.error_code
                msg = exc.error_message
                details = exc.error_details
                req_id = uuid4()
            else:
                ec = ErrorCode.INTERNAL_ERROR
                msg = "Internal server error"
                details = {"exc_type": type(exc).__name__}
                try:
                    request = ser.loads_request(raw_request)
                    req_id = request.id
                except Exception:
                    req_id = uuid4()

            error_resp = RpcResponse(
                id=req_id,
                type="error",
                data=ErrorInfo(error_code=ec, error_message=msg, error_details=details),
            )
            return ser.dumps_response(error_resp)

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

        return self._serializers[None]

    def _validate_args(
        self,
        info: HandlerInfo,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        request: RpcRequest,
    ) -> RpcResponse | None:
        if not info.options.get("validate_types"):
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
            return RpcResponse(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.VALIDATION_ERROR,
                    error_message="; ".join(errors),
                ),
            )

        return None

    async def _dispatch(self, request: RpcRequest) -> RpcResponse:
        method = request.method
        info = self._handlers.get(method)
        if info is None:
            return RpcResponse(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.METHOD_NOT_FOUND,
                    error_message=f"No handler registered for method: {method}",
                ),
            )

        error_response = self._validate_args(info, request.args, request.kwds, request)
        if error_response is not None:
            return error_response

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
            return RpcResponse(id=request.id, type="result", data={"result": result})
        except TimeoutError:
            return RpcResponse(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.TIMEOUT,
                    error_message="Handler execution timed out",
                ),
            )
        except RpcError as exc:
            return RpcResponse(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=exc.error_code,
                    error_message=exc.error_message,
                    error_details=exc.error_details,
                ),
            )
        except Exception as exc:
            return RpcResponse(
                id=request.id,
                type="error",
                data=ErrorInfo(
                    error_code=ErrorCode.INTERNAL_ERROR,
                    error_message=str(exc),
                    error_details={"exc_type": type(exc).__name__},
                ),
            )

    async def __aenter__(self) -> RpcApp:
        """Enter async context: calls :meth:`init` and returns self."""
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context: calls :meth:`close`."""
        await self.close()
