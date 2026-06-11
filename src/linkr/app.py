from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from functools import wraps
from typing import Any, get_args, get_origin, get_type_hints
from uuid import uuid4

from pydantic import TypeAdapter as _TypeAdapter
from pydantic import ValidationError as _PydanticValidationError

from linkr.di import Depends, DiContainer
from linkr.exceptions import RpcError
from linkr.middleware.base import AppMiddleware, WireMiddleware
from linkr.models import HandlerInfo, RpcRequest, RpcResponse
from linkr.serializer import JsonSerializer, Serializer
from linkr.transports import Transport


class RpcCall:
    """
    Builder for executing a prepared RPC request.

    Returned by :meth:`RpcApp.make` to allow deferred or repeated execution
    of the same request with optional per-call overrides for timeout, TTL,
    and rTTL.
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
        **kwds: Any,
    ) -> Any:
        """
        Execute the RPC call.

        Shorthand for ``await self.app.call(self.request, ...)``.

        Args:
            timeout: Maximum seconds to wait for a response.
            ttl: Message time-to-live in seconds (broker discards expired).
            rttl: Response TTL in seconds.
            **kwds: Forwarded to :meth:`RpcApp.call`.

        Returns:
            The handler's return value.

        Raises:
            RpcError: If the server returned an error response.
            RuntimeError: If the app is closed.
        """
        return await self._app.call(self._request, timeout=timeout, ttl=ttl, rttl=rttl, **kwds)

    async def call(
        self,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        **kwds: Any,
    ) -> Any:
        """
        Execute the RPC call.

        Shorthand for ``await self.app.call(self.request, ...)``.
        Identical to :meth:`__call__`.

        Args:
            timeout: Maximum seconds to wait for a response.
            ttl: Message time-to-live in seconds (broker discards expired).
            rttl: Response TTL in seconds.
            **kwds: Forwarded to :meth:`RpcApp.call`.

        Returns:
            The handler's return value.

        Raises:
            RpcError: If the server returned an error response.
            RuntimeError: If the app is closed.
        """
        return await self._app.call(self._request, timeout=timeout, ttl=ttl, rttl=rttl, **kwds)


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
        timeout: Default timeout in seconds for all calls.
        ttl: Default message TTL in seconds.
        rttl: Default response TTL in seconds.
        serializer: Serializer for request/response encoding.
            Defaults to :class:`JsonSerializer`.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: Serializer | None = None,
    ) -> None:
        self._transport = transport
        self._timeout = timeout
        self._ttl = ttl
        self._rttl = rttl
        self._closed = False
        self._handlers: dict[str, HandlerInfo] = {}
        self._app_mw: list[AppMiddleware] = []
        self._wire_mw: list[WireMiddleware] = []
        self._serializer = serializer or JsonSerializer()
        self.dependencies = DiContainer()

    def add_middleware(self, mw: AppMiddleware | WireMiddleware) -> None:
        """
        Register a middleware.

        App-level middleware is applied in registration order
        around request/response processing.
        Wire-level middleware is applied in send/receive order around the
        transport.

        Args:
            mw: Middleware instance. AppMiddleware is added to the app chain
                (deserialized objects); WireMiddleware is added to the wire
                chain (raw bytes).
        """
        if isinstance(mw, WireMiddleware):
            self._wire_mw.append(mw)
        else:
            self._app_mw.append(mw)

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

    def _routing_key(self, method: str) -> str:
        if "/" not in method:
            return "rpc.server"
        group = method.rsplit("/", 1)[0]
        return f"rpc.server.{group}"

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
            headers={"routing_key": self._routing_key(method)},
            data={"method": method, "args": args, "kwds": kwds},
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

    async def close(self) -> None:
        """
        Shut down the application.

        Order of shutdown:

        1. Stop consuming requests.
        2. Close app-level middleware (reverse order).
        3. Close wire-level middleware (reverse order).
        4. Close transport.
        """
        await self.stop_consume()
        for amw in reversed(self._app_mw):
            await amw.close()
        for wmw in reversed(self._wire_mw):
            await wmw.close()
        await self._transport.close()
        self._closed = True

    async def consume(self) -> None:
        """
        Start listening for incoming RPC requests on the transport.

        Registers the internal request handler on the main server queue and
        on any group-specific queues derived from method names containing
        ``/`` (e.g. ``"api/user/get"`` creates a queue for group ``"api"``).

        Raises:
            RuntimeError: If the app has already been closed.
        """
        if self._closed:
            raise RuntimeError("RpcApp is closed")
        await self._transport.consume(self._request_handler)

        groups = set()
        for full_name in self._handlers:
            if "/" in full_name:
                group = full_name.rsplit("/", 1)[0]
                groups.add(group)
        for group in groups:
            await self._transport.consume(self._request_handler, queue=group)

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
        **kwds: Any,
    ) -> Any:
        """
        Send an RPC request and await the response.

        Runs the full middleware pipeline: app-level request middleware,
        serialization, wire-level send middleware, transport request,
        wire-level receive middleware, deserialization, app-level response
        middleware.

        Args:
            request: The prepared RPC request to send.
            timeout: Maximum seconds to wait for a response.
                Falls back to the app-level default if not set.
            ttl: Message time-to-live in seconds (broker discards expired).
                Falls back to the app-level default; if neither is set
                but *timeout* is given, TTL is set to the same value.
            rttl: Response TTL in seconds.
            **kwds: Forwarded to the transport layer.

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

        for amw in self._app_mw:
            request = await amw.process_request(request, **kwds)

        body, wire = self._serializer.dumps_request(request)

        for wmw in self._wire_mw:
            body, wire = await wmw.send(body, wire, request, **kwds)

        response_bytes, response_wire = await self._transport.request(
            body,
            original=request,
            wire_headers=wire or None,
            **kwds,
        )

        body = response_bytes
        wire = response_wire or {}

        for wmw in self._wire_mw:
            body, wire = await wmw.receive(body, wire, request, **kwds)

        response = self._serializer.loads_response(body, wire)

        for amw in self._app_mw:
            response = await amw.process_response(request, response, **kwds)

        if response.data and "error_code" in response.data:
            raise RpcError(
                error_code=response.data["error_code"],
                error_message=response.data.get("error_message", ""),
                error_details=response.data.get("error_details"),
            )
        if response.data is None:
            return None
        return response.data.get("result")

    async def publish(
        self,
        request: RpcRequest,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        **kwds: Any,
    ) -> None:
        """
        Publish an RPC request (fire-and-forget).

        The message is sent but no response is expected. Useful for
        notifications or one-way events. The middleware pipeline is
        processed up to transport send; the response path is skipped.

        Args:
            request: The prepared RPC request to publish.
            timeout: Default call timeout (stored in request headers).
            ttl: Message time-to-live. Falls back to *timeout* if not set.
            rttl: Response TTL (stored in request headers).
            **kwds: Forwarded to the transport layer.

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

        for amw in self._app_mw:
            request = await amw.process_request(request, **kwds)

        body, wire = self._serializer.dumps_request(request)

        for wmw in self._wire_mw:
            body, wire = await wmw.send(body, wire, request, **kwds)

        await self._transport.publish(
            body, original=request, wire_headers=wire or None, **kwds,
        )

    async def _request_handler(
        self,
        data: bytes,
        original: RpcRequest,
        wire_headers: dict[str, str] | None = None,
    ) -> tuple[bytes, RpcResponse | None, dict[str, str]] | None:
        body = data
        headers = wire_headers or {}

        for wmw in self._wire_mw:
            body, headers = await wmw.receive(body, headers, original)

        request = self._serializer.loads_request(body, headers)

        for amw in self._app_mw:
            request = await amw.process_request(request)

        response = await self._dispatch(request)

        if response is None:
            return None

        rttl = request.headers.get("rttl")
        if rttl is not None:
            response.headers["rttl"] = rttl

        for amw in self._app_mw:
            response = await amw.process_response(request, response)

        body, wire = self._serializer.dumps_response(response)

        for wmw in self._wire_mw:
            body, wire = await wmw.send(body, wire, request, response)

        return (body, response, wire)

    def _validate_args(
        self, info: HandlerInfo, args: tuple[Any, ...], kwds: dict[str, Any], request: RpcRequest
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
                _TypeAdapter(hint).validate_python(value)
            except _PydanticValidationError as e:
                errors.append(f"{pname}: {e.errors(include_input=False, include_url=False)}")

        if errors:
            return RpcResponse(
                id=request.id,
                data={
                    "error_code": "ValidationError",
                    "error_message": "; ".join(errors),
                },
            )

        return None

    async def _dispatch(self, request: RpcRequest) -> RpcResponse:
        data = request.data
        if not isinstance(data, dict) or "method" not in data:
            return RpcResponse(
                id=request.id,
                data={
                    "error_code": "BadRequest",
                    "error_message": "Missing 'method' in request data",
                },
            )

        method = data["method"]
        info = self._handlers.get(method)
        if info is None:
            return RpcResponse(
                id=request.id,
                data={
                    "error_code": "MethodNotFound",
                    "error_message": f"No handler registered for method: {method}",
                },
            )

        error_response = self._validate_args(info, data.get("args", ()), data.get("kwds", {}), request)
        if error_response is not None:
            return error_response

        try:
            deps = self._resolve_deps(info)
            kwds = {**deps, **data.get("kwds", {})}
            result = info.fn(*data.get("args", []), **kwds)
            if asyncio.iscoroutine(result):
                exec_timeout = request.headers.get("timeout")
                if exec_timeout is not None:
                    result = await asyncio.wait_for(result, timeout=exec_timeout)
                else:
                    result = await result
            return RpcResponse(id=request.id, data={"result": result})
        except TimeoutError:
            return RpcResponse(
                id=request.id,
                data={
                    "error_code": "Timeout",
                    "error_message": "Handler execution timed out",
                },
            )
        except Exception as exc:
            return RpcResponse(
                id=request.id,
                data={
                    "error_code": "InternalError",
                    "error_message": str(exc),
                    "error_details": {"exc_type": type(exc).__name__},
                },
            )

    async def __aenter__(self) -> RpcApp:
        """Enter async context: calls :meth:`init` and returns self."""
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit async context: calls :meth:`close`."""
        await self.close()
