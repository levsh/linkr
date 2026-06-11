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
from linkr.models import HandlerInfo, RpcContext, RpcRequest, RpcResponse
from linkr.serializer import JsonSerializer, Serializer
from linkr.transports import Transport


class RpcCall:
    """
    Builder for executing a prepared RPC request.

    Returned by RpcApp.make(). Wraps a request and its origin app,
    providing call() / __call__() convenience methods.
    """

    def __init__(self, app: RpcApp, request: RpcRequest) -> None:
        self._app = app
        self._request = request

    @property
    def app(self) -> RpcApp:
        return self._app

    @property
    def request(self) -> RpcRequest:
        return self._request

    async def __call__(
        self,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        **kwds: Any,
    ) -> Any:
        return await self._app.call(self._request, timeout=timeout, ttl=ttl, rttl=rttl, **kwds)

    async def call(
        self,
        *,
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        **kwds: Any,
    ) -> Any:
        return await self._app.call(self._request, timeout=timeout, ttl=ttl, rttl=rttl, **kwds)


class RpcApp:
    """
    Main RPC application: register handlers, send requests, manage middleware.

    Orchestrates serialization, encoding (compression/encryption), object-level
    middleware and transport communication.
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
        if isinstance(mw, WireMiddleware):
            self._wire_mw.append(mw)
        else:
            self._app_mw.append(mw)

    def method(
        self,
        name: str,
        **options: Any,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that registers an RPC handler."""

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
        return self._handlers.get(name)

    @property
    def methods(self) -> dict[str, HandlerInfo]:
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
        request = RpcRequest(
            id=uuid4(),
            headers={"routing_key": self._routing_key(method)},
            data={"method": method, "args": args, "kwds": kwds},
        )
        return RpcCall(self, request)

    async def init(self) -> None:
        await self._transport.init()
        for amw in self._app_mw:
            await amw.init(self)
        for wmw in self._wire_mw:
            await wmw.init(self)

    async def close(self) -> None:
        await self.stop_consume()
        for amw in reversed(self._app_mw):
            await amw.close()
        for wmw in reversed(self._wire_mw):
            await wmw.close()
        await self._transport.close()
        self._closed = True

    async def consume(self) -> None:
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

        ctx = RpcContext(app=self, direction="request", role="client", request=request)
        ctx = await self._run_app_mw(ctx)

        body, wire = self._serializer.dumps_request(request)
        ctx.body = body
        ctx.wire_headers.update(wire)

        ctx.state["call_kwds"] = kwds
        ctx = await self._run_wire_mw(ctx)

        response_bytes, response_wire = await self._transport.request(
            ctx.body,
            original=request,
            wire_headers=dict(ctx.wire_headers) or None,
        )

        ctx.direction = "response"
        ctx.body = response_bytes
        ctx.wire_headers = response_wire or {}
        ctx = await self._run_wire_mw(ctx)

        response = self._serializer.loads_response(ctx.body, dict(ctx.wire_headers))

        ctx.direction = "response"
        ctx.response = response
        ctx = await self._run_app_mw(ctx)

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

        ctx = RpcContext(app=self, direction="request", role="client", request=request)
        ctx = await self._run_app_mw(ctx)

        body, wire = self._serializer.dumps_request(request)
        ctx.body = body
        ctx.wire_headers.update(wire)

        ctx.state["call_kwds"] = kwds
        ctx = await self._run_wire_mw(ctx)

        await self._transport.publish(ctx.body, original=request, wire_headers=dict(ctx.wire_headers) or None)

    async def _request_handler(
        self,
        data: bytes,
        original: RpcRequest,
        wire_headers: dict[str, str] | None = None,
    ) -> tuple[bytes, RpcResponse | None, dict[str, str]] | None:
        ctx = RpcContext(
            app=self,
            direction="request",
            role="server",
            request=original,
            body=data,
            wire_headers=wire_headers or {},
        )
        ctx = await self._run_wire_mw(ctx)

        request = self._serializer.loads_request(ctx.body, dict(ctx.wire_headers))
        ctx.request = request

        ctx = await self._run_app_mw(ctx)

        if ctx.response is None:
            ctx.response = await self._dispatch(request)

        rttl = request.headers.get("rttl")
        if rttl is not None and ctx.response is not None:
            ctx.response.headers["rttl"] = rttl

        if ctx.response is None:
            return None

        ctx.direction = "response"
        ctx = await self._run_app_mw(ctx)

        ctx.wire_headers = {}
        body, wire = self._serializer.dumps_response(ctx.response)  # type: ignore[arg-type]
        ctx.body = body
        ctx.wire_headers.update(wire)

        ctx = await self._run_wire_mw(ctx)

        return (ctx.body, ctx.response, dict(ctx.wire_headers))

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

    async def _run_app_mw(self, ctx: RpcContext) -> RpcContext:

        async def runner(idx: int, c: RpcContext) -> RpcContext:
            if idx >= len(self._app_mw):
                return c
            mw = self._app_mw[idx]

            async def call_next(inner_ctx: RpcContext) -> RpcContext:
                return await runner(idx + 1, inner_ctx)

            return await mw.dispatch(c, call_next)

        return await runner(0, ctx)

    async def _run_wire_mw(self, ctx: RpcContext) -> RpcContext:

        async def runner(idx: int, c: RpcContext) -> RpcContext:
            if idx >= len(self._wire_mw):
                return c
            mw = self._wire_mw[idx]

            async def call_next(inner_ctx: RpcContext) -> RpcContext:
                return await runner(idx + 1, inner_ctx)

            return await mw.dispatch(c, call_next)

        return await runner(0, ctx)

    async def __aenter__(self) -> RpcApp:
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
