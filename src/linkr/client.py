from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

from .exceptions import ErrorCode, RequestError, RPCError
from .hooks import RequestPublishedEvent, ResponseReceivedEvent
from .middleware import apply_middleware, wrap_app_client, wrap_wire_client
from .models import ErrorInfo, Message, Request, Response
from .serializer import Serializer

if TYPE_CHECKING:
    from .app import App


def _norm_timeout(value: float | None) -> float | None:
    if value is None:
        return None
    return max(float(value), 0.0)


class Client:
    """Client-side API for :class:`App` (call/send/emit and request pipeline)."""

    def __init__(
        self,
        app: App,
        rpc_client_timeout: float | None,
        rpc_server_timeout: float | None,
        rpc_ttl: float | None,
        rpc_rttl: float | None,
        task_client_timeout: float | None,
        task_server_timeout: float | None,
        task_ttl: float | None,
        event_client_timeout: float | None,
        event_ttl: float | None,
    ) -> None:
        self._app = app
        self._serializer = app.serializers[0]
        self._serializers_map = {s.name: s for s in app.serializers}
        self._app_mw = app.app_middlewares
        self._wire_mw = app.wire_middlewares
        self._rpc_client_timeout = _norm_timeout(rpc_client_timeout)
        self._rpc_server_timeout = _norm_timeout(rpc_server_timeout)
        self._rpc_ttl = rpc_ttl
        self._rpc_rttl = rpc_rttl
        self._task_client_timeout = _norm_timeout(task_client_timeout)
        self._task_server_timeout = _norm_timeout(task_server_timeout)
        self._task_ttl = task_ttl
        self._event_client_timeout = _norm_timeout(event_client_timeout)
        self._event_ttl = event_ttl

    # ------------------------------------------------------------------
    # Client routing helpers
    # ------------------------------------------------------------------

    def _resolve_queue(self, name: str) -> str:
        if "." in name:
            return name.rsplit(".", 1)[0]
        return self._app.name

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
        client_timeout: float | None = None,
        server_timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Any:
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        effective_client_timeout = _norm_timeout(
            client_timeout if client_timeout is not None else self._rpc_client_timeout
        )
        request = self._build_rpc_request(
            method,
            args,
            kwds,
            request_id=request_id,
            server_timeout=server_timeout,
            ttl=ttl,
            rttl=rttl,
        )
        return await self._invoke_rpc_request(
            request,
            client_timeout=effective_client_timeout,
            serializer=serializer,
            context=context,
        )

    async def send(
        self,
        task: str,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        *,
        request_id: UUID | None = None,
        client_timeout: float | None = None,
        server_timeout: float | None = None,
        ttl: float | None = None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        effective_client_timeout = _norm_timeout(
            client_timeout if client_timeout is not None else self._task_client_timeout
        )
        request = self._build_task_request(
            task,
            args,
            kwds,
            request_id=request_id,
            server_timeout=server_timeout,
            ttl=ttl,
        )
        return await self._invoke_task_request(
            request,
            client_timeout=effective_client_timeout,
            serializer=serializer,
            context=context,
        )

    async def emit(
        self,
        topic: str,
        event: dict[str, Any],
        *,
        request_id: UUID | None = None,
        client_timeout: float | None = None,
        ttl: float | None = None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if self._app.is_closed:
            raise RuntimeError("App is closed")
        effective_client_timeout = _norm_timeout(
            client_timeout if client_timeout is not None else self._event_client_timeout
        )
        request = self._build_event_request(
            topic,
            event,
            request_id=request_id,
            ttl=ttl,
        )
        return await self._invoke_event_request(
            request,
            client_timeout=effective_client_timeout,
            serializer=serializer,
            context=context,
        )

    # ------------------------------------------------------------------
    # Client request builders
    # ------------------------------------------------------------------

    def _build_rpc_request(
        self,
        method: str,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        *,
        request_id: UUID | None = None,
        server_timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
    ) -> Request:
        queue = self._resolve_queue(method)
        headers: dict[str, Any] = {"queue": queue}
        server_timeout = _norm_timeout(server_timeout if server_timeout is not None else self._rpc_server_timeout)
        ttl = ttl if ttl is not None else self._rpc_ttl
        rttl = rttl if rttl is not None else self._rpc_rttl
        if server_timeout is not None:
            headers["server_timeout"] = server_timeout
        if ttl is not None:
            headers["ttl"] = ttl
        if rttl is not None:
            headers["rttl"] = rttl
        method_leaf = method.rsplit(".", 1)[-1] if "." in method else method
        return Request(
            id=request_id or uuid4(),
            type="rpc",
            data={"method": method_leaf, "args": list(args), "kwds": kwds},
            headers=headers,
        )

    def _build_task_request(
        self,
        task: str,
        args: tuple[Any, ...],
        kwds: dict[str, Any],
        *,
        request_id: UUID | None = None,
        server_timeout: float | None = None,
        ttl: float | None = None,
    ) -> Request:
        queue = self._resolve_queue(task)
        headers: dict[str, Any] = {"queue": queue}
        server_timeout = _norm_timeout(server_timeout if server_timeout is not None else self._task_server_timeout)
        ttl = ttl if ttl is not None else self._task_ttl
        if server_timeout is not None:
            headers["server_timeout"] = server_timeout
        if ttl is not None:
            headers["ttl"] = ttl
        task_leaf = task.rsplit(".", 1)[-1] if "." in task else task
        return Request(
            id=request_id or uuid4(),
            type="task",
            data={"task": task_leaf, "args": list(args), "kwds": kwds},
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
        headers: dict[str, Any] = {}
        ttl = ttl if ttl is not None else self._event_ttl
        if ttl is not None:
            headers["ttl"] = ttl
        return Request(
            id=request_id or uuid4(),
            type="event",
            data={"topic": topic, "event": event},
            headers=headers,
        )

    # ------------------------------------------------------------------
    # Client dispatch
    # ------------------------------------------------------------------

    async def _invoke_rpc_request(
        self,
        request: Request,
        *,
        client_timeout: float | None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Any:
        if context is None:
            context = {}

        async def emit_published(outcome: Literal["ok", "error"], error: BaseException | None = None) -> None:
            if self._app.hooks.on_request_published:
                await self._app.hooks.on_request_published.emit(
                    RequestPublishedEvent(
                        app=self._app.name,
                        request=request,
                        outcome=outcome,
                        error=error,
                    )
                )

        async def emit_response_received(response: Response) -> None:
            if not self._app.hooks.on_response_received:
                return
            if response.type == "error":
                outcome: Literal["ok", "error", "timeout"] = "error"
                if isinstance(response.data, ErrorInfo):
                    outcome = "timeout" if response.data.error_code == ErrorCode.TIMEOUT else "error"
                elif isinstance(response.data, dict):
                    outcome = "timeout" if response.data.get("error_code") == ErrorCode.TIMEOUT else "error"
            else:
                outcome = "ok"
            await self._app.hooks.on_response_received.emit(
                ResponseReceivedEvent(
                    app=self._app.name,
                    request=request,
                    response=response,
                    outcome=outcome,
                )
            )

        def make_timeout_response() -> Response:
            return Response(
                id=request.id,
                type="error",
                data=ErrorInfo(error_code=ErrorCode.TIMEOUT, error_message="Client wait timed out"),
            )

        def make_missing_response() -> Response:
            return Response(
                id=request.id,
                type="error",
                data=ErrorInfo(error_code=ErrorCode.INTERNAL_ERROR, error_message="middleware returned no response"),
            )

        async def serialize_and_backend() -> Response | None:
            ser = self._get_serializer(serializer)
            request_message = ser.dumps_request(request)
            request_message.headers["serializer"] = ser.name
            ttl = request.headers.get("ttl")
            if ttl is not None:
                request_message.headers["ttl"] = ttl

            async def backend_call_and_wait() -> Message:
                try:
                    future = await self._app.backend.request(
                        request.headers["queue"],
                        request_message,
                        context=context,
                    )
                except Exception as exc:
                    await emit_published("error", exc)
                    raise
                await emit_published("ok")

                try:
                    if client_timeout is None:
                        response_message = await future
                    else:
                        response_message = await asyncio.wait_for(future, client_timeout)
                except TimeoutError as exc:
                    await emit_response_received(make_timeout_response())
                    raise RPCError(
                        error_code=ErrorCode.TIMEOUT,
                        error_message="Client wait timed out",
                    ) from exc
                return response_message

            if not self._wire_mw:
                response_message: Message | None = await backend_call_and_wait()
            else:
                wire_handler = apply_middleware(
                    self._wire_mw,
                    backend_call_and_wait,
                    lambda mw, n: wrap_wire_client(mw, n, request, request_message, context),
                )
                response_message = await wire_handler()

            if response_message is None:
                await emit_response_received(make_missing_response())
                raise RPCError(error_code=ErrorCode.INTERNAL_ERROR, error_message="middleware returned no response")
            resp_ser_name = response_message.headers.get("serializer")
            resp_ser = self._get_serializer(resp_ser_name) if resp_ser_name else ser
            return resp_ser.loads_response(response_message)

        if not self._app_mw:
            resp = await serialize_and_backend()
        else:
            app_handler = apply_middleware(
                self._app_mw,
                serialize_and_backend,
                lambda mw, n: wrap_app_client(mw, n, request, context),
            )
            resp = await app_handler()

        if resp is None:
            await emit_response_received(make_missing_response())
            raise RPCError(error_code=ErrorCode.INTERNAL_ERROR, error_message="middleware returned no response")

        await emit_response_received(resp)

        return self._unpack_rpc_response(resp)

    async def _invoke_task_request(
        self,
        request: Request,
        *,
        client_timeout: float | None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if context is None:
            context = {}

        async def emit_published(outcome: Literal["ok", "error"], error: BaseException | None = None) -> None:
            if self._app.hooks.on_request_published:
                await self._app.hooks.on_request_published.emit(
                    RequestPublishedEvent(
                        app=self._app.name,
                        request=request,
                        outcome=outcome,
                        error=error,
                    )
                )

        async def serialize_and_backend() -> Response | None:
            ser = self._get_serializer(serializer)
            request_message = ser.dumps_request(request)
            request_message.headers["serializer"] = ser.name
            ttl = request.headers.get("ttl")
            if ttl is not None:
                request_message.headers["ttl"] = ttl

            async def backend_call() -> None:
                try:
                    if client_timeout is None:
                        await self._app.backend.send(
                            request.headers["queue"],
                            request_message,
                            context=context,
                        )
                    else:
                        await asyncio.wait_for(
                            self._app.backend.send(
                                request.headers["queue"],
                                request_message,
                                context=context,
                            ),
                            client_timeout,
                        )
                except TimeoutError as exc:
                    timeout_exc = RequestError(error_message="Task publish timed out")
                    await emit_published("error", timeout_exc)
                    raise timeout_exc from exc
                except Exception as exc:
                    await emit_published("error", exc)
                    raise
                await emit_published("ok")

            if not self._wire_mw:
                await backend_call()
                return None

            wire_handler = apply_middleware(
                self._wire_mw,
                backend_call,
                lambda mw, n: wrap_wire_client(mw, n, request, request_message, context),
            )
            await wire_handler()
            return None

        if not self._app_mw:
            await serialize_and_backend()
            return

        app_handler = apply_middleware(
            self._app_mw,
            serialize_and_backend,
            lambda mw, n: wrap_app_client(mw, n, request, context),
        )
        await app_handler()

    async def _invoke_event_request(
        self,
        request: Request,
        *,
        client_timeout: float | None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if context is None:
            context = {}

        async def emit_published(outcome: Literal["ok", "error"], error: BaseException | None = None) -> None:
            if self._app.hooks.on_request_published:
                await self._app.hooks.on_request_published.emit(
                    RequestPublishedEvent(
                        app=self._app.name,
                        request=request,
                        outcome=outcome,
                        error=error,
                    )
                )

        async def serialize_and_backend() -> Response | None:
            ser = self._get_serializer(serializer)
            request_message = ser.dumps_request(request)
            request_message.headers["serializer"] = ser.name
            ttl = request.headers.get("ttl")
            if ttl is not None:
                request_message.headers["ttl"] = ttl

            async def backend_call() -> None:
                try:
                    if client_timeout is None:
                        await self._app.backend.emit(
                            request.data["topic"],
                            request_message,
                            context=context,
                        )
                    else:
                        await asyncio.wait_for(
                            self._app.backend.emit(
                                request.data["topic"],
                                request_message,
                                context=context,
                            ),
                            client_timeout,
                        )
                except TimeoutError as exc:
                    timeout_exc = RequestError(error_message="Event publish timed out")
                    await emit_published("error", timeout_exc)
                    raise timeout_exc from exc
                except Exception as exc:
                    await emit_published("error", exc)
                    raise
                await emit_published("ok")

            if not self._wire_mw:
                await backend_call()
                return None

            wire_handler = apply_middleware(
                self._wire_mw,
                backend_call,
                lambda mw, n: wrap_wire_client(mw, n, request, request_message, context),
            )
            await wire_handler()
            return None

        if not self._app_mw:
            await serialize_and_backend()
            return

        app_handler = apply_middleware(
            self._app_mw,
            serialize_and_backend,
            lambda mw, n: wrap_app_client(mw, n, request, context),
        )
        await app_handler()

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

    @staticmethod
    def _unpack_rpc_response(response: Response) -> Any:
        if response.type == "error":
            data = response.data
            if isinstance(data, ErrorInfo):
                raise RPCError(
                    error_code=data.error_code,
                    error_message=data.error_message,
                    error_details=data.error_details,
                )
            if isinstance(data, dict):
                raise RPCError(
                    error_code=data.get("error_code", ErrorCode.INTERNAL_ERROR),
                    error_message=data.get("error_message", "Internal server error"),
                    error_details=data.get("error_details"),
                )
            raise RPCError(error_code=ErrorCode.INTERNAL_ERROR, error_message=str(data))
        return response.data
