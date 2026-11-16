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


class Client:
    """Client-side API for :class:`App` (call/send/emit and request pipeline)."""

    def __init__(
        self,
        app: App,
        rpc_timeout: float,
        rpc_ttl: float,
        rpc_rttl: float,
        task_timeout: float,
        task_ttl: float,
        event_ttl: float,
    ) -> None:
        """
        Create a client for an application.

        Args:
            app: Owning application instance.
            rpc_timeout: Default RPC handler timeout.
            rpc_ttl: Default RPC request time-to-live.
            rpc_rttl: Default RPC response time-to-live.
            task_timeout: Default task handler timeout.
            task_ttl: Default task time-to-live.
            event_ttl: Default event time-to-live.
        """
        self._app = app
        self._serializer = app.serializers[0]
        self._serializers_map = {s.name: s for s in app.serializers}
        self._app_mw = app.app_middlewares
        self._wire_mw = app.wire_middlewares
        self._rpc_timeout = rpc_timeout
        self._rpc_ttl = rpc_ttl
        self._rpc_rttl = rpc_rttl
        self._task_timeout = task_timeout
        self._task_ttl = task_ttl
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
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Any:
        """
        Invoke an RPC method and return its result.

        Args:
            method: Fully qualified RPC method name (e.g. ``"app.service.add"``).
            args: Positional arguments passed to the remote handler.
            kwds: Keyword arguments passed to the remote handler.
            request_id: Optional request identifier.
            timeout: Handler execution timeout override in seconds.
            ttl: Request time-to-live override in seconds.
            rttl: Response time-to-live override in seconds.
            serializer: Serializer name to use for this request.
            context: Backend-specific publish/context metadata.

        Returns:
            The value returned by the remote RPC handler.
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
        Send a task for asynchronous execution without waiting for a result.

        Args:
            task: Fully qualified task name (e.g. ``"app.worker.process"``).
            args: Positional arguments passed to the task handler.
            kwds: Keyword arguments passed to the task handler.
            request_id: Optional request identifier.
            timeout: Handler execution timeout override in seconds.
            ttl: Request time-to-live override in seconds.
            serializer: Serializer name to use for this request.
            context: Backend-specific publish/context metadata.
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
        Publish an event to a topic.

        Args:
            topic: Event topic or routing key.
            event: Event payload delivered to subscribers as keyword arguments.
            request_id: Optional request identifier.
            ttl: Event time-to-live override in seconds.
            serializer: Serializer name to use for this request.
            context: Backend-specific publish/context metadata.
        """
        request = self._build_event_request(
            topic,
            event,
            request_id=request_id,
            ttl=ttl,
        )
        return await self._invoke_event_request(request, serializer=serializer, context=context)

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
        timeout: float | None = None,
        ttl: float | None = None,
        rttl: float | None = None,
    ) -> Request:
        queue = self._resolve_queue(method)

        headers: dict[str, Any] = {"queue": queue}

        timeout = timeout if timeout is not None else self._rpc_timeout
        ttl = ttl if ttl is not None else self._rpc_ttl
        rttl = rttl if rttl is not None else self._rpc_rttl

        if timeout is not None:
            headers["timeout"] = timeout
        if ttl is not None:
            headers["ttl"] = ttl
        if rttl is not None:
            headers["rttl"] = rttl

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

        timeout = timeout if timeout is not None else self._task_timeout
        ttl = ttl if ttl is not None else self._task_ttl

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
        headers: dict[str, Any] = {}

        ttl = ttl if ttl is not None else self._event_ttl
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

    # ------------------------------------------------------------------
    # Client pipeline
    # ------------------------------------------------------------------

    async def _invoke_rpc_request(
        self,
        request: Request,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> Any:
        if self._app.is_closed:
            raise RuntimeError("App is closed")

        _serializer = self._get_serializer(serializer)
        _context = context or {}

        client_timeout = (
            max(float(request.headers.get("timeout") or 0) + float(request.headers.get("ttl") or 0), 0) or None
        )

        published = False
        response_outcome: Literal["ok", "error", "timeout"] = "ok"
        response_error: BaseException | None = None
        response: Response | None = None
        result: Any = None

        ttl = request.headers.get("ttl")

        # Fast path: no middleware
        if not self._wire_mw and not self._app_mw:
            request_message = _serializer.dumps_request(request)

            if ttl is not None:
                request_message.headers["ttl"] = ttl

            future = await self._app.backend.request(request.headers["queue"], request_message, context=_context)
            published = True
            if self._app.hooks.on_request_published:
                await self._app.hooks.on_request_published.emit(
                    RequestPublishedEvent(app=self._app.name, request=request, outcome="ok", error=None)
                )
            try:
                async with asyncio.timeout(client_timeout):
                    res_message = await future
            except TimeoutError as exc:
                response_outcome = "timeout"
                response_error = RPCError(
                    error_code=ErrorCode.TIMEOUT,
                    error_message="RPC call timed out waiting for response",
                )
                if self._app.hooks.on_response_received:
                    await self._app.hooks.on_response_received.emit(
                        ResponseReceivedEvent(
                            app=self._app.name,
                            request=request,
                            response=None,
                            outcome=response_outcome,
                            error=response_error,
                        )
                    )
                raise response_error from exc
            if res_message is None:
                return None
            response = _serializer.loads_response(res_message)

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
                if self._app.hooks.on_response_received:
                    await self._app.hooks.on_response_received.emit(
                        ResponseReceivedEvent(
                            app=self._app.name,
                            request=request,
                            response=None,
                            outcome=response_outcome,
                            error=response_error,
                        )
                    )
                raise response_error
            else:
                result = response.data
            if self._app.hooks.on_response_received:
                await self._app.hooks.on_response_received.emit(
                    ResponseReceivedEvent(
                        app=self._app.name,
                        request=request,
                        response=result,
                        outcome="ok",
                        error=None,
                    )
                )
            return result

        # Full pipeline with middleware
        async def core() -> Response | None:
            nonlocal published
            request_message = _serializer.dumps_request(request)

            if ttl is not None:
                request_message.headers["ttl"] = ttl

            async def backend_request() -> Message | None:
                nonlocal published
                future = await self._app.backend.request(request.headers["queue"], request_message, context=_context)
                published = True
                if self._app.hooks.on_request_published:
                    await self._app.hooks.on_request_published.emit(
                        RequestPublishedEvent(
                            app=self._app.name,
                            request=request,
                            outcome="ok",
                            error=None,
                        )
                    )
                res_message = await future
                return res_message

            wire_handler = apply_middleware(
                self._wire_mw,
                backend_request,
                lambda mw, n: wrap_wire_client(mw, n, request, request_message, _context),
            )

            res_message = await wire_handler()
            if res_message is None:
                return None
            return _serializer.loads_response(res_message)

        handler = apply_middleware(
            self._app_mw,
            core,
            lambda mw, n: wrap_app_client(mw, n, request, _context),
        )

        try:

            try:
                async with asyncio.timeout(client_timeout):
                    response = await handler()
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
            if self._app.hooks.on_response_received:
                await self._app.hooks.on_response_received.emit(
                    ResponseReceivedEvent(
                        app=self._app.name,
                        request=request,
                        response=result,
                        outcome="ok",
                        error=None,
                    )
                )
            return result
        except BaseException as exc:
            if not published:
                if self._app.hooks.on_request_published:
                    err = exc if isinstance(exc, BaseException) else Exception(str(exc))
                    await self._app.hooks.on_request_published.emit(
                        RequestPublishedEvent(
                            app=self._app.name,
                            request=request,
                            outcome="error",
                            error=err,
                        )
                    )
            else:
                if response_error is None:
                    response_outcome = "error"
                    response_error = exc
                if self._app.hooks.on_response_received:
                    await self._app.hooks.on_response_received.emit(
                        ResponseReceivedEvent(
                            app=self._app.name,
                            request=request,
                            response=None,
                            outcome=response_outcome,
                            error=response_error,
                        )
                    )
            raise

    async def _invoke_task_request(
        self,
        request: Request,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if self._app.is_closed:
            raise RuntimeError("App is closed")

        _serializer = self._get_serializer(serializer)
        _context = context or {}
        published = False

        ttl = request.headers.get("ttl")

        # Fast path: no middleware
        if not self._wire_mw and not self._app_mw:
            request_message = _serializer.dumps_request(request)

            if ttl is not None:
                request_message.headers["ttl"] = ttl

            await self._app.backend.send(request.headers["queue"], request_message, context=_context)
            published = True
            if self._app.hooks.on_request_published:
                await self._app.hooks.on_request_published.emit(
                    RequestPublishedEvent(
                        app=self._app.name,
                        request=request,
                        outcome="ok",
                        error=None,
                    )
                )
            return

        # Full pipeline with middleware
        async def core() -> None:
            nonlocal published
            request_message = _serializer.dumps_request(request)

            if ttl is not None:
                request_message.headers["ttl"] = ttl

            async def backend_send() -> Message | None:
                nonlocal published
                await self._app.backend.send(request.headers["queue"], request_message, context=_context)
                published = True
                if self._app.hooks.on_request_published:
                    await self._app.hooks.on_request_published.emit(
                        RequestPublishedEvent(
                            app=self._app.name,
                            request=request,
                            outcome="ok",
                            error=None,
                        )
                    )
                return None

            wire_handler = apply_middleware(
                self._wire_mw,
                backend_send,
                lambda mw, n: wrap_wire_client(mw, n, request, request_message, _context),
            )

            await wire_handler()

        handler = apply_middleware(
            self._app_mw,
            core,
            lambda mw, n: wrap_app_client(mw, n, request, _context),
        )

        try:
            await handler()
        except BaseException as exc:
            if not published:
                if self._app.hooks.on_request_published:
                    err = exc
                    await self._app.hooks.on_request_published.emit(
                        RequestPublishedEvent(
                            app=self._app.name,
                            request=request,
                            outcome="error",
                            error=err,
                        )
                    )
            raise

    async def _invoke_event_request(
        self,
        request: Request,
        serializer: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> None:
        if self._app.is_closed:
            raise RuntimeError("App is closed")

        _serializer = self._get_serializer(serializer)
        _context = context or {}
        published = False

        ttl = request.headers.get("ttl")

        # Fast path: no middleware
        if not self._wire_mw and not self._app_mw:
            request_message = _serializer.dumps_request(request)

            if ttl is not None:
                request_message.headers["ttl"] = ttl

            await self._app.backend.emit(request.data["topic"], request_message, context=_context)
            published = True
            if self._app.hooks.on_request_published:
                await self._app.hooks.on_request_published.emit(
                    RequestPublishedEvent(
                        app=self._app.name,
                        request=request,
                        outcome="ok",
                        error=None,
                    )
                )
            return

        # Full pipeline with middleware
        async def core() -> None:
            nonlocal published
            request_message = _serializer.dumps_request(request)

            if ttl is not None:
                request_message.headers["ttl"] = ttl

            async def backend_emit() -> Message | None:
                nonlocal published
                await self._app.backend.emit(request.data["topic"], request_message, context=_context)
                published = True
                if self._app.hooks.on_request_published:
                    await self._app.hooks.on_request_published.emit(
                        RequestPublishedEvent(
                            app=self._app.name,
                            request=request,
                            outcome="ok",
                            error=None,
                        )
                    )
                return None

            wire_handler = apply_middleware(
                self._wire_mw,
                backend_emit,
                lambda mw, n: wrap_wire_client(mw, n, request, request_message, _context),
            )

            await wire_handler()

        handler = apply_middleware(
            self._app_mw,
            core,
            lambda mw, n: wrap_app_client(mw, n, request, _context),
        )

        try:
            await handler()
        except BaseException as exc:
            if not published:
                if self._app.hooks.on_request_published:
                    err = exc
                    await self._app.hooks.on_request_published.emit(
                        RequestPublishedEvent(
                            app=self._app.name,
                            request=request,
                            outcome="error",
                            error=err,
                        )
                    )
            raise

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_serializer(self, name: str | None) -> Serializer:
        if name is None:
            return self._serializer
        try:
            return self._serializers_map[name]
        except KeyError as exc:
            raise RequestError(f"unknown serializer: {name}") from exc
