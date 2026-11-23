from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from . import utils
from .backends import Backend
from .client import Client
from .hooks import AppHooks, HookRegistry
from .middleware import AppMiddleware, WireMiddleware
from .models import HandlerInfo, HealthStatus, QueueOptions
from .serializer import JsonSerializer, Serializer
from .server import RPC, Bus, Worker, _Server

logger = logging.getLogger("linkr")


class App:
    """Application: infrastructure, middleware/hooks, client API and server factory."""

    RPC_CLIENT_TIMEOUT: float = 60.0
    RPC_SERVER_TIMEOUT: float = 60.0
    RPC_TTL: float = 60.0
    RPC_RTTL: float = 60.0

    TASK_CLIENT_TIMEOUT: float = 30.0
    TASK_SERVER_TIMEOUT: float = 3600.0
    TASK_TTL: float = 3600.0

    EVENT_CLIENT_TIMEOUT: float = 30.0
    EVENT_TTL: float = 3600.0

    def __init__(
        self,
        name: str,
        *,
        backend: Backend,
        serializer: Serializer | list[Serializer] | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
        rpc_client_timeout: float | None = RPC_CLIENT_TIMEOUT,
        rpc_server_timeout: float | None = RPC_SERVER_TIMEOUT,
        rpc_ttl: float | None = RPC_TTL,
        rpc_rttl: float | None = RPC_RTTL,
        task_client_timeout: float | None = TASK_CLIENT_TIMEOUT,
        task_server_timeout: float | None = TASK_SERVER_TIMEOUT,
        task_ttl: float | None = TASK_TTL,
        event_client_timeout: float | None = EVENT_CLIENT_TIMEOUT,
        event_ttl: float | None = EVENT_TTL,
        validate_types: bool = False,
    ) -> None:
        self._instance_id = uuid4().hex
        self._name = name
        self._backend = backend
        self._serializers: Sequence[Serializer]
        if serializer is None:
            self._serializers = [JsonSerializer()]
        elif isinstance(serializer, Serializer):
            self._serializers = [serializer]
        else:
            self._serializers = list(serializer)
        if not self._serializers:
            raise ValueError("at least one serializer is required")
        self._middlewares = middlewares or []
        self._hooks = HookRegistry()
        self._exception_handlers: dict[type[Exception], Callable[[Exception], Any]] = {}
        self._validate_types = validate_types
        self._client = Client(
            self,
            rpc_client_timeout=rpc_client_timeout,
            rpc_server_timeout=rpc_server_timeout,
            rpc_ttl=rpc_ttl,
            rpc_rttl=rpc_rttl,
            task_client_timeout=task_client_timeout,
            task_server_timeout=task_server_timeout,
            task_ttl=task_ttl,
            event_client_timeout=event_client_timeout,
            event_ttl=event_ttl,
        )
        self._rpc_servers: dict[str | None, RPC] = {}
        self._worker_servers: dict[str | None, Worker] = {}
        self._bus_servers: dict[str | None, Bus] = {}
        self._closed = False

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def middlewares(self) -> Sequence[AppMiddleware | WireMiddleware]:
        return list(self._middlewares)

    @property
    def app_middlewares(self) -> list[AppMiddleware]:
        return [mw for mw in self._middlewares if isinstance(mw, AppMiddleware)]

    @property
    def wire_middlewares(self) -> list[WireMiddleware]:
        return [mw for mw in self._middlewares if isinstance(mw, WireMiddleware)]

    @property
    def serializers(self) -> Sequence[Serializer]:
        return list(self._serializers)

    @property
    def backend(self) -> Backend:
        return self._backend

    @property
    def client(self) -> Client:
        return self._client

    @property
    def servers(self) -> list[_Server]:
        return [
            *self._rpc_servers.values(),
            *self._worker_servers.values(),
            *self._bus_servers.values(),
        ]

    @property
    def hooks(self) -> HookRegistry:
        return self._hooks

    @property
    def hook(self) -> HookRegistry:
        return self._hooks

    @property
    def validate_types(self) -> bool:
        return self._validate_types

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def methods(self) -> dict[str, HandlerInfo]:
        result: dict[str, HandlerInfo] = {}
        for server in self._rpc_servers.values():
            for info in server.methods.values():
                result[info.name] = info
        return result

    @property
    def tasks(self) -> dict[str, HandlerInfo]:
        result: dict[str, HandlerInfo] = {}
        for server in self._worker_servers.values():
            for info in server.tasks.values():
                result[info.name] = info
        return result

    @property
    def subscriptions(self) -> dict[str, list[HandlerInfo]]:
        grouped: dict[str, list[HandlerInfo]] = {}
        for server in self._bus_servers.values():
            for topic, handlers in server.subscriptions.items():
                grouped.setdefault(topic, []).extend(handlers)
        return dict(sorted(grouped.items()))

    @property
    def exception_handlers(self) -> MappingProxyType[type[Exception], Callable[[Exception], Any]]:
        return MappingProxyType(self._exception_handlers)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def add_hooks(self, hooks: AppHooks) -> None:
        self._hooks.add_hooks(hooks)

    def remove_hooks(self, hooks: AppHooks) -> None:
        self._hooks.remove_hooks(hooks)

    # ------------------------------------------------------------------
    # Servers
    # ------------------------------------------------------------------

    def rpc(
        self,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
        validate_types: bool | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    ) -> RPC:
        existing = self._rpc_servers.get(path)
        normalized = utils.normalize_queue_options(options)
        normalized_mw = tuple(middlewares or ())
        if existing is not None:
            if (
                existing.options != normalized
                or existing.validate_types != validate_types
                or existing.own_middlewares != normalized_mw
            ):
                raise ValueError(f"rpc server already configured for path {path!r}")
            return existing
        server = RPC(self, path, options=options, validate_types=validate_types, middlewares=normalized_mw)
        self._rpc_servers[path] = server
        return server

    def worker(
        self,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
        validate_types: bool | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    ) -> Worker:
        existing = self._worker_servers.get(path)
        normalized = utils.normalize_queue_options(options)
        normalized_mw = tuple(middlewares or ())
        if existing is not None:
            if (
                existing.options != normalized
                or existing.validate_types != validate_types
                or existing.own_middlewares != normalized_mw
            ):
                raise ValueError(f"worker server already configured for path {path!r}")
            return existing
        server = Worker(self, path, options=options, validate_types=validate_types, middlewares=normalized_mw)
        self._worker_servers[path] = server
        return server

    def bus(
        self,
        path: str | None = None,
        *,
        options: QueueOptions | Sequence[QueueOptions] | None = None,
        validate_types: bool | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
    ) -> Bus:
        existing = self._bus_servers.get(path)
        normalized = utils.normalize_queue_options(options)
        normalized_mw = tuple(middlewares or ())
        if existing is not None:
            if (
                existing.options != normalized
                or existing.validate_types != validate_types
                or existing.own_middlewares != normalized_mw
            ):
                raise ValueError(f"bus server already configured for path {path!r}")
            return existing
        server = Bus(self, path, options=options, validate_types=validate_types, middlewares=normalized_mw)
        self._bus_servers[path] = server
        return server

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        if self._closed:
            raise RuntimeError("App is closed")
        await self._backend.init()
        initialized: set[int] = set()
        for mw in self._middlewares:
            if id(mw) in initialized:
                continue
            initialized.add(id(mw))
            await mw.init()
        for server in self.servers:
            for mw in server.own_middlewares:
                if id(mw) in initialized:
                    continue
                initialized.add(id(mw))
                await mw.init()

    async def close(self, timeout: float | None = None) -> None:
        for server in self.servers:
            await server.stop_consume()
        to_close: list[AppMiddleware | WireMiddleware] = []
        seen: set[int] = set()
        for mw in self._middlewares:
            if id(mw) in seen:
                continue
            seen.add(id(mw))
            to_close.append(mw)
        for server in self.servers:
            for mw in server.own_middlewares:
                if id(mw) in seen:
                    continue
                seen.add(id(mw))
                to_close.append(mw)
        for mw in reversed(to_close):
            await mw.close()
        await self._backend.close()
        self._closed = True

    async def __aenter__(self) -> App:
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    async def healthcheck(self) -> HealthStatus:
        """Return application readiness and health status."""
        if self._closed:
            return HealthStatus(ready=False, health=None)
        try:
            backend_status = await self._backend.healthcheck()
        except Exception:
            return HealthStatus(ready=False, health=None)
        if not backend_status.ready:
            return HealthStatus(ready=False, health=None)
        return HealthStatus(ready=True, health=backend_status.health)

    # ------------------------------------------------------------------
    # Exception handlers
    # ------------------------------------------------------------------

    def exception_handler(
        self,
        exc_class: type[Exception],
    ) -> Callable[[Callable[[Exception], Any]], Callable[[Exception], Any]]:
        def decorator(fn: Callable[[Exception], Any]) -> Callable[[Exception], Any]:
            self._exception_handlers[exc_class] = fn
            return fn

        return decorator
