from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from .backends import Backend
from .client import Client
from .contracts import AppContract, describe_app, describe_app_json
from .hooks import AppHooks, HookRegistry
from .middleware import AppMiddleware, WireMiddleware
from .models import HandlerInfo, QueueOptions
from .serializer import JsonSerializer, Serializer
from .server import Server

logger = logging.getLogger("linkr")


class App:
    """
    Application: infrastructure, middleware/hooks, client API and server factory.

    Servers own handler registration and consuming; ``App`` provides lifecycle and
    client operations (``call/send/emit``).
    """

    RPC_TIMEOUT: float = 60.0
    RPC_TTL: float = 60.0
    RPC_RTTL: float = 60.0

    TASK_TIMEOUT: float = 3600.0
    TASK_TTL: float = 3600.0

    EVENT_TTL: float = 3600.0

    def __init__(
        self,
        name: str,
        *,
        backend: Backend,
        serializer: Serializer | list[Serializer] | None = None,
        middlewares: Sequence[AppMiddleware | WireMiddleware] | None = None,
        rpc_timeout: float = RPC_TIMEOUT,
        rpc_ttl: float = RPC_TTL,
        rpc_rttl: float = RPC_RTTL,
        task_timeout: float = TASK_TIMEOUT,
        task_ttl: float = TASK_TTL,
        event_ttl: float = EVENT_TTL,
        validate_types: bool = False,
    ) -> None:
        """
        Create an application instance.

        Args:
            name: Application name used as the root queue scope.
            backend: Transport backend implementation.
            serializer: One serializer or a list of serializers (default: ``JsonSerializer``).
            middlewares: Optional app/wire middlewares applied to client and server pipelines.
            rpc_timeout: Default handler execution timeout for RPC requests.
            rpc_ttl: Default time-to-live for RPC requests.
            rpc_rttl: Default response time-to-live for RPC requests.
            task_timeout: Default handler execution timeout for task requests.
            task_ttl: Default time-to-live for task requests.
            event_ttl: Default time-to-live for event requests.
            validate_types: Enable argument validation for handlers by default.
        """
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
            rpc_timeout=rpc_timeout,
            rpc_ttl=rpc_ttl,
            rpc_rttl=rpc_rttl,
            task_timeout=task_timeout,
            task_ttl=task_ttl,
            event_ttl=event_ttl,
        )
        self._servers: dict[str | None, Server] = {}
        self._closed = False

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def instance_id(self) -> str:
        """Unique identifier of this app instance."""
        return self._instance_id

    @property
    def name(self) -> str:
        """Application name."""
        return self._name

    @property
    def middlewares(self) -> Sequence[AppMiddleware | WireMiddleware]:
        """Configured middlewares in registration order."""
        return list(self._middlewares)

    @property
    def app_middlewares(self) -> list[AppMiddleware]:
        """Configured application-level middlewares."""
        return [mw for mw in self._middlewares if isinstance(mw, AppMiddleware)]

    @property
    def wire_middlewares(self) -> list[WireMiddleware]:
        """Configured wire-level middlewares."""
        return [mw for mw in self._middlewares if isinstance(mw, WireMiddleware)]

    @property
    def serializers(self) -> Sequence[Serializer]:
        """Available serializers (the first is used by default)."""
        return list(self._serializers)

    @property
    def backend(self) -> Backend:
        """Transport backend used by the application."""
        return self._backend

    @property
    def client(self) -> Client:
        """Client used for ``call``, ``send`` and ``emit`` operations."""
        return self._client

    @property
    def servers(self) -> list[Server]:
        """Servers registered on this application."""
        return list(self._servers.values())

    @property
    def hooks(self) -> HookRegistry:
        """Hook registry for application events."""
        return self._hooks

    @property
    def hook(self) -> HookRegistry:
        """Alias for :attr:`hooks`."""
        return self._hooks

    @property
    def validate_types(self) -> bool:
        """Whether handler argument validation is enabled by default."""
        return self._validate_types

    @property
    def is_closed(self) -> bool:
        """Whether the application has been closed."""
        return self._closed

    @property
    def methods(self) -> dict[str, HandlerInfo]:
        """All RPC handlers registered on application servers, keyed by fully qualified name."""
        result: dict[str, HandlerInfo] = {}
        for server in self._servers.values():
            for _leaf, info in server.methods.items():
                result[info.name] = info
        return result

    @property
    def tasks(self) -> dict[str, HandlerInfo]:
        """All task handlers registered on application servers, keyed by fully qualified name."""
        result: dict[str, HandlerInfo] = {}
        for server in self._servers.values():
            for _leaf, info in server.tasks.items():
                result[info.name] = info
        return result

    @property
    def subscriptions(self) -> dict[str, list[HandlerInfo]]:
        """All event subscriptions registered on application servers, grouped by topic pattern."""
        grouped: dict[str, list[HandlerInfo]] = {}
        for server in self._servers.values():
            for topic, handlers in server.subscriptions.items():
                grouped.setdefault(topic, []).extend(handlers)
        return dict(sorted(grouped.items()))

    @property
    def exception_handlers(self) -> MappingProxyType[type[Exception], Callable[[Exception], Any]]:
        """Registered exception handlers (read-only view)."""
        return MappingProxyType(self._exception_handlers)

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    def add_hooks(self, hooks: AppHooks) -> None:
        """
        Register all overridden hook methods from an :class:`AppHooks` observer.

        Args:
            hooks: Hook observer instance providing hook callbacks.
        """
        self._hooks.add_hooks(hooks)

    def remove_hooks(self, hooks: AppHooks) -> None:
        """
        Unregister hook methods previously added from an :class:`AppHooks` observer.

        Args:
            hooks: Hook observer instance to remove.
        """
        self._hooks.remove_hooks(hooks)

    # ------------------------------------------------------------------
    # Servers
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

    def server(
        self,
        path: str | None = None,
        *,
        rpc_options: QueueOptions | Sequence[QueueOptions] | None = None,
        task_options: QueueOptions | Sequence[QueueOptions] | None = None,
        event_options: QueueOptions | Sequence[QueueOptions] | None = None,
    ) -> Server:
        """
        Return a server bound to this app and queue scope, creating it if needed.

        Args:
            path: Optional server scope path (``None`` for root). Must not be empty when provided.
            rpc_options: Backend-specific queue options for RPC consuming.
            task_options: Backend-specific queue options for task consuming.
            event_options: Backend-specific queue options for event consuming.

        Returns:
            The existing or newly created :class:`Server` for *path*.
        """
        if path is not None and (not isinstance(path, str) or path == ""):
            raise ValueError("server path must not be empty")

        normalized = {
            "rpc": self._normalize_queue_options(rpc_options),
            "task": self._normalize_queue_options(task_options),
            "event": self._normalize_queue_options(event_options),
        }

        existing = self._servers.get(path)
        if existing is not None:
            if not existing.options_match(normalized):
                raise ValueError(f"server already configured for path {path!r}")
            return existing

        srv = Server(self, path, options=normalized)
        self._servers[path] = srv
        return srv

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """
        Initialize the backend and middlewares.

        Raises:
            RuntimeError: If the application has already been closed.
        """
        if self._closed:
            raise RuntimeError("App is closed")

        await self._backend.init()

        for mw in self._middlewares:
            await mw.init()

    async def close(self) -> None:
        """
        Stop consumers, close middlewares and the backend, and mark the app closed.
        """
        for server in list(self._servers.values()):
            await server.stop_consume()

        for mw in reversed(self._middlewares):
            await mw.close()

        await self._backend.close()

        self._closed = True

    async def __aenter__(self) -> App:
        """
        Initialize the application for async context-manager use.

        Returns:
            The initialized application instance.
        """
        await self.init()
        return self

    async def __aexit__(self, *args: Any) -> None:
        """
        Close the application when leaving an async context manager.

        Args:
            *args: Exception information supplied by the context manager protocol.
        """
        await self.close()

    # ------------------------------------------------------------------
    # Exception handlers
    # ------------------------------------------------------------------

    def exception_handler(
        self,
        exc_class: type[Exception],
    ) -> Callable[[Callable[[Exception], Any]], Callable[[Exception], Any]]:
        """
        Register a handler for exceptions of *exc_class*.

        The handler receives the raised exception and may return one of:

        - :class:`~linkr.exceptions.RPCError` — returned as-is.
        - ``(error_code, error_message[, error_details])`` tuple — converted to ``RPCError``.
        - ``dict`` with ``error_code`` / ``error_message`` / ``error_details`` keys.
        - ``str`` — used as ``error_message`` with ``ErrorCode.INTERNAL_ERROR``.
        - ``None`` or any other value — treated as unhandled (the default error response is used).

        Raising :class:`~linkr.exceptions.RPCError` from the handler is also supported.

        Args:
            exc_class: Exception type to handle.

        Returns:
            A decorator that registers and returns the handler function.
        """

        def decorator(fn: Callable[[Exception], Any]) -> Callable[[Exception], Any]:
            self._exception_handlers[exc_class] = fn
            return fn

        return decorator

    # ------------------------------------------------------------------
    # Contracts
    # ------------------------------------------------------------------

    def describe(
        self,
        *,
        include: set[str] | None = None,
        include_internal: bool = False,
    ) -> AppContract:
        """
        Build and return the application contract.

        Args:
            include: Contract sections to include (``methods``, ``tasks``, ``events``).
            include_internal: Whether to include internal handlers/topics.

        Returns:
            The generated application contract.
        """
        return describe_app(self, include=include, include_internal=include_internal)

    def describe_json(self, *, indent: int | None = 2, **kwargs: Any) -> str:
        """
        Return the application contract serialized as JSON.

        Args:
            indent: JSON indentation level.
            **kwargs: Additional arguments forwarded to :meth:`describe`.

        Returns:
            A JSON string representing the application contract.
        """
        return describe_app_json(self, indent=indent, **kwargs)

    def describe_html(self, *, title: str | None = None, **describe_kwargs: Any) -> str:
        """
        Return the application contract rendered as an HTML document.

        Args:
            title: Optional document title.
            **describe_kwargs: Additional arguments forwarded to :meth:`describe`.

        Returns:
            An HTML document string representing the application contract.
        """
        return self.describe(**describe_kwargs).to_html(title=title)

    def expose_describe(
        self,
        name: str = "describe",
        server: Server | None = None,
        **handler_options: Any,
    ) -> Callable[[], dict[str, Any]]:
        """
        Register an RPC method that returns the application contract as a dict.

        Args:
            name: Local RPC method name to register.
            server: Server to register the method on (defaults to the root server).
            **handler_options: Additional options forwarded to ``server.method(...)``.

        Returns:
            The registered handler function.
        """
        if server is None:
            server = self.server()

        def describe_handler() -> dict[str, Any]:
            return self.describe().to_dict()

        server.method(name, **handler_options)(describe_handler)
        return describe_handler
