from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any

from ..models import RawMessage, RpcRequest


class Transport(ABC):
    """
    Abstract transport for RPC message exchange.

    Transport operates on raw bytes. Serialization and encoding are
    handled by :class:`RpcApp` before data reaches the transport.

    Implementations must provide :meth:`init`, :meth:`close`,
    :meth:`publish`, :meth:`request`, :meth:`consume`, and
    :meth:`stop_consume`.
    """

    @abstractmethod
    async def init(self) -> None:
        """
        Open connections and declare required infrastructure.

        Called once during application startup before any messages are
        sent or received.
        """

    @abstractmethod
    async def close(self, timeout: float | None = None) -> None:
        """
        Gracefully shut down the transport.

        Called during application shutdown. Should release all
        connections and resources.

        Args:
            timeout: Max seconds to wait for pending operations.
                ``None`` means wait indefinitely.
        """

    @abstractmethod
    async def consume(
        self,
        handler: Callable[[RawMessage], Awaitable[RawMessage | None]],
        queue: str | None = None,
    ) -> None:
        """
        Start consuming incoming RPC requests.

        Args:
            handler: Async callable that receives a :class:`RawMessage`
                and returns a :class:`RawMessage` or ``None`` for
                fire-and-forget.
            queue: Optional routing prefix. When set, a dedicated queue
                ``{server_queue_name}.{queue}`` is declared and consumed.
                Used by :class:`RpcApp` to route method groups based on
                the method name prefix (e.g. ``"api/user/get"`` routes to
                queue ``{server_queue_name}.api``).
        """

    @abstractmethod
    async def stop_consume(self) -> None:
        """
        Stop consuming incoming requests without closing the transport.

        After this call the transport can still be used for sending,
        but will no longer deliver incoming messages to the registered
        handler.
        """

    @abstractmethod
    async def publish(
        self,
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        """
        Publish a fire-and-forget RPC request.

        Args:
            request: The original RpcRequest (for routing metadata).
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.
        """

    @abstractmethod
    async def request(
        self,
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage:
        """
        Send an RPC request and wait for a matching response.

        Args:
            request: The original RpcRequest (for routing metadata).
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.

        Returns:
            The response as a :class:`RawMessage`.
        """
