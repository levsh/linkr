from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any

from linkr.models import RpcRequest, RpcResponse


class Transport(ABC):
    """
    Abstract transport for RPC message exchange.

    Transport operates on raw bytes. Serialization and encoding are
    handled by RpcApp before data reaches the transport.
    """

    @abstractmethod
    async def init(self) -> None:
        """Open connections and declare required infrastructure."""

    @abstractmethod
    async def close(self) -> None:
        """Gracefully shut down the transport."""

    @abstractmethod
    async def consume(
        self,
        handler: Callable[
            [bytes, RpcRequest, dict[str, str] | None],
            Awaitable[tuple[bytes, RpcResponse | None, dict[str, str]] | None],
        ],
        queue: str | None = None,
    ) -> None:
        """
        Start consuming incoming RPC requests.

        Args:
            handler: Async callable that receives
                (request_bytes, original_request, wire_headers) and returns
                (response_bytes, original_response, response_wire_headers)
                or None for fire-and-forget.
            queue: Optional queue name segment for group routing.
        """

    @abstractmethod
    async def stop_consume(self) -> None:
        """Stop consuming incoming requests."""

    @abstractmethod
    async def publish(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> None:
        """
        Publish a fire-and-forget RPC request.

        Args:
            data: Serialized and encoded request bytes.
            original: The original RpcRequest (for routing metadata).
            wire_headers: Additional wire-level headers (e.g. content-encoding).
        """

    @abstractmethod
    async def request(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> tuple[bytes, dict[str, str]]:
        """
        Send an RPC request and wait for a matching response.

        Args:
            data: Serialized and encoded request bytes.
            original: The original RpcRequest (for routing metadata).
            wire_headers: Additional wire-level headers.

        Returns:
            Tuple of (raw_response_bytes, response_wire_headers).
        """
