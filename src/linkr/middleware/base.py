from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from linkr.models import RpcRequest, RpcResponse


class BaseMiddleware(ABC):
    """
    Base class for all middleware.

    Provides optional lifecycle hooks that are called by :class:`RpcApp`
    during startup and shutdown.
    """

    async def init(self) -> None:
        """
        Initialise middleware resources when the app starts.

        Override this method to set up connections, open files, etc.
        """

    async def close(self) -> None:
        """
        Clean up middleware resources on app shutdown.

        Override this method to release connections, close files, etc.
        """


class AppMiddleware(BaseMiddleware):
    """
    Middleware that works with deserialised request/response objects.

    App-level middleware operates on the parsed :class:`RpcRequest` and
    :class:`RpcResponse` so it has full access to the message content
    without needing to deal with serialization.
    """

    @abstractmethod
    async def process_request(self, request: RpcRequest, **kwds: Any) -> RpcRequest:
        """
        Process a request before it reaches the handler.

        Called twice for every synchronous call:
        once on the client side (after serialization) and once on the
        server side (before dispatch). Raise an exception to abort
        processing.

        Args:
            request: The incoming or outgoing RPC request.
            **kwds: Additional call context forwarded from the caller.

        Returns:
            The (possibly modified) request.
        """

    @abstractmethod
    async def process_response(self, request: RpcRequest, response: RpcResponse, **kwds: Any) -> RpcResponse:
        """
        Process a response after the handler has run.

        Called twice for every synchronous call:
        once on the server side (after dispatch) and once on the client
        side (before returning to the caller).

        Args:
            request: The original RPC request (read-only).
            response: The outgoing or incoming RPC response.
            **kwds: Additional call context forwarded from the caller.

        Returns:
            The (possibly modified) response.
        """


class WireMiddleware(BaseMiddleware):
    """
    Middleware that works with raw bytes and wire-level headers.

    Wire-level middleware operates on the serialised byte payload and
    wire-level metadata headers. This is suitable for compression,
    encryption, or custom encoding layers.
    """

    async def send(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
        response: RpcResponse | None = None,
        **kwds: Any,
    ) -> tuple[bytes, dict[str, str]]:
        """
        Transform data being sent TO the transport.

        Called for both client requests (before transport send) and
        server responses (before transport send back to client).

        Args:
            data: The raw payload bytes.
            headers: Wire-level headers (e.g. content_type, content_encoding).
            request: The original RPC request.
            response: The RPC response, if available (``None`` for request path).
            **kwds: Additional call context forwarded from the caller.

        Returns:
            The (possibly modified) ``(data, headers)`` tuple.
        """
        return data, headers

    async def receive(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
        **kwds: Any,
    ) -> tuple[bytes, dict[str, str]]:
        """
        Transform data received FROM the transport.

        Called for both server requests (after transport receive) and
        client responses (after transport receive).

        Args:
            data: The raw payload bytes.
            headers: Wire-level headers (e.g. content_type, content_encoding).
            request: The original RPC request.
            **kwds: Additional call context forwarded from the caller.

        Returns:
            The (possibly modified) ``(data, headers)`` tuple.
        """
        return data, headers
