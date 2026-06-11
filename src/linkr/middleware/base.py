from __future__ import annotations

from abc import ABC, abstractmethod

from linkr.models import RpcRequest, RpcResponse


class BaseMiddleware(ABC):
    """Base class for all middleware. Optional lifecycle hooks: init() and close()."""

    async def init(self) -> None:
        """Initialize middleware when the app starts."""

    async def close(self) -> None:
        """Clean up middleware resources on app shutdown."""


class AppMiddleware(BaseMiddleware):
    """Middleware that works with deserialized request/response objects."""

    @abstractmethod
    async def process_request(self, request: RpcRequest) -> RpcRequest:
        """Called before handler. Return (possibly modified) request. Raise exception to cancel."""

    @abstractmethod
    async def process_response(self, request: RpcRequest, response: RpcResponse) -> RpcResponse:
        """Called after handler. Return (possibly modified) response."""


class WireMiddleware(BaseMiddleware):
    """Middleware that works with raw bytes and wire-level headers."""

    async def send(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
        response: RpcResponse | None = None,
    ) -> tuple[bytes, dict[str, str]]:
        """Transform data going TO the transport (client req + server res)."""
        return data, headers

    async def receive(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
    ) -> tuple[bytes, dict[str, str]]:
        """Transform data coming FROM the transport (server req + client res)."""
        return data, headers
