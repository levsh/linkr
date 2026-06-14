from __future__ import annotations

from abc import ABC
from collections.abc import Awaitable, Callable
from typing import Any

from ..models import RawMessage, RpcRequest, RpcResponse


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

    App-level middleware uses the onion (dispatch) pattern so it can
    perform pre- and post-processing around the inner handler using
    local variables.

    Subclasses should override :meth:`dispatch_client` and/or
    :meth:`dispatch_server` to transform requests and responses.
    """

    async def dispatch_client(
        self,
        call_next: Callable[[], Awaitable[RpcResponse | None]],
        request: RpcRequest,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RpcResponse | None:
        """
        Wrap a client-side RPC call.

        Override this method to perform actions before and/or after
        the rest of the client pipeline (serialize → wire-level
        dispatch_client → transport → deserialize). Call
        ``await call_next()`` to invoke the next layer.

        Args:
            call_next: The next layer in the middleware chain.
            request: The outgoing RPC request.
            kwds: Additional call context forwarded from the caller.

        Returns:
            The RPC response, or ``None`` for fire-and-forget.
        """
        return await call_next()

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[RpcResponse | None]],
        request: RpcRequest,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RpcResponse | None:
        """
        Wrap a server-side request handler.

        Override this method to perform actions before and/or after
        the handler is dispatched. Call ``await call_next()`` to
        invoke the next layer.

        Args:
            call_next: The next layer in the middleware chain.
            request: The incoming RPC request.
        Returns:
            The RPC response, or ``None`` if no response is sent.
        """
        return await call_next()


class WireMiddleware(BaseMiddleware):
    """
    Middleware that works with raw bytes and wire-level headers.

    Wire-level middleware operates on the serialised byte payload and
    wire-level metadata headers. This is suitable for compression,
    encryption, or custom encoding layers.

    Subclasses should override :meth:`dispatch_client` and/or
    :meth:`dispatch_server` to transform data using the onion pattern.
    Mutate ``request_raw_message.data`` / ``request_raw_message.headers``
    in place before calling ``call_next()`` to affect the outgoing data.
    The response (if any) can be similarly mutated after ``call_next()``
    returns.
    """

    async def dispatch_client(
        self,
        call_next: Callable[[], Awaitable[RawMessage | None]],
        request_raw_message: RawMessage,
        request: RpcRequest,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage | None:
        """
        Wrap a client-side RPC call (request → transport → response).

        Override this method to transform the outgoing request bytes
        (by mutating *request_raw_message*) before ``await call_next()``,
        and/or transform the incoming response bytes (by mutating the
        returned :class:`RawMessage`) after ``await call_next()``.

        Args:
            call_next: The next layer in the wire middleware chain.
            request_raw_message: The serialised request (mutate in place).
            request: The original RPC request object.
            kwds: Additional call context forwarded from the caller.

        Returns:
            The (possibly modified) response :class:`RawMessage`,
            or ``None`` for fire-and-forget.
        """
        return await call_next()

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[tuple[RawMessage, RpcResponse] | tuple[None, None]]],
        request_raw_message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
        """
        Wrap a server-side request handler (request → dispatch → response).

        Override this method to transform the incoming request bytes
        (by mutating *request_raw_message*) before ``await call_next()``,
        and/or transform the outgoing response bytes (by mutating the
        returned :class:`RawMessage`) after ``await call_next()``.

        Args:
            call_next: The next layer in the wire middleware chain.
            request_raw_message: The serialised incoming request
                (mutate in place).

        Returns:
            A ``(raw_response, response)`` tuple where *raw_response* is
            the serialised response bytes and *response* is the
            deserialised :class:`RpcResponse`.
        """
        return await call_next()
