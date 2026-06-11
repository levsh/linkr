from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from linkr.models import RpcRequest, RpcResponse
from linkr.transports import Transport


class MockTransport(Transport):
    """
    In-memory transport for testing.

    Does not require a broker. Messages are routed directly via a
    registered handler callback. This is the default transport for
    unit tests and local development.

    Attributes:
        sent_messages: All :class:`RpcRequest` objects that were published
            or sent via this transport, in order.
    """

    def __init__(self) -> None:
        self._handler: (
            Callable[
                [bytes, RpcRequest, dict[str, str] | None],
                Awaitable[tuple[bytes, RpcResponse | None, dict[str, str]] | None],
            ]
            | None
        ) = None
        self._is_consuming = False
        self._closed = asyncio.Event()
        self.sent_messages: list[RpcRequest] = []

    async def init(self) -> None:
        """No-op for mock transport."""

    async def close(self) -> None:
        """
        Reset the transport and signal cancellation to pending requests.

        Clears the handler, marks the transport as not consuming, and
        sets the internal shutdown event so that in-flight requests
        are cancelled.
        """
        self._is_consuming = False
        self._handler = None
        self._closed.set()

    async def consume(
        self,
        handler: Callable[
            [bytes, RpcRequest, dict[str, str] | None],
            Awaitable[tuple[bytes, RpcResponse | None, dict[str, str]] | None],
        ],
        queue: str | None = None,
    ) -> None:
        """
        Register a handler to process incoming requests.

        Args:
            handler: Async callable that receives
                ``(request_bytes, original_request, wire_headers)`` and
                returns ``(response_bytes, original_response, response_wire_headers)``
                or ``None`` for fire-and-forget.
            queue: Ignored. Present for interface compatibility with
                :class:`RmqTransport`.
        """
        self._closed.clear()
        self._handler = handler
        self._is_consuming = True

    async def stop_consume(self) -> None:
        """Clear the handler and mark as not consuming."""
        self._is_consuming = False
        self._handler = None

    async def publish(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
        **kwds: Any,
    ) -> None:
        """
        Publish a fire-and-forget message.

        Appends the request to :attr:`sent_messages`. No response is
        expected.

        Args:
            data: Serialised request bytes (not used by mock).
            original: The original RPC request (stored in sent_messages).
            wire_headers: Wire-level headers (not used by mock).
            **kwds: Ignored. Present for interface compatibility.
        """
        self.sent_messages.append(original)

    async def request(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
        **kwds: Any,
    ) -> tuple[bytes, dict[str, str]]:
        """
        Send a request and return the handler's response.

        Appends the request to :attr:`sent_messages`, then invokes the
        registered handler inline and returns its result. If the
        transport is closed while waiting, raises
        :class:`asyncio.CancelledError`.

        Args:
            data: Serialised request bytes (forwarded to handler).
            original: The original RPC request (forwarded to handler and
                stored in sent_messages).
            wire_headers: Wire-level headers (forwarded to handler).
            **kwds: Ignored. Present for interface compatibility.

        Returns:
            ``(response_bytes, response_wire_headers)`` as returned by the
            registered handler.

        Raises:
            RuntimeError: If no handler is registered.
            asyncio.CancelledError: If the transport is closed while
                waiting for a response.
        """
        self.sent_messages.append(original)
        if self._handler is None:
            raise RuntimeError("No consumer registered")

        handler_task = asyncio.ensure_future(self._handler(data, original, wire_headers))
        close_task = asyncio.create_task(self._closed.wait())
        done, _ = await asyncio.wait(
            [handler_task, close_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if close_task in done:
            handler_task.cancel()
            raise asyncio.CancelledError("Transport closed")

        result = handler_task.result()
        if result is None:
            raise RuntimeError("Handler returned None, expected tuple[bytes, RpcResponse | None, dict[str, str]]")
        response_bytes, _, response_wire = result
        return (response_bytes, response_wire)
