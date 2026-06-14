from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from ..models import RawMessage, RpcRequest
from . import Transport


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
                [RawMessage],
                Awaitable[RawMessage | None],
            ]
            | None
        ) = None
        self._is_consuming = False
        self._closed = asyncio.Event()
        self.sent_messages: list[RpcRequest] = []

    async def init(self) -> None:
        """No-op for mock transport."""

    async def close(self, timeout: float | None = None) -> None:
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
            [RawMessage],
            Awaitable[RawMessage | None],
        ],
        queue: str | None = None,
    ) -> None:
        """
        Register a handler to process incoming requests.

        Args:
            handler: Async callable that receives a :class:`RawMessage`
                and returns a :class:`RawMessage` or ``None`` for
                fire-and-forget.
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
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        """
        Publish a fire-and-forget message.

        Appends the request to :attr:`sent_messages`. No response is
        expected.

        Args:
            request: The original RPC request (stored in sent_messages).
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.
        """
        self.sent_messages.append(request)

    async def request(
        self,
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage:
        """
        Send a request and return the handler's response.

        Appends the request to :attr:`sent_messages`, then invokes the
        registered handler inline and returns its result. If the
        transport is closed while waiting, raises
        :class:`asyncio.CancelledError`.

        Args:
            request: The original RPC request (forwarded to handler and
                stored in sent_messages).
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.

        Returns:
            The response as a :class:`RawMessage`.

        Raises:
            RuntimeError: If no handler is registered.
            asyncio.CancelledError: If the transport is closed while
                waiting for a response.
        """
        self.sent_messages.append(request)
        if self._handler is None:
            raise RuntimeError("No consumer registered")

        handler_task = asyncio.ensure_future(self._handler(message))
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
            raise RuntimeError("Handler returned None, expected RawMessage")
        return result
