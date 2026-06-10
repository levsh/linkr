from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from linkr.models import RpcRequest, RpcResponse
from linkr.transports import Transport


class MockTransport(Transport):
    """
    In-memory transport for testing.
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
        self._closed.clear()
        self._handler = handler
        self._is_consuming = True

    async def stop_consume(self) -> None:
        self._is_consuming = False
        self._handler = None

    async def publish(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> None:
        self.sent_messages.append(original)

    async def request(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> tuple[bytes, dict[str, str]]:
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
