from __future__ import annotations

import gzip
from collections.abc import Awaitable, Callable
from typing import Any

from ..models import Message, Request, Response
from . import WireMiddleware


def _merge_ce(headers: dict[str, Any], name: str) -> None:
    existing = headers.get("content_encoding", "")
    encodings = [part.strip() for part in existing.split(",") if part.strip()] if existing else []
    if name not in encodings:
        encodings.append(name)
    headers["content_encoding"] = ",".join(encodings)


def _has_ce(headers: dict[str, Any], name: str) -> bool:
    value = headers.get("content_encoding", "")
    if not value:
        return False
    return name in [part.strip() for part in value.split(",") if part.strip()]


class GzipMiddleware(WireMiddleware):
    """
    Compress request/response body with gzip when the payload is large enough.

    Only payloads whose size in bytes is at least *min_size* are compressed.
    The ``content_encoding`` wire header is used to signal that compression
    has been applied.
    """

    features = ("gzip",)

    def __init__(self, min_size: int = 1024) -> None:
        """
        Args:
            min_size: Minimum payload size in bytes to trigger compression.
                Defaults to 1024.
        """
        self._min_size = min_size

    async def dispatch_client(
        self,
        call_next: Callable[[], Awaitable[Message | None]],
        request: Request,
        request_message: Message,
        *,
        context: dict[str, Any] | None = None,
    ) -> Message | None:
        """
        Compress outgoing request data and decompress incoming response data.

        Args:
            call_next: Next layer in the wire middleware chain.
            request: Outgoing request object.
            request_message: Serialized request message (mutated in place).
            context: Backend-specific publish/context metadata.

        Returns:
            The response message from downstream, or ``None`` for fire-and-forget.
        """
        if len(request_message.data) >= self._min_size:
            request_message.data = gzip.compress(request_message.data)
            _merge_ce(request_message.headers, "gzip")

        response_message = await call_next()

        if response_message is not None and _has_ce(response_message.headers, "gzip"):
            response_message.data = gzip.decompress(response_message.data)

        return response_message

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[tuple[Message, Response] | tuple[None, None]]],
        request_message: Message,
    ) -> tuple[Message, Response] | tuple[None, None]:
        """
        Decompress incoming request data and compress outgoing response data.

        Args:
            call_next: Next layer in the wire middleware chain.
            request_message: Serialized incoming request message (mutated in place).

        Returns:
            A ``(response_message, response)`` tuple, or ``(None, None)``.
        """
        if _has_ce(request_message.headers, "gzip"):
            request_message.data = gzip.decompress(request_message.data)

        result = await call_next()

        if result is None or result[0] is None:
            return None, None

        response_message, response = result
        if len(response_message.data) >= self._min_size:
            response_message.data = gzip.compress(response_message.data)
            _merge_ce(response_message.headers, "gzip")

        return response_message, response
