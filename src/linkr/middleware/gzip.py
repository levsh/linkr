from __future__ import annotations

import gzip
from collections.abc import Awaitable, Callable
from typing import Any

from ..models import Message, Request, Response
from . import WireMiddleware


def _merge_ce(headers: dict[str, Any], name: str) -> None:
    existing = headers.get("content_encoding", "")
    headers["content_encoding"] = f"{existing},{name}" if existing else name


class GzipMiddleware(WireMiddleware):
    """
    Compress request/response body with gzip when the payload is large enough.

    Only payloads whose size in bytes is at least *min_size* are compressed.
    The ``content_encoding`` wire header is used to signal that compression
    has been applied.
    """

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
        request_message: Message,
        request: Request,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> Message | None:
        """
        Compress outgoing request, decompress incoming response.

        Mutates *request_message* in place before calling the next layer,
        and mutates the returned response :class:`Message` in place
        before returning.
        """
        if len(request_message.data) >= self._min_size:
            request_message.data = gzip.compress(request_message.data)
            _merge_ce(request_message.headers, "gzip")

        response_message = await call_next()

        if response_message is not None and "gzip" in response_message.headers.get("content_encoding", ""):
            response_message.data = gzip.decompress(response_message.data)

        return response_message

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[tuple[Message, Response] | tuple[None, None]]],
        request_message: Message,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> tuple[Message, Response] | tuple[None, None]:
        """
        Decompress incoming request, compress outgoing response.

        Mutates *request_message* in place before calling the next layer,
        and mutates the returned response :class:`Message` in place
        before returning.
        """
        if "gzip" in request_message.headers.get("content_encoding", ""):
            request_message.data = gzip.decompress(request_message.data)

        result = await call_next()

        if result is None or result[0] is None:
            return None, None

        response_message, response = result
        if len(response_message.data) >= self._min_size:
            response_message.data = gzip.compress(response_message.data)
            _merge_ce(response_message.headers, "gzip")

        return response_message, response
