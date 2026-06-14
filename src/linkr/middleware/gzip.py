from __future__ import annotations

import gzip
from collections.abc import Awaitable, Callable
from typing import Any

from ..models import RawMessage, RpcRequest, RpcResponse
from .base import WireMiddleware


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
        call_next: Callable[[], Awaitable[RawMessage | None]],
        request_raw_message: RawMessage,
        request: RpcRequest,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage | None:
        """
        Compress outgoing request, decompress incoming response.

        Mutates *request_raw_message* in place before calling the next layer,
        and mutates the returned response :class:`RawMessage` in place
        before returning.
        """
        if len(request_raw_message.data) >= self._min_size:
            request_raw_message.data = gzip.compress(request_raw_message.data)
            _merge_ce(request_raw_message.headers, "gzip")

        raw_response = await call_next()

        if raw_response is not None and "gzip" in raw_response.headers.get("content_encoding", ""):
            raw_response.data = gzip.decompress(raw_response.data)

        return raw_response

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[tuple[RawMessage, RpcResponse] | tuple[None, None]]],
        request_raw_message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
        """
        Decompress incoming request, compress outgoing response.

        Mutates *request_raw_message* in place before calling the next layer,
        and mutates the returned response :class:`RawMessage` in place
        before returning.
        """
        if "gzip" in request_raw_message.headers.get("content_encoding", ""):
            request_raw_message.data = gzip.decompress(request_raw_message.data)

        result = await call_next()

        if result is None or result[0] is None:
            return None, None

        raw_response, response = result
        if len(raw_response.data) >= self._min_size:
            raw_response.data = gzip.compress(raw_response.data)
            _merge_ce(raw_response.headers, "gzip")

        return raw_response, response
