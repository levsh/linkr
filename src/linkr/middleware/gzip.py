from __future__ import annotations

import gzip

from linkr.middleware.base import WireMiddleware
from linkr.models import RpcRequest, RpcResponse


def _merge_ce(headers: dict[str, str], name: str) -> None:
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

    async def send(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
        response: RpcResponse | None = None,
    ) -> tuple[bytes, dict[str, str]]:
        """
        Compress data with gzip if its size is at least *min_size*.

        If compressed, the ``content_encoding`` header is set or extended
        with ``"gzip"``.

        Args:
            data: Raw payload bytes.
            headers: Wire-level headers (mutated in-place if compressed).
            request: The original RPC request.
            response: The RPC response, if available (``None`` for request path).

        Returns:
            The (possibly compressed) ``(data, headers)`` tuple.
        """
        if len(data) >= self._min_size:
            data = gzip.compress(data)
            _merge_ce(headers, "gzip")
        return data, headers

    async def receive(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
    ) -> tuple[bytes, dict[str, str]]:
        """
        Decompress gzip-encoded data.

        Only decompresses when the ``content_encoding`` header
        contains ``"gzip"``.

        Args:
            data: Possibly compressed payload bytes.
            headers: Wire-level headers (checked for ``content_encoding``).
            request: The original RPC request.

        Returns:
            The (possibly decompressed) ``(data, headers)`` tuple.
        """
        if "gzip" in headers.get("content_encoding", ""):
            data = gzip.decompress(data)
        return data, headers
