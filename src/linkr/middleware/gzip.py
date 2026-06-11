from __future__ import annotations

import gzip

from linkr.middleware.base import WireMiddleware
from linkr.models import RpcRequest, RpcResponse


def _merge_ce(headers: dict[str, str], name: str) -> None:
    existing = headers.get("content_encoding", "")
    headers["content_encoding"] = f"{existing},{name}" if existing else name


class GzipMiddleware(WireMiddleware):
    """Compress request/response body with gzip if body size >= min_size."""

    def __init__(self, min_size: int = 1024) -> None:
        self._min_size = min_size

    async def send(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
        response: RpcResponse | None = None,
    ) -> tuple[bytes, dict[str, str]]:
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
        if "gzip" in headers.get("content_encoding", ""):
            data = gzip.decompress(data)
        return data, headers
