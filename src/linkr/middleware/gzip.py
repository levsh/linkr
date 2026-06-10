from __future__ import annotations

import gzip
from collections.abc import Callable
from typing import Any

from linkr.middleware.base import WireMiddleware
from linkr.models import RpcContext


def _merge_ce(headers: dict[str, str], name: str) -> None:
    existing = headers.get("content_encoding", "")
    headers["content_encoding"] = f"{existing},{name}" if existing else name


class GzipMiddleware(WireMiddleware):
    """Compress request/response body with gzip if body size >= min_size."""

    def __init__(self, min_size: int = 1024) -> None:
        self._min_size = min_size

    async def dispatch(self, ctx: RpcContext, call_next: Callable[..., Any]) -> RpcContext:  # type: ignore[override]
        if ctx.direction == "request" and ctx.role == "client":
            if len(ctx.body) >= self._min_size:
                ctx.body = gzip.compress(ctx.body)
                _merge_ce(ctx.wire_headers, "gzip")
        elif ctx.direction == "request" and ctx.role == "server":
            if "gzip" in ctx.wire_headers.get("content_encoding", ""):
                ctx.body = gzip.decompress(ctx.body)

        ctx = await call_next(ctx)

        if ctx.direction == "response" and ctx.role == "server":
            if len(ctx.body) >= self._min_size:
                ctx.body = gzip.compress(ctx.body)
                _merge_ce(ctx.wire_headers, "gzip")
        elif ctx.direction == "response" and ctx.role == "client":
            if "gzip" in ctx.wire_headers.get("content_encoding", ""):
                ctx.body = gzip.decompress(ctx.body)

        return ctx
