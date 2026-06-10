from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from linkr.models import RpcContext

if TYPE_CHECKING:
    from linkr.app import RpcApp


class BaseMiddleware:
    """
    Base class for RPC middleware.

    Subclasses override dispatch() to intercept the request/response
    flow. Optional lifecycle hooks: init() and close().
    """

    async def init(self, app: RpcApp) -> None:
        """
        Initialize middleware when the app starts.

        Args:
            app: The RpcApp instance.
        """

    async def close(self) -> None:
        """Clean up middleware resources on app shutdown."""

    async def dispatch(
        self,
        ctx: RpcContext,
        call_next: Callable[..., Any],
    ) -> RpcContext:
        """
        Intercept the request/response context.

        Call call_next(ctx) to continue the chain, or return ctx early
        to short-circuit.

        Args:
            ctx: The current RpcContext.
            call_next: Continuation that invokes the next middleware or handler.

        Returns:
            The (possibly mutated) RpcContext.
        """
        return await call_next(ctx)


class AppMiddleware(BaseMiddleware):
    """Base for middleware that works with deserialized request/response objects."""


class WireMiddleware(BaseMiddleware):
    """Base for middleware that works with raw bytes in RpcContext.body."""
