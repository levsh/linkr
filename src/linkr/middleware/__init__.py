from __future__ import annotations

from abc import ABC
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

from ..models import Message, Request, Response


class BaseMiddleware(ABC):
    """
    Base class for all middleware.

    Provides optional lifecycle hooks that are called by :class:`App`
    during startup and shutdown.
    """

    async def init(self) -> None:
        """
        Initialise middleware resources when the app starts.

        Override this method to set up connections, open files, etc.
        """

    async def close(self) -> None:
        """
        Clean up middleware resources on app shutdown.

        Override this method to release connections, close files, etc.
        """


class AppMiddleware(BaseMiddleware):
    """
    Middleware that works with deserialised request/response objects.

    App-level middleware uses the onion (dispatch) pattern so it can
    perform pre- and post-processing around the inner handler using
    local variables.

    Subclasses should override :meth:`dispatch_client` and/or
    :meth:`dispatch_server` to transform requests and responses.
    """

    async def dispatch_client(
        self,
        call_next: Callable[[], Awaitable[Response | None]],
        request: Request,
        *,
        context: dict[str, Any],
    ) -> Response | None:
        """
        Wrap a client-side call.

        Override this method to perform actions before and/or after
        the rest of the client pipeline (serialize -> wire-level
        dispatch_client -> backend -> deserialize). Call
        ``await call_next()`` to invoke the next layer.

        Args:
            call_next: The next layer in the middleware chain.
            request: The outgoing request.
            context: Additional call context forwarded from the caller.

        Returns:
            The response, or ``None`` for fire-and-forget.
        """
        return await call_next()

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[Response | None]],
        request: Request,
    ) -> Response | None:
        """
        Wrap a server-side request handler.

        Override this method to perform actions before and/or after
        the handler is dispatched. Call ``await call_next()`` to
        invoke the next layer.

        Args:
            call_next: The next layer in the middleware chain.
            request: The deserialised incoming request.

        Returns:
            The response, or ``None`` if no response is sent.
        """
        return await call_next()


class WireMiddleware(BaseMiddleware):
    """
    Middleware that works with raw bytes and wire-level headers.

    Wire-level middleware operates on the serialised byte payload and
    wire-level metadata headers. This is suitable for compression,
    encryption, or custom encoding layers.

    Subclasses should override :meth:`dispatch_client` and/or
    :meth:`dispatch_server` to transform data using the onion pattern.
    Mutate ``request_message.data`` / ``request_message.headers``
    in place before calling ``call_next()`` to affect the outgoing data.
    The response (if any) can be similarly mutated after ``call_next()``
    returns.
    """

    async def dispatch_client(
        self,
        call_next: Callable[[], Awaitable[Message | None]],
        request: Request,
        request_message: Message,
        *,
        context: dict[str, Any],
    ) -> Message | None:
        """
        Wrap a client-side call (request -> backend -> response).

        Override this method to transform the outgoing request bytes
        (by mutating *request_message*) before ``await call_next()``,
        and/or transform the incoming response bytes (by mutating the
        returned :class:`Message`) after ``await call_next()``.

        Args:
            call_next: The next layer in the wire middleware chain.
            request: The original request object.
            request_message: The serialised request (mutate in place).
            context: Additional call context forwarded from the caller.

        Returns:
            The (possibly modified) response :class:`Message`,
            or ``None`` for fire-and-forget.
        """
        return await call_next()

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[tuple[Message, Response] | tuple[None, None]]],
        request_message: Message,
    ) -> tuple[Message, Response] | tuple[None, None]:
        """
        Wrap a server-side request handler (request -> dispatch -> response).

        Override this method to transform the incoming request bytes
        (by mutating *request_message*) before ``await call_next()``,
        and/or transform the outgoing response bytes (by mutating the
        returned :class:`Message`) after ``await call_next()``.

        Args:
            call_next: The next layer in the wire middleware chain.
            request_message: The serialised incoming request (mutate in place).

        Returns:
            A ``(response_message, response)`` tuple where *response_message* is
            the serialised response bytes and *response* is the
            deserialised :class:`Response`.
        """
        return await call_next()


T = TypeVar("T")
Next = Callable[[], Awaitable[T]]


def apply_middleware(
    middlewares: Sequence[Any],
    handler: Next[T],
    wrap: Callable[[Any, Next[T]], Next[T]],
) -> Next[T]:
    """
    Compose middlewares around *handler* using the provided wrapper.

    Args:
        middlewares: Middleware instances in outer-to-inner registration order.
        handler: Innermost handler/callable.
        wrap: Function that wraps a middleware around the next callable.

    Returns:
        The composed callable with all middlewares applied.
    """
    for mw in reversed(middlewares):
        handler = wrap(mw, handler)
    return handler


def wrap_app_client(
    mw: AppMiddleware,
    nxt: Next[Response | None],
    request: Request,
    context: dict[str, Any],
) -> Next[Response | None]:
    """
    Wrap a client pipeline step with an app-level middleware dispatch.

    Args:
        mw: App-level middleware instance.
        nxt: Next callable in the pipeline.
        request: Outgoing request object.
        context: Backend-specific publish/context metadata.

    Returns:
        Callable that executes the middleware dispatch.
    """
    async def call() -> Response | None:
        return await mw.dispatch_client(nxt, request, context=context)

    return call


def wrap_wire_client(
    mw: WireMiddleware,
    nxt: Next[Message | None],
    request: Request,
    message: Message,
    context: dict[str, Any],
) -> Next[Message | None]:
    """
    Wrap a client wire pipeline step with a wire-level middleware dispatch.

    Args:
        mw: Wire-level middleware instance.
        nxt: Next callable in the pipeline.
        request: Outgoing request object.
        message: Serialized request message.
        context: Backend-specific publish/context metadata.

    Returns:
        Callable that executes the middleware dispatch.
    """
    async def call() -> Message | None:
        return await mw.dispatch_client(nxt, request, message, context=context)

    return call


def wrap_app_server(
    mw: AppMiddleware,
    nxt: Next[Response | None],
    request: Request,
) -> Next[Response | None]:
    """
    Wrap a server dispatch step with an app-level middleware dispatch.

    Args:
        mw: App-level middleware instance.
        nxt: Next callable in the pipeline.
        request: Incoming request object.

    Returns:
        Callable that executes the middleware dispatch.
    """
    async def call() -> Response | None:
        return await mw.dispatch_server(nxt, request)

    return call


def wrap_wire_server(
    mw: WireMiddleware,
    nxt: Next[tuple[Message, Response] | tuple[None, None]],
    message: Message,
) -> Next[tuple[Message, Response] | tuple[None, None]]:
    """
    Wrap a server wire pipeline step with a wire-level middleware dispatch.

    Args:
        mw: Wire-level middleware instance.
        nxt: Next callable in the pipeline.
        message: Serialized incoming request message.

    Returns:
        Callable that executes the middleware dispatch.
    """
    async def call() -> tuple[Message, Response] | tuple[None, None]:
        return await mw.dispatch_server(nxt, message)

    return call
