from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Callable
from typing import Any, Generic, Literal, NamedTuple, TypeVar

from .models import HandlerInfo, Request

logger = logging.getLogger("linkr")

Mode = Literal["rpc", "task", "event"]

DropReason = Literal[
    "invalid_request",
    "handler_not_found",
    "validation_failed",
    "no_subscribers",
    "decode_error",
]

ConsumeStopReason = Literal["all", "queue_drained", "topic_unsubscribed"]


class ConsumeStartEvent(NamedTuple):
    """Event emitted when consuming starts for a queue."""

    app: str
    mode: Mode
    queue: str
    topics: tuple[str, ...]


class ConsumeStopEvent(NamedTuple):
    """Event emitted when consuming stops or a subscription is removed."""

    app: str
    mode: Mode
    queue: str | None
    topic: str | None
    reason: ConsumeStopReason


class RequestPublishedEvent(NamedTuple):
    """Event emitted after a client-side request publish/send attempt completes."""

    app: str
    request: Request
    outcome: Literal["ok", "error"]
    error: BaseException | None = None


class ResponseReceivedEvent(NamedTuple):
    """Event emitted after a client-side RPC response/outcome is received."""

    app: str
    request: Request
    response: Any
    outcome: Literal["ok", "error", "timeout"]
    error: BaseException | None = None


class RequestReceivedEvent(NamedTuple):
    """Event emitted after an incoming request is deserialized."""

    app: str
    mode: Mode
    queue: str
    request: Request


class BeforeHandlerEvent(NamedTuple):
    """Event emitted before a handler is executed."""

    app: str
    mode: Mode
    queue: str
    request: Request
    handler: HandlerInfo
    topic: str | None = None


class AfterHandlerEvent(NamedTuple):
    """Event emitted after a handler completes successfully."""

    app: str
    mode: Mode
    queue: str
    request: Request
    handler: HandlerInfo
    topic: str | None = None
    result: Any = None


class HandlerErrorEvent(NamedTuple):
    """Event emitted when a handler fails or times out."""

    app: str
    mode: Mode
    queue: str
    request: Request
    handler: HandlerInfo
    topic: str | None = None
    error: BaseException | None = None
    error_code: str | None = None
    error_message: str | None = None
    outcome: Literal["error", "timeout"] = "error"


class MessageDroppedEvent(NamedTuple):
    """Event emitted when a message is dropped without handler execution."""

    app: str
    mode: Mode | None
    queue: str | None
    reason: DropReason
    request: Request | None = None
    handler: HandlerInfo | None = None
    topic: str | None = None
    error: BaseException | None = None
    details: Any = None


E = TypeVar("E")
Listener = Callable[[E], Any]


class HookPoint(Generic[E]):
    """Subscription point for a specific hook event."""

    def __init__(self, name: str) -> None:
        """
        Args:
            name: Human-readable name for this hook point.
        """
        self.name = name
        self._listeners: list[Listener[E]] = []

    def subscribe(self, listener: Listener[E]) -> Listener[E]:
        """Register *listener* for this hook point. Returns the listener."""

        if listener not in self._listeners:
            self._listeners.append(listener)
        return listener

    __iadd__ = subscribe

    def unsubscribe(self, listener: Listener[E]) -> None:
        """Remove *listener* from this hook point (no-op if not registered)."""

        try:
            self._listeners.remove(listener)
        except ValueError:
            pass

    def __call__(self, listener: Listener[E]) -> Listener[E]:
        """Allow usage as a decorator."""
        return self.subscribe(listener)

    def __bool__(self) -> bool:
        return bool(self._listeners)

    async def emit(self, event: E) -> None:
        """Emit *event* to all registered listeners concurrently."""

        if not self._listeners:
            return

        listeners = list(self._listeners)

        async def run_listener(listener: Listener[E]) -> None:
            try:
                result = listener(event)
                if inspect.isawaitable(result):
                    await result
            except Exception:
                logger.exception("Hook listener failed: %s", self.name)

        await asyncio.gather(*(run_listener(listener) for listener in listeners))


class AppHooks:
    """Base class for app-level hook observers. Override only needed methods."""

    async def on_consume_start(self, event: ConsumeStartEvent) -> None:
        """Called when consuming starts for a queue."""

    async def on_consume_stop(self, event: ConsumeStopEvent) -> None:
        """Called when consuming stops or a subscription is removed."""

    async def on_request_published(self, event: RequestPublishedEvent) -> None:
        """Called after a client-side request publish/send attempt completes."""

    async def on_response_received(self, event: ResponseReceivedEvent) -> None:
        """Called after a client-side RPC response/outcome is received."""

    async def on_request_received(self, event: RequestReceivedEvent) -> None:
        """Called after an incoming request is deserialized."""

    async def before_handler(self, event: BeforeHandlerEvent) -> None:
        """Called before a handler is executed."""

    async def after_handler(self, event: AfterHandlerEvent) -> None:
        """Called after a handler completes successfully."""

    async def on_handler_error(self, event: HandlerErrorEvent) -> None:
        """Called when a handler fails or times out."""

    async def on_message_dropped(self, event: MessageDroppedEvent) -> None:
        """Called when a message is dropped without handler execution."""


HOOK_NAMES = (
    "on_consume_start",
    "on_consume_stop",
    "on_request_published",
    "on_response_received",
    "on_request_received",
    "before_handler",
    "after_handler",
    "on_handler_error",
    "on_message_dropped",
)


class HookRegistry:
    """Registry of hook points for an :class:`App`."""

    def __init__(self) -> None:
        """Create a registry with all built-in hook points."""
        self.on_consume_start: HookPoint[ConsumeStartEvent] = HookPoint("on_consume_start")
        self.on_consume_stop: HookPoint[ConsumeStopEvent] = HookPoint("on_consume_stop")
        self.on_request_published: HookPoint[RequestPublishedEvent] = HookPoint("on_request_published")
        self.on_response_received: HookPoint[ResponseReceivedEvent] = HookPoint("on_response_received")
        self.on_request_received: HookPoint[RequestReceivedEvent] = HookPoint("on_request_received")
        self.before_handler: HookPoint[BeforeHandlerEvent] = HookPoint("before_handler")
        self.after_handler: HookPoint[AfterHandlerEvent] = HookPoint("after_handler")
        self.on_handler_error: HookPoint[HandlerErrorEvent] = HookPoint("on_handler_error")
        self.on_message_dropped: HookPoint[MessageDroppedEvent] = HookPoint("on_message_dropped")
        self._hook_objects: dict[int, dict[str, Listener[Any]]] = {}

    def _point(self, name: str) -> HookPoint[Any]:
        return getattr(self, name)

    async def emit(self, name: str, event: Any) -> None:
        """Emit *event* to the hook point named *name*."""
        await self._point(name).emit(event)

    def add_hooks(self, hooks: AppHooks) -> None:
        """Register all non-default hook methods of *hooks*."""
        key = id(hooks)
        if key in self._hook_objects:
            return
        bound: dict[str, Listener[Any]] = {}
        for name in HOOK_NAMES:
            method = getattr(hooks, name, None)
            base = getattr(AppHooks, name, None)
            if method is None or method is base:
                continue
            self._point(name).subscribe(method)
            bound[name] = method
        if bound:
            self._hook_objects[key] = bound

    def remove_hooks(self, hooks: AppHooks) -> None:
        """Unregister all hook methods previously registered from *hooks*."""
        key = id(hooks)
        bound = self._hook_objects.pop(key, None)
        if bound is None:
            return
        for name, listener in bound.items():
            self._point(name).unsubscribe(listener)
