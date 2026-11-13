from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from ..models import Message, QueueOptions, RequestType

Mode = RequestType
Handler = Callable[[Message], Awaitable[Message | None]]


class Backend(ABC):
    """
    Abstract backend.

    Backend operates on raw bytes. Serialization and encoding are
    handled by :class:`App` before data reaches the backend.

    The backend exposes three independent delivery modes:

    * ``rpc`` — competing consumers, one request is handled by exactly one
      consumer and receives a reply.
    * ``task`` — competing consumers, one task is handled by exactly one
      consumer, no reply.
    * ``event`` — fan-out, every subscribed consumer receives the event,
      no reply.
    """

    @abstractmethod
    async def init(self) -> None:
        """Initialize backend and declare required infrastructure."""

    @abstractmethod
    async def close(self, timeout: float | None = None) -> None:
        """Gracefully shut down the backend."""

    @abstractmethod
    async def consume_rpc(
        self,
        queue: str,
        handler: Handler,
        *,
        options: Sequence[QueueOptions] | None = None,
    ) -> None:
        """Register an RPC consumer for *queue*."""

    @abstractmethod
    async def consume_task(
        self,
        queue: str,
        handler: Handler,
        *,
        options: Sequence[QueueOptions] | None = None,
    ) -> None:
        """Register a task consumer for *queue*."""

    @abstractmethod
    async def consume_event(
        self,
        queue: str,
        topics: list[str],
        handler: Handler,
    ) -> None:
        """
        Register an event consumer.

        Args:
            queue: Physical queue owned by this consumer instance.
            topics: Topic patterns this queue is bound to.
            handler: Async callable receiving a raw event message.
        """

    @abstractmethod
    async def stop_consume_rpc(self, queue: str | None = None) -> None:
        """
        Stop RPC consumers owned by this backend instance.

        Args:
            queue: When ``None``, stop all RPC consumers. Otherwise stop the
                consumer for the specified queue.
        """

    @abstractmethod
    async def stop_consume_tasks(self, queue: str | None = None) -> None:
        """
        Stop task consumers owned by this backend instance.

        Args:
            queue: When ``None``, stop all task consumers. Otherwise stop the
                consumer for the specified queue.
        """

    @abstractmethod
    async def stop_consume_events(self, queue: str | None = None, topic: str | None = None) -> None:
        """
        Stop event consumers or unbind event subscriptions owned by this
        backend instance.

        Args:
            queue: When ``None``, stop all event consumers. Otherwise target
                the specified event queue.
            topic: When provided together with *queue*, remove only the
                subscription for this topic pattern without stopping the
                consumer. When ``None``, stop the targeted event consumer(s).
        """

    async def stop_consume(self, mode: Mode | None = None) -> None:
        """Deprecated compatibility shim. Use ``stop_consume_*`` methods."""
        if mode in (None, "rpc"):
            await self.stop_consume_rpc()
        if mode in (None, "task"):
            await self.stop_consume_tasks()
        if mode in (None, "event"):
            await self.stop_consume_events()

    @abstractmethod
    async def request(
        self,
        queue: str,
        message: Message,
        *,
        context: dict[str, Any] | None = None,
    ) -> asyncio.Future[Message]:
        """
        Send an RPC request and return a Future for the response.

        The method completes once the request has been published and response
        waiting has been registered. Await the returned Future to receive the
        response message.
        """

    @abstractmethod
    async def send(
        self,
        queue: str,
        message: Message,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Send a task to *queue*. No response is expected."""

    @abstractmethod
    async def emit(
        self,
        topic: str,
        message: Message,
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Publish an event to *topic*. No response is expected."""
