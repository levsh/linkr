from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from ..models import RawMessage, Request
from . import Transport


class LocalTransport(Transport):
    """
    In-process local transport for inter-app communication without external brokers.

    Messages are routed via class-level registries based on queue names,
    allowing multiple App instances to communicate locally within the same process.
    """

    _handlers: dict[str, list[tuple[LocalTransport, Callable[[RawMessage], Awaitable[RawMessage | None]]]]] = {}

    def __init__(self, server_queue_name: str = "rpc") -> None:
        self._server_queue_name = server_queue_name
        self._registered_queues: set[tuple[str, Callable[[RawMessage], Awaitable[RawMessage | None]]]] = set()

    @classmethod
    def reset(cls) -> None:
        """Reset all registered handlers and state (useful for tests)."""
        cls._handlers.clear()

    async def init(self) -> None:
        """No-op for local transport."""

    async def close(self, timeout: float | None = None) -> None:
        """Unregister all queues owned by this transport instance."""
        await self.stop_consume()

    async def consume(
        self,
        handler: Callable[[RawMessage], Awaitable[RawMessage | None]],
        queue: str | None = None,
    ) -> None:
        """
        Register a request handler to consume from a local queue.
        """
        queue_name = (
            f"{self._server_queue_name}.{queue.replace('/', '.')}" if queue is not None else self._server_queue_name
        )
        self._handlers.setdefault(queue_name, []).append((self, handler))
        self._registered_queues.add((queue_name, handler))

    async def stop_consume(self) -> None:
        """Unregister all consuming queues owned by this transport instance."""
        for q, handler in self._registered_queues:
            if q in self._handlers:
                self._handlers[q] = [item for item in self._handlers[q] if item[0] != self or item[1] != handler]
                if not self._handlers[q]:
                    self._handlers.pop(q, None)
        self._registered_queues.clear()

    def _get_queue_name(self, request: Request) -> str:
        q = request.headers.get("queue")
        if q:
            return f"{self._server_queue_name}.{q.replace('/', '.')}"
        return self._server_queue_name

    async def publish(
        self,
        request: Request,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        """Publish a fire-and-forget message: to specific queue or fan-out to all subscriber queues for the topic."""
        topic = request.method
        evt_prefix = f"{self._server_queue_name}.evt.{topic}."

        subscriber_handlers = []
        for q, h_list in self._handlers.items():
            if q.startswith(evt_prefix) or q == f"{self._server_queue_name}.evt.{topic}":
                subscriber_handlers.extend([item[1] for item in h_list])

        if subscriber_handlers:
            await asyncio.gather(*(h(message) for h in subscriber_handlers), return_exceptions=True)
        else:
            queue_name = self._get_queue_name(request)
            h_list = self._handlers.get(queue_name, [])
            if h_list:
                await h_list[0][1](message)

    async def request(
        self,
        request: Request,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage:
        """Send an RPC request and await the response from the consumer."""
        queue_name = self._get_queue_name(request)
        h_list = self._handlers.get(queue_name, [])
        if not h_list:
            raise RuntimeError(f"no consumer registered for queue: {queue_name}")

        response = await h_list[0][1](message)
        if response is None:
            raise RuntimeError("handler returned None, expected RawMessage")
        return response
