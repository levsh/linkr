from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Any

from ..models import Message
from ..utils import topic_to_regex
from . import Backend, Handler

logger = logging.getLogger("linkr")


@dataclass
class _Consumer:
    backend: LocalBackend
    handler: Handler


@dataclass
class _EventConsumer:
    backend: LocalBackend
    queue: str
    patterns: dict[str, re.Pattern[str]]
    handler: Handler

    def matches(self, topic: str) -> bool:
        return any(pattern.fullmatch(topic) for pattern in self.patterns.values())


class LocalBackend(Backend):
    """
    In-process backend.

    RPC and task consumers compete for messages: every message is delivered
    to exactly one consumer registered for the queue. Event consumers receive
    every matching event.
    """

    _rpc_consumers: dict[str, list[_Consumer]] = {}
    _task_consumers: dict[str, list[_Consumer]] = {}
    _event_consumers: dict[str, _EventConsumer] = {}
    _round_robin: dict[tuple[str, str], int] = {}

    def __init__(self, name: str | None = None) -> None:
        """
        Args:
            name: Optional backend name. Kept for compatibility; the local
                backend routes by queue name, not by backend name.
        """
        self.name = name or "local"

    async def init(self) -> None:
        """No-op for local backend."""

    async def close(self, timeout: float | None = None) -> None:
        """Unregister all consumers owned by this backend instance."""
        await self.stop_consume_rpc()
        await self.stop_consume_tasks()
        await self.stop_consume_events()

    async def consume_rpc(
        self,
        queue: str,
        handler: Handler,
        *,
        options: Any = None,
    ) -> None:
        """Register an RPC consumer for *queue*."""
        self._add_consumer(self._rpc_consumers, queue, handler)

    async def consume_task(
        self,
        queue: str,
        handler: Handler,
        *,
        options: Any = None,
    ) -> None:
        """Register a task consumer for *queue*."""
        self._add_consumer(self._task_consumers, queue, handler)

    async def consume_event(
        self,
        queue: str,
        topics: list[str],
        handler: Handler,
        *,
        options: Any = None,
    ) -> None:
        """
        Register an event consumer.

        Args:
            queue: Physical queue owned by this consumer instance.
            topics: Topic patterns this queue is bound to.
            handler: Async callable receiving a raw event message.
            options: Optional backend-specific queue/consumer options (ignored).
        """
        self._event_consumers[queue] = _EventConsumer(
            backend=self,
            queue=queue,
            patterns={topic: topic_to_regex(topic) for topic in topics},
            handler=handler,
        )

    async def stop_consume_rpc(self, queue: str | None = None) -> None:
        """Stop RPC consumers owned by this backend instance."""
        self._remove_consumers(self._rpc_consumers, queue=queue)
        self._clear_round_robin("rpc", queue)

    async def stop_consume_tasks(self, queue: str | None = None) -> None:
        """Stop task consumers owned by this backend instance."""
        self._remove_consumers(self._task_consumers, queue=queue)
        self._clear_round_robin("task", queue)

    async def stop_consume_events(self, queue: str | None = None, topic: str | None = None) -> None:
        """Stop event consumers or unbind subscriptions owned by this backend instance."""
        if topic is not None and queue is not None:
            consumer = self._event_consumers.get(queue)
            if consumer is not None and consumer.backend is self:
                consumer.patterns.pop(topic, None)
            return

        if queue is not None:
            consumer = self._event_consumers.get(queue)
            if consumer is not None and consumer.backend is self:
                self._event_consumers.pop(queue, None)
            return

        for queue_name, consumer in list(self._event_consumers.items()):
            if consumer.backend is self:
                self._event_consumers.pop(queue_name, None)

    async def request(
        self,
        queue: str,
        message: Message,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> asyncio.Future[Message]:
        """Send an RPC request and return a Future for the response."""
        handler = self._pick("rpc", queue, self._rpc_consumers.get(queue))
        if handler is None:
            raise RuntimeError(f"no RPC consumer registered for queue: {queue}")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message] = loop.create_future()

        async def run() -> None:
            try:
                response = await handler(message)
                if response is None:
                    if not future.done():
                        future.set_exception(
                            RuntimeError(f"RPC handler for queue {queue!r} returned no response")
                        )
                    return
                if not future.done():
                    future.set_result(response)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)

        asyncio.create_task(run())
        return future

    async def send(
        self,
        queue: str,
        message: Message,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        """Deliver a task to exactly one consumer."""
        handler = self._pick("task", queue, self._task_consumers.get(queue))
        if handler is None:
            logger.warning("No task consumer registered for queue: %s", queue)
            return

        try:
            await handler(message)
        except Exception:
            logger.exception("Task handler failed for queue %s", queue)

    async def emit(
        self,
        topic: str,
        message: Message,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        """Deliver an event to every matching consumer."""
        consumers = [consumer for consumer in list(self._event_consumers.values()) if consumer.matches(topic)]
        if not consumers:
            return

        await asyncio.gather(
            *(self._deliver_event(consumer, message) for consumer in consumers),
        )

    async def _deliver_event(self, consumer: _EventConsumer, message: Message) -> None:
        message = Message(data=message.data, headers=dict(message.headers))
        try:
            await consumer.handler(message)
        except Exception:
            logger.exception("Event handler failed for queue %s", consumer.queue)

    def _add_consumer(
        self,
        registry: dict[str, list[_Consumer]],
        queue: str,
        handler: Handler,
    ) -> None:
        consumers = registry.setdefault(queue, [])
        consumers[:] = [consumer for consumer in consumers if consumer.backend is not self]
        consumers.append(_Consumer(backend=self, handler=handler))

    def _remove_consumers(self, registry: dict[str, list[_Consumer]], queue: str | None = None) -> None:
        queues = [queue] if queue is not None else list(registry)
        for queue_name in queues:
            consumers = registry.get(queue_name)
            if consumers is None:
                continue
            consumers[:] = [consumer for consumer in consumers if consumer.backend is not self]
            if not consumers:
                registry.pop(queue_name, None)

    def _clear_round_robin(self, mode: str, queue: str | None = None) -> None:
        if queue is not None:
            self._round_robin.pop((mode, queue), None)
            return
        for key in [key for key in self._round_robin if key[0] == mode]:
            self._round_robin.pop(key, None)

    def _pick(
        self,
        mode: str,
        queue: str,
        consumers: list[_Consumer] | None,
    ) -> Handler | None:
        if not consumers:
            return None

        key = (mode, queue)
        index = self._round_robin.get(key, 0) % len(consumers)
        self._round_robin[key] = index + 1
        return consumers[index].handler

    @classmethod
    def reset(cls) -> None:
        """Clear all process-wide consumer registries."""
        cls._rpc_consumers.clear()
        cls._task_consumers.clear()
        cls._event_consumers.clear()
        cls._round_robin.clear()
