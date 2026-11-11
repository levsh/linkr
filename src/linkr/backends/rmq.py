from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from uuid import uuid4

import aio_pika
from aio_pika import DeliveryMode, ExchangeType
from aio_pika import Message as RobustMessage
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)

from ..models import Message as LinkrMessage
from ..models import QueueOptions
from . import Backend, Handler, Mode

logger = logging.getLogger("linkr")


@dataclass(frozen=True)
class RmqQueueOptions(QueueOptions):
    """RabbitMQ-specific queue/consumer options."""

    durable: bool | None = None
    exclusive: bool | None = None
    auto_delete: bool | None = None
    passive: bool | None = None
    arguments: dict[str, Any] | None = None
    prefetch_count: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)


class RmqBackend(Backend):
    """
    RabbitMQ backend for linkr using aio-pika.

    Declares three named exchanges:

    * ``linkr.rpc`` (direct) — RPC requests
    * ``linkr.tasks`` (direct) — task distribution
    * ``linkr.events`` (topic) — event fan-out with ``*`` and ``#`` wildcards

    RPC responses are delivered through an exclusive, auto-delete callback
    queue matched by ``correlation_id``.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        login: str = "guest",
        password: str = "guest",
        virtualhost: str = "/",
        *,
        ssl_context: ssl.SSLContext | None = None,
        auth_mechanism: str = "PLAIN",
        connection_name: str | None = None,
        rpc_exchange: str = "linkr.rpc",
        task_exchange: str = "linkr.tasks",
        event_exchange: str = "linkr.events",
        exchange_durable: bool = True,
        queue_durable: bool = True,
        prefetch_count: int = 10,
        heartbeat: int = 30,
        blocked_connection_timeout: int = 30,
        results_queue_prefix: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._login = login
        self._password = password
        self._virtualhost = virtualhost
        self._ssl_context = ssl_context
        self._auth_mechanism = auth_mechanism
        self._connection_name = connection_name
        self._rpc_exchange_name = rpc_exchange
        self._task_exchange_name = task_exchange
        self._event_exchange_name = event_exchange
        self._exchange_durable = exchange_durable
        self._queue_durable = queue_durable
        self._prefetch_count = prefetch_count
        self._heartbeat = heartbeat
        self._blocked_connection_timeout = blocked_connection_timeout

        results_queue_prefix = results_queue_prefix or "linkr.res."
        self._results_queue = f"{results_queue_prefix}{uuid4().hex[-8:]}"

        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._rpc_exchange: AbstractExchange | None = None
        self._task_exchange: AbstractExchange | None = None
        self._event_exchange: AbstractExchange | None = None
        self._callback_queue: AbstractQueue | None = None
        self._futures: dict[str, asyncio.Future[LinkrMessage]] = {}
        self._consumers: dict[str, dict[str, tuple[AbstractChannel, AbstractQueue, str]]] = {}
        self._closing = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        connect_kwds: dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "virtualhost": self._virtualhost,
            "heartbeat": self._heartbeat,
            "blocked_connection_timeout": self._blocked_connection_timeout,
        }

        if self._ssl_context is not None:
            connect_kwds["ssl_context"] = self._ssl_context

        if self._auth_mechanism == "PLAIN":
            connect_kwds["login"] = self._login
            connect_kwds["password"] = self._password

        if self._connection_name:
            connect_kwds["client_properties"] = {
                "connection_name": self._connection_name,
            }

        self._connection = await aio_pika.connect_robust(**connect_kwds)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._prefetch_count)

        self._rpc_exchange = await self._channel.declare_exchange(
            self._rpc_exchange_name,
            type=ExchangeType.DIRECT,
            durable=self._exchange_durable,
        )
        self._task_exchange = await self._channel.declare_exchange(
            self._task_exchange_name,
            type=ExchangeType.DIRECT,
            durable=self._exchange_durable,
        )
        self._event_exchange = await self._channel.declare_exchange(
            self._event_exchange_name,
            type=ExchangeType.TOPIC,
            durable=self._exchange_durable,
        )

        self._callback_queue = await self._channel.declare_queue(
            self._results_queue,
            exclusive=True,
            auto_delete=True,
        )
        await self._callback_queue.consume(self._on_rpc_response)

    async def close(self, timeout: float | None = None) -> None:
        self._closing = True
        await self.stop_consume_rpc()
        await self.stop_consume_tasks()
        await self.stop_consume_events()

        if self._callback_queue is not None:
            try:
                await self._callback_queue.delete(if_unused=False, if_empty=False)
            except Exception:
                logger.exception("Failed to delete callback queue")
            self._callback_queue = None

        if self._channel is not None:
            try:
                await self._channel.close()
            except Exception:
                logger.exception("Failed to close channel")
            self._channel = None

        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception:
                logger.exception("Failed to close connection")
            self._connection = None

    async def stop_consume_rpc(self, queue: str | None = None) -> None:
        await self._stop_consumers("rpc", queue=queue)

    async def stop_consume_tasks(self, queue: str | None = None) -> None:
        await self._stop_consumers("task", queue=queue)

    async def stop_consume_events(self, queue: str | None = None, topic: str | None = None) -> None:
        if topic is not None and queue is not None:
            consumer = self._consumers.get("event", {}).get(queue)
            if consumer is None:
                return
            channel, rmq_queue, _ = consumer
            try:
                exchange = await self._get_exchange(channel, "event")
                await rmq_queue.unbind(exchange, routing_key=topic)
            except Exception:  # pragma: no cover - best effort cleanup
                logger.exception("Failed to unbind event topic %s for queue %s", topic, queue)
            return

        await self._stop_consumers("event", queue=queue)

    async def _stop_consumers(self, mode: str, queue: str | None = None) -> None:
        mode_consumers = self._consumers.get(mode, {})
        queue_names = [queue] if queue is not None else list(mode_consumers)
        for queue_name in queue_names:
            consumer = mode_consumers.pop(queue_name, None)
            if consumer is None:
                continue
            channel, rmq_queue, consumer_tag = consumer
            try:
                await rmq_queue.cancel(consumer_tag=consumer_tag)
            except Exception:  # pragma: no cover - best effort cleanup
                logger.exception("Failed to cancel consumer for %s queue %s", mode, queue_name)
            if channel is not self._channel:
                try:
                    await channel.close()
                except Exception:  # pragma: no cover - best effort cleanup
                    logger.exception("Failed to close dedicated channel for %s queue %s", mode, queue_name)
        if not mode_consumers and mode in self._consumers:
            self._consumers.pop(mode, None)

    # ------------------------------------------------------------------
    # Options
    # ------------------------------------------------------------------

    @staticmethod
    def _select_options(options: Sequence[QueueOptions] | None) -> RmqQueueOptions | None:
        for option in options or ():
            if isinstance(option, RmqQueueOptions):
                return option
        return None

    async def _channel_for_prefetch(self, prefetch_count: int | None) -> AbstractChannel:
        if self._connection is None or self._channel is None:
            raise RuntimeError("backend not initialized")
        if prefetch_count is None:
            return self._channel
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=prefetch_count)
        return channel

    async def _get_exchange(
        self,
        channel: AbstractChannel,
        mode: Mode,
    ) -> AbstractExchange:
        if mode == "rpc":
            name, ex_type = self._rpc_exchange_name, ExchangeType.DIRECT
        elif mode == "task":
            name, ex_type = self._task_exchange_name, ExchangeType.DIRECT
        else:
            name, ex_type = self._event_exchange_name, ExchangeType.TOPIC
        return await channel.declare_exchange(
            name,
            type=ex_type,
            durable=self._exchange_durable,
        )

    @staticmethod
    def _queue_declare_kwargs(
        mode: Mode,
        default_durable: bool,
        options: RmqQueueOptions | None,
    ) -> dict[str, Any]:
        if mode == "event":
            durable = False
            exclusive = True
            auto_delete = False
        else:
            durable = default_durable
            exclusive = False
            auto_delete = False

        if options is not None:
            if options.durable is not None:
                durable = options.durable
            if options.exclusive is not None:
                exclusive = options.exclusive
            if options.auto_delete is not None:
                auto_delete = options.auto_delete

        kwargs: dict[str, Any] = {
            "durable": durable,
            "exclusive": exclusive,
            "auto_delete": auto_delete,
        }
        if options is not None and options.passive is not None:
            kwargs["passive"] = options.passive
        if options is not None and options.arguments is not None:
            kwargs["arguments"] = options.arguments
        return kwargs

    # ------------------------------------------------------------------
    # Message conversion helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_robust_message(
        message: LinkrMessage,
        *,
        correlation_id: str | None = None,
        reply_to: str | None = None,
    ) -> RobustMessage:
        headers: dict[str, Any] = dict(message.headers or {})
        if correlation_id is not None:
            headers["correlation_id"] = correlation_id

        ttl = headers.get("ttl")
        expiration: timedelta | None = None
        if ttl is not None:
            expiration = timedelta(seconds=float(ttl))

        return RobustMessage(
            body=message.data,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            expiration=expiration,
        )

    @staticmethod
    def _to_linkr_message(incoming: AbstractIncomingMessage) -> LinkrMessage:
        return LinkrMessage(
            data=incoming.body,
            headers=dict(incoming.headers or {}),
        )

    # ------------------------------------------------------------------
    # Queue conversion helpers
    # ------------------------------------------------------------------

    def linkr_queue_name_to_rmq_queue_name(self, queue: str, mode: Mode) -> str:
        if mode == "rpc":
            return f"linkr.rpc.{queue}"
        if mode == "task":
            return f"linkr.task.{queue}"
        if mode == "event":
            return f"linkr.event.{queue}"
        return queue

    # ------------------------------------------------------------------
    # RPC
    # ------------------------------------------------------------------

    async def consume_rpc(
        self,
        queue: str,
        handler: Handler,
        *,
        options: Sequence[QueueOptions] | None = None,
    ) -> None:
        if self._rpc_exchange is None:
            raise RuntimeError("backend not initialized")

        selected = self._select_options(options)
        channel = await self._channel_for_prefetch(None if selected is None else selected.prefetch_count)
        exchange = await self._get_exchange(channel, "rpc")
        queue_name = self.linkr_queue_name_to_rmq_queue_name(queue, "rpc")
        declare_kwargs = self._queue_declare_kwargs("rpc", self._queue_durable, selected)

        rmq_queue = await channel.declare_queue(queue_name, **declare_kwargs)
        await rmq_queue.bind(exchange, routing_key=queue)

        async def _rpc_handler(incoming: AbstractIncomingMessage) -> None:
            await self._on_rpc_message(incoming, handler)

        consumer_tag = await rmq_queue.consume(_rpc_handler)
        self._consumers.setdefault("rpc", {})[queue] = (channel, rmq_queue, consumer_tag)

    async def _on_rpc_message(
        self,
        incoming: AbstractIncomingMessage,
        handler: Handler,
    ) -> None:
        async with incoming.process():
            linkr_message = self._to_linkr_message(incoming)
            response_message = await handler(linkr_message)

            if response_message is not None and incoming.reply_to and self._channel is not None:
                await self._channel.default_exchange.publish(
                    RobustMessage(
                        body=response_message.data,
                        headers=response_message.headers,
                        correlation_id=incoming.correlation_id,
                        content_type="application/json",
                        delivery_mode=DeliveryMode.PERSISTENT,
                    ),
                    routing_key=incoming.reply_to,
                )

    async def request(
        self,
        queue: str,
        message: LinkrMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> asyncio.Future[LinkrMessage]:
        if self._channel is None or self._rpc_exchange is None or self._callback_queue is None:
            raise RuntimeError("backend not initialized")

        correlation_id = str(uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[LinkrMessage] = loop.create_future()
        self._futures[correlation_id] = future

        try:
            robust_message = self._to_robust_message(
                message,
                correlation_id=correlation_id,
                reply_to=self._callback_queue.name,
            )
            await self._rpc_exchange.publish(robust_message, routing_key=queue)
        except Exception:
            self._futures.pop(correlation_id, None)
            if not future.done():
                future.cancel()
            raise

        def _cleanup(_fut: asyncio.Future[LinkrMessage]) -> None:
            self._futures.pop(correlation_id, None)

        future.add_done_callback(_cleanup)
        return future

    async def _on_rpc_response(self, incoming: AbstractIncomingMessage) -> None:
        correlation_id = incoming.correlation_id
        if correlation_id is None:
            await incoming.ack()
            return

        future = self._futures.pop(correlation_id, None)
        if future is None:
            await incoming.ack()
            return

        response = self._to_linkr_message(incoming)
        if not future.done():
            future.set_result(response)

        await incoming.ack()

    # ------------------------------------------------------------------
    # Task
    # ------------------------------------------------------------------

    async def consume_task(
        self,
        queue: str,
        handler: Handler,
        *,
        options: Sequence[QueueOptions] | None = None,
    ) -> None:
        if self._task_exchange is None:
            raise RuntimeError("backend not initialized")

        selected = self._select_options(options)
        channel = await self._channel_for_prefetch(None if selected is None else selected.prefetch_count)
        exchange = await self._get_exchange(channel, "task")
        queue_name = self.linkr_queue_name_to_rmq_queue_name(queue, "task")
        declare_kwargs = self._queue_declare_kwargs("task", self._queue_durable, selected)

        rmq_queue = await channel.declare_queue(queue_name, **declare_kwargs)
        await rmq_queue.bind(exchange, routing_key=queue)

        async def _task_handler(incoming: AbstractIncomingMessage) -> None:
            await self._on_task_message(incoming, handler)

        consumer_tag = await rmq_queue.consume(_task_handler)
        self._consumers.setdefault("task", {})[queue] = (channel, rmq_queue, consumer_tag)

    async def _on_task_message(
        self,
        incoming: AbstractIncomingMessage,
        handler: Handler,
    ) -> None:
        async with incoming.process():
            linkr_message = self._to_linkr_message(incoming)
            await handler(linkr_message)

    async def send(
        self,
        queue: str,
        message: LinkrMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        if self._channel is None or self._task_exchange is None:
            raise RuntimeError("backend not initialized")

        robust_message = self._to_robust_message(message)
        await self._task_exchange.publish(robust_message, routing_key=queue)

    # ------------------------------------------------------------------
    # Event
    # ------------------------------------------------------------------

    async def consume_event(
        self,
        queue: str,
        topics: list[str],
        handler: Handler,
        *,
        options: Sequence[QueueOptions] | None = None,
    ) -> None:
        if self._event_exchange is None:
            raise RuntimeError("backend not initialized")

        selected = self._select_options(options)
        channel = await self._channel_for_prefetch(None if selected is None else selected.prefetch_count)
        exchange = await self._get_exchange(channel, "event")
        queue_name = self.linkr_queue_name_to_rmq_queue_name(queue, "event")
        declare_kwargs = self._queue_declare_kwargs("event", self._queue_durable, selected)

        rmq_queue = await channel.declare_queue(queue_name, **declare_kwargs)
        for topic in topics:
            await rmq_queue.bind(exchange, routing_key=topic)

        async def _event_handler(incoming: AbstractIncomingMessage) -> None:
            await self._on_event_message(incoming, handler)

        consumer_tag = await rmq_queue.consume(_event_handler)
        self._consumers.setdefault("event", {})[queue] = (channel, rmq_queue, consumer_tag)

    async def _on_event_message(
        self,
        incoming: AbstractIncomingMessage,
        handler: Handler,
    ) -> None:
        async with incoming.process():
            linkr_message = self._to_linkr_message(incoming)
            await handler(linkr_message)

    async def emit(
        self,
        topic: str,
        message: LinkrMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        if self._channel is None or self._event_exchange is None:
            raise RuntimeError("backend not initialized")

        robust_message = self._to_robust_message(message)
        await self._event_exchange.publish(robust_message, routing_key=topic)
