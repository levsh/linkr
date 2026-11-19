from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Callable, Sequence
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

from ..models import HealthStatus, QueueOptions
from ..models import Message as LinkrMessage
from . import Backend, Handler, Mode

logger = logging.getLogger("linkr")


@dataclass(frozen=True)
class RmqQueueOptions(QueueOptions):
    """
    RabbitMQ-specific queue/consumer options.

    Attributes:
        durable: Queue durability. ``None`` uses the backend/mode default.
        exclusive: Queue exclusivity. ``None`` uses the backend/mode default.
        auto_delete: Auto-delete queue when no consumers. ``None`` uses the
            backend/mode default.
        passive: Declare queue passively (do not create). ``None`` omits.
        arguments: Additional queue declaration arguments (e.g. ``x-max-length``).
        prefetch_count: Consumer prefetch count. ``None`` omits (unlimited).
        extra: Reserved for future backend-specific options.
    """

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

    Exchange topology is configured via the ``exchange`` parameter:
    * **Single exchange (default):** ``exchange="linkr"`` (str) — one TOPIC exchange
      shared by RPC, tasks, and events.
    * **Per-mode exchanges:** ``exchange={"rpc": "...", "task": "...", "event": "..."}``
      — separate exchanges for each mode (all three keys required). RPC and tasks use
      DIRECT type; events use TOPIC type.

    RPC responses are delivered through an exclusive callback queue matched by ``correlation_id``.
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
        exchange: str | dict[Mode, str] = "linkr",
        exchange_durable: bool | dict[Mode, bool] = True,
        queue_name_template: dict[Mode, str] | Callable[[str, Mode], str] | None = None,
        heartbeat: int = 30,
        blocked_connection_timeout: int = 30,
        results_queue_prefix: str | None = None,
    ) -> None:
        """
        Create a RabbitMQ backend.

        Args:
            host: RabbitMQ host.
            port: RabbitMQ port.
            login: Login used for AMQP authentication.
            password: Password used for AMQP authentication.
            virtualhost: AMQP virtual host.
            ssl_context: Optional ``ssl.SSLContext`` for a TLS connection.
            auth_mechanism: SASL authentication mechanism (default ``"PLAIN"``).
            connection_name: Optional name exposed to the broker.
            exchange: Exchange name(s). A single ``str`` creates one shared
                TOPIC exchange for all modes; a ``dict[Mode, str]`` creates
                separate exchanges per mode (all keys required).
            exchange_durable: Whether the exchange(s) are durable. ``bool`` or
                ``dict[Mode, bool]`` (same shape as *exchange*).
            queue_name_template: Mapping from delivery mode to a format string
                (or callable ``(queue, mode) -> name``) used to derive the
                physical RabbitMQ queue name from the logical linkr queue name.
                Must contain keys ``"rpc"``, ``"task"``, ``"event"``. The
                resulting ``rpc`` and ``task`` names must differ. Example:
                ``{"rpc": "{queue}", "task": "{queue}.tasks", "event": "{queue}.events"}``.
            heartbeat: Heartbeat interval in seconds.
            blocked_connection_timeout: Timeout in seconds for blocked connections.
            results_queue_prefix: Prefix for the exclusive reply queue used by
                RPC calls. Defaults to ``"linkr.res."``.
        """
        self._host = host
        self._port = port
        self._login = login
        self._password = password
        self._virtualhost = virtualhost
        self._ssl_context = ssl_context
        self._auth_mechanism = auth_mechanism
        self._connection_name = connection_name
        self._heartbeat = heartbeat
        self._blocked_connection_timeout = blocked_connection_timeout

        results_queue_prefix = results_queue_prefix or "linkr.res."
        self._results_queue = f"{results_queue_prefix}{uuid4().hex[-8:]}"

        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._exchanges: dict[Mode, AbstractExchange] = {}
        self._callback_queue: AbstractQueue | None = None
        self._futures: dict[str, asyncio.Future[LinkrMessage]] = {}
        self._consumers: dict[str, dict[str, tuple[AbstractChannel, AbstractQueue, str]]] = {}

        self._op_lock = asyncio.Lock()

        self._initialized = False
        self._closed = False

        # Normalize exchange names and durables
        modes: tuple[Mode, ...] = ("rpc", "task", "event")
        if isinstance(exchange, str):
            exchange_names = {mode: exchange for mode in modes}
            exchange_types = {mode: ExchangeType.TOPIC for mode in modes}
        else:
            if not isinstance(exchange, dict):
                raise TypeError("exchange must be str or dict[Mode, str]")
            missing = [m for m in modes if m not in exchange]
            if missing:
                raise ValueError(f"exchange dict must contain all modes: {missing}")
            extra = [k for k in exchange if k not in modes]
            if extra:
                raise ValueError(f"exchange dict contains invalid mode keys: {extra}")
            exchange_names = {mode: str(exchange[mode]) for mode in modes}
            for mode, name in exchange_names.items():
                if not name:
                    raise ValueError(f"exchange name for mode {mode!r} must not be empty")
            exchange_types = {
                "rpc": ExchangeType.DIRECT,
                "task": ExchangeType.DIRECT,
                "event": ExchangeType.TOPIC,
            }

        if isinstance(exchange_durable, bool):
            exchange_durables = {mode: exchange_durable for mode in modes}
        else:
            if not isinstance(exchange_durable, dict):
                raise TypeError("exchange_durable must be bool or dict[Mode, bool]")
            missing = [m for m in modes if m not in exchange_durable]
            if missing:
                raise ValueError(f"exchange_durable dict must contain all modes: {missing}")
            extra = [k for k in exchange_durable if k not in modes]
            if extra:
                raise ValueError(f"exchange_durable dict contains invalid mode keys: {extra}")
            exchange_durables = {mode: bool(exchange_durable[mode]) for mode in modes}

        # Normalize queue name template (default = legacy prefixes)
        if queue_name_template is None:
            queue_name_template = {
                "rpc": "linkr.{queue}.rpc",
                "task": "linkr.{queue}.tasks",
                "event": "linkr.{queue}.events",
            }
        if isinstance(queue_name_template, dict):
            missing = [m for m in ("rpc", "task", "event") if m not in queue_name_template]
            if missing:
                raise ValueError(f"queue_name_template missing modes: {missing}")
            names = {m: queue_name_template[m].format(queue="__q__") for m in ("rpc", "task", "event")}
            if names["rpc"] == names["task"]:
                raise ValueError("rpc and task queue names must differ")

        self._exchange_names = exchange_names
        self._exchange_types = exchange_types
        self._exchange_durables = exchange_durables
        self._queue_name_template: dict[Mode, str] | Callable[[str, Mode], str] = queue_name_template  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Initialize the RabbitMQ backend: connect, declare exchanges and the reply queue."""

        async with self._op_lock:
            if self._initialized and not self._closed:
                return

            # Reset runtime state (supports re-init after close or failed init).
            self._connection = None
            self._channel = None
            self._callback_queue = None
            self._exchanges = {}
            self._consumers = {}
            self._fail_pending_futures(RuntimeError("backend reinitializing"))
            self._initialized = False
            self._closed = False

            try:
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

                for mode in ("rpc", "task", "event"):
                    self._exchanges[mode] = await self._channel.declare_exchange(
                        self._exchange_names[mode],
                        type=self._exchange_types[mode],
                        durable=self._exchange_durables[mode],
                    )

                self._callback_queue = await self._channel.declare_queue(
                    self._results_queue,
                    exclusive=True,
                    auto_delete=False,
                )
                await self._callback_queue.consume(self._on_rpc_response)

                self._initialized = True
                self._closed = False
            except Exception:
                await self._teardown_resources(delete_callback_queue=False)
                self._fail_pending_futures(RuntimeError("backend initialization failed"))
                self._exchanges = {}
                self._consumers = {}
                self._initialized = False
                self._closed = True
                raise

    async def close(self) -> None:
        """Close the RabbitMQ backend: stop consumers, drop the reply queue and disconnect."""
        async with self._op_lock:
            if self._closed and not self._initialized:
                return

            await self._stop_consumers("rpc")
            await self._stop_consumers("task")
            await self._stop_consumers("event")

            self._fail_pending_futures(RuntimeError("backend closed"))

            await self._teardown_resources(delete_callback_queue=True)

            self._exchanges = {}
            self._consumers = {}
            self._initialized = False
            self._closed = True

    def _fail_pending_futures(self, exc: BaseException) -> None:
        futures = list(self._futures.values())
        self._futures.clear()
        for future in futures:
            if not future.done():
                future.set_exception(exc)

    async def _teardown_resources(self, *, delete_callback_queue: bool) -> None:
        callback_queue = self._callback_queue
        channel = self._channel
        connection = self._connection
        self._callback_queue = None
        self._channel = None
        self._connection = None

        if callback_queue is not None and delete_callback_queue:
            try:
                await callback_queue.delete(if_unused=False, if_empty=False)
            except Exception:
                logger.exception("Failed to delete callback queue")

        if channel is not None:
            try:
                await channel.close()
            except Exception:
                logger.exception("Failed to close channel")

        if connection is not None:
            try:
                await connection.close()
            except Exception:
                logger.exception("Failed to close connection")

    async def stop_consume_rpc(self, queue: str | None = None) -> None:
        """Stop RPC consumers owned by this backend instance.

        Args:
            queue: When ``None``, stop all RPC consumers. Otherwise stop the
                consumer for the specified queue.
        """

        async with self._op_lock:
            await self._stop_consumers("rpc", queue=queue)

    async def stop_consume_tasks(self, queue: str | None = None) -> None:
        """Stop task consumers owned by this backend instance.

        Args:
            queue: When ``None``, stop all task consumers. Otherwise stop the
                consumer for the specified queue.
        """

        async with self._op_lock:
            await self._stop_consumers("task", queue=queue)

    async def stop_consume_events(self, queue: str | None = None, topic: str | None = None) -> None:
        """Stop event consumers or unbind event subscriptions owned by this backend instance.

        Args:
            queue: When ``None``, stop all event consumers. Otherwise target
                the specified event queue.
            topic: When provided together with *queue*, remove only the
                subscription for this topic pattern without stopping the
                consumer. When ``None``, stop the targeted event consumer(s).
        """

        async with self._op_lock:
            if topic is not None and queue is not None:
                consumer = self._consumers.get("event", {}).get(queue)
                if consumer is None:
                    return
                channel, rmq_queue, _ = consumer
                try:
                    exchange = await self._get_exchange(channel, "event")
                    await rmq_queue.unbind(exchange, routing_key=topic)
                except Exception:  # pragma: no cover - best effort cleanup
                    logger.debug("Failed to unbind event topic %s for queue %s", topic, queue, exc_info=True)
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
        if self._exchanges.get(mode) is not None and channel is self._channel:
            return self._exchanges[mode]
        return await channel.declare_exchange(
            self._exchange_names[mode],
            type=self._exchange_types[mode],
            durable=self._exchange_durables[mode],
        )

    @staticmethod
    def _queue_declare_kwargs(
        mode: Mode,
        options: RmqQueueOptions | None,
    ) -> dict[str, Any]:
        if mode == "event":
            durable = False
            exclusive = True
            auto_delete = False
        else:
            durable = True
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
        headers = message.headers
        if correlation_id is not None:
            headers = {**headers, "correlation_id": correlation_id}

        ttl = headers.get("ttl")
        expiration: timedelta | None = None
        if ttl is not None:
            expiration = timedelta(seconds=float(ttl))

        return RobustMessage(
            body=message.data,
            headers=headers,
            correlation_id=correlation_id,
            reply_to=reply_to,
            content_type=headers.get("content_type"),
            content_encoding=headers.get("content_encoding"),
            delivery_mode=DeliveryMode.PERSISTENT,
            expiration=expiration,
        )

    @staticmethod
    def _to_linkr_message(incoming: AbstractIncomingMessage) -> LinkrMessage:
        return LinkrMessage(
            data=incoming.body,
            headers=incoming.headers or {},
        )

    # ------------------------------------------------------------------
    # Queue conversion helpers
    # ------------------------------------------------------------------

    def queue_name_to_rmq_queue_name(self, queue: str, mode: Mode) -> str:
        """
        Map a linkr queue name to the physical RabbitMQ queue name for *mode*.

        Uses the configured ``queue_name_template``.

        Args:
            queue: Logical linkr queue name.
            mode: Delivery mode (``"rpc"``, ``"task"``, or ``"event"``).

        Returns:
            The physical RabbitMQ queue name.
        """
        if callable(self._queue_name_template):
            return self._queue_name_template(queue, mode)
        fmt = self._queue_name_template.get(mode)  # type: ignore[union-attr]
        if fmt is None:
            raise ValueError(f"no template for mode {mode!r}")
        return fmt.format(queue=queue)

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
        """
        Register an RPC consumer for *queue*.

        Args:
            queue: Queue name to consume from.
            handler: Async callable that processes incoming messages.
            options: Optional backend-specific queue options.

        Raises:
            RuntimeError: If the backend is not initialized.
        """
        async with self._op_lock:
            if self._closed or not self._initialized or self._exchanges.get("rpc") is None:
                raise RuntimeError("backend not initialized")
            if queue in self._consumers.get("rpc", {}):
                return

            selected = self._select_options(options)
            channel = await self._channel_for_prefetch(None if selected is None else selected.prefetch_count)
            exchange = await self._get_exchange(channel, "rpc")
            queue_name = self.queue_name_to_rmq_queue_name(queue, "rpc")
            declare_kwargs = self._queue_declare_kwargs("rpc", selected)

            consumer_tag: str | None = None
            try:
                rmq_queue = await channel.declare_queue(queue_name, **declare_kwargs)
                await rmq_queue.bind(exchange, routing_key=queue_name)

                async def _rpc_handler(incoming: AbstractIncomingMessage) -> None:
                    await self._on_rpc_message(incoming, handler)

                consumer_tag = await rmq_queue.consume(_rpc_handler)
                self._consumers.setdefault("rpc", {})[queue] = (channel, rmq_queue, consumer_tag)
            except Exception:
                if consumer_tag is not None:
                    try:
                        await rmq_queue.cancel(consumer_tag=consumer_tag)
                    except Exception:  # pragma: no cover - best effort cleanup
                        logger.exception("Failed to rollback RPC consumer for queue %s", queue)
                if channel is not self._channel:
                    try:
                        await channel.close()
                    except Exception:  # pragma: no cover - best effort cleanup
                        logger.exception("Failed to close dedicated RPC channel for queue %s", queue)
                raise

    async def _on_rpc_message(
        self,
        incoming: AbstractIncomingMessage,
        handler: Handler,
    ) -> None:
        async with incoming.process():
            linkr_message = self._to_linkr_message(incoming)
            response_message = await handler(linkr_message)

            if response_message is not None and incoming.reply_to and self._channel is not None:
                rttl = response_message.headers.get("rttl")
                expiration: timedelta | None = None
                if rttl is not None:
                    expiration = timedelta(seconds=float(rttl))

                await self._channel.default_exchange.publish(
                    RobustMessage(
                        body=response_message.data,
                        headers=response_message.headers,
                        correlation_id=incoming.correlation_id,
                        content_type=response_message.headers.get("content_type"),
                        content_encoding=response_message.headers.get("content_encoding"),
                        delivery_mode=DeliveryMode.PERSISTENT,
                        expiration=expiration,
                    ),
                    routing_key=incoming.reply_to,
                )

    async def request(
        self,
        queue: str,
        message: LinkrMessage,
        *,
        context: dict[str, Any] | None = None,
    ) -> asyncio.Future[LinkrMessage]:
        """
        Send an RPC request and return a Future for the response.

        The method completes once the request has been published and response
        waiting has been registered. Await the returned Future to receive the
        response message.

        Args:
            queue: Target queue name.
            message: Serialized request message.
            context: Backend-specific publish/context metadata (unused).

        Returns:
            A future that resolves to the response message.

        Raises:
            RuntimeError: If the backend is not initialized.
        """
        if self._channel is None or self._exchanges.get("rpc") is None or self._callback_queue is None:
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
            routing_key = self.queue_name_to_rmq_queue_name(queue, "rpc")
            await self._exchanges["rpc"].publish(robust_message, routing_key=routing_key)
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
        """
        Register a task consumer for *queue*.

        Args:
            queue: Queue name to consume from.
            handler: Async callable that processes incoming messages.
            options: Optional backend-specific queue options.

        Raises:
            RuntimeError: If the backend is not initialized.
        """
        async with self._op_lock:
            if self._closed or not self._initialized or self._exchanges.get("task") is None:
                raise RuntimeError("backend not initialized")
            if queue in self._consumers.get("task", {}):
                return

            selected = self._select_options(options)
            channel = await self._channel_for_prefetch(None if selected is None else selected.prefetch_count)
            exchange = await self._get_exchange(channel, "task")
            queue_name = self.queue_name_to_rmq_queue_name(queue, "task")
            declare_kwargs = self._queue_declare_kwargs("task", selected)

            consumer_tag: str | None = None
            try:
                rmq_queue = await channel.declare_queue(queue_name, **declare_kwargs)
                await rmq_queue.bind(exchange, routing_key=queue_name)

                async def _task_handler(incoming: AbstractIncomingMessage) -> None:
                    await self._on_task_message(incoming, handler)

                consumer_tag = await rmq_queue.consume(_task_handler)
                self._consumers.setdefault("task", {})[queue] = (channel, rmq_queue, consumer_tag)
            except Exception:
                if consumer_tag is not None:
                    try:
                        await rmq_queue.cancel(consumer_tag=consumer_tag)
                    except Exception:  # pragma: no cover - best effort cleanup
                        logger.exception("Failed to rollback task consumer for queue %s", queue)
                if channel is not self._channel:
                    try:
                        await channel.close()
                    except Exception:  # pragma: no cover - best effort cleanup
                        logger.exception("Failed to close dedicated task channel for queue %s", queue)
                raise

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
        context: dict[str, Any] | None = None,
    ) -> None:
        """
        Send a task to *queue*. No response is expected.

        Args:
            queue: Target queue name.
            message: Serialized task message.
            context: Backend-specific publish/context metadata (unused).

        Raises:
            RuntimeError: If the backend is not initialized.
        """
        if self._channel is None or self._exchanges.get("task") is None:
            raise RuntimeError("backend not initialized")

        robust_message = self._to_robust_message(message)
        routing_key = self.queue_name_to_rmq_queue_name(queue, "task")
        await self._exchanges["task"].publish(robust_message, routing_key=routing_key)

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
        """Register an event consumer.

        Args:
            queue: Physical queue owned by this consumer instance.
            topics: Topic patterns this queue is bound to.
            handler: Async callable receiving a raw event message.
            options: Optional backend-specific queue options.
        """

        async with self._op_lock:
            if self._closed or not self._initialized or self._exchanges.get("event") is None:
                raise RuntimeError("backend not initialized")
            if queue in self._consumers.get("event", {}):
                return

            selected = self._select_options(options)
            channel = await self._channel_for_prefetch(None if selected is None else selected.prefetch_count)
            exchange = await self._get_exchange(channel, "event")
            queue_name = self.queue_name_to_rmq_queue_name(queue, "event")
            declare_kwargs = self._queue_declare_kwargs("event", selected)

            consumer_tag: str | None = None
            try:
                rmq_queue = await channel.declare_queue(queue_name, **declare_kwargs)
                for topic in topics:
                    await rmq_queue.bind(exchange, routing_key=topic)

                async def _event_handler(incoming: AbstractIncomingMessage) -> None:
                    await self._on_event_message(incoming, handler)

                consumer_tag = await rmq_queue.consume(_event_handler)
                self._consumers.setdefault("event", {})[queue] = (channel, rmq_queue, consumer_tag)
            except Exception:
                if consumer_tag is not None:
                    try:
                        await rmq_queue.cancel(consumer_tag=consumer_tag)
                    except Exception:  # pragma: no cover - best effort cleanup
                        logger.exception("Failed to rollback event consumer for queue %s", queue)
                if channel is not self._channel:
                    try:
                        await channel.close()
                    except Exception:  # pragma: no cover - best effort cleanup
                        logger.exception("Failed to close dedicated event channel for queue %s", queue)
                raise

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
        context: dict[str, Any] | None = None,
    ) -> None:
        """
        Publish an event to *topic*. No response is expected.

        Args:
            topic: Event topic or routing key.
            message: Serialized event message.
            context: Backend-specific publish/context metadata (unused).

        Raises:
            RuntimeError: If the backend is not initialized.
        """
        if self._channel is None or self._exchanges.get("event") is None:
            raise RuntimeError("backend not initialized")

        robust_message = self._to_robust_message(message)
        await self._exchanges["event"].publish(robust_message, routing_key=topic)

    async def healthcheck(self) -> HealthStatus:
        """Return backend readiness and health status."""
        ready = bool(self._initialized and not self._closed)
        if not ready:
            return HealthStatus(ready=False, health=None)

        conn = self._connection
        ch = self._channel
        if (
            conn is None
            or bool(getattr(conn, "is_closed", True))
            or ch is None
            or bool(getattr(ch, "is_closed", True))
        ):
            return HealthStatus(ready=True, health=False)

        active: list[tuple[str, str]] = [
            (mode, logical_queue)
            for mode, queues in self._consumers.items()
            for logical_queue in queues
        ]
        if not active:
            return HealthStatus(ready=True, health=True)

        channel = ch

        async def _queue_has_consumers(mode: str, logical_queue: str) -> bool:
            queue_name = self.queue_name_to_rmq_queue_name(logical_queue, mode)  # type: ignore[arg-type]
            rmq_queue = await channel.declare_queue(queue_name, passive=True)
            decl = getattr(rmq_queue, "declaration_result", None)
            consumer_count = getattr(decl, "consumer_count", None) if decl is not None else None
            if consumer_count is None:
                consumer_count = getattr(rmq_queue, "consumer_count", None)
            return consumer_count is not None and int(consumer_count) > 0

        try:
            results = await asyncio.gather(
                *(_queue_has_consumers(mode, logical_queue) for mode, logical_queue in active),
                return_exceptions=True,
            )
        except Exception:
            return HealthStatus(ready=True, health=False)

        if any(isinstance(result, BaseException) or not result for result in results):
            return HealthStatus(ready=True, health=False)

        return HealthStatus(ready=True, health=True)
