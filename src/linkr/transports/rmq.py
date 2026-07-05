from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from contextlib import suppress
from typing import Any, TypeVar

from rmqaio import BindSpec, ConsumerSpec, ExchangeSpec, Ops, QueueSpec, Repeat, RetryPolicy, SharedConnection

from ..models import RawMessage, RpcRequest
from . import Transport

logger = logging.getLogger("linkr")


DEFAULT_EXCHANGE_NAME = "rpc"
DEFAULT_EXCHANGE_TYPE = "direct"
DEFAULT_EXCHANGE_DURABLE = True

DEFAULT_OPEN_RETRY_DELAYS = (1, 3, 5)
DEFAULT_REOPEN_RETRY_DELAYS = Repeat(5)
DEFAULT_OPS_TIMEOUT = 30.0

DEFAULT_SERVER_QUEUE_NAME = "rpc"
DEFAULT_SERVER_QUEUE_DURABLE = False
DEFAULT_SERVER_QUEUE_EXCLUSIVE = True

DEFAULT_REPLY_QUEUE_PREFIX = "rpc.reply"
DEFAULT_REPLY_QUEUE_DURABLE = False
DEFAULT_REPLY_QUEUE_EXCLUSIVE = True


class RmqTransport(Transport):
    """
    RabbitMQ transport.

    Operates on raw bytes. Serialization and encoding are handled
    by :class:`RpcApp` before data reaches the transport.
    """

    def __init__(
        self,
        url: str,
        *,
        exchange_name: str = DEFAULT_EXCHANGE_NAME,
        exchange_type: str = DEFAULT_EXCHANGE_TYPE,
        exchange_durable: bool = DEFAULT_EXCHANGE_DURABLE,
        server_queue_name: str = DEFAULT_SERVER_QUEUE_NAME,
        server_queue_durable: bool = DEFAULT_SERVER_QUEUE_DURABLE,
        server_queue_exclusive: bool = DEFAULT_SERVER_QUEUE_EXCLUSIVE,
        open_retry_delays: tuple[float, ...] = DEFAULT_OPEN_RETRY_DELAYS,
        reopen_retry_delays: Repeat = DEFAULT_REOPEN_RETRY_DELAYS,
        ops_timeout: float = DEFAULT_OPS_TIMEOUT,
        reply_queue_prefix: str = DEFAULT_REPLY_QUEUE_PREFIX,
        reply_queue_durable: bool = DEFAULT_REPLY_QUEUE_DURABLE,
        reply_queue_exclusive: bool = DEFAULT_REPLY_QUEUE_EXCLUSIVE,
    ) -> None:
        """
        Args:
            url: AMQP connection URL (e.g. ``amqp://guest:guest@localhost:5672/%2F``).
            exchange_name: Name of the direct exchange for RPC messages.
                Defaults to ``"rpc"``.
            exchange_type: Exchange type. Defaults to ``"direct"``.
            exchange_durable: Whether the exchange survives broker restarts.
                Defaults to ``True``.
            server_queue_name: Name of the main server queue.
                Defaults to ``"rpc"``.
            server_queue_durable: Whether the server queue survives broker restarts.
                Defaults to ``False``.
            server_queue_exclusive: Whether the server queue is exclusive to
                this connection. Defaults to ``True``.
            open_retry_delays: Delays in seconds between initial connection
                attempts. Defaults to ``(1, 3, 5)``.
            reopen_retry_delays: Retry policy for reconnection after the
                initial connection is established. Defaults to
                ``Repeat(5)`` (infinite retries every 5 seconds).
            ops_timeout: Timeout in seconds for individual RabbitMQ operations.
                Defaults to ``30.0``.
            reply_queue_prefix: Prefix for auto-generated reply queue names.
                Defaults to ``"rpc.reply"``.
            reply_queue_durable: Whether reply queues survive broker restarts.
                Defaults to ``False``.
            reply_queue_exclusive: Whether reply queues are exclusive to
                this consumer. Defaults to ``True``.
        """
        self._conn = SharedConnection(
            url,
            open_retry_policy=RetryPolicy(delays=list(open_retry_delays)),
            reopen_retry_policy=RetryPolicy(delays=reopen_retry_delays),
        )
        self._ops = Ops(self._conn, timeout=ops_timeout)

        self._exchange_spec = ExchangeSpec(
            name=exchange_name,
            type=exchange_type,  # type: ignore[arg-type]
            durable=exchange_durable,
        )
        self._server_queue_spec = QueueSpec(
            name=server_queue_name,
            durable=server_queue_durable,
            exclusive=server_queue_exclusive,
        )

        reply_name = f"{reply_queue_prefix}.{uuid.uuid4().hex}"
        self._reply_queue_spec = QueueSpec(
            name=reply_name,
            durable=reply_queue_durable,
            exclusive=reply_queue_exclusive,
        )

        self._pending: dict[str, asyncio.Future[RawMessage]] = {}
        self._handler: (
            Callable[
                [RawMessage],
                Awaitable[RawMessage | None],
            ]
            | None
        ) = None
        self._consumers: dict[str, str] = {}

    async def _send(
        self,
        reply_to: str,
        correlation_id: str,
        message: RawMessage,
    ) -> None:
        props: dict[str, Any] = {
            "correlation_id": correlation_id,
        }
        if message.headers:
            if "content_type" in message.headers:
                props["content_type"] = message.headers["content_type"]
            if "content_encoding" in message.headers:
                props["content_encoding"] = message.headers["content_encoding"]
            props["headers"] = message.headers
        await self._ops.publish(
            exchange="",
            data=message.data,
            routing_key=reply_to,
            properties=props,
        )

    async def _on_reply(self, channel: Any, message: Any) -> None:
        correlation_id = message.header.properties.correlation_id
        if correlation_id and correlation_id in self._pending:
            wire = getattr(message.header.properties, "headers", None) or {}
            fut = self._pending.pop(correlation_id)
            if not fut.done():
                fut.set_result(RawMessage(data=message.body, headers=wire))

    async def _on_request(self, channel: Any, message: Any) -> None:
        if self._handler is None:
            return

        try:
            request_id = message.header.properties.message_id
            if not request_id:
                return

            wire_headers = getattr(message.header.properties, "headers", None) or {}
            raw_request = RawMessage(data=message.body, headers=wire_headers)

            result = await self._handler(raw_request)
            if result is None:
                return

            reply_to = message.header.properties.reply_to
            if reply_to is None:
                return

            corellation_id = message.header.properties.correlation_id
            if not corellation_id:
                return

            await self._send(reply_to, corellation_id, result)

        except Exception as e:
            logger.exception(e)

    async def init(self) -> None:
        """
        Open the RabbitMQ connection and declare required infrastructure.

        Declares the exchange, the reply queue (with consumer), and the
        server queue (with binding). Should be called once before any
        other operations.
        """
        await self._ops.exchange_declare(self._exchange_spec, restore=True)

        await self._ops.queue_declare(self._reply_queue_spec, restore=True, force=True)
        await self._ops.consume(
            ConsumerSpec(
                queue=self._reply_queue_spec.name,
                callback=self._on_reply,
                prefetch_count=1,
                auto_ack=True,
            ),
            restore=True,
        )

    async def close(self, timeout: float | None = None) -> None:
        """
        Shut down the transport.

        Stops all consumers, cancels any pending request futures, and
        closes the underlying RabbitMQ connection.

        Args:
            timeout: If set, ``close`` waits at most this many total
                seconds across all steps.  Each step gets the remaining
                budget; ``TimeoutError`` is caught and suppressed so
                shutdown proceeds as far as possible.
        """
        deadline = None if timeout is None else asyncio.get_running_loop().time() + timeout

        def _left() -> float | None:
            if deadline is None:
                return None
            return max(deadline - asyncio.get_running_loop().time(), 0.1)

        with suppress(asyncio.TimeoutError):
            await self._ops.stop_consume(timeout=_left())

        with suppress(asyncio.TimeoutError):
            if self._pending:
                done, pending = await asyncio.wait(list(self._pending.values()), timeout=_left())
                for fut in pending:
                    fut.cancel()
            self._pending.clear()

        with suppress(asyncio.TimeoutError):
            await self._conn.close(timeout=_left())

    async def consume(
        self,
        handler: Callable[
            [RawMessage],
            Awaitable[RawMessage | None],
        ],
        queue: str | None = None,
    ) -> None:
        """
        Register a request handler and start consuming from a queue.

        If *queue* is ``None``, the main server queue is used. If a
        routing prefix is provided, a separate queue named
        ``{server_queue_name}.{queue}`` is declared, bound, and consumed.

        Args:
            handler: Async callable that receives a :class:`RawMessage`
                and returns a :class:`RawMessage` or ``None`` for
                fire-and-forget.
            queue: Optional routing prefix for segmented consumption.
                For example, if ``server_queue_name`` is ``"rpc"``
                and *queue* is ``"api"``, the queue will be named
                ``"rpc.api"``.
        """
        self._handler = handler

        if queue is None:
            queue_name = self._server_queue_spec.name
            if queue_name in self._consumers:
                return
            await self._ops.queue_declare(self._server_queue_spec, restore=True, force=True)
            await self._ops.bind(
                BindSpec(
                    src=self._exchange_spec.name,
                    dst=queue_name,
                    routing_key=queue_name,
                    kind="queue",
                ),
                restore=True,
            )
        else:
            queue_name = f"{self._server_queue_spec.name}.{queue}"
            if queue_name in self._consumers:
                return
            group_spec = QueueSpec(
                name=queue_name,
                durable=False,
                exclusive=False,
            )
            await self._ops.queue_declare(group_spec, restore=True, force=True)
            await self._ops.bind(
                BindSpec(
                    src=self._exchange_spec.name,
                    dst=queue_name,
                    routing_key=queue_name,
                    kind="queue",
                ),
                restore=True,
            )

        consumer = await self._ops.consume(
            ConsumerSpec(
                queue=queue_name,
                callback=self._on_request,
                prefetch_count=1,
                auto_ack=True,
            ),
            restore=True,
        )
        self._consumers[queue_name] = consumer.consumer_tag

    async def stop_consume(self) -> None:
        """Stop all consumers and clear the registered handler."""
        for consumer_tag in self._consumers.values():
            await self._ops.stop_consume(consumer_tag)
        self._consumers.clear()
        self._handler = None

    def _resolve_routing_key(self, message: RpcRequest) -> str:
        if message.headers.get("queue"):
            return f"{self._server_queue_spec.name}.{message.headers['queue']}"
        return self._server_queue_spec.name

    def _build_properties(
        self,
        message: RpcRequest,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        wire_headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {}
        properties["message_id"] = str(message.id)
        if correlation_id is not None:
            properties["correlation_id"] = correlation_id
        if reply_to is not None:
            properties["reply_to"] = reply_to

        ttl = message.headers.get("ttl")
        if ttl is not None:
            properties["expiration"] = str(int(ttl * 1000))

        if wire_headers:
            if "content_type" in wire_headers:
                properties["content_type"] = wire_headers["content_type"]
            if "content_encoding" in wire_headers:
                properties["content_encoding"] = wire_headers["content_encoding"]
            properties["headers"] = wire_headers

        return properties

    async def publish(
        self,
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        """
        Publish a fire-and-forget message to the exchange.

        No response is expected. The message is routed based on the
        ``routing_key`` stored in the original request headers.

        Args:
            request: The original RPC request (used for routing and
                header extraction).
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.
        """
        routing_key = self._resolve_routing_key(request)
        properties = self._build_properties(request, wire_headers=message.headers)
        await self._ops.publish(
            self._exchange_spec.name,
            message.data,
            routing_key,
            properties=properties,
        )

    async def request(
        self,
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage:
        """
        Publish a message and wait for the matching reply.

        The correlation ID is set to the request UUID. The reply is
        matched by correlation ID on the auto-generated reply queue.

        Args:
            request: The original RPC request (used for correlation ID,
                routing, and header extraction).
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.

        Returns:
            The response as a :class:`RawMessage`.

        Raises:
            asyncio.TimeoutError: If the reply is not received within
                the operation timeout.
        """
        correlation_id = str(request.id)
        fut: asyncio.Future[RawMessage] = asyncio.get_running_loop().create_future()
        self._pending[correlation_id] = fut

        routing_key = self._resolve_routing_key(request)
        properties = self._build_properties(
            request,
            correlation_id=correlation_id,
            reply_to=self._reply_queue_spec.name,
            wire_headers=message.headers,
        )

        await self._ops.publish(
            self._exchange_spec.name,
            message.data,
            routing_key,
            properties=properties,
        )

        try:
            return await fut
        finally:
            self._pending.pop(correlation_id, None)


T = TypeVar("T")


class ThreadSafeRmqTransport(RmqTransport):
    """
    RabbitMQ transport with thread-safe :meth:`publish` / :meth:`request`.

    Preserves a single RabbitMQ connection regardless of which event
    loop calls :meth:`publish` or :meth:`request`.  Both methods are
    bridged to the **owner loop** (the one that called :meth:`init`)
    via :func:`asyncio.run_coroutine_threadsafe` when necessary.

    This is useful when the transport is used from worker threads
    (e.g. in a multi-threaded web server).
    """

    async def init(self) -> None:
        """Record the owner loop, then delegate to :meth:`RmqTransport.init`."""
        self._owner_loop = asyncio.get_running_loop()
        await super().init()

    async def _bridge(self, coro: Coroutine[Any, Any, T]) -> T:
        loop = asyncio.get_running_loop()
        if loop is self._owner_loop:
            return await coro
        return await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(coro, self._owner_loop),
        )

    async def request(
        self,
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage:
        """
        Send a request and wait for the reply, safe for cross-loop usage.

        If called from a different event loop than the one that called
        :meth:`init`, the call is bridged to the owner loop.

        Args:
            request: The original RPC request.
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.

        Returns:
            The response as a :class:`RawMessage`.
        """
        return await self._bridge(super().request(request, message, kwds=kwds))

    async def publish(
        self,
        request: RpcRequest,
        message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> None:
        """
        Publish a fire-and-forget message, safe for cross-loop usage.

        If called from a different event loop than the one that called
        :meth:`init`, the call is bridged to the owner loop.

        Args:
            request: The original RPC request.
            message: The serialised request as a :class:`RawMessage`.
            kwds: Additional call context forwarded from the caller.
        """
        return await self._bridge(super().publish(request, message, kwds=kwds))
