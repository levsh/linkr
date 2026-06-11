from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeVar

logger = logging.getLogger("linkr")

from rmqaio import BindSpec, ConsumerSpec, ExchangeSpec, Ops, QueueSpec, Repeat, RetryPolicy, SharedConnection

from linkr.models import RpcRequest, RpcResponse
from linkr.transports import Transport

DEFAULT_EXCHANGE_NAME = "rpc"
DEFAULT_EXCHANGE_TYPE = "direct"
DEFAULT_EXCHANGE_DURABLE = True

DEFAULT_OPEN_RETRY_DELAYS = (1, 3, 5)
DEFAULT_REOPEN_RETRY_DELAYS = Repeat(5)
DEFAULT_OPS_TIMEOUT = 30.0

DEFAULT_SERVER_QUEUE_NAME = "rpc.server"
DEFAULT_SERVER_QUEUE_DURABLE = False
DEFAULT_SERVER_QUEUE_EXCLUSIVE = True

DEFAULT_REPLY_QUEUE_PREFIX = "rpc.reply"
DEFAULT_REPLY_QUEUE_DURABLE = False
DEFAULT_REPLY_QUEUE_EXCLUSIVE = True


class RmqTransport(Transport):
    """
    RabbitMQ transport using rmqaio.

    Operates on raw bytes. Serialization and encoding are handled
    by RpcApp before data reaches the transport.
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

        self._pending: dict[str, asyncio.Future[tuple[bytes, dict[str, str]]]] = {}
        self._handler: (
            Callable[
                [bytes, RpcRequest, dict[str, str] | None],
                Awaitable[tuple[bytes, RpcResponse | None, dict[str, str]] | None],
            ]
            | None
        ) = None
        self._consumers: dict[str, str] = {}

    async def _send(
        self,
        reply_to: str,
        correlation_id: str,
        data: bytes,
        wire_headers: dict[str, str] | None = None,
    ) -> None:
        props: dict[str, Any] = {
            "correlation_id": correlation_id,
        }
        if wire_headers:
            if "content_type" in wire_headers:
                props["content_type"] = wire_headers["content_type"]
            if "content_encoding" in wire_headers:
                props["content_encoding"] = wire_headers["content_encoding"]
        await self._ops.publish(
            exchange="",
            data=data,
            routing_key=reply_to,
            properties=props,
        )

    async def _on_reply(self, channel: Any, message: Any) -> None:
        correlation_id = message.header.properties.correlation_id
        if correlation_id and correlation_id in self._pending:
            ce = getattr(message.header.properties, "content_encoding", None)
            wire = {"content_encoding": ce} if ce else {}
            fut = self._pending.pop(correlation_id)
            if not fut.done():
                fut.set_result((message.body, wire))

    async def _on_request(self, channel: Any, message: Any) -> None:
        if self._handler is None:
            return

        reply_to = message.header.properties.reply_to
        if not reply_to:
            return

        cid = message.header.properties.correlation_id or ""
        if cid:
            original = RpcRequest(id=uuid.UUID(cid), headers={})
        else:
            original = RpcRequest(headers={})

        ce = getattr(message.header.properties, "content_encoding", None)
        wire_headers = {"content_encoding": ce} if ce else None

        try:
            result = await self._handler(message.body, original, wire_headers)
        except Exception:
            logger.exception("Handler error for %s", cid)
            error_resp = RpcResponse(
                id=original.id,
                data={
                    "error_code": "InternalError",
                    "error_message": "Handler execution failed",
                },
            )
            await self._send(reply_to, cid or str(original.id), error_resp.model_dump_json().encode())
            return

        if result is None:
            return

        response_bytes, response, response_wire = result
        await self._send(reply_to, cid or str(response.id), response_bytes, wire_headers=response_wire)  # type: ignore[union-attr]

    async def init(self) -> None:
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

        await self._ops.queue_declare(self._server_queue_spec, restore=True, force=True)
        await self._ops.bind(
            BindSpec(
                src=self._exchange_spec.name,
                dst=self._server_queue_spec.name,
                routing_key=self._server_queue_spec.name,
                kind="queue",
            ),
            restore=True,
        )

    async def close(self) -> None:
        await self._ops.stop_consume()
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()
        await self._conn.close()

    async def consume(
        self,
        handler: Callable[
            [bytes, RpcRequest, dict[str, str] | None],
            Awaitable[tuple[bytes, RpcResponse | None, dict[str, str]] | None],
        ],
        queue: str | None = None,
    ) -> None:
        self._handler = handler

        if queue is not None:
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
        else:
            queue_name = self._server_queue_spec.name
            if queue_name in self._consumers:
                return

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
        for consumer_tag in self._consumers.values():
            await self._ops.stop_consume(consumer_tag)
        self._consumers.clear()
        self._handler = None

    def _resolve_routing_key(self, message: RpcRequest) -> str:
        return message.headers.get("routing_key", self._server_queue_spec.name)

    def _build_properties(
        self,
        message: RpcRequest,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        wire_headers: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        properties: dict[str, Any] = {}
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

        return properties

    async def publish(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> None:
        routing_key = self._resolve_routing_key(original)
        properties = self._build_properties(original, wire_headers=wire_headers)
        await self._ops.publish(
            self._exchange_spec.name,
            data,
            routing_key,
            properties=properties,
        )

    async def request(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> tuple[bytes, dict[str, str]]:
        correlation_id = str(original.id)
        fut: asyncio.Future[tuple[bytes, dict[str, str]]] = asyncio.get_running_loop().create_future()
        self._pending[correlation_id] = fut

        routing_key = self._resolve_routing_key(original)
        properties = self._build_properties(
            original,
            correlation_id=correlation_id,
            reply_to=self._reply_queue_spec.name,
            wire_headers=wire_headers,
        )

        await self._ops.publish(
            self._exchange_spec.name,
            data,
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
    RabbitMQ transport with thread-safe publish/request.

    Preserves a single RabbitMQ connection regardless of which
    event loop calls publish() or request().  Both methods are
    bridged to the owner loop (the one that called init())
    via run_coroutine_threadsafe when necessary.
    """

    async def init(self) -> None:
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
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> tuple[bytes, dict[str, str]]:
        return await self._bridge(super().request(data, original=original, wire_headers=wire_headers))

    async def publish(
        self,
        data: bytes,
        *,
        original: RpcRequest,
        wire_headers: dict[str, Any] | None = None,
    ) -> None:
        return await self._bridge(super().publish(data, original=original, wire_headers=wire_headers))
