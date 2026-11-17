from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from linkr.backends.rmq import RmqBackend
from linkr.models import HealthStatus, Message


class _FakeCallbackQueue:
    def __init__(self, name: str = "cbq") -> None:
        self.name = name


class _FakeExchange:
    def __init__(self) -> None:
        self.published: list[tuple[Any, str, dict[str, Any]]] = []

    async def publish(self, message, routing_key, **kwargs):
        self.published.append((message, routing_key, kwargs))


class _FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = _FakeExchange()
        self.is_closed = False
        self.declare_queue_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.declare_queue_results: dict[str, Any] = {}
        self.declare_queue_error: Exception | None = None
        self.declare_queue_errors: dict[str, Exception] = {}

    async def declare_queue(self, name: str, *args: Any, **kwargs: Any) -> Any:
        self.declare_queue_calls.append(((name, *args), dict(kwargs)))
        if self.declare_queue_error is not None:
            raise self.declare_queue_error
        error = self.declare_queue_errors.get(name)
        if error is not None:
            raise error
        if name in self.declare_queue_results:
            return self.declare_queue_results[name]
        return SimpleNamespace(declaration_result=SimpleNamespace(consumer_count=1))


class _FakeProcessContext:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeIncomingMessage:
    def __init__(
        self,
        body: bytes,
        *,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        self.body = body
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.headers = headers or {}

    def process(self):
        return _FakeProcessContext()


def _wire_backend() -> tuple[RmqBackend, _FakeChannel, dict[str, _FakeExchange], _FakeCallbackQueue]:
    backend = RmqBackend(queue_name_template={"rpc": "linkr.rpc.{queue}", "task": "linkr.tasks.{queue}", "event": "linkr.events.{queue}"})
    channel = _FakeChannel()
    exchanges = {"rpc": _FakeExchange(), "task": _FakeExchange(), "event": _FakeExchange()}
    callback = _FakeCallbackQueue()
    backend._connection = SimpleNamespace(is_closed=False)  # type: ignore[assignment]
    backend._channel = channel  # type: ignore[assignment]
    backend._exchanges = exchanges  # type: ignore[assignment]
    backend._callback_queue = callback  # type: ignore[assignment]
    backend._initialized = True
    backend._closed = False
    backend._consumers = {}
    backend._futures = {}
    return backend, channel, exchanges, callback


class TestRmqBackend:
    async def test_publish_expiration_from_request_ttl(self):
        backend, _channel, exchanges, callback = _wire_backend()

        fut = await backend.request("svc.queue", Message(data=b"rpc", headers={"ttl": 5}))
        await backend.send("svc.tasks", Message(data=b"task", headers={"ttl": 7}))
        await backend.emit("topic.a", Message(data=b"event", headers={"ttl": 9}))

        rpc_msg, rpc_rk, _ = exchanges["rpc"].published[0]
        task_msg, task_rk, _ = exchanges["task"].published[0]
        event_msg, event_rk, _ = exchanges["event"].published[0]

        assert rpc_rk == "svc.queue"
        assert rpc_msg.expiration == timedelta(seconds=5)
        assert rpc_msg.reply_to == callback.name
        assert rpc_msg.correlation_id is not None

        assert task_rk == "svc.tasks"
        assert task_msg.expiration == timedelta(seconds=7)

        assert event_rk == "topic.a"
        assert event_msg.expiration == timedelta(seconds=9)

        fut.cancel()

    async def test_rpc_response_expiration_from_rttl(self):
        backend, channel, _exchanges, _callback = _wire_backend()

        async def handler(_message: Message) -> Message:
            return Message(data=b"resp", headers={"rttl": 11})

        incoming = _FakeIncomingMessage(
            b"req",
            correlation_id="corr-1",
            reply_to="reply-queue",
            headers={"ttl": 1},
        )

        await backend._on_rpc_message(incoming, handler)

        assert len(channel.default_exchange.published) == 1
        msg, routing_key, _ = channel.default_exchange.published[0]
        assert routing_key == "reply-queue"
        assert msg.correlation_id == "corr-1"
        assert msg.expiration == timedelta(seconds=11)
        assert msg.body == b"resp"

    async def test_publish_without_ttl_has_no_expiration(self):
        backend, _channel, exchanges, _callback = _wire_backend()

        fut = await backend.request("svc.queue", Message(data=b"rpc", headers={}))
        await backend.send("svc.tasks", Message(data=b"task", headers={}))
        await backend.emit("topic.a", Message(data=b"event", headers={}))

        assert exchanges["rpc"].published[0][0].expiration is None
        assert exchanges["task"].published[0][0].expiration is None
        assert exchanges["event"].published[0][0].expiration is None

        fut.cancel()

    async def test_publish_propagates_content_type_and_encoding(self):
        backend, _channel, exchanges, callback = _wire_backend()

        fut = await backend.request(
            "svc.queue",
            Message(
                data=b"rpc",
                headers={"content_type": "application/x-msgpack", "content_encoding": "gzip", "ttl": 5},
            ),
        )
        await backend.send(
            "svc.tasks",
            Message(data=b"task", headers={"content_type": "application/json"}),
        )
        await backend.emit(
            "topic.a",
            Message(data=b"event", headers={"content_type": "text/plain", "content_encoding": "br"}),
        )

        rpc_msg, rpc_rk, _ = exchanges["rpc"].published[0]
        task_msg = exchanges["task"].published[0][0]
        event_msg = exchanges["event"].published[0][0]

        assert rpc_rk == "svc.queue"
        assert rpc_msg.reply_to == callback.name
        assert rpc_msg.content_type == "application/x-msgpack"
        assert rpc_msg.content_encoding == "gzip"
        assert task_msg.content_type == "application/json"
        assert task_msg.content_encoding is None
        assert event_msg.content_type == "text/plain"
        assert event_msg.content_encoding == "br"

        fut.cancel()

    async def test_rpc_response_propagates_content_type_and_encoding(self):
        backend, channel, _exchanges, _callback = _wire_backend()

        async def handler(_message: Message) -> Message:
            return Message(
                data=b"resp",
                headers={"content_type": "application/cbor", "content_encoding": "gzip", "rttl": 11},
            )

        incoming = _FakeIncomingMessage(b"req", correlation_id="corr-1", reply_to="reply-queue")

        await backend._on_rpc_message(incoming, handler)

        assert len(channel.default_exchange.published) == 1
        msg, routing_key, _ = channel.default_exchange.published[0]
        assert routing_key == "reply-queue"
        assert msg.correlation_id == "corr-1"
        assert msg.content_type == "application/cbor"
        assert msg.content_encoding == "gzip"
        assert msg.expiration == timedelta(seconds=11)

    async def test_healthcheck_not_initialized(self):
        backend = RmqBackend()

        assert await backend.healthcheck() == HealthStatus(ready=False, health=None)

    async def test_healthcheck_closed(self):
        backend, _channel, _exchanges, _callback = _wire_backend()
        backend._closed = True

        assert await backend.healthcheck() == HealthStatus(ready=False, health=None)

    async def test_healthcheck_connection_closed(self):
        backend, _channel, _exchanges, _callback = _wire_backend()
        backend._connection = SimpleNamespace(is_closed=True)  # type: ignore[assignment]

        assert await backend.healthcheck() == HealthStatus(ready=True, health=False)

    async def test_healthcheck_channel_closed(self):
        backend, channel, _exchanges, _callback = _wire_backend()
        channel.is_closed = True

        assert await backend.healthcheck() == HealthStatus(ready=True, health=False)

    async def test_healthcheck_no_active_queues(self):
        backend, channel, _exchanges, _callback = _wire_backend()
        backend._consumers = {}

        assert await backend.healthcheck() == HealthStatus(ready=True, health=True)
        assert channel.declare_queue_calls == []

    async def test_healthcheck_active_queues_have_consumers(self):
        backend, channel, _exchanges, _callback = _wire_backend()
        backend._consumers = {
            "rpc": {"svc": object()},
            "task": {"jobs": object()},
            "event": {"app.1": object()},
        }
        channel.declare_queue_results = {
            "linkr.rpc.svc": SimpleNamespace(declaration_result=SimpleNamespace(consumer_count=1)),
            "linkr.tasks.jobs": SimpleNamespace(consumer_count=2),
            "linkr.events.app.1": SimpleNamespace(declaration_result=SimpleNamespace(consumer_count=3)),
        }

        assert await backend.healthcheck() == HealthStatus(ready=True, health=True)
        names = {call.args[0] for call in [SimpleNamespace(args=c[0], kwargs=c[1]) for c in channel.declare_queue_calls]}
        assert names == {"linkr.rpc.svc", "linkr.tasks.jobs", "linkr.events.app.1"}
        assert all(call.kwargs.get("passive") is True for call in [SimpleNamespace(args=c[0], kwargs=c[1]) for c in channel.declare_queue_calls])

    async def test_healthcheck_active_queue_without_consumers(self):
        backend, channel, _exchanges, _callback = _wire_backend()
        backend._consumers = {"rpc": {"svc": object()}, "task": {"jobs": object()}}
        channel.declare_queue_results = {
            "linkr.rpc.svc": SimpleNamespace(declaration_result=SimpleNamespace(consumer_count=1)),
            "linkr.tasks.jobs": SimpleNamespace(declaration_result=SimpleNamespace(consumer_count=0)),
        }

        assert await backend.healthcheck() == HealthStatus(ready=True, health=False)

    async def test_healthcheck_passive_declare_error(self):
        backend, channel, _exchanges, _callback = _wire_backend()
        backend._consumers = {"rpc": {"svc": object()}}
        channel.declare_queue_error = RuntimeError("not found")

        assert await backend.healthcheck() == HealthStatus(ready=True, health=False)

    async def test_healthcheck_checks_active_queues_in_parallel(self):
        backend, channel, _exchanges, _callback = _wire_backend()
        backend._consumers = {"rpc": {"a": object()}, "task": {"b": object()}, "event": {"c": object()}}

        started: list[str] = []
        release = asyncio.Event()

        async def declare_queue(name: str, *args: Any, **kwargs: Any) -> Any:
            channel.declare_queue_calls.append(((name, *args), dict(kwargs)))
            started.append(name)
            if len(started) == 3:
                release.set()
            await asyncio.wait_for(release.wait(), timeout=1)
            return SimpleNamespace(declaration_result=SimpleNamespace(consumer_count=1))

        channel.declare_queue = declare_queue  # type: ignore[assignment]

        assert await backend.healthcheck() == HealthStatus(ready=True, health=True)
        assert set(started) == {"linkr.rpc.a", "linkr.tasks.b", "linkr.events.c"}
