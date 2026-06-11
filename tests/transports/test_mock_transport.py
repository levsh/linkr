from __future__ import annotations

import asyncio

import pytest

from linkr.models import RpcRequest, RpcResponse
from linkr.serializer import JsonSerializer
from linkr.transports.mock import MockTransport

serializer = JsonSerializer()


@pytest.fixture
async def transport():
    t = MockTransport()
    await t.init()
    yield t
    await t.close()


async def test_consume_and_request(transport: MockTransport):
    async def handler(data: bytes, original: RpcRequest, wire_headers: dict[str, str] | None = None) -> tuple[bytes, RpcResponse | None, dict[str, str]] | None:
        req = serializer.loads_request(data, wire_headers or {})
        response = RpcResponse(id=req.id, data=req.data)
        resp_bytes, _ = serializer.dumps_response(response)
        return (resp_bytes, response, {})

    await transport.consume(handler)
    req = RpcRequest(data={"text": "hello"})
    data, _ = serializer.dumps_request(req)
    resp_bytes, resp_wire = await transport.request(data, original=req)
    resp = serializer.loads_response(resp_bytes, {})
    assert resp.data == {"text": "hello"}


async def test_request_no_consumer(transport: MockTransport):
    req = RpcRequest(data={"text": "hello"})
    data, _ = serializer.dumps_request(req)
    with pytest.raises(RuntimeError, match="No consumer registered"):
        await transport.request(data, original=req)


async def test_publish(transport: MockTransport):
    req = RpcRequest(data={"text": "fire-and-forget"})
    data, _ = serializer.dumps_request(req)
    await transport.publish(data, original=req)
    assert len(transport.sent_messages) == 1
    assert transport.sent_messages[0].data == {"text": "fire-and-forget"}


async def test_consume_and_publish_without_handler(transport: MockTransport):
    req = RpcRequest(data={"text": "no one listens"})
    data, _ = serializer.dumps_request(req)
    await transport.publish(data, original=req)
    assert len(transport.sent_messages) == 1


async def test_handler_returns_none(transport: MockTransport):
    async def handler(data: bytes, original: RpcRequest, wire_headers: dict[str, str] | None = None) -> tuple[bytes, RpcResponse | None, dict[str, str]] | None:
        return None

    await transport.consume(handler)
    req = RpcRequest(data={"text": "hello"})
    data, _ = serializer.dumps_request(req)
    with pytest.raises(RuntimeError, match="Handler returned None"):
        await transport.request(data, original=req)


async def test_stop_consume(transport: MockTransport):
    async def handler(data: bytes, original: RpcRequest, wire_headers: dict[str, str] | None = None) -> tuple[bytes, RpcResponse | None, dict[str, str]] | None:
        req = serializer.loads_request(data, wire_headers or {})
        response = RpcResponse(id=req.id, data={"status": "ok"})
        resp_bytes, _ = serializer.dumps_response(response)
        return (resp_bytes, response, {})

    await transport.consume(handler)
    await transport.stop_consume()
    req = RpcRequest(data={"text": "hello"})
    data, _ = serializer.dumps_request(req)
    with pytest.raises(RuntimeError, match="No consumer registered"):
        await transport.request(data, original=req)


async def test_close_cancels_requests(transport: MockTransport):
    async def handler(data: bytes, original: RpcRequest, wire_headers: dict[str, str] | None = None) -> tuple[bytes, RpcResponse | None, dict[str, str]] | None:
        await asyncio.sleep(10)
        req = serializer.loads_request(data, wire_headers or {})
        response = RpcResponse(id=req.id, data={"status": "ok"})
        resp_bytes, _ = serializer.dumps_response(response)
        return (resp_bytes, response, {})

    await transport.consume(handler)
    req = RpcRequest(data={"text": "hello"})
    data, _ = serializer.dumps_request(req)
    task = asyncio.create_task(
        transport.request(data, original=req),
    )
    await asyncio.sleep(0)
    await transport.close()

    with pytest.raises(asyncio.CancelledError):
        await task
