from __future__ import annotations

import asyncio

import pytest

from linkr.models import RawMessage, RpcRequest, RpcResponse
from linkr.serializer import JsonSerializer
from linkr.transports.mock import MockTransport

serializer = JsonSerializer()


def _raw_request(req: RpcRequest) -> RawMessage:
    return serializer.dumps_request(req)


class TestMockTransport:
    @pytest.fixture
    async def transport(self):
        t = MockTransport()
        await t.init()
        yield t
        await t.close()

    async def test_consume_and_request(self, transport: MockTransport):
        async def handler(raw: RawMessage):
            req = serializer.loads_request(raw)
            response = RpcResponse(id=req.id, type="result", data=req.kwds)
            return serializer.dumps_response(response)

        await transport.consume(handler)
        req = RpcRequest(method="echo", args=(), kwds={"text": "hello"})
        resp_raw = await transport.request(req, _raw_request(req))
        resp = serializer.loads_response(resp_raw)
        assert resp.data == {"text": "hello"}

    async def test_request_no_consumer(self, transport: MockTransport):
        req = RpcRequest(method="echo", args=(), kwds={"text": "hello"})
        with pytest.raises(RuntimeError, match="No consumer registered"):
            await transport.request(req, _raw_request(req))

    async def test_publish(self, transport: MockTransport):
        req = RpcRequest(method="echo", args=(), kwds={"text": "fire-and-forget"})
        await transport.publish(req, _raw_request(req))
        assert len(transport.sent_messages) == 1
        assert transport.sent_messages[0].kwds == {"text": "fire-and-forget"}

    async def test_consume_and_publish_without_handler(self, transport: MockTransport):
        req = RpcRequest(method="echo", args=(), kwds={"text": "no one listens"})
        await transport.publish(req, _raw_request(req))
        assert len(transport.sent_messages) == 1

    async def test_handler_returns_none(self, transport: MockTransport):
        async def handler(raw: RawMessage):
            return None

        await transport.consume(handler)
        req = RpcRequest(method="echo", args=(), kwds={"text": "hello"})
        with pytest.raises(RuntimeError, match="Handler returned None"):
            await transport.request(req, _raw_request(req))

    async def test_stop_consume(self, transport: MockTransport):
        async def handler(raw: RawMessage):
            req = serializer.loads_request(raw)
            response = RpcResponse(id=req.id, type="result", data={"status": "ok"})
            return serializer.dumps_response(response)

        await transport.consume(handler)
        await transport.stop_consume()
        req = RpcRequest(method="echo", args=(), kwds={"text": "hello"})
        with pytest.raises(RuntimeError, match="No consumer registered"):
            await transport.request(req, _raw_request(req))

    async def test_close_cancels_requests(self, transport: MockTransport):
        async def handler(raw: RawMessage):
            await asyncio.sleep(10)
            req = serializer.loads_request(raw)
            response = RpcResponse(id=req.id, type="result", data={"status": "ok"})
            return serializer.dumps_response(response)

        await transport.consume(handler)
        req = RpcRequest(method="echo", args=(), kwds={"text": "hello"})
        task = asyncio.create_task(transport.request(req, _raw_request(req)))
        await asyncio.sleep(0)
        await transport.close()

        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_close_with_timeout_cancels_requests(self, transport: MockTransport):
        async def handler(raw: RawMessage):
            await asyncio.sleep(10)
            req = serializer.loads_request(raw)
            response = RpcResponse(id=req.id, type="result", data={"status": "ok"})
            return serializer.dumps_response(response)

        await transport.consume(handler)
        req = RpcRequest(method="echo", args=(), kwds={"text": "hello"})
        task = asyncio.create_task(transport.request(req, _raw_request(req)))
        await asyncio.sleep(0)
        await transport.close(timeout=0.5)

        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_close_without_timeout_clears_handler(self, transport: MockTransport):
        async def handler(raw: RawMessage):
            req = serializer.loads_request(raw)
            response = RpcResponse(id=req.id, type="result", data={"status": "ok"})
            return serializer.dumps_response(response)

        await transport.consume(handler)
        await transport.close()
        with pytest.raises(RuntimeError, match="No consumer registered"):
            await transport.request(
                RpcRequest(method="echo", args=(), kwds={}),
                _raw_request(RpcRequest(method="echo", args=(), kwds={})),
            )
