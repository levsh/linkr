from __future__ import annotations

import gzip

from linkr import MockTransport, RpcApp
from linkr.middleware.gzip import GzipMiddleware
from linkr.models import RawMessage, RpcRequest, RpcResponse


class TestGzipMiddleware:
    """Unit tests for GzipMiddleware via direct dispatch calls."""

    LARGE = b"x" * 2000
    SMALL = b"hello"

    async def test_small_request_not_compressed(self):
        mw = GzipMiddleware()
        raw = RawMessage(data=self.SMALL, headers={})
        request = RpcRequest(method="test", args=(), kwds={})

        async def call_next() -> RawMessage | None:
            return RawMessage(data=b"ok", headers={})

        await mw.dispatch_client(call_next, raw, request)
        assert raw.data == self.SMALL
        assert "gzip" not in raw.headers.get("content_encoding", "")

    async def test_large_request_compressed(self):
        mw = GzipMiddleware()
        raw = RawMessage(data=self.LARGE, headers={})
        request = RpcRequest(method="test", args=(), kwds={})

        async def call_next() -> RawMessage | None:
            return RawMessage(data=b"ok", headers={})

        await mw.dispatch_client(call_next, raw, request)
        assert raw.data != self.LARGE
        assert gzip.decompress(raw.data) == self.LARGE
        assert raw.headers.get("content_encoding") == "gzip"

    async def test_client_decompresses_response(self):
        mw = GzipMiddleware()
        raw = RawMessage(data=b"tiny", headers={})
        request = RpcRequest(method="test", args=(), kwds={})
        compressed = gzip.compress(self.LARGE)

        async def call_next() -> RawMessage | None:
            return RawMessage(data=compressed, headers={"content_encoding": "gzip"})

        result = await mw.dispatch_client(call_next, raw, request)
        assert result is not None
        assert result.data == self.LARGE

    async def test_server_decompresses_request(self):
        mw = GzipMiddleware()
        compressed = gzip.compress(self.LARGE)
        raw = RawMessage(data=compressed, headers={"content_encoding": "gzip"})
        req = RpcRequest(method="test", args=(), kwds={})
        response = RpcResponse(id=req.id, type="result", data={"result": "ok"})

        async def call_next() -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
            return RawMessage(data=b"ok", headers={}), response

        await mw.dispatch_server(call_next, raw)
        assert raw.data == self.LARGE
        assert raw.headers.get("content_encoding") == "gzip"

    async def test_server_compresses_large_response(self):
        mw = GzipMiddleware()
        raw = RawMessage(data=b"small", headers={})
        large_data = b"y" * 2000
        req = RpcRequest(method="test", args=(), kwds={})
        response = RpcResponse(id=req.id, type="result", data={"result": "large"})

        async def call_next() -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
            return RawMessage(data=large_data, headers={}), response

        raw_resp, resp = await mw.dispatch_server(call_next, raw)
        assert raw_resp is not None
        assert raw_resp.data != large_data
        assert gzip.decompress(raw_resp.data) == large_data
        assert "gzip" in raw_resp.headers.get("content_encoding", "")

    async def test_server_handles_none_response(self):
        mw = GzipMiddleware()
        raw = RawMessage(data=b"small", headers={})

        async def call_next() -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
            return None, None

        result = await mw.dispatch_server(call_next, raw)
        assert result == (None, None)

    async def test_custom_min_size(self):
        mw = GzipMiddleware(min_size=1)
        raw = RawMessage(data=b"x", headers={})
        request = RpcRequest(method="test", args=(), kwds={})

        async def call_next() -> RawMessage | None:
            return RawMessage(data=b"ok", headers={})

        await mw.dispatch_client(call_next, raw, request)
        assert raw.data != b"x"
        assert raw.headers.get("content_encoding") == "gzip"

    async def test_existing_content_encoding_merged(self):
        mw = GzipMiddleware()
        raw = RawMessage(data=self.LARGE, headers={"content_encoding": "custom"})
        request = RpcRequest(method="test", args=(), kwds={})

        async def call_next() -> RawMessage | None:
            return RawMessage(data=b"ok", headers={})

        await mw.dispatch_client(call_next, raw, request)
        ce = raw.headers.get("content_encoding", "")
        assert "custom" in ce
        assert "gzip" in ce

    async def test_existing_content_encoding_merged_on_server(self):
        mw = GzipMiddleware()
        large_data = b"z" * 2000
        raw = RawMessage(data=b"small", headers={})
        req = RpcRequest(method="test", args=(), kwds={})
        response = RpcResponse(id=req.id, type="result", data={"result": "large"})

        async def call_next() -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
            return RawMessage(data=large_data, headers={"content_encoding": "custom"}), response

        raw_resp, _ = await mw.dispatch_server(call_next, raw)
        assert raw_resp is not None
        ce = raw_resp.headers.get("content_encoding", "")
        assert "custom" in ce
        assert "gzip" in ce


class TestGzipMiddlewareWithApp:
    """Integration tests for GzipMiddleware through the full RpcApp pipeline."""

    async def test_gzip_roundtrip_small_payload(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        app.add_middleware(GzipMiddleware())
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"
        await app.close()

    async def test_gzip_large_request(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        app.add_middleware(GzipMiddleware())
        await app.init()

        @app.method("echo")
        def echo(data: str) -> str:
            return data

        large = "x" * 2000
        await app.consume()
        result = await app.make("echo", data=large).call()
        assert result == large
        await app.close()

    async def test_gzip_large_response(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        app.add_middleware(GzipMiddleware())
        await app.init()

        @app.method("get_large")
        def get_large() -> str:
            return "y" * 2000

        await app.consume()
        result = await app.make("get_large").call()
        assert result == "y" * 2000
        await app.close()

    async def test_gzip_add_middleware_appends(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        assert len(app._wire_mw) == 0
        app.add_middleware(GzipMiddleware())
        assert len(app._wire_mw) == 1
