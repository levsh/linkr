from __future__ import annotations

import gzip

from linkr import App, LocalBackend
from linkr.middleware.gzip import GzipMiddleware
from linkr.models import Message, Request, Response


def make_request() -> Request:
    return Request(type="rpc", data={"method": "test", "args": (), "kwds": {}})


class TestGzipMiddleware:
    """Unit tests for GzipMiddleware via direct dispatch calls."""

    LARGE = b"x" * 2000
    SMALL = b"hello"

    async def test_small_request_not_compressed(self):
        mw = GzipMiddleware()
        raw = Message(data=self.SMALL, headers={})
        request = make_request()

        async def call_next() -> Message | None:
            return Message(data=b"ok", headers={})

        await mw.dispatch_client(call_next, request, raw)
        assert raw.data == self.SMALL
        assert "gzip" not in raw.headers.get("content_encoding", "")

    async def test_large_request_compressed(self):
        mw = GzipMiddleware()
        raw = Message(data=self.LARGE, headers={})
        request = make_request()

        async def call_next() -> Message | None:
            return Message(data=b"ok", headers={})

        await mw.dispatch_client(call_next, request, raw)
        assert raw.data != self.LARGE
        assert gzip.decompress(raw.data) == self.LARGE
        assert raw.headers.get("content_encoding") == "gzip"

    async def test_client_decompresses_response(self):
        mw = GzipMiddleware()
        raw = Message(data=b"tiny", headers={})
        request = make_request()
        compressed = gzip.compress(self.LARGE)

        async def call_next() -> Message | None:
            return Message(data=compressed, headers={"content_encoding": "gzip"})

        result = await mw.dispatch_client(call_next, request, raw)
        assert result is not None
        assert result.data == self.LARGE

    async def test_server_decompresses_request(self):
        mw = GzipMiddleware()
        compressed = gzip.compress(self.LARGE)
        raw = Message(data=compressed, headers={"content_encoding": "gzip"})
        response = Response(id=make_request().id, type="result", data={"result": "ok"})

        async def call_next() -> tuple[Message, Response] | tuple[None, None]:
            return Message(data=b"ok", headers={}), response

        await mw.dispatch_server(call_next, raw)
        assert raw.data == self.LARGE
        assert raw.headers.get("content_encoding") == "gzip"

    async def test_server_compresses_large_response(self):
        mw = GzipMiddleware()
        raw = Message(data=b"small", headers={})
        large_data = b"y" * 2000
        response = Response(id=make_request().id, type="result", data={"result": "large"})

        async def call_next() -> tuple[Message, Response] | tuple[None, None]:
            return Message(data=large_data, headers={}), response

        raw_resp, resp = await mw.dispatch_server(call_next, raw)
        assert raw_resp is not None
        assert raw_resp.data != large_data
        assert gzip.decompress(raw_resp.data) == large_data
        assert "gzip" in raw_resp.headers.get("content_encoding", "")

    async def test_server_handles_none_response(self):
        mw = GzipMiddleware()
        raw = Message(data=b"small", headers={})

        async def call_next() -> tuple[Message, Response] | tuple[None, None]:
            return None, None

        result = await mw.dispatch_server(call_next, raw)
        assert result == (None, None)

    async def test_custom_min_size(self):
        mw = GzipMiddleware(min_size=1)
        raw = Message(data=b"x", headers={})
        request = make_request()

        async def call_next() -> Message | None:
            return Message(data=b"ok", headers={})

        await mw.dispatch_client(call_next, request, raw)
        assert raw.data != b"x"
        assert raw.headers.get("content_encoding") == "gzip"

    async def test_existing_content_encoding_merged(self):
        mw = GzipMiddleware()
        raw = Message(data=self.LARGE, headers={"content_encoding": "custom"})
        request = make_request()

        async def call_next() -> Message | None:
            return Message(data=b"ok", headers={})

        await mw.dispatch_client(call_next, request, raw)
        ce = raw.headers.get("content_encoding", "")
        assert "custom" in ce
        assert "gzip" in ce

    async def test_existing_content_encoding_merged_on_server(self):
        mw = GzipMiddleware()
        large_data = b"z" * 2000
        raw = Message(data=b"small", headers={})
        response = Response(id=make_request().id, type="result", data={"result": "large"})

        async def call_next() -> tuple[Message, Response] | tuple[None, None]:
            return Message(data=large_data, headers={"content_encoding": "custom"}), response

        raw_resp, _ = await mw.dispatch_server(call_next, raw)
        assert raw_resp is not None
        ce = raw_resp.headers.get("content_encoding", "")
        assert "custom" in ce
        assert "gzip" in ce


class TestGzipMiddlewareWithApp:
    """Integration tests for GzipMiddleware through the full App pipeline."""

    async def test_gzip_roundtrip_small_payload(self):
        app = App("app", backend=LocalBackend())
        app.add_middleware(GzipMiddleware())
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        assert await app.call("app.ping", (), {}) == "pong"
        await app.close()

    async def test_gzip_large_request(self):
        app = App("app", backend=LocalBackend())
        app.add_middleware(GzipMiddleware())
        await app.init()

        @app.method("echo")
        def echo(data: str) -> str:
            return data

        large = "x" * 2000
        await app.consume()
        assert await app.call("app.echo", (), {"data": large}) == large
        await app.close()

    async def test_gzip_large_response(self):
        app = App("app", backend=LocalBackend())
        app.add_middleware(GzipMiddleware())
        await app.init()

        @app.method("get_large")
        def get_large() -> str:
            return "y" * 2000

        await app.consume()
        assert await app.call("app.get_large", (), {}) == "y" * 2000
        await app.close()

    async def test_gzip_add_middleware_appends(self):
        app = App("app", backend=LocalBackend())
        assert len(app._wire_mw) == 0
        app.add_middleware(GzipMiddleware())
        assert len(app._wire_mw) == 1
