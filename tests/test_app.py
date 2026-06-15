from __future__ import annotations

import asyncio

import pytest

from linkr import (
    AppMiddleware,
    Depends,
    ErrorCode,
    JsonRpcSerializer,
    JsonSerializer,
    MockTransport,
    RpcApp,
    RpcError,
    WireMiddleware,
)


class TestAppCore:
    async def test_register_method(self, app: RpcApp):
        @app.method("ping")
        def pong() -> str:
            return "pong"

        assert "ping" in app.methods
        info = app.methods["ping"]
        assert info.name == "ping"
        assert info.fn() == "pong"

    async def test_call_success(self, app: RpcApp):
        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        result = await app.make("add", x=2, y=3).call()
        assert result == 5

    async def test_call_with_no_args(self, app: RpcApp):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"

    async def test_call_error_from_handler(self, app: RpcApp):
        @app.method("fail")
        def fail() -> str:
            msg = "something went wrong"
            raise ValueError(msg)

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("fail").call()
        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR

    async def test_call_method_not_found(self, app: RpcApp):
        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("nonexistent").call()
        assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

    async def test_publish(self, app: RpcApp, transport: MockTransport):
        req = app.make("test", text="hello")
        await app.publish(req.request)
        assert len(transport.sent_messages) == 1

    async def test_call_via_make_call(self, app: RpcApp):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping")(timeout=5)
        assert result == "pong"

    async def test_call_timeout(self, app: RpcApp):
        @app.method("slow")
        async def slow() -> str:
            await asyncio.sleep(10)
            return "done"

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("slow").call(timeout=0.1)
        assert exc_info.value.error_code == ErrorCode.TIMEOUT

    async def test_async_handler(self, app: RpcApp):
        @app.method("greet")
        async def greet(name: str) -> str:
            return f"Hello, {name}!"

        await app.consume()
        result = await app.make("greet", name="World").call()
        assert result == "Hello, World!"

    async def test_handlerinfo_includes_signature(self, app: RpcApp):
        @app.method("add")
        def add(x: int, y: int = 0) -> int:
            return x + y

        info = app.methods["add"]
        assert info.name == "add"
        assert "x" in info.signature
        assert "y" in info.signature
        assert info.options == {}

    async def test_rpc_error_from_handler_preserves_custom_code(self, app: RpcApp):
        @app.method("auth")
        def auth(token: str) -> str:
            raise RpcError(error_code="Unauthorized", error_message="bad token")

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("auth", token="invalid").call()
        assert exc_info.value.error_code == "Unauthorized"
        assert exc_info.value.error_message == "bad token"

    async def test_rpc_error_from_middleware_preserves_custom_code(self, app: RpcApp):
        events: list[str] = []

        class AuthMiddleware(AppMiddleware):
            async def dispatch_server(self, call_next, request, *, kwds=None):
                events.append("auth_check")
                raise RpcError(
                    error_code="Unauthorized",
                    error_message="access denied",
                    error_details={"method": request.method},
                )

        app.add_middleware(AuthMiddleware())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("ping").call()
        assert exc_info.value.error_code == "Unauthorized"
        assert exc_info.value.error_message == "access denied"
        assert exc_info.value.error_details == {"method": "ping"}


class TestAppLifecycle:
    async def test_app_is_context_manager(self):
        transport = MockTransport()
        async with RpcApp(transport=transport) as app:

            @app.method("ping")
            def ping() -> str:
                return "pong"

            await app.consume()
            result = await app.make("ping").call()
            assert result == "pong"

    async def test_call_on_closed_app_raises(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="RpcApp is closed"):
            await app.make("ping").call()

    async def test_publish_on_closed_app_raises(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()
        req = app.make("test")
        await app.close()
        with pytest.raises(RuntimeError, match="RpcApp is closed"):
            await app.publish(req.request)

    async def test_consume_on_closed_app_raises(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="RpcApp is closed"):
            await app.consume()

    async def test_default_timeout(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, timeout=0.1)
        await app.init()

        @app.method("slow")
        async def slow() -> str:
            await asyncio.sleep(10)
            return "done"

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("slow").call()
        assert exc_info.value.error_code == ErrorCode.TIMEOUT

    async def test_app_close_passes_timeout_to_transport(self):
        timeout_passed: list[float | None] = []

        class SpyTransport(MockTransport):
            async def close(self, timeout: float | None = None) -> None:
                timeout_passed.append(timeout)
                await super().close(timeout=timeout)

        transport = SpyTransport()
        app = RpcApp(transport=transport)
        await app.init()
        await app.close(timeout=42.0)
        assert timeout_passed == [42.0]

    async def test_get_handler_returns_none_for_unknown(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        assert app.get_handler("nonexistent") is None

    async def test_get_handler_returns_info(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        info = app.get_handler("ping")
        assert info is not None
        assert info.name == "ping"
        assert info.fn() == "pong"


class TestAppMiddleware:
    async def test_add_middleware(self, app: RpcApp):
        events: list[str] = []

        class TestMiddleware(AppMiddleware):
            async def dispatch_client(self, call_next, request, *, kwds=None):
                events.append("process_request")
                response = await call_next()
                events.append("process_response")
                return response

            async def dispatch_server(self, call_next, request, *, kwds=None):
                events.append("process_request")
                response = await call_next()
                events.append("process_response")
                return response

        middleware = TestMiddleware()
        app.add_middleware(middleware)

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"
        assert events.count("process_request") == 2  # client + server
        assert events.count("process_response") == 2  # server + client

    async def test_multiple_middleware_order(self, app: RpcApp):
        events: list[str] = []

        class MwA(AppMiddleware):
            async def dispatch_client(self, call_next, request, *, kwds=None):
                events.append("A-process_request")
                response = await call_next()
                events.append("A-process_response")
                return response

            async def dispatch_server(self, call_next, request, *, kwds=None):
                events.append("A-process_request")
                response = await call_next()
                events.append("A-process_response")
                return response

        class MwB(AppMiddleware):
            async def dispatch_client(self, call_next, request, *, kwds=None):
                events.append("B-process_request")
                response = await call_next()
                events.append("B-process_response")
                return response

            async def dispatch_server(self, call_next, request, *, kwds=None):
                events.append("B-process_request")
                response = await call_next()
                events.append("B-process_response")
                return response

        app.add_middleware(MwA())
        app.add_middleware(MwB())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"

        # dispatch runs A then B for client and server phases.
        # On each path A wraps B: A pre, B pre, B post, A post.
        assert events == [
            "A-process_request",
            "B-process_request",
            "A-process_request",
            "B-process_request",
            "B-process_response",
            "A-process_response",
            "B-process_response",
            "A-process_response",
        ]

    async def test_middleware_lifecycle(self):
        class LifecycleMiddleware(AppMiddleware):
            def __init__(self):
                self.init_called = False
                self.close_called = False

            async def init(self):
                self.init_called = True

            async def close(self):
                self.close_called = True

            async def dispatch_client(self, call_next, request, *, kwds=None):
                return await call_next()

            async def dispatch_server(self, call_next, request, *, kwds=None):
                return await call_next()

        mw = LifecycleMiddleware()
        transport = MockTransport()
        async with RpcApp(transport=transport) as app:
            app.add_middleware(mw)
            assert not mw.init_called
            assert not mw.close_called
            await app.init()
            assert mw.init_called
            assert not mw.close_called
        assert mw.close_called

    async def test_wire_middleware_order(self):
        events: list[str] = []

        class MwA(WireMiddleware):
            async def dispatch_client(self, call_next, request_raw_message, request, *, kwds=None):
                events.append("A-send")
                raw_response = await call_next()
                events.append("A-receive")
                return raw_response

            async def dispatch_server(self, call_next, request_raw_message, *, kwds=None):
                events.append("A-receive")
                result = await call_next()
                events.append("A-send")
                return result

        class MwB(WireMiddleware):
            async def dispatch_client(self, call_next, request_raw_message, request, *, kwds=None):
                events.append("B-send")
                raw_response = await call_next()
                events.append("B-receive")
                return raw_response

            async def dispatch_server(self, call_next, request_raw_message, *, kwds=None):
                events.append("B-receive")
                result = await call_next()
                events.append("B-send")
                return result

        transport = MockTransport()
        app = RpcApp(transport=transport)
        app.add_middleware(MwA())
        app.add_middleware(MwB())
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"

        # Onion pattern: outermost middleware pre runs first, post runs last.
        # On client: A-send, B-send, transport, B-receive, A-receive
        # On server: A-receive, B-receive, dispatch, B-send, A-send
        assert events == [
            "A-send",
            "B-send",
            "A-receive",
            "B-receive",
            "B-send",
            "A-send",
            "B-receive",
            "A-receive",
        ]

    async def test_wire_headers_roundtrip(self):
        server_headers: list[dict[str, str]] = []

        class HeaderInjector(WireMiddleware):
            async def dispatch_client(self, call_next, request_raw_message, request, *, kwds=None):
                request_raw_message.headers["x-custom"] = "test-value"
                return await call_next()

            async def dispatch_server(self, call_next, request_raw_message, *, kwds=None):
                server_headers.append(dict(request_raw_message.headers))
                return await call_next()

        transport = MockTransport()
        app = RpcApp(transport=transport)
        app.add_middleware(HeaderInjector())
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"
        assert server_headers[0].get("x-custom") == "test-value"

    async def test_custom_wire_header_survives_roundtrip(self):
        server_seen: list[str] = []

        class CustomWireHeaderMw(WireMiddleware):
            async def dispatch_client(self, call_next, request_raw_message, request, *, kwds=None):
                request_raw_message.headers["x-custom"] = "hello"
                return await call_next()

            async def dispatch_server(self, call_next, request_raw_message, *, kwds=None):
                server_seen.append(request_raw_message.headers.get("x-custom", ""))
                return await call_next()

        transport = MockTransport()
        app = RpcApp(transport=transport)
        app.add_middleware(CustomWireHeaderMw())
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"
        assert server_seen[0] == "hello"


class TestAppTimeoutsRouting:
    async def test_call_ttl_defaults_to_timeout(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call(timeout=5, ttl=None)
        assert result == "pong"
        assert transport.sent_messages[0].headers.get("ttl") == 5
        assert transport.sent_messages[0].headers.get("timeout") == 5

    async def test_call_explicit_ttl_rttl(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call(ttl=30, rttl=60)
        assert result == "pong"
        assert transport.sent_messages[0].headers.get("ttl") == 30
        assert transport.sent_messages[0].headers.get("rttl") == 60
        assert "timeout" not in transport.sent_messages[0].headers

    async def test_default_ttl(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, ttl=10)
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        await app.make("ping").call()
        assert transport.sent_messages[0].headers.get("ttl") == 10

    async def test_default_rttl(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, rttl=60)
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"

    async def test_publish_sends_timeout(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        req = app.make("test", text="hello")
        await app.publish(req.request, timeout=5)
        assert transport.sent_messages[0].headers.get("timeout") == 5
        assert transport.sent_messages[0].headers.get("ttl") == 5

    async def test_publish_sends_timeout_from_default(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, timeout=7)
        await app.init()

        req = app.make("test", text="hello")
        await app.publish(req.request)
        assert transport.sent_messages[0].headers.get("timeout") == 7
        assert transport.sent_messages[0].headers.get("ttl") == 7

    async def test_routing_key_without_group(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        call = app.make("add", 1, 2)
        assert call.request.headers["queue"] is None

    async def test_routing_key_with_group(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("service/add")
        def add(x: int, y: int) -> int:
            return x + y

        call = app.make("service/add", 1, 2)
        assert call.request.headers["queue"] == "service"

    async def test_group_method_call_success(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("service/add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        result = await app.make("service/add", 2, 3).call()
        assert result == 5


class TestAppValidation:
    async def test_validate_types_passes(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("add", validate_types=True)
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        result = await app.make("add", x=2, y=3).call()
        assert result == 5

    async def test_validate_types_fails_positional(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("add", validate_types=True)
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("add", "not", 3).call()
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR

    async def test_validate_types_fails_keyword(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("greet", validate_types=True)
        def greet(name: str, age: int) -> str:
            return f"{name} is {age}"

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("greet", name="Alice", age="old").call()
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR

    async def test_validate_types_disabled_by_default(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        with pytest.raises(RpcError) as exc_info:
            await app.make("add", "not", 3).call()
        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR

    async def test_validate_types_skips_depends(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        app.dependencies.add_singleton(str, lambda: "injected")
        await app.init()

        @app.method("ping", validate_types=True)
        def ping(db: Depends[str]) -> str:
            return db  # type: ignore

        await app.consume()
        result = await app.make("ping").call()
        assert result == "injected"

    async def test_validate_types_skips_unannotated(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("echo", validate_types=True)
        def echo(x) -> str:
            return str(x)

        await app.consume()
        result = await app.make("echo", x=42).call()
        assert result == "42"

    async def test_validate_types_optional(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("maybe", validate_types=True)
        def maybe(value: int | None = None) -> int | None:
            return value

        await app.consume()
        result = await app.make("maybe").call()
        assert result is None

    async def test_validate_types_complex(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("sum_list", validate_types=True)
        def sum_list(values: list[int]) -> int:
            return sum(values)

        @app.method("maybe", validate_types=True)
        def maybe(value: str | None = None) -> str | None:
            return value

        @app.method("identity", validate_types=True)
        def identity(value: int | str) -> int | str:
            return value

        await app.consume()

        result = await app.make("sum_list", values=[1, 2, 3]).call()
        assert result == 6

        result = await app.make("maybe").call()
        assert result is None

        result = await app.make("maybe", value="hello").call()
        assert result == "hello"

        result = await app.make("identity", value=42).call()
        assert result == 42

        result = await app.make("identity", value="text").call()
        assert result == "text"


class TestAppMultiSerializer:
    async def test_serializer_name(self):
        assert JsonSerializer().name == "linkr1.0"
        assert JsonRpcSerializer().name == "jsonrpc2.0"

    async def test_app_default_serializer(self):
        transport = MockTransport()
        app = RpcApp(transport=transport)
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"

    async def test_app_single_jsonrpc_serializer(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, serializer=JsonRpcSerializer())
        await app.init()

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        result = await app.make("add", 2, 3).call()
        assert result == 5

    async def test_app_multi_serializer_call_with_name(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, serializer=[JsonRpcSerializer(), JsonSerializer()])
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").call(serializer="linkr1.0")
        assert result == "pong"

    async def test_app_multi_serializer_default_is_first(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, serializer=[JsonRpcSerializer(), JsonSerializer()])
        await app.init()

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        result = await app.make("add", 2, 3).call()
        assert result == 5  # default = jsonrpc2.0, server auto-detects

    async def test_app_multi_serializer_auto_detect(self):
        transport = MockTransport()
        app = RpcApp(transport=transport, serializer=[JsonSerializer(), JsonRpcSerializer()])
        await app.init()

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()

        # Send using jsonrpc2.0, server should auto-detect
        result = await app.make("ping").call(serializer="jsonrpc2.0")
        assert result == "pong"
