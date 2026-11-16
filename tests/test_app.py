from __future__ import annotations

import asyncio

import pytest

from linkr import (
    App,
    AppMiddleware,
    Depends,
    ErrorCode,
    JsonRpcSerializer,
    JsonSerializer,
    LocalBackend,
    RequestError,
    RPCError,
    WireMiddleware,
    get_current_request,
)
from tests.helpers import call, emit, send


class TestAppCore:
    async def test_register_method(self, app: App, server):
        @server.method("ping")
        def pong() -> str:
            return "pong"

        assert "ping" in server.methods
        info = server.methods["ping"]
        assert info.name == "app.ping"
        assert info.request_type == "rpc"
        assert info.fn() == "pong"
        assert info.queue == "app"

    async def test_call_success(self, app: App, server):
        @server.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await server.consume()
        assert await call(app, "app.add", 2, 3) == 5

    async def test_call_with_context(self, app: App, server):
        @server.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await server.consume()
        assert await app.client.call("app.add", (), {"x": 2, "y": 3}) == 5

    async def test_call_with_no_args(self, app: App, server):
        @server.method("ping")
        def ping() -> str:
            return "pong"

        await server.consume()
        assert await app.client.call("app.ping", (), {}) == "pong"

    async def test_call_error_from_handler(self, app: App, server):
        @server.method("fail")
        def fail() -> str:
            msg = "something went wrong"
            raise ValueError(msg)

        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.fail")
        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR

    async def test_custom_exception_handler_returns_rpc_error(self, app: App, server):
        class CustomError(Exception):
            pass

        @app.exception_handler(CustomError)
        def handle_custom(exc: Exception):
            return RPCError(error_code="CustomCode", error_message=str(exc))

        @server.method("raise_custom")
        def raise_custom():
            raise CustomError("bad stuff")

        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.raise_custom")
        assert exc_info.value.error_code == "CustomCode"
        assert exc_info.value.error_message == "bad stuff"

    async def test_custom_exception_handler_returns_tuple(self, app: App, server):
        @app.exception_handler(ValueError)
        def handle_value_error(exc: Exception):
            return ("InvalidValueError", str(exc), {"field": "x"})

        @server.method("raise_value")
        def raise_value():
            raise ValueError("bad value")

        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.raise_value")
        assert exc_info.value.error_code == "InvalidValueError"
        assert exc_info.value.error_message == "bad value"
        assert exc_info.value.error_details == {"field": "x"}

    async def test_call_method_not_found(self, app: App, server):
        @server.method("ping")
        def ping() -> str:
            return "pong"

        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.invalid")
        assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

    async def test_task_send(self, app: App, server):
        received: list[str] = []

        @server.task("echo")
        def echo(text: str):
            received.append(text)

        await server.consume()
        await send(app, "app.echo", "hello")
        assert received == ["hello"]

    async def test_event_publish(self, app: App, server):
        received: list[str] = []

        @server.subscribe("test")
        def on_test(text: str):
            received.append(text)

        await server.consume()
        await emit(app, "test", text="hello")
        assert received == ["hello"]

    async def test_call_timeout(self, app: App, server):
        @server.method("sleep")
        async def sleep():
            await asyncio.sleep(10)

        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.sleep", (), {}, timeout=0.1)
        assert exc_info.value.error_code == ErrorCode.TIMEOUT

    async def test_async_handler(self, app: App, server):
        @server.method("greet")
        async def greet(name: str) -> str:
            return f"Hello, {name}!"

        await server.consume()
        assert await app.client.call("app.greet", (), {"name": "World"}) == "Hello, World!"

    async def test_handlerinfo_includes_signature(self, app: App, server):
        @server.method("add")
        def add(x: int, y: int = 0) -> int:
            return x + y

        info = server.methods["add"]
        assert info.name == "app.add"
        assert "x" in info.signature
        assert "y" in info.signature
        assert info.options == {}

    async def test_rpc_error_from_handler_preserves_custom_code(self, app: App, server):
        @server.method("auth")
        def auth(token: str) -> str:
            raise RPCError(error_code="Unauthorized", error_message="bad token")

        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.auth", token="invalid")
        assert exc_info.value.error_code == "Unauthorized"
        assert exc_info.value.error_message == "bad token"

    async def test_rpc_error_from_middleware_preserves_custom_code(self):
        events: list[str] = []

        class AuthMiddleware(AppMiddleware):
            async def dispatch_server(self, call_next, request):
                events.append("auth_check")
                raise RPCError(
                    error_code="Unauthorized",
                    error_message="access denied",
                    error_details={"method": request.data["method"]},
                )

        app = App("app", backend=LocalBackend(), middlewares=[AuthMiddleware()])
        server = app.server()

        @server.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.ping")
        assert exc_info.value.error_code == "Unauthorized"
        assert exc_info.value.error_message == "access denied"
        assert exc_info.value.error_details == {"method": "ping"}
        assert events == ["auth_check"]
        await app.close()


class TestAppLifecycle:
    async def test_app_is_context_manager(self, server):
        backend = LocalBackend()
        async with App("app", backend=backend) as app:

            @server.method("ping")
            def ping() -> str:
                return "pong"

            await server.consume()
            assert await app.client.call("app.ping", (), {}) == "pong"

    async def test_call_on_closed_app_raises(self, server):
        app = App("app", backend=LocalBackend())
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await app.client.call("app.ping", (), {})

    async def test_send_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await app.client.send("app.test", (), {})

    async def test_consume_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        srv = app.server()
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await srv.consume()

    async def test_default_timeout(self):
        app = App("app", backend=LocalBackend(), rpc_timeout=0.1)
        srv = app.server()
        await app.init()

        @srv.method("sleep")
        async def sleep(value: int):
            await asyncio.sleep(value)

        await srv.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.sleep", (10,), {})
        assert exc_info.value.error_code == ErrorCode.TIMEOUT
        await app.close()


class TestAppMiddleware:
    async def test_add_middleware(self):
        events: list[str] = []

        class TestMiddleware(AppMiddleware):
            async def dispatch_client(self, call_next, request, *, context=None):
                events.append("process_request")
                response = await call_next()
                events.append("process_response")
                return response

            async def dispatch_server(self, call_next, request):
                events.append("process_request")
                response = await call_next()
                events.append("process_response")
                return response

        app = App("app", backend=LocalBackend(), middlewares=[TestMiddleware()])
        server = app.server()

        @server.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await server.consume()

        assert await app.client.call("app.ping", (), {}) == "pong"
        assert events.count("process_request") == 2
        assert events.count("process_response") == 2
        await app.close()

    async def test_multiple_middleware_order(self):
        events: list[str] = []

        class MwA(AppMiddleware):
            async def dispatch_client(self, call_next, request, *, context=None):
                events.append("A-process_request")
                response = await call_next()
                events.append("A-process_response")
                return response

            async def dispatch_server(self, call_next, request):
                events.append("A-process_request")
                response = await call_next()
                events.append("A-process_response")
                return response

        class MwB(AppMiddleware):
            async def dispatch_client(self, call_next, request, *, context=None):
                events.append("B-process_request")
                response = await call_next()
                events.append("B-process_response")
                return response

            async def dispatch_server(self, call_next, request):
                events.append("B-process_request")
                response = await call_next()
                events.append("B-process_response")
                return response

        app = App("app", backend=LocalBackend(), middlewares=[MwA(), MwB()])
        server = app.server()

        @server.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await server.consume()
        assert await app.client.call("app.ping", (), {}) == "pong"

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
        await app.close()

    async def test_middleware_lifecycle(self):
        class LifecycleMiddleware(AppMiddleware):
            def __init__(self):
                self.init_called = False
                self.close_called = False

            async def init(self):
                self.init_called = True

            async def close(self):
                self.close_called = True

        mw = LifecycleMiddleware()
        app = App("app", backend=LocalBackend(), middlewares=[mw])

        await app.init()
        assert mw.init_called
        assert not mw.close_called

        await app.close()
        assert mw.close_called

    async def test_wire_middleware_order(self):
        events: list[str] = []

        class MwA(WireMiddleware):
            async def dispatch_client(self, call_next, request, request_message, *, context=None):
                events.append("A-send")
                response = await call_next()
                events.append("A-receive")
                return response

            async def dispatch_server(self, call_next, request_message):
                events.append("A-receive")
                result = await call_next()
                events.append("A-send")
                return result

        class MwB(WireMiddleware):
            async def dispatch_client(self, call_next, request, request_message, *, context=None):
                events.append("B-send")
                response = await call_next()
                events.append("B-receive")
                return response

            async def dispatch_server(self, call_next, request_message):
                events.append("B-receive")
                result = await call_next()
                events.append("B-send")
                return result

        app = App("app", backend=LocalBackend(), middlewares=[MwA(), MwB()])
        srv = app.server()

        @srv.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await srv.consume()

        assert await app.client.call("app.ping", (), {}) == "pong"
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
        await app.close()

    async def test_wire_headers_roundtrip(self):
        server_headers: list[dict[str, str]] = []

        class HeaderInjector(WireMiddleware):
            async def dispatch_client(self, call_next, request, request_message, *, context=None):
                request_message.headers["x-custom"] = "test-value"
                return await call_next()

            async def dispatch_server(self, call_next, request_message):
                server_headers.append(dict(request_message.headers))
                return await call_next()

        app = App("app", backend=LocalBackend(), middlewares=[HeaderInjector()])
        srv = app.server()

        @srv.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await srv.consume()

        assert await app.client.call("app.ping", (), {}) == "pong"
        assert server_headers[0].get("x-custom") == "test-value"
        await app.close()


class TestAppTimeoutsRouting:
    async def test_call_ttl_defaults_to_timeout(self, server):
        app = App("app", backend=LocalBackend())
        headers: dict[str, object] = {}

        @server.method("ping")
        def ping() -> str:
            headers.update(get_current_request().headers)
            return "pong"

        await app.init()
        await server.consume()

        assert await app.client.call("app.ping", (), {}, timeout=5) == "pong"
        assert headers.get("ttl") == App.RPC_TTL
        assert headers.get("timeout") == 5
        await app.close()

    async def test_call_explicit_ttl_rttl(self, server):
        app = App("app", backend=LocalBackend())
        headers: dict[str, object] = {}

        @server.method("ping")
        def ping() -> str:
            headers.update(get_current_request().headers)
            return "pong"

        await app.init()
        await server.consume()

        assert await app.client.call("app.ping", (), {}, ttl=30, rttl=60) == "pong"
        assert headers.get("ttl") == 30
        assert headers.get("rttl") == 60
        assert headers.get("timeout") == App.RPC_TIMEOUT
        await app.close()

    async def test_default_ttl(self, server):
        app = App("app", backend=LocalBackend(), rpc_ttl=10)
        headers: dict[str, object] = {}

        @server.method("ping")
        def ping() -> str:
            headers.update(get_current_request().headers)
            return "pong"

        await app.init()
        await server.consume()
        await app.client.call("app.ping", (), {})
        assert headers.get("ttl") == 10
        await app.close()

    async def test_default_rttl(self, server):
        app = App("app", backend=LocalBackend(), rpc_rttl=60)

        @server.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await server.consume()
        assert await app.client.call("app.ping", (), {}) == "pong"
        await app.close()

    async def test_task_sends_ttl(self, server):
        app = App("app", backend=LocalBackend())
        headers: dict[str, object] = {}

        @server.task("test")
        def test_handler(text: str) -> None:
            headers.update(get_current_request().headers)

        await app.init()
        await server.consume()
        await app.client.send("app.test", ("hello",), {}, ttl=5)
        assert headers.get("ttl") == 5
        await app.close()

    async def test_task_ttl_from_default(self):
        app = App("app", backend=LocalBackend(), task_ttl=7)
        srv = app.server()
        headers: dict[str, object] = {}

        @srv.task("test")
        def test_handler(text: str) -> None:
            headers.update(get_current_request().headers)

        await app.init()
        await srv.consume()
        await app.client.send("app.test", ("hello",), {})
        assert headers.get("ttl") == 7
        await app.close()

    async def test_routing_key_without_group(self, server):
        app = App("app", backend=LocalBackend())
        seen: dict[str, object] = {}

        @server.method("add")
        def add(x: int, y: int) -> int:
            request = get_current_request()
            seen["method"] = request.data["method"]
            seen["queue"] = request.headers["queue"]
            return x + y

        await app.init()
        await server.consume()
        await app.client.call("app.add", (1, 2), {})
        assert seen["method"] == "add"
        assert seen["queue"] == "app"
        await app.close()

    async def test_routing_key_with_group(self):
        app = App("app", backend=LocalBackend())
        grp = app.server("group")
        seen: dict[str, object] = {}

        @grp.method("add")
        def add(x: int, y: int) -> int:
            request = get_current_request()
            seen["method"] = request.data["method"]
            seen["queue"] = request.headers["queue"]
            return x + y

        await app.init()
        await grp.consume()
        await app.client.call("app.group.add", (1, 2), {})
        assert seen["method"] == "add"
        assert seen["queue"] == "app.group"
        await app.close()

    async def test_group_method_call_success(self):
        app = App("app", backend=LocalBackend())
        grp = app.server("group")

        @grp.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await grp.consume()
        assert await app.client.call("app.group.add", (2, 3), {}) == 5
        await app.close()


class TestAppValidation:
    async def test_validate_types_passes(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("add", validate_types=True)
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await server.consume()
        assert await app.client.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_validate_types_fails_positional(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("add", validate_types=True)
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_validate_types_fails_keyword(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("greet", validate_types=True)
        def greet(name: str, age: int) -> str:
            return f"{name} is {age}"

        await app.init()
        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.greet", (), {"name": "Alice", "age": "old"})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_validate_types_disabled_by_default(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR
        await app.close()

    async def test_validate_types_skips_depends(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("ping", validate_types=True)
        def ping(db: Depends[str, lambda: "injected"]) -> str:  # noqa: UP037,F821
            return db

        await app.init()
        await server.consume()
        assert await app.client.call("app.ping", (), {}) == "injected"
        await app.close()

    async def test_validate_types_skips_unannotated(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("echo", validate_types=True)
        def echo(x) -> str:
            return str(x)

        await app.init()
        await server.consume()
        assert await app.client.call("app.echo", (), {"x": 42}) == "42"
        await app.close()

    async def test_validate_types_optional(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("maybe", validate_types=True)
        def maybe(value: int | None = None) -> int | None:
            return value

        await app.init()
        await server.consume()
        assert await app.client.call("app.maybe", (), {}) is None
        await app.close()

    async def test_validate_types_complex(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("sum_list", validate_types=True)
        def sum_list(values: list[int]) -> int:
            return sum(values)

        @server.method("identity", validate_types=True)
        def identity(value: int | str) -> int | str:
            return value

        await app.init()
        await server.consume()

        assert await app.client.call("app.sum_list", (), {"values": [1, 2, 3]}) == 6
        assert await app.client.call("app.identity", (), {"value": 42}) == 42
        assert await app.client.call("app.identity", (), {"value": "text"}) == "text"
        await app.close()

    async def test_global_validate_types_enabled(self):
        app = App("app", backend=LocalBackend(), validate_types=True)
        srv = app.server()

        @srv.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await srv.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_global_validate_types_override_local(self, server):
        app = App("app", backend=LocalBackend(), validate_types=True)

        @server.method("add", validate_types=False)
        def add(x: int, y: int) -> str:
            return str(x) + str(y)

        await app.init()
        await server.consume()
        assert await app.client.call("app.add", ("not", 3), {}) == "not3"
        await app.close()


class TestAppTaskValidation:
    async def test_task_validate_types_passes(self, app: App, server):
        received: list[dict[str, int]] = []

        @server.task("process", validate_types=True)
        def process(data: dict[str, int]) -> None:
            received.append(data)

        await server.consume()
        await app.client.send("app.process", (), {"data": {"x": 1}})
        assert received == [{"x": 1}]
        await app.close()

    async def test_task_validate_types_fails_logs_warning(self, app: App, server, caplog):
        received: list[dict[str, int]] = []

        @server.task("process", validate_types=True)
        def process(data: dict[str, int]) -> None:
            received.append(data)

        await server.consume()
        # Invalid type - should log warning, not call handler
        await app.client.send("app.process", (), {"data": {"x": "bad"}})
        await asyncio.sleep(0.1)  # allow async logging
        assert received == []
        assert any("Invalid arguments for task" in r.message for r in caplog.records)
        await app.close()


class TestAppEventValidation:
    async def test_event_validate_types_passes(self, app: App, server):
        received: list[int] = []

        @server.subscribe("user.created", validate_types=True)
        def on_user(user_id: int) -> None:
            received.append(user_id)

        await server.consume()
        await app.client.emit("user.created", {"user_id": 1})
        await asyncio.sleep(0.1)
        assert received == [1]
        await app.close()

    async def test_event_validate_types_fails_logs_warning(self, app: App, server, caplog):
        received: list[int] = []

        @server.subscribe("user.created", validate_types=True)
        def on_user(user_id: int) -> None:
            received.append(user_id)

        await server.consume()
        # Invalid type - should log warning, not call handler
        await app.client.emit("user.created", {"user_id": "bad"})
        await asyncio.sleep(0.1)
        assert received == []
        assert any("Invalid event arguments for" in r.message for r in caplog.records)
        await app.close()


class TestAppMultiSerializer:
    async def test_serializer_name(self, server):
        assert JsonSerializer().name == "linkr1.0"
        assert JsonRpcSerializer().name == "jsonrpc2.0"

    async def test_app_default_serializer(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await server.consume()
        assert await app.client.call("app.ping", (), {}) == "pong"
        await app.close()

    async def test_app_single_jsonrpc_serializer(self):
        app = App("app", backend=LocalBackend(), serializer=JsonRpcSerializer())
        srv = app.server()

        @srv.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await srv.consume()
        assert await app.client.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_app_multi_serializer_call_with_name(self, server):
        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonRpcSerializer(), JsonSerializer()],
        )

        @server.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await server.consume()
        assert await app.client.call("app.ping", (), {}, serializer="linkr1.0") == "pong"
        await app.close()

    async def test_app_multi_serializer_default_is_first(self):
        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonRpcSerializer(), JsonSerializer()],
        )
        srv = app.server()

        @srv.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await srv.consume()
        assert await app.client.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_app_multi_serializer_auto_detect(self):
        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonSerializer(), JsonRpcSerializer()],
        )
        srv = app.server()

        @srv.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await srv.consume()
        assert await app.client.call("app.ping", (), {}, serializer="jsonrpc2.0") == "pong"
        await app.close()

    async def test_unknown_serializer_raises_request_error(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await server.consume()
        with pytest.raises(RequestError) as exc_info:
            await app.client.call("app.ping", (), {}, serializer="missing")
        assert "unknown serializer: missing" in str(exc_info.value)
        await app.close()

    async def test_jsonrpc_rejects_task_request(self, server):
        app = App("app", backend=LocalBackend(), serializer=JsonRpcSerializer())
        with pytest.raises(ValueError, match="only RPC"):
            await app.client.send("app.test", (), {})


class TestStopConsume:
    async def test_stop_consume_rpc_pause_and_resume(self, app: App, server):
        @server.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await server.consume(mode="rpc")
            assert await call(app, "app.ping") == "pong"

            await server.stop_consume(mode="rpc")
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.ping")

            await server.consume(mode="rpc")
            assert await call(app, "app.ping") == "pong"
            assert "ping" in server.methods

    async def test_stop_consume_rpc_idempotent(self, server):
        backend = LocalBackend()
        app = App("app", backend=backend)
        srv = app.server()

        @srv.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await srv.consume(mode="rpc")
            await srv.stop_consume(mode="rpc")
            await srv.stop_consume(mode="rpc")
            await srv.consume(mode="rpc")
            await srv.consume(mode="rpc")
            assert await call(app, "app.ping") == "pong"

    async def test_stop_consume_tasks_pause_and_resume(self):
        app = App("worker", backend=LocalBackend())
        srv = app.server()
        received: list[str] = []

        @srv.task("job")
        def job(value: str) -> None:
            received.append(value)

        async with app:
            await srv.consume(mode="task")
            await send(app, "worker.job", "a")
            await srv.stop_consume(mode="task")
            await send(app, "worker.job", "b")
            await srv.consume(mode="task")
            await send(app, "worker.job", "c")

        assert received == ["a", "c"]
        assert "job" in srv.tasks

    async def test_stop_consume_events_pause_and_resume(self):
        app = App("app", backend=LocalBackend())
        srv = app.server()
        seen: list[int] = []

        @srv.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(user_id)

        async with app:
            await srv.consume(mode="event")
            queue = srv.event_queue
            assert queue is not None
            await emit(app, "user.created", user_id=1)

            await srv.stop_consume(mode="event")
            await emit(app, "user.created", user_id=2)

            await srv.consume(mode="event")
            await emit(app, "user.created", user_id=3)

        assert seen == [1, 3]
        assert "user.created" in srv.subscriptions

    async def test_stop_consume_all_modes(self):
        app = App("app", backend=LocalBackend())
        srv = app.server()
        events: list[str] = []

        @srv.method("ping")
        def ping() -> str:
            return "pong"

        @srv.task("job")
        def job(v: str) -> None:
            events.append(f"t:{v}")

        @srv.subscribe("evt")
        def on_evt(v: str) -> None:
            events.append(f"e:{v}")

        async with app:
            await srv.consume()
            assert await call(app, "app.ping") == "pong"
            await send(app, "app.job", "a")
            await emit(app, "evt", v="a")

            await srv.stop_consume()
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.ping")
            await send(app, "app.job", "b")
            await emit(app, "evt", v="b")

            await srv.consume()
            assert await call(app, "app.ping") == "pong"
            await send(app, "app.job", "c")
            await emit(app, "evt", v="c")

        assert "t:a" in events and "t:c" in events and "t:b" not in events
        assert "e:a" in events and "e:c" in events and "e:b" not in events
