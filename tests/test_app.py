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
    RPCError,
    RequestError,
    WireMiddleware,
    get_current_request,
)
from tests.helpers import call, emit, send


class TestAppCore:
    async def test_register_method(self, app: App):
        @app.method("ping")
        def pong() -> str:
            return "pong"

        assert "ping" in app.methods
        info = app.methods["ping"]
        assert info.name == "ping"
        assert info.request_type == "rpc"
        assert info.fn() == "pong"
        assert info.queue == "app"

    async def test_call_success(self, app: App):
        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        assert await call(app, "app.add", 2, 3) == 5

    async def test_call_with_context(self, app: App):
        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.consume()
        assert await app.call("app.add", (), {"x": 2, "y": 3}) == 5

    async def test_call_with_no_args(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        assert await app.call("app.ping", (), {}) == "pong"

    async def test_call_error_from_handler(self, app: App):
        @app.method("fail")
        def fail() -> str:
            msg = "something went wrong"
            raise ValueError(msg)

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.fail")
        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR

    async def test_custom_exception_handler_returns_rpc_error(self, app: App):
        class CustomError(Exception):
            pass

        @app.exception_handler(CustomError)
        def handle_custom(exc: Exception):
            return RPCError(error_code="CustomCode", error_message=str(exc))

        @app.method("raise_custom")
        def raise_custom():
            raise CustomError("bad stuff")

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.raise_custom")
        assert exc_info.value.error_code == "CustomCode"
        assert exc_info.value.error_message == "bad stuff"

    async def test_custom_exception_handler_returns_tuple(self, app: App):
        @app.exception_handler(ValueError)
        def handle_value_error(exc: Exception):
            return ("InvalidValueError", str(exc), {"field": "x"})

        @app.method("raise_value")
        def raise_value():
            raise ValueError("bad value")

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.raise_value")
        assert exc_info.value.error_code == "InvalidValueError"
        assert exc_info.value.error_message == "bad value"
        assert exc_info.value.error_details == {"field": "x"}

    async def test_call_method_not_found(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.invalid")
        assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

    async def test_task_send(self, app: App):
        received: list[str] = []

        @app.task("echo")
        def echo(text: str):
            received.append(text)

        await app.consume()
        await send(app, "app.echo", "hello")
        assert received == ["hello"]

    async def test_event_publish(self, app: App):
        received: list[str] = []

        @app.subscribe("test")
        def on_test(text: str):
            received.append(text)

        await app.consume()
        await emit(app, "test", text="hello")
        assert received == ["hello"]

    async def test_call_timeout(self, app: App):
        @app.method("sleep")
        async def sleep():
            await asyncio.sleep(10)

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.call("app.sleep", (), {}, timeout=0.1)
        assert exc_info.value.error_code == ErrorCode.TIMEOUT

    async def test_async_handler(self, app: App):
        @app.method("greet")
        async def greet(name: str) -> str:
            return f"Hello, {name}!"

        await app.consume()
        assert await app.call("app.greet", (), {"name": "World"}) == "Hello, World!"

    async def test_handlerinfo_includes_signature(self, app: App):
        @app.method("add")
        def add(x: int, y: int = 0) -> int:
            return x + y

        info = app.methods["add"]
        assert info.name == "add"
        assert "x" in info.signature
        assert "y" in info.signature
        assert info.options == {}

    async def test_rpc_error_from_handler_preserves_custom_code(self, app: App):
        @app.method("auth")
        def auth(token: str) -> str:
            raise RPCError(error_code="Unauthorized", error_message="bad token")

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.auth", token="invalid")
        assert exc_info.value.error_code == "Unauthorized"
        assert exc_info.value.error_message == "bad token"

    async def test_rpc_error_from_middleware_preserves_custom_code(self, app: App):
        events: list[str] = []

        class AuthMiddleware(AppMiddleware):
            async def dispatch_server(self, call_next, request):
                events.append("auth_check")
                raise RPCError(
                    error_code="Unauthorized",
                    error_message="access denied",
                    error_details={"method": request.data["method"]},
                )

        app.add_middleware(AuthMiddleware())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.ping")
        assert exc_info.value.error_code == "Unauthorized"
        assert exc_info.value.error_message == "access denied"
        assert exc_info.value.error_details == {"method": "ping"}
        assert events == ["auth_check"]


class TestAppLifecycle:
    async def test_app_is_context_manager(self):
        backend = LocalBackend()
        async with App("app", backend=backend) as app:

            @app.method("ping")
            def ping() -> str:
                return "pong"

            await app.consume()
            assert await app.call("app.ping", (), {}) == "pong"

    async def test_call_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await app.call("app.ping", (), {})

    async def test_send_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await app.send("app.test", (), {})

    async def test_consume_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await app.consume()

    async def test_default_timeout(self):
        app = App("app", backend=LocalBackend(), rpc_timeout=0.1)
        await app.init()

        @app.method("sleep")
        async def sleep(value: int):
            await asyncio.sleep(value)

        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.call("app.sleep", (10,), {})
        assert exc_info.value.error_code == ErrorCode.TIMEOUT
        await app.close()

    async def test_app_close_passes_timeout_to_backend(self):
        timeout_passed: list[float | None] = []

        class SpyBackend(LocalBackend):
            async def close(self, timeout: float | None = None) -> None:
                timeout_passed.append(timeout)
                await super().close(timeout=timeout)

        app = App("app", backend=SpyBackend())
        await app.init()
        await app.close(timeout=42.0)
        assert timeout_passed == [42.0]


class TestAppMiddleware:
    async def test_add_middleware(self):
        app = App("app", backend=LocalBackend())
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

        app.add_middleware(TestMiddleware())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()

        assert await app.call("app.ping", (), {}) == "pong"
        assert events.count("process_request") == 2
        assert events.count("process_response") == 2
        await app.close()

    async def test_multiple_middleware_order(self):
        app = App("app", backend=LocalBackend())
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

        app.add_middleware(MwA())
        app.add_middleware(MwB())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()
        assert await app.call("app.ping", (), {}) == "pong"

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
        app = App("app", backend=LocalBackend())
        app.add_middleware(mw)

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

        app = App("app", backend=LocalBackend())
        app.add_middleware(MwA())
        app.add_middleware(MwB())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()

        assert await app.call("app.ping", (), {}) == "pong"
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

        app = App("app", backend=LocalBackend())
        app.add_middleware(HeaderInjector())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()

        assert await app.call("app.ping", (), {}) == "pong"
        assert server_headers[0].get("x-custom") == "test-value"
        await app.close()


class TestAppTimeoutsRouting:
    async def test_call_ttl_defaults_to_timeout(self):
        app = App("app", backend=LocalBackend())
        headers: dict[str, object] = {}

        @app.method("ping")
        def ping() -> str:
            headers.update(get_current_request().headers)
            return "pong"

        await app.init()
        await app.consume()

        assert await app.call("app.ping", (), {}, timeout=5) == "pong"
        assert headers.get("ttl") == App.RPC_TTL
        assert headers.get("timeout") == 5
        await app.close()

    async def test_call_explicit_ttl_rttl(self):
        app = App("app", backend=LocalBackend())
        headers: dict[str, object] = {}

        @app.method("ping")
        def ping() -> str:
            headers.update(get_current_request().headers)
            return "pong"

        await app.init()
        await app.consume()

        assert await app.call("app.ping", (), {}, ttl=30, rttl=60) == "pong"
        assert headers.get("ttl") == 30
        assert headers.get("rttl") == 60
        assert headers.get("timeout") == App.RPC_TIMEOUT
        await app.close()

    async def test_default_ttl(self):
        app = App("app", backend=LocalBackend(), rpc_ttl=10)
        headers: dict[str, object] = {}

        @app.method("ping")
        def ping() -> str:
            headers.update(get_current_request().headers)
            return "pong"

        await app.init()
        await app.consume()
        await app.call("app.ping", (), {})
        assert headers.get("ttl") == 10
        await app.close()

    async def test_default_rttl(self):
        app = App("app", backend=LocalBackend(), rpc_rttl=60)

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()
        assert await app.call("app.ping", (), {}) == "pong"
        await app.close()

    async def test_task_sends_ttl(self):
        app = App("app", backend=LocalBackend())
        headers: dict[str, object] = {}

        @app.task("test")
        def test_handler(text: str) -> None:
            headers.update(get_current_request().headers)

        await app.init()
        await app.consume()
        await app.send("app.test", ("hello",), {}, ttl=5)
        assert headers.get("ttl") == 5
        await app.close()

    async def test_task_ttl_from_default(self):
        app = App("app", backend=LocalBackend(), rpc_ttl=7)
        headers: dict[str, object] = {}

        @app.task("test")
        def test_handler(text: str) -> None:
            headers.update(get_current_request().headers)

        await app.init()
        await app.consume()
        await app.send("app.test", ("hello",), {})
        assert headers.get("ttl") == 7
        await app.close()

    async def test_routing_key_without_group(self):
        app = App("app", backend=LocalBackend())
        seen: dict[str, object] = {}

        @app.method("add")
        def add(x: int, y: int) -> int:
            request = get_current_request()
            seen["method"] = request.data["method"]
            seen["queue"] = request.headers["queue"]
            return x + y

        await app.init()
        await app.consume()
        await app.call("app.add", (1, 2), {})
        assert seen["method"] == "add"
        assert seen["queue"] == "app"
        await app.close()

    async def test_routing_key_with_group(self):
        app = App("app", backend=LocalBackend())
        seen: dict[str, object] = {}

        @app.method("group.add")
        def add(x: int, y: int) -> int:
            request = get_current_request()
            seen["method"] = request.data["method"]
            seen["queue"] = request.headers["queue"]
            return x + y

        await app.init()
        await app.consume()
        await app.call("app.group.add", (1, 2), {})
        assert seen["method"] == "add"
        assert seen["queue"] == "app.group"
        await app.close()

    async def test_group_method_call_success(self):
        app = App("app", backend=LocalBackend())

        @app.method("group.add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        assert await app.call("app.group.add", (2, 3), {}) == 5
        await app.close()


class TestAppValidation:
    async def test_validate_types_passes(self):
        app = App("app", backend=LocalBackend())

        @app.method("add", validate_types=True)
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        assert await app.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_validate_types_fails_positional(self):
        app = App("app", backend=LocalBackend())

        @app.method("add", validate_types=True)
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_validate_types_fails_keyword(self):
        app = App("app", backend=LocalBackend())

        @app.method("greet", validate_types=True)
        def greet(name: str, age: int) -> str:
            return f"{name} is {age}"

        await app.init()
        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.call("app.greet", (), {"name": "Alice", "age": "old"})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_validate_types_disabled_by_default(self):
        app = App("app", backend=LocalBackend())

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR
        await app.close()

    async def test_validate_types_skips_depends(self):
        app = App("app", backend=LocalBackend())

        @app.method("ping", validate_types=True)
        def ping(db: Depends[str, lambda: "injected"]) -> str:  # noqa: UP037,F821
            return db

        await app.init()
        await app.consume()
        assert await app.call("app.ping", (), {}) == "injected"
        await app.close()

    async def test_validate_types_skips_unannotated(self):
        app = App("app", backend=LocalBackend())

        @app.method("echo", validate_types=True)
        def echo(x) -> str:
            return str(x)

        await app.init()
        await app.consume()
        assert await app.call("app.echo", (), {"x": 42}) == "42"
        await app.close()

    async def test_validate_types_optional(self):
        app = App("app", backend=LocalBackend())

        @app.method("maybe", validate_types=True)
        def maybe(value: int | None = None) -> int | None:
            return value

        await app.init()
        await app.consume()
        assert await app.call("app.maybe", (), {}) is None
        await app.close()

    async def test_validate_types_complex(self):
        app = App("app", backend=LocalBackend())

        @app.method("sum_list", validate_types=True)
        def sum_list(values: list[int]) -> int:
            return sum(values)

        @app.method("identity", validate_types=True)
        def identity(value: int | str) -> int | str:
            return value

        await app.init()
        await app.consume()

        assert await app.call("app.sum_list", (), {"values": [1, 2, 3]}) == 6
        assert await app.call("app.identity", (), {"value": 42}) == 42
        assert await app.call("app.identity", (), {"value": "text"}) == "text"
        await app.close()

    async def test_global_validate_types_enabled(self):
        app = App("app", backend=LocalBackend(), validate_types=True)

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_global_validate_types_override_local(self):
        app = App("app", backend=LocalBackend(), validate_types=True)

        @app.method("add", validate_types=False)
        def add(x: int, y: int) -> str:
            return str(x) + str(y)

        await app.init()
        await app.consume()
        assert await app.call("app.add", ("not", 3), {}) == "not3"
        await app.close()


class TestAppTaskValidation:
    async def test_task_validate_types_passes(self, app: App):
        received: list[dict[str, int]] = []

        @app.task("process", validate_types=True)
        def process(data: dict[str, int]) -> None:
            received.append(data)

        await app.consume()
        await app.send("app.process", (), {"data": {"x": 1}})
        assert received == [{"x": 1}]
        await app.close()

    async def test_task_validate_types_fails_logs_warning(self, app: App, caplog):
        received: list[dict[str, int]] = []

        @app.task("process", validate_types=True)
        def process(data: dict[str, int]) -> None:
            received.append(data)

        await app.consume()
        # Invalid type - should log warning, not call handler
        await app.send("app.process", (), {"data": {"x": "bad"}})
        await asyncio.sleep(0.1)  # allow async logging
        assert received == []
        assert any("Invalid arguments for task" in r.message for r in caplog.records)
        await app.close()


class TestAppEventValidation:
    async def test_event_validate_types_passes(self, app: App):
        received: list[int] = []

        @app.subscribe("user.created", validate_types=True)
        def on_user(user_id: int) -> None:
            received.append(user_id)

        await app.consume()
        await app.emit("user.created", {"user_id": 1})
        await asyncio.sleep(0.1)
        assert received == [1]
        await app.close()

    async def test_event_validate_types_fails_logs_warning(self, app: App, caplog):
        received: list[int] = []

        @app.subscribe("user.created", validate_types=True)
        def on_user(user_id: int) -> None:
            received.append(user_id)

        await app.consume()
        # Invalid type - should log warning, not call handler
        await app.emit("user.created", {"user_id": "bad"})
        await asyncio.sleep(0.1)
        assert received == []
        assert any("Invalid event arguments for" in r.message for r in caplog.records)
        await app.close()


class TestAppMultiSerializer:
    async def test_serializer_name(self):
        assert JsonSerializer().name == "linkr1.0"
        assert JsonRpcSerializer().name == "jsonrpc2.0"

    async def test_app_default_serializer(self):
        app = App("app", backend=LocalBackend())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()
        assert await app.call("app.ping", (), {}) == "pong"
        await app.close()

    async def test_app_single_jsonrpc_serializer(self):
        app = App("app", backend=LocalBackend(), serializer=JsonRpcSerializer())

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        assert await app.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_app_multi_serializer_call_with_name(self):
        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonRpcSerializer(), JsonSerializer()],
        )

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()
        assert await app.call("app.ping", (), {}, serializer="linkr1.0") == "pong"
        await app.close()

    async def test_app_multi_serializer_default_is_first(self):
        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonRpcSerializer(), JsonSerializer()],
        )

        @app.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await app.consume()
        assert await app.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_app_multi_serializer_auto_detect(self):
        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonSerializer(), JsonRpcSerializer()],
        )

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()
        assert await app.call("app.ping", (), {}, serializer="jsonrpc2.0") == "pong"
        await app.close()

    async def test_unknown_serializer_raises_request_error(self):
        app = App("app", backend=LocalBackend())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()
        with pytest.raises(RequestError) as exc_info:
            await app.call("app.ping", (), {}, serializer="missing")
        assert "unknown serializer: missing" in str(exc_info.value)
        await app.close()

    async def test_jsonrpc_rejects_task_request(self):
        app = App("app", backend=LocalBackend(), serializer=JsonRpcSerializer())
        with pytest.raises(ValueError, match="only RPC"):
            await app.send("app.test", (), {})


class TestStopConsume:
    async def test_stop_consume_rpc_specific_method_keeps_shared_queue(self, app: App):
        @app.method("foo")
        def foo() -> str:
            return "foo"

        @app.method("bar")
        def bar() -> str:
            return "bar"

        await app.consume()

        await app.stop_consume_rpc("foo")

        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.foo")
        assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND
        assert await call(app, "app.bar") == "bar"
        assert "foo" not in app.methods
        assert "bar" in app.methods
        assert "app" in app._consumed_rpc

    async def test_stop_consume_rpc_last_method_stops_queue(self):
        calls: list[str | None] = []

        class SpyBackend(LocalBackend):
            async def stop_consume_rpc(self, queue: str | None = None) -> None:
                calls.append(queue)
                await super().stop_consume_rpc(queue=queue)

        backend = SpyBackend()
        app = App("app", backend=backend)

        @app.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await app.consume()
            await app.stop_consume_rpc("ping")

        assert calls[0] == "app"
        assert "app" not in app._consumed_rpc
        assert "ping" not in app.methods

    async def test_stop_consume_rpc_all(self):
        calls: list[tuple[str, str | None]] = []

        class SpyBackend(LocalBackend):
            async def stop_consume_rpc(self, queue: str | None = None) -> None:
                calls.append(("rpc", queue))
                await super().stop_consume_rpc(queue=queue)

        backend = SpyBackend()
        app = App("app", backend=backend)

        @app.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await app.consume()
            await app.stop_consume_rpc()

        assert ("rpc", None) in calls
        assert app._consumed_rpc == set()

    async def test_stop_consume_rpc_unknown_raises(self, app: App):
        with pytest.raises(ValueError, match="RPC handler not registered"):
            await app.stop_consume_rpc("missing")

    async def test_stop_consume_tasks_specific_task_keeps_shared_queue(self):
        app = App("worker", backend=LocalBackend())
        received: list[str] = []

        @app.task("a")
        def task_a(value: str) -> None:
            received.append(f"a:{value}")

        @app.task("b")
        def task_b(value: str) -> None:
            received.append(f"b:{value}")

        async with app:
            await app.consume()
            await app.stop_consume_tasks("a")
            await send(app, "worker.a", "x")
            await send(app, "worker.b", "y")

            assert received == ["b:y"]
            assert "a" not in app.tasks
            assert "b" in app.tasks
            assert "worker" in app._consumed_tasks

    async def test_stop_consume_tasks_last_task_stops_queue(self):
        calls: list[str | None] = []

        class SpyBackend(LocalBackend):
            async def stop_consume_tasks(self, queue: str | None = None) -> None:
                calls.append(queue)
                await super().stop_consume_tasks(queue=queue)

        backend = SpyBackend()
        app = App("worker", backend=backend)

        @app.task("email.send")
        def send_email(email: str) -> None:
            pass

        async with app:
            await app.consume()
            await app.stop_consume_tasks("email.send")

        assert calls[0] == "worker.email"
        assert "worker.email" not in app._consumed_tasks
        assert "email.send" not in app.tasks

    async def test_stop_consume_tasks_unknown_raises(self, app: App):
        with pytest.raises(ValueError, match="task handler not registered"):
            await app.stop_consume_tasks("missing")

    async def test_stop_consume_events_specific_topic_keeps_others(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        seen: list[str] = []

        @app.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(f"created:{user_id}")

        @app.subscribe("user.deleted")
        def on_deleted(user_id: int) -> None:
            seen.append(f"deleted:{user_id}")

        async with app:
            await app.consume()
            await app.stop_consume_events("user.created")
            await emit(app, "user.created", user_id=1)
            await emit(app, "user.deleted", user_id=2)

            assert seen == ["deleted:2"]
            assert app._event_queue is not None
            assert "user.created" not in app._subscription_patterns
            assert "user.deleted" in app._subscription_patterns

    async def test_stop_consume_events_last_topic_stops_queue(self):
        calls: list[tuple[str | None, str | None]] = []

        class SpyBackend(LocalBackend):
            async def stop_consume_events(self, queue: str | None = None, topic: str | None = None) -> None:
                calls.append((queue, topic))
                await super().stop_consume_events(queue=queue, topic=topic)

        backend = SpyBackend()
        app = App("app", backend=backend)

        @app.subscribe("user.created")
        def on_created(user_id: int) -> None:
            pass

        async with app:
            await app.consume()
            queue = app._event_queue
            await app.stop_consume_events("user.created")

        assert calls[0] == (queue, None)
        assert app._event_queue is None
        assert app._subscription_patterns == set()

    async def test_stop_consume_events_all(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        seen: list[int] = []

        @app.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(user_id)

        async with app:
            await app.consume()
            await app.stop_consume_events()
            await emit(app, "user.created", user_id=1)

        assert seen == []
        assert app._event_queue is None
        assert app._subscription_patterns == set()

    async def test_stop_consume_events_unknown_raises(self, app: App):
        with pytest.raises(ValueError, match="event subscription not registered"):
            await app.stop_consume_events("missing")

    async def test_stop_consume_rpc_then_reconsume(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        await app.stop_consume_rpc("ping")

        @app.method("ping")
        def ping_again() -> str:
            return "pong again"

        await app.consume_rpc()
        assert await call(app, "app.ping") == "pong again"

    async def test_stop_consume_tasks_then_reconsume(self):
        app = App("worker", backend=LocalBackend())
        received: list[str] = []

        @app.task("job")
        def job(value: str) -> None:
            received.append(f"v1:{value}")

        async with app:
            await app.consume()
            await app.stop_consume_tasks("job")

            @app.task("job")
            def job2(value: str) -> None:
                received.append(f"v2:{value}")

            await app.consume_tasks()
            await send(app, "worker.job", "x")

        assert received == ["v2:x"]
