from __future__ import annotations

import asyncio

import pytest

from linkr import (
    App,
    AppMiddleware,
    Depends,
    ErrorCode,
    HandlerContext,
    HealthStatus,
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

    async def test_task_send(self, app: App, worker):
        received: list[str] = []

        @worker.task("echo")
        def echo(text: str):
            received.append(text)

        await worker.consume()
        await send(app, "app.echo", "hello")
        assert received == ["hello"]

    async def test_event_publish(self, app: App, bus):
        received: list[str] = []

        @bus.subscribe("test")
        def on_test(text: str):
            received.append(text)

        await bus.consume()
        await emit(app, "test", text="hello")
        assert received == ["hello"]

    async def test_call_timeout(self, app: App, server):
        @server.method("sleep")
        async def sleep():
            await asyncio.sleep(10)

        await server.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.sleep", (), {}, client_timeout=0.1)
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
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await rpc.consume()
        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.ping")
        assert exc_info.value.error_code == "Unauthorized"
        assert exc_info.value.error_message == "access denied"
        assert exc_info.value.error_details == {"method": "ping"}
        assert events == ["auth_check"]
        await app.close()


class TestAppLifecycle:
    async def test_app_is_context_manager(self):
        backend = LocalBackend()
        async with App("app", backend=backend) as app:
            rpc = app.rpc()

            @rpc.method("ping")
            def ping() -> str:
                return "pong"

            await rpc.consume()
            assert await app.client.call("app.ping", (), {}) == "pong"

    async def test_call_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError):
            await app.client.call("app.ping", (), {})

    async def test_send_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await app.client.send("app.test", (), {})

    async def test_consume_on_closed_app_raises(self):
        app = App("app", backend=LocalBackend())
        rpc = app.rpc()
        await app.init()
        await app.close()
        with pytest.raises(RuntimeError, match="App is closed"):
            await rpc.consume()

    async def test_default_timeout(self):
        app = App("app", backend=LocalBackend(), rpc_server_timeout=0.1)
        rpc = app.rpc()
        await app.init()

        @rpc.method("sleep")
        async def sleep(value: int):
            await asyncio.sleep(value)

        await rpc.consume()
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
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await rpc.consume()

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
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await rpc.consume()
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
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await rpc.consume()

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
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await rpc.consume()

        assert await app.client.call("app.ping", (), {}) == "pong"
        assert server_headers[0].get("x-custom") == "test-value"
        await app.close()


class TestHandlerMiddleware:
    async def test_dispatch_handler_receives_context(self):
        seen: list[HandlerContext] = []

        class HandlerMw(AppMiddleware):
            async def dispatch_handler(self, call_next, request, handler_ctx):
                seen.append(handler_ctx)
                return await call_next()

        app = App("app", backend=LocalBackend(), middlewares=[HandlerMw()])
        rpc = app.rpc()

        @rpc.method("ping", validate_types=True)
        def ping() -> str:
            return "pong"

        async with app:
            await rpc.consume()
            assert await call(app, "app.ping") == "pong"

        assert len(seen) == 1
        ctx = seen[0]
        assert isinstance(ctx, HandlerContext)
        assert ctx.name == "app.ping"
        assert ctx.request_type == "rpc"
        assert ctx.queue == "app"
        assert ctx.validate_args is False
        assert dict(ctx.options) == {"validate_types": True}

    async def test_dispatch_handler_runs_after_before_handler_hook(self):
        events: list[str] = []

        class HandlerMw(AppMiddleware):
            async def dispatch_handler(self, call_next, request, handler_ctx):
                events.append("middleware:before")
                result = await call_next()
                events.append("middleware:after")
                return result

        app = App("app", backend=LocalBackend(), middlewares=[HandlerMw()])
        rpc = app.rpc()

        @app.hook.before_handler
        async def on_before(event):
            events.append("hook:before")

        @app.hook.after_handler
        async def on_after(event):
            events.append("hook:after")

        @rpc.method("ping")
        def ping() -> str:
            events.append("handler")
            return "pong"

        async with app:
            await rpc.consume()
            assert await call(app, "app.ping") == "pong"

        assert events == ["hook:before", "middleware:before", "handler", "middleware:after", "hook:after"]

    async def test_dispatch_handler_can_abort_request(self):
        called = False

        class AuthMw(AppMiddleware):
            async def dispatch_handler(self, call_next, request, handler_ctx):
                if handler_ctx.options.get("protected"):
                    raise RPCError(error_code="Unauthorized", error_message="denied")
                return await call_next()

        app = App("app", backend=LocalBackend(), middlewares=[AuthMw()])
        rpc = app.rpc()

        @rpc.method("secret", protected=True)
        def secret() -> str:
            nonlocal called
            called = True
            return "top-secret"

        async with app:
            await rpc.consume()
            with pytest.raises(RPCError) as exc_info:
                await call(app, "app.secret")
            assert exc_info.value.error_code == "Unauthorized"
            assert called is False

    async def test_dispatch_handler_context_is_read_only(self):
        class MutateMw(AppMiddleware):
            async def dispatch_handler(self, call_next, request, handler_ctx):
                with pytest.raises(Exception):
                    handler_ctx.options["x"] = 1  # type: ignore[index]
                return await call_next()

        app = App("app", backend=LocalBackend(), middlewares=[MutateMw()])
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await rpc.consume()
            assert await call(app, "app.ping") == "pong"

    async def test_dispatch_handler_for_task(self):
        seen: list[HandlerContext] = []

        class HandlerMw(AppMiddleware):
            async def dispatch_handler(self, call_next, request, handler_ctx):
                seen.append(handler_ctx)
                return await call_next()

        app = App("app", backend=LocalBackend(), middlewares=[HandlerMw()])
        worker = app.worker()

        @worker.task("job", retries=3)
        def job() -> None:
            return None

        async with app:
            await worker.consume()
            await send(app, "app.job")

        assert len(seen) == 1
        ctx = seen[0]
        assert ctx.name == "app.job"
        assert ctx.request_type == "task"
        assert ctx.queue == "app"
        assert dict(ctx.options) == {"retries": 3}

    async def test_dispatch_handler_for_event(self):
        seen: list[HandlerContext] = []

        class HandlerMw(AppMiddleware):
            async def dispatch_handler(self, call_next, request, handler_ctx):
                seen.append(handler_ctx)
                return await call_next()

        app = App("app", backend=LocalBackend(), middlewares=[HandlerMw()])
        bus = app.bus()

        @bus.subscribe("user.*", group="analytics")
        def on_user(user_id: int) -> None:
            return None

        async with app:
            await bus.consume()
            event_queue = bus.event_queue
            await emit(app, "user.created", user_id=1)

        assert len(seen) == 1
        ctx = seen[0]
        assert ctx.name == "user.*"
        assert ctx.request_type == "event"
        assert ctx.queue == event_queue
        assert dict(ctx.options) == {"group": "analytics"}


class TestAppTimeoutsRouting:
    async def test_call_server_timeout_default_from_ttl_params(self, server):
        app = App("app", backend=LocalBackend())
        headers: dict[str, object] = {}

        @server.method("ping")
        def ping() -> str:
            headers.update(get_current_request().headers)
            return "pong"

        await app.init()
        await server.consume()

        assert await app.client.call("app.ping", (), {}, server_timeout=5) == "pong"
        assert headers.get("ttl") == App.RPC_TTL
        assert headers.get("server_timeout") == 5
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
        assert headers.get("server_timeout") == App.RPC_SERVER_TIMEOUT
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

    async def test_task_sends_ttl_and_server_timeout(self):
        app = App("app", backend=LocalBackend())
        worker = app.worker()
        headers: dict[str, object] = {}

        @worker.task("test")
        def test_handler(text: str) -> None:
            headers.update(get_current_request().headers)

        await app.init()
        await worker.consume()
        await app.client.send("app.test", ("hello",), {}, ttl=5, server_timeout=7)
        assert headers.get("ttl") == 5
        assert headers.get("server_timeout") == 7
        await app.close()

    async def test_task_ttl_and_server_timeout_from_default(self):
        app = App("app", backend=LocalBackend(), task_ttl=7)
        worker = app.worker()
        headers: dict[str, object] = {}

        @worker.task("test")
        def test_handler(text: str) -> None:
            headers.update(get_current_request().headers)

        await app.init()
        await worker.consume()
        await app.client.send("app.test", ("hello",), {})
        assert headers.get("ttl") == 7
        assert headers.get("server_timeout") == App.TASK_SERVER_TIMEOUT
        await app.close()

    async def test_event_sends_ttl_without_server_timeout(self):
        app = App("app", backend=LocalBackend(), event_ttl=11)
        bus = app.bus()
        headers: dict[str, object] = {}

        @bus.subscribe("evt")
        def on_evt(x: int) -> None:
            headers.update(get_current_request().headers)

        await app.init()
        await bus.consume()
        await app.client.emit("evt", {"x": 1}, ttl=9)
        await asyncio.sleep(0.05)
        assert headers.get("ttl") == 9
        assert "server_timeout" not in headers
        await app.close()

    async def test_negative_timeouts_normalized_to_zero(self, server):
        app = App("app", backend=LocalBackend())

        @server.method("ping")
        def ping() -> str:
            return "pong"

        @server.method("slow")
        async def slow() -> str:
            await asyncio.sleep(0.2)
            return "ok"

        await app.init()
        await server.consume()

        # sync handler returns immediately; server_timeout applies to async handler execution
        assert await app.client.call("app.ping", (), {}, server_timeout=-5) == "pong"

        # negative client_timeout treated as 0 -> immediate client wait timeout
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.slow", (), {}, client_timeout=-5)
        assert exc_info.value.error_code == ErrorCode.TIMEOUT
        assert exc_info.value.error_message == "Client wait timed out"

        # negative server_timeout treated as 0 -> immediate handler execution timeout
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.slow", (), {}, server_timeout=-5)
        assert exc_info.value.error_code == ErrorCode.TIMEOUT
        assert exc_info.value.error_message == "Handler execution timed out"
        await app.close()

    async def test_negative_default_timeouts_normalized(self):
        app = App(
            "app",
            backend=LocalBackend(),
            rpc_client_timeout=-1,
            rpc_server_timeout=-1,
            task_client_timeout=-1,
            task_server_timeout=-1,
            event_client_timeout=-1,
        )
        client = app.client
        assert client._rpc_client_timeout == 0.0
        assert client._rpc_server_timeout == 0.0
        assert client._task_client_timeout == 0.0
        assert client._task_server_timeout == 0.0
        assert client._event_client_timeout == 0.0
        await app.init()
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
        grp = app.rpc("group")
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
        grp = app.rpc("group")

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
        rpc = app.rpc()

        @rpc.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await rpc.consume()
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

    async def test_server_validate_types_enabled(self):
        app = App("app", backend=LocalBackend(), validate_types=False)
        rpc = app.rpc(validate_types=True)

        @rpc.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await rpc.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_server_validate_types_overrides_app(self):
        app = App("app", backend=LocalBackend(), validate_types=True)
        rpc = app.rpc(validate_types=False)

        @rpc.method("add")
        def add(x: int, y: int) -> str:
            return str(x) + str(y)

        await app.init()
        await rpc.consume()
        assert await app.client.call("app.add", ("not", 3), {}) == "not3"
        await app.close()

    async def test_handler_validate_types_overrides_server(self):
        app = App("app", backend=LocalBackend(), validate_types=False)
        rpc = app.rpc(validate_types=False)

        @rpc.method("add", validate_types=True)
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await rpc.consume()
        with pytest.raises(RPCError) as exc_info:
            await app.client.call("app.add", ("not", 3), {})
        assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR
        await app.close()

    async def test_server_validate_types_idempotent_same_config(self):
        app = App("app", backend=LocalBackend())
        s1 = app.rpc("svc", validate_types=True)
        s2 = app.rpc("svc", validate_types=True)
        assert s1 is s2

    async def test_server_validate_types_conflict_rejected(self):
        app = App("app", backend=LocalBackend())
        app.rpc("svc", validate_types=True)
        with pytest.raises(ValueError, match="already configured"):
            app.rpc("svc", validate_types=False)


class TestAppTaskValidation:
    async def test_task_validate_types_passes(self, app: App, worker):
        received: list[dict[str, int]] = []

        @worker.task("process", validate_types=True)
        def process(data: dict[str, int]) -> None:
            received.append(data)

        await worker.consume()
        await app.client.send("app.process", (), {"data": {"x": 1}})
        assert received == [{"x": 1}]

    async def test_task_validate_types_fails_logs_warning(self, app: App, worker, caplog):
        received: list[dict[str, int]] = []

        @worker.task("process", validate_types=True)
        def process(data: dict[str, int]) -> None:
            received.append(data)

        await worker.consume()
        # Invalid type - should log warning, not call handler
        await app.client.send("app.process", (), {"data": {"x": "bad"}})
        await asyncio.sleep(0.1)  # allow async logging
        assert received == []
        assert any("Invalid arguments for task" in r.message for r in caplog.records)


class TestAppEventValidation:
    async def test_event_validate_types_passes(self, app: App, bus):
        received: list[int] = []

        @bus.subscribe("user.created", validate_types=True)
        def on_user(user_id: int) -> None:
            received.append(user_id)

        await bus.consume()
        await app.client.emit("user.created", {"user_id": 1})
        await asyncio.sleep(0.1)
        assert received == [1]

    async def test_event_validate_types_fails_logs_warning(self, app: App, bus, caplog):
        received: list[int] = []

        @bus.subscribe("user.created", validate_types=True)
        def on_user(user_id: int) -> None:
            received.append(user_id)

        await bus.consume()
        # Invalid type - should log warning, not call handler
        await app.client.emit("user.created", {"user_id": "bad"})
        await asyncio.sleep(0.1)
        assert received == []
        assert any("Invalid event arguments for" in r.message for r in caplog.records)


class TestAppMultiSerializer:
    async def test_serializer_name(self, server):
        assert JsonSerializer().name == "linkr1.0"

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
        from linkr import JsonRpcSerializer

        app = App("app", backend=LocalBackend(), serializer=JsonRpcSerializer())
        srv = app.rpc()

        @srv.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await srv.consume()
        assert await app.client.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_app_multi_serializer_call_with_name(self, server):
        from linkr import JsonRpcSerializer

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
        from linkr import JsonRpcSerializer

        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonRpcSerializer(), JsonSerializer()],
        )
        srv = app.rpc()

        @srv.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await app.init()
        await srv.consume()
        assert await app.client.call("app.add", (2, 3), {}) == 5
        await app.close()

    async def test_app_multi_serializer_auto_detect(self):
        from linkr import JsonRpcSerializer

        app = App(
            "app",
            backend=LocalBackend(),
            serializer=[JsonSerializer(), JsonRpcSerializer()],
        )
        srv = app.rpc()

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
        from linkr import JsonRpcSerializer

        app = App("app", backend=LocalBackend(), serializer=JsonRpcSerializer())
        with pytest.raises(ValueError, match="only RPC"):
            await app.client.send("app.test", (), {})


class TestStopConsume:
    async def test_stop_consume_rpc_pause_and_resume(self):
        app = App("app", backend=LocalBackend())
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await rpc.consume()
            assert await call(app, "app.ping") == "pong"

            await rpc.stop_consume()
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.ping")

            await rpc.consume()
            assert await call(app, "app.ping") == "pong"
            assert "ping" in rpc.methods

    async def test_stop_consume_rpc_idempotent(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await rpc.consume()
            await rpc.stop_consume()
            await rpc.stop_consume()
            await rpc.consume()
            await rpc.consume()
            assert await call(app, "app.ping") == "pong"

    async def test_stop_consume_tasks_pause_and_resume(self):
        app = App("worker", backend=LocalBackend())
        worker = app.worker()
        received: list[str] = []

        @worker.task("job")
        def job(value: str) -> None:
            received.append(value)

        async with app:
            await worker.consume()
            await send(app, "worker.job", "a")
            await worker.stop_consume()
            await send(app, "worker.job", "b")
            await worker.consume()
            await send(app, "worker.job", "c")

        assert received == ["a", "c"]
        assert "job" in worker.tasks

    async def test_stop_consume_events_pause_and_resume(self):
        app = App("app", backend=LocalBackend())
        bus = app.bus()
        seen: list[int] = []

        @bus.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(user_id)

        async with app:
            await bus.consume()
            queue = bus.event_queue
            assert queue is not None
            await emit(app, "user.created", user_id=1)

            await bus.stop_consume()
            await emit(app, "user.created", user_id=2)

            await bus.consume()
            await emit(app, "user.created", user_id=3)

        assert seen == [1, 3]
        assert "user.created" in bus.subscriptions

    async def test_stop_consume_all_modes(self):
        app = App("app", backend=LocalBackend())
        rpc = app.rpc()
        worker = app.worker()
        bus = app.bus()
        events: list[str] = []

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        @worker.task("job")
        def job(v: str) -> None:
            events.append(f"t:{v}")

        @bus.subscribe("evt")
        def on_evt(v: str) -> None:
            events.append(f"e:{v}")

        async with app:
            await rpc.consume()
            await worker.consume()
            await bus.consume()
            assert await call(app, "app.ping") == "pong"
            await send(app, "app.job", "a")
            await emit(app, "evt", v="a")

            await rpc.stop_consume()
            await worker.stop_consume()
            await bus.stop_consume()
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.ping")
            await send(app, "app.job", "b")
            await emit(app, "evt", v="b")

            await rpc.consume()
            await worker.consume()
            await bus.consume()
            assert await call(app, "app.ping") == "pong"
            await send(app, "app.job", "c")
            await emit(app, "evt", v="c")

        assert "t:a" in events and "t:c" in events and "t:b" not in events
        assert "e:a" in events and "e:c" in events and "e:b" not in events


class TestAppHealthcheck:
    async def test_ready_and_healthy_returns_new_status(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        await app.init()
        try:
            backend_status = await backend.healthcheck()
            status = await app.healthcheck()

            assert status == backend_status
            assert status.details == {"connection": "ok", "queues": "ok"}
            assert status is not backend_status
        finally:
            await app.close()

    async def test_closed_app_not_ready(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        await app.init()
        await app.close()

        assert await app.healthcheck() == HealthStatus(ready=False, health=None)

    async def test_backend_not_ready_returns_not_ready(self):
        class NotReadyBackend(LocalBackend):
            async def healthcheck(self, fast: bool | None = None) -> HealthStatus:
                return HealthStatus(
                    ready=False,
                    health=None,
                    details={"connection": "error"},
                )

        app = App("app", backend=NotReadyBackend())
        await app.init()
        try:
            assert await app.healthcheck() == HealthStatus(
                ready=False,
                health=None,
                details={"connection": "error"},
            )
        finally:
            await app.close()

    async def test_backend_unhealthy_returns_new_status(self):
        class UnhealthyBackend(LocalBackend):
            async def healthcheck(self, fast: bool | None = None) -> HealthStatus:
                return HealthStatus(
                    ready=True,
                    health=False,
                    details={"connection": "ok", "queues": [{"mode": "rpc", "queue": "svc", "reason": "no_consumer"}]},
                )

        backend = UnhealthyBackend()
        app = App("app", backend=backend)
        await app.init()
        try:
            backend_status = await backend.healthcheck()
            status = await app.healthcheck()

            assert status == backend_status
            assert status is not backend_status
        finally:
            await app.close()

    async def test_backend_error_propagates(self):
        class BrokenBackend(LocalBackend):
            async def healthcheck(self, fast: bool | None = None) -> HealthStatus:
                raise RuntimeError("boom")

        app = App("app", backend=BrokenBackend())
        await app.init()
        try:
            with pytest.raises(RuntimeError, match="boom"):
                await app.healthcheck()
        finally:
            await app.close()
