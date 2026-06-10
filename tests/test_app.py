from __future__ import annotations

import asyncio

import pytest

from linkr import AppMiddleware, Depends, GzipMiddleware, MockTransport, RpcApp, RpcError, WireMiddleware
from linkr.models import RpcResponse


async def test_register_method(app: RpcApp):
    @app.method("ping")
    def pong() -> str:
        return "pong"

    assert "ping" in app.methods
    info = app.methods["ping"]
    assert info.name == "ping"
    assert info.fn() == "pong"


async def test_call_success(app: RpcApp):
    @app.method("add")
    def add(x: int, y: int) -> int:
        return x + y

    await app.consume()
    result = await app.make("add", x=2, y=3).call()
    assert result == 5


async def test_call_with_no_args(app: RpcApp):
    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    result = await app.make("ping").call()
    assert result == "pong"


async def test_call_error_from_handler(app: RpcApp):
    @app.method("fail")
    def fail() -> str:
        msg = "something went wrong"
        raise ValueError(msg)

    await app.consume()
    with pytest.raises(RpcError) as exc_info:
        await app.make("fail").call()
    assert exc_info.value.error_code == "InternalError"


async def test_call_method_not_found(app: RpcApp):
    await app.consume()
    with pytest.raises(RpcError) as exc_info:
        await app.make("nonexistent").call()
    assert exc_info.value.error_code == "MethodNotFound"


async def test_publish(app: RpcApp, transport: MockTransport):
    req = app.make("test", text="hello")
    await app.publish(req.request)
    assert len(transport.sent_messages) == 1


async def test_call_via_make_call(app: RpcApp):
    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    result = await app.make("ping")(timeout=5)
    assert result == "pong"


async def test_call_timeout(app: RpcApp):
    @app.method("slow")
    async def slow() -> str:
        await asyncio.sleep(10)
        return "done"

    await app.consume()
    with pytest.raises(RpcError) as exc_info:
        await app.make("slow").call(timeout=0.1)
    assert exc_info.value.error_code == "Timeout"


async def test_add_middleware(app: RpcApp):
    events: list[str] = []

    class TestMiddleware(AppMiddleware):
        async def dispatch(self, ctx, call_next):
            events.append(f"before-{ctx.direction}-{ctx.role}")
            result = await call_next(ctx)
            events.append(f"after-{ctx.direction}-{ctx.role}")
            return result

    middleware = TestMiddleware()
    app.add_middleware(middleware)

    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    result = await app.make("ping").call()
    assert result == "pong"
    assert "before-request-client" in events
    assert "after-request-client" in events
    assert "before-response-client" in events
    assert "after-response-client" in events


async def test_multiple_middleware_order(app: RpcApp):
    events: list[str] = []

    class MwA(AppMiddleware):
        async def dispatch(self, ctx, call_next):
            events.append(f"A-enter-{ctx.direction}-{ctx.role}")
            r = await call_next(ctx)
            events.append(f"A-exit-{ctx.direction}-{ctx.role}")
            return r

    class MwB(AppMiddleware):
        async def dispatch(self, ctx, call_next):
            events.append(f"B-enter-{ctx.direction}-{ctx.role}")
            r = await call_next(ctx)
            events.append(f"B-exit-{ctx.direction}-{ctx.role}")
            return r

    app.add_middleware(MwA())
    app.add_middleware(MwB())

    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    result = await app.make("ping").call()
    assert result == "pong"

    # Middleware chain runs independently for each of the 4 phases.
    # In each phase, A wraps B: A-enter < B-enter < B-exit < A-exit.
    phases = ["request-client", "request-server", "response-server", "response-client"]
    assert len(events) == 4 * 4  # 4 phases × 4 events per chain
    for phase in phases:
        a_enter = events.index(f"A-enter-{phase}")
        b_enter = events.index(f"B-enter-{phase}")
        b_exit = events.index(f"B-exit-{phase}")
        a_exit = events.index(f"A-exit-{phase}")
        assert a_enter < b_enter < b_exit < a_exit, f"A should wrap B in {phase}"


async def test_middleware_can_set_response(app: RpcApp):
    class CacheMiddleware(AppMiddleware):
        async def dispatch(self, ctx, call_next):
            if ctx.direction == "request" and ctx.role == "server":
                ctx.response = RpcResponse(id=ctx.request.id, data={"result": "cached"})
                return ctx
            return await call_next(ctx)

    app.add_middleware(CacheMiddleware())

    @app.method("ping")
    def ping() -> str:
        return "uncached"

    await app.consume()
    result = await app.make("ping").call()
    assert result == "cached"


async def test_middleware_lifecycle():
    class LifecycleMiddleware(AppMiddleware):
        def __init__(self):
            self.init_called = False
            self.close_called = False

        async def init(self, a):
            self.init_called = True

        async def close(self):
            self.close_called = True

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


async def test_async_handler(app: RpcApp):
    @app.method("greet")
    async def greet(name: str) -> str:
        return f"Hello, {name}!"

    await app.consume()
    result = await app.make("greet", name="World").call()
    assert result == "Hello, World!"


async def test_app_is_context_manager():
    transport = MockTransport()
    async with RpcApp(transport=transport) as app:
        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.init()
        await app.consume()
        result = await app.make("ping").call()
        assert result == "pong"


async def test_handlerinfo_includes_signature(app: RpcApp):
    @app.method("add")
    def add(x: int, y: int = 0) -> int:
        return x + y

    info = app.methods["add"]
    assert info.name == "add"
    assert "x" in info.signature
    assert "y" in info.signature
    assert info.options == {}


async def test_call_on_closed_app_raises():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()
    await app.close()
    with pytest.raises(RuntimeError, match="RpcApp is closed"):
        await app.make("ping").call()


async def test_publish_on_closed_app_raises():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()
    req = app.make("test")
    await app.close()
    with pytest.raises(RuntimeError, match="RpcApp is closed"):
        await app.publish(req.request)


async def test_consume_on_closed_app_raises():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()
    await app.close()
    with pytest.raises(RuntimeError, match="RpcApp is closed"):
        await app.consume()


async def test_default_timeout():
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
    assert exc_info.value.error_code == "Timeout"


async def test_call_ttl_defaults_to_timeout():
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


async def test_call_explicit_ttl_rttl():
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


async def test_default_ttl():
    transport = MockTransport()
    app = RpcApp(transport=transport, ttl=10)
    await app.init()

    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    await app.make("ping").call()
    assert transport.sent_messages[0].headers.get("ttl") == 10


async def test_default_rttl():
    transport = MockTransport()
    app = RpcApp(transport=transport, rttl=60)
    await app.init()

    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    result = await app.make("ping").call()
    assert result == "pong"


async def test_routing_key_without_group():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("add")
    def add(x: int, y: int) -> int:
        return x + y

    call = app.make("add", 1, 2)
    assert call.request.headers["routing_key"] == "rpc.server"


async def test_routing_key_with_group():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("service/add")
    def add(x: int, y: int) -> int:
        return x + y

    call = app.make("service/add", 1, 2)
    assert call.request.headers["routing_key"] == "rpc.server.service"


async def test_group_method_call_success():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("service/add")
    def add(x: int, y: int) -> int:
        return x + y

    await app.consume()
    result = await app.make("service/add", 2, 3).call()
    assert result == 5


async def test_get_handler_returns_none_for_unknown():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    assert app.get_handler("nonexistent") is None


async def test_get_handler_returns_info():
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


async def test_validate_types_passes():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("add", validate_types=True)
    def add(x: int, y: int) -> int:
        return x + y

    await app.consume()
    result = await app.make("add", x=2, y=3).call()
    assert result == 5


async def test_validate_types_fails_positional():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("add", validate_types=True)
    def add(x: int, y: int) -> int:
        return x + y

    await app.consume()
    with pytest.raises(RpcError) as exc_info:
        await app.make("add", "not", 3).call()
    assert exc_info.value.error_code == "ValidationError"


async def test_validate_types_fails_keyword():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("greet", validate_types=True)
    def greet(name: str, age: int) -> str:
        return f"{name} is {age}"

    await app.consume()
    with pytest.raises(RpcError) as exc_info:
        await app.make("greet", name="Alice", age="old").call()
    assert exc_info.value.error_code == "ValidationError"


async def test_validate_types_disabled_by_default():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("add")
    def add(x: int, y: int) -> int:
        return x + y

    await app.consume()
    with pytest.raises(RpcError) as exc_info:
        await app.make("add", "not", 3).call()
    assert exc_info.value.error_code == "InternalError"


async def test_validate_types_skips_depends():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    app.dependencies.add_singleton(str, lambda: "injected")
    await app.init()

    @app.method("ping", validate_types=True)
    def ping(db: Depends[str]) -> str:
        return db

    await app.consume()
    result = await app.make("ping").call()
    assert result == "injected"


async def test_validate_types_skips_unannotated():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("echo", validate_types=True)
    def echo(x) -> str:
        return str(x)

    await app.consume()
    result = await app.make("echo", x=42).call()
    assert result == "42"


async def test_validate_types_optional():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("maybe", validate_types=True)
    def maybe(value: int | None = None) -> int | None:
        return value

    await app.consume()
    result = await app.make("maybe").call()
    assert result is None


async def test_encoding_gzip_roundtrip():
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


async def test_encoding_add_middleware_appends():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    assert len(app._wire_mw) == 0
    app.add_middleware(GzipMiddleware())
    assert len(app._wire_mw) == 1


async def test_encoding_without_encoders_still_works():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    @app.method("add")
    def add(x: int, y: int) -> int:
        return x + y

    await app.consume()
    result = await app.make("add", 2, 3).call()
    assert result == 5


async def test_wire_middleware_order():
    events: list[str] = []

    class MwA(WireMiddleware):
        async def dispatch(self, ctx, call_next):
            events.append(f"A-enter-{ctx.direction}-{ctx.role}")
            r = await call_next(ctx)
            events.append(f"A-exit-{ctx.direction}-{ctx.role}")
            return r

    class MwB(WireMiddleware):
        async def dispatch(self, ctx, call_next):
            events.append(f"B-enter-{ctx.direction}-{ctx.role}")
            r = await call_next(ctx)
            events.append(f"B-exit-{ctx.direction}-{ctx.role}")
            return r

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

    phases = ["request-client", "request-server", "response-server", "response-client"]
    assert len(events) == 4 * 4
    for phase in phases:
        a_enter = events.index(f"A-enter-{phase}")
        b_enter = events.index(f"B-enter-{phase}")
        b_exit = events.index(f"B-exit-{phase}")
        a_exit = events.index(f"A-exit-{phase}")
        assert a_enter < b_enter < b_exit < a_exit, f"A should wrap B in {phase}"


async def test_middleware_can_share_state_via_context():
    events: list[str] = []

    class StateClientMw(AppMiddleware):
        async def dispatch(self, ctx, call_next):
            if ctx.direction == "request" and ctx.role == "client":
                ctx.state["client_step"] = "seen"
            result = await call_next(ctx)
            if ctx.direction == "response" and ctx.role == "client":
                events.append(ctx.state.get("client_step", ""))
            return result

    class StateServerMw(AppMiddleware):
        async def dispatch(self, ctx, call_next):
            if ctx.direction == "request" and ctx.role == "server":
                ctx.state["server_step"] = "seen"
            result = await call_next(ctx)
            if ctx.direction == "response" and ctx.role == "server":
                events.append(ctx.state.get("server_step", ""))
            return result

    transport = MockTransport()
    app = RpcApp(transport=transport)
    app.add_middleware(StateClientMw())
    app.add_middleware(StateServerMw())
    await app.init()

    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    result = await app.make("ping").call()
    assert result == "pong"
    assert events.count("seen") == 2


async def test_wire_headers_roundtrip():
    class HeaderInjector(WireMiddleware):
        async def dispatch(self, ctx, call_next):
            if ctx.direction == "request" and ctx.role == "client":
                ctx.wire_headers["x-custom"] = "test-value"
            elif ctx.direction == "request" and ctx.role == "server":
                ctx.state["received_headers"] = dict(ctx.wire_headers)
            result = await call_next(ctx)
            return result

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


async def test_validate_types_complex():
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


async def test_publish_sends_timeout():
    transport = MockTransport()
    app = RpcApp(transport=transport)
    await app.init()

    req = app.make("test", text="hello")
    await app.publish(req.request, timeout=5)
    assert transport.sent_messages[0].headers.get("timeout") == 5
    assert transport.sent_messages[0].headers.get("ttl") == 5


async def test_publish_sends_timeout_from_default():
    transport = MockTransport()
    app = RpcApp(transport=transport, timeout=7)
    await app.init()

    req = app.make("test", text="hello")
    await app.publish(req.request)
    assert transport.sent_messages[0].headers.get("timeout") == 7
    assert transport.sent_messages[0].headers.get("ttl") == 7


async def test_custom_wire_header_survives_roundtrip():
    class CustomWireHeaderMw(WireMiddleware):
        async def dispatch(self, ctx, call_next):
            if ctx.direction == "request" and ctx.role == "client":
                ctx.wire_headers["x-custom"] = "hello"
            elif ctx.direction == "request" and ctx.role == "server":
                ctx.state["server_seen"] = ctx.wire_headers.get("x-custom", "")
            return await call_next(ctx)

    class StateReader(AppMiddleware):
        async def dispatch(self, ctx, call_next):
            if ctx.direction == "response" and ctx.role == "client":
                ctx.state["check"] = ctx.request.data
            return await call_next(ctx)

    transport = MockTransport()
    app = RpcApp(transport=transport)
    app.add_middleware(CustomWireHeaderMw())
    app.add_middleware(StateReader())
    await app.init()

    @app.method("ping")
    def ping() -> str:
        return "pong"

    await app.consume()
    result = await app.make("ping").call()
    assert result == "pong"
