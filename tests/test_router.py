from __future__ import annotations

from dataclasses import dataclass

import pytest

from linkr import App, LocalBackend, QueueOptions
from tests.helpers import call, send


@dataclass(frozen=True)
class DummyQueueOptions(QueueOptions):
    label: str


class SpyBackend(LocalBackend):
    def __init__(self) -> None:
        super().__init__()
        self.rpc_options: list[tuple[str, object]] = []
        self.task_options: list[tuple[str, object]] = []
        self.event_options: list[tuple[str, object]] = []

    async def consume_rpc(self, queue, handler, *, options=None):  # type: ignore[override]
        self.rpc_options.append((queue, options))
        await super().consume_rpc(queue, handler, options=options)

    async def consume_task(self, queue, handler, *, options=None):  # type: ignore[override]
        self.task_options.append((queue, options))
        await super().consume_task(queue, handler, options=options)

    async def consume_event(self, queue, topics, handler, *, options=None):  # type: ignore[override]
        self.event_options.append((queue, options))
        await super().consume_event(queue, topics, handler, options=options)


class LegacyBackend(LocalBackend):
    """Backend with old consume signatures (no options kwarg)."""

    async def consume_rpc(self, queue, handler):  # type: ignore[override]
        await super().consume_rpc(queue, handler)

    async def consume_task(self, queue, handler):  # type: ignore[override]
        await super().consume_task(queue, handler)

    async def consume_event(self, queue, topics, handler):  # type: ignore[override]
        await super().consume_event(queue, topics, handler)


class TestServer:
    async def test_named_server_configures_rpc_queue_and_registers(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        server = app.server("serviceA", rpc_options=[DummyQueueOptions("svc-a")])
        assert server.path == "serviceA"
        assert server.queue_base == "orders.serviceA"

        @server.method("foo")
        def foo() -> str:
            return "foo"

        async with app:
            await server.consume()
            assert await call(app, "orders.serviceA.foo") == "foo"

        assert ("orders.serviceA", (DummyQueueOptions("svc-a"),)) in backend.rpc_options

    async def test_named_server_configures_task_queue(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        seen: list[str] = []
        server = app.server("email", task_options=[DummyQueueOptions("email")])

        @server.task("cleanup")
        def cleanup(value: str) -> None:
            seen.append(value)

        async with app:
            await server.consume(mode="task")
            await send(app, "orders.email.cleanup", "ok")

        assert seen == ["ok"]
        assert ("orders.email", (DummyQueueOptions("email"),)) in backend.task_options

    async def test_named_server_groups_handlers_on_same_queue(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        server = app.server("serviceA.path1.path2", rpc_options=[DummyQueueOptions("grp")])

        @server.method("bar")
        def bar() -> str:
            return "bar"

        @server.method("baz")
        def baz() -> str:
            return "baz"

        async with app:
            await server.consume(mode="rpc")
            assert await call(app, "orders.serviceA.path1.path2.bar") == "bar"
            assert await call(app, "orders.serviceA.path1.path2.baz") == "baz"

        assert server.methods["bar"].name == "orders.serviceA.path1.path2.bar"
        assert server.methods["bar"].queue == "orders.serviceA.path1.path2"
        assert server.methods["baz"].name == "orders.serviceA.path1.path2.baz"
        assert ("orders.serviceA.path1.path2", (DummyQueueOptions("grp"),)) in backend.rpc_options

    async def test_named_server_groups_task_handlers_on_same_queue(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        seen: list[str] = []
        server = app.server("email", task_options=[DummyQueueOptions("email")])

        @server.task("send")
        def send_email(user_id: int) -> None:
            seen.append(f"send:{user_id}")

        @server.task("notify")
        def notify(user_id: int) -> None:
            seen.append(f"notify:{user_id}")

        async with app:
            await server.consume(mode="task")
            await send(app, "orders.email.send", 1)
            await send(app, "orders.email.notify", 2)

        assert sorted(seen) == ["notify:2", "send:1"]
        assert ("orders.email", (DummyQueueOptions("email"),)) in backend.task_options

    async def test_server_event_queue_is_per_server_and_accepts_options(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        root = app.server(event_options=[DummyQueueOptions("ev")])
        seen: list[str] = []

        @root.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(f"u:{user_id}")

        async with app:
            await root.consume(mode="event")
            assert root.event_queue == f"orders.{app._instance_id}"
            await app.client.emit("user.created", {"user_id": 7})

        assert seen == ["u:7"]
        assert (f"orders.{app._instance_id}", (DummyQueueOptions("ev"),)) in backend.event_options

    async def test_server_accepts_options_list_for_multiple_backends(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)

        @dataclass(frozen=True)
        class OtherOptions(QueueOptions):
            value: int

        opts = [DummyQueueOptions("rmq-like"), OtherOptions(7)]
        server = app.server("billing", rpc_options=opts)

        @server.method("charge")
        def charge() -> str:
            return "ok"

        async with app:
            await server.consume(mode="rpc")

        assert ("orders.billing", tuple(opts)) in backend.rpc_options

    async def test_path_validation(self):
        app = App("orders", backend=LocalBackend())
        with pytest.raises(ValueError, match="server path must not be empty"):
            app.server("")

    async def test_idempotent_same_options_returns_same_server(self):
        app = App("orders", backend=LocalBackend())
        opts = [DummyQueueOptions("x")]
        s1 = app.server("svc", rpc_options=opts)
        s2 = app.server("svc", rpc_options=opts)
        assert s1 is s2

    async def test_conflicting_options_rejected(self):
        app = App("orders", backend=LocalBackend())
        app.server("svc", rpc_options=[DummyQueueOptions("a")])
        with pytest.raises(ValueError, match="already configured"):
            app.server("svc", rpc_options=[DummyQueueOptions("b")])

    async def test_duplicate_option_types_rejected(self):
        app = App("orders", backend=LocalBackend())
        with pytest.raises(ValueError, match="duplicate queue options"):
            app.server("svc", rpc_options=[DummyQueueOptions("a"), DummyQueueOptions("b")])

    async def test_handler_name_must_be_leaf(self):
        app = App("orders", backend=LocalBackend())
        server = app.server("svc")
        with pytest.raises(ValueError, match="must not contain '.'"):
            server.method("sub.method")
        with pytest.raises(ValueError, match="handler name must not be empty"):
            server.method("")

    async def test_stop_and_resume_consume(self):
        app = App("orders", backend=LocalBackend())
        server = app.server("svc")

        @server.method("foo")
        def foo() -> str:
            return "foo"

        async with app:
            await server.consume(mode="rpc")
            assert await call(app, "orders.svc.foo") == "foo"
            await server.stop_consume(mode="rpc")
            await server.consume(mode="rpc")
            assert await call(app, "orders.svc.foo") == "foo"

    async def test_legacy_backend_without_options_param_still_works(self):
        backend = LegacyBackend()
        app = App("orders", backend=backend)
        server = app.server("svc", rpc_options=[DummyQueueOptions("x")])

        @server.method("foo")
        def foo() -> str:
            return "foo"

        async with app:
            await server.consume(mode="rpc")
            assert await call(app, "orders.svc.foo") == "foo"

    def test_old_router_api_removed(self):
        assert not hasattr(app := App("orders", backend=LocalBackend()), "rpc_router")
        assert not hasattr(app, "task_router")
        assert not hasattr(app, "method")
