from __future__ import annotations

from dataclasses import dataclass

import pytest

from linkr import App, LocalBackend, QueueOptions
from tests.helpers import call, emit, send


@dataclass(frozen=True)
class DummyQueueOptions(QueueOptions):
    label: str


class SpyBackend(LocalBackend):
    def __init__(self) -> None:
        super().__init__()
        self.rpc_options: list[tuple[str, object]] = []
        self.task_options: list[tuple[str, object]] = []
        self.event_options: list[tuple[str, tuple[str, ...], object]] = []

    async def consume_rpc(self, queue, handler, *, options=None):  # type: ignore[override]
        self.rpc_options.append((queue, options))
        await super().consume_rpc(queue, handler, options=options)

    async def consume_task(self, queue, handler, *, options=None):  # type: ignore[override]
        self.task_options.append((queue, options))
        await super().consume_task(queue, handler, options=options)

    async def consume_event(self, queue, topics, handler, *, options=None):  # type: ignore[override]
        self.event_options.append((queue, tuple(topics), options))
        await super().consume_event(queue, topics, handler, options=options)


class LegacyBackend(LocalBackend):
    """Backend with old consume signatures (no options kwarg)."""

    async def consume_rpc(self, queue, handler):  # type: ignore[override]
        await super().consume_rpc(queue, handler)

    async def consume_task(self, queue, handler):  # type: ignore[override]
        await super().consume_task(queue, handler)

    async def consume_event(self, queue, topics, handler):  # type: ignore[override]
        await super().consume_event(queue, topics, handler)


class TestRouter:
    async def test_default_rpc_router_configures_default_queue_and_registers(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)

        router = app.router("rpc", options=[DummyQueueOptions("default-rpc")])

        @router("foo")
        def foo() -> str:
            return "foo"

        async with app:
            await app.consume()
            assert await call(app, "orders.foo") == "foo"

        assert ("orders", (DummyQueueOptions("default-rpc"),)) in backend.rpc_options

    async def test_default_task_router_configures_default_queue(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        seen: list[str] = []

        router = app.router("task", options=[DummyQueueOptions("default-task")])

        @router("cleanup")
        def cleanup(value: str) -> None:
            seen.append(value)

        async with app:
            await app.consume()
            await send(app, "orders.cleanup", "ok")

        assert seen == ["ok"]
        assert ("orders", (DummyQueueOptions("default-task"),)) in backend.task_options

    async def test_named_rpc_router_groups_handlers_on_same_queue(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)

        router = app.router("rpc", path="serviceA.path1.path2", options=[DummyQueueOptions("grp")])

        @router("bar")
        def bar() -> str:
            return "bar"

        @router("baz")
        def baz() -> str:
            return "baz"

        async with app:
            await app.consume()
            assert await call(app, "orders.serviceA.path1.path2.bar") == "bar"
            assert await call(app, "orders.serviceA.path1.path2.baz") == "baz"

        assert app.methods["bar"].name == "serviceA.path1.path2.bar"
        assert app.methods["bar"].queue == "orders.serviceA.path1.path2"
        assert app.methods["baz"].name == "serviceA.path1.path2.baz"
        assert app.methods["baz"].queue == "orders.serviceA.path1.path2"
        assert ("orders.serviceA.path1.path2", (DummyQueueOptions("grp"),)) in backend.rpc_options

    async def test_named_task_router_configures_group_queue(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        seen: list[str] = []

        router = app.router("task", path="email", options=[DummyQueueOptions("email")])

        @router("send")
        def send_email(user_id: int) -> None:
            seen.append(f"send:{user_id}")

        @router("notify")
        def notify(user_id: int) -> None:
            seen.append(f"notify:{user_id}")

        async with app:
            await app.consume()
            await send(app, "orders.email.send", 1)
            await send(app, "orders.email.notify", 2)

        assert sorted(seen) == ["notify:2", "send:1"]
        assert ("orders.email", (DummyQueueOptions("email"),)) in backend.task_options

    async def test_options_apply_to_handlers_registered_via_app_api(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        app.router("task", path="email", options=[DummyQueueOptions("email")])

        @app.task("email.send")
        def send_email(user_id: int) -> int:
            return user_id

        async with app:
            await app.consume()

        assert ("orders.email", (DummyQueueOptions("email"),)) in backend.task_options

    async def test_event_router_configures_instance_queue(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)
        seen: list[int] = []

        events = app.router("event", options=[DummyQueueOptions("events")])

        @events("user.created")
        def on_user_created(user_id: int) -> None:
            seen.append(user_id)

        async with app:
            await app.consume()
            await emit(app, "user.created", user_id=42)

        assert seen == [42]
        assert backend.event_options
        queue, topics, options = backend.event_options[0]
        assert queue == app._event_queue_name()
        assert topics == ("user.created",)
        assert options == (DummyQueueOptions("events"),)

    async def test_router_accepts_options_list_for_multiple_backends(self):
        backend = SpyBackend()
        app = App("orders", backend=backend)

        @dataclass(frozen=True)
        class OtherOptions(QueueOptions):
            value: int

        opts = [DummyQueueOptions("rmq-like"), OtherOptions(7)]
        app.router("rpc", path="billing", options=opts)

        @app.method("billing.charge")
        def charge() -> str:
            return "ok"

        async with app:
            await app.consume()

        assert ("orders.billing", tuple(opts)) in backend.rpc_options

    async def test_idempotent_same_options_allowed(self):
        app = App("orders", backend=LocalBackend())
        opts = [DummyQueueOptions("x")]
        app.router("rpc", path="svc", options=opts)
        app.router("rpc", path="svc", options=opts)

    async def test_conflicting_options_rejected(self):
        app = App("orders", backend=LocalBackend())
        app.router("rpc", path="svc", options=[DummyQueueOptions("a")])
        with pytest.raises(ValueError, match="already configured"):
            app.router("rpc", path="svc", options=[DummyQueueOptions("b")])

    async def test_duplicate_option_types_rejected(self):
        app = App("orders", backend=LocalBackend())
        with pytest.raises(ValueError, match="duplicate queue options"):
            app.router("rpc", path="svc", options=[DummyQueueOptions("a"), DummyQueueOptions("b")])

    async def test_router_name_must_be_leaf(self):
        app = App("orders", backend=LocalBackend())
        router = app.router("rpc", path="svc")
        with pytest.raises(ValueError, match="must not contain '.'"):
            router("sub.method")

    async def test_event_router_rejects_path(self):
        app = App("orders", backend=LocalBackend())
        with pytest.raises(ValueError, match="does not accept a path"):
            app.router("event", path="users")

    async def test_cannot_reconfigure_while_consuming(self):
        app = App("orders", backend=LocalBackend())

        @app.method("foo")
        def foo() -> str:
            return "foo"

        async with app:
            await app.consume()
            with pytest.raises(RuntimeError, match="cannot configure queue while consuming"):
                app.router("rpc", options=[DummyQueueOptions("late")])

    async def test_legacy_backend_without_options_param_still_works(self):
        backend = LegacyBackend()
        app = App("orders", backend=backend)

        router = app.router("rpc", options=[DummyQueueOptions("x")])

        @router("foo")
        def foo() -> str:
            return "foo"

        async with app:
            await app.consume()
            assert await call(app, "orders.foo") == "foo"
