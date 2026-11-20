from __future__ import annotations

import pytest

from linkr import App, LocalBackend, RegistrationAlreadyExistsError, RegistrationNotFoundError
from tests.helpers import call, emit, send


class TestDynamicRpcRegistration:
    async def test_method_decorator_raises_while_consuming(self, app: App, rpc):
        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await rpc.consume()
        with pytest.raises(RuntimeError, match="add_method"):
            @rpc.method("pong")
            def pong() -> str:
                return "ping"

    async def test_add_method_works_while_consuming(self, app: App, rpc):
        await rpc.consume()

        async def add_method():
            def late() -> str:
                return "late"

            await rpc.add_method("late", late)

        await add_method()
        assert await call(app, "app.late") == "late"

    async def test_add_method_works_before_consume(self, app: App, rpc):
        def early() -> str:
            return "early"

        await rpc.add_method("early", early)
        await rpc.consume()
        assert await call(app, "app.early") == "early"

    async def test_remove_method(self, app: App, rpc):
        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await rpc.consume()
        assert await call(app, "app.ping") == "pong"
        await rpc.remove_method("ping")

        with pytest.raises(Exception):
            await call(app, "app.ping")

    async def test_remove_method_not_found(self, app: App, rpc):
        with pytest.raises(RegistrationNotFoundError) as exc_info:
            await rpc.remove_method("missing")
        assert exc_info.value.kind == "method"
        assert exc_info.value.name == "missing"

    async def test_add_method_duplicate_same_handler_and_options_idempotent(self, app: App, rpc):
        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        same = await rpc.add_method("ping", ping)
        assert same is rpc.methods["ping"].fn
        again = await rpc.add_method("ping", same)
        assert again is same
        await rpc.consume()
        assert await call(app, "app.ping") == "pong"

    async def test_add_method_duplicate_conflict_raises(self, app: App, rpc):
        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        with pytest.raises(RegistrationAlreadyExistsError) as exc_info:
            await rpc.add_method("ping", lambda: "dup")
        assert exc_info.value.kind == "method"
        assert exc_info.value.name == "ping"

    async def test_add_method_duplicate_same_handler_different_options_raises(self, app: App, rpc):
        @rpc.method("ping", validate_types=True)
        def ping(x: int) -> int:
            return x

        with pytest.raises(RegistrationAlreadyExistsError):
            await rpc.add_method("ping", ping)


class TestDynamicTaskRegistration:
    async def test_task_decorator_raises_while_consuming(self, app: App, worker):
        @worker.task("job")
        def job() -> None:
            return None

        await worker.consume()
        with pytest.raises(RuntimeError, match="add_task"):
            @worker.task("late")
            def late() -> None:
                return None

    async def test_add_task_works_while_consuming(self, app: App, worker):
        seen: list[str] = []
        await worker.consume()

        def late(value: str) -> None:
            seen.append(value)

        await worker.add_task("late", late)
        await send(app, "app.late", "ok")
        assert seen == ["ok"]

    async def test_remove_task(self, app: App, worker):
        seen: list[str] = []

        @worker.task("job")
        def job(value: str) -> None:
            seen.append(value)

        await worker.consume()
        await send(app, "app.job", "a")
        await worker.remove_task("job")
        await send(app, "app.job", "b")
        assert seen == ["a"]

    async def test_remove_task_not_found(self, app: App, worker):
        with pytest.raises(RegistrationNotFoundError) as exc_info:
            await worker.remove_task("missing")
        assert exc_info.value.kind == "task"
        assert exc_info.value.name == "missing"

    async def test_add_task_duplicate_same_handler_and_options_idempotent(self, app: App, worker):
        seen: list[str] = []

        @worker.task("job")
        def job(value: str) -> None:
            seen.append(value)

        same = await worker.add_task("job", job)
        assert same is worker.tasks["job"].fn
        again = await worker.add_task("job", same)
        assert again is same
        await worker.consume()
        await send(app, "app.job", "x")
        assert seen == ["x"]

    async def test_add_task_duplicate_conflict_raises(self, app: App, worker):
        @worker.task("job")
        def job() -> None:
            return None

        with pytest.raises(RegistrationAlreadyExistsError) as exc_info:
            await worker.add_task("job", lambda: None)
        assert exc_info.value.kind == "task"


class TestDynamicBusRegistration:
    async def test_subscribe_decorator_raises_while_consuming(self, app: App, bus):
        @bus.subscribe("a")
        def on_a() -> None:
            return None

        await bus.consume()
        with pytest.raises(RuntimeError, match="add_subscriber"):
            @bus.subscribe("b")
            def on_b() -> None:
                return None

    async def test_add_subscriber_new_pattern_while_consuming(self, app: App, bus):
        seen: list[int] = []

        @bus.subscribe("boot")
        def boot() -> None:
            return None

        await bus.consume()

        def on_user(user_id: int) -> None:
            seen.append(user_id)

        await bus.add_subscriber("user.created", on_user)
        await emit(app, "user.created", user_id=7)
        assert seen == [7]

    async def test_add_subscriber_existing_pattern_while_consuming(self, app: App, bus):
        seen: list[str] = []

        @bus.subscribe("user.created")
        def first(user_id: int) -> None:
            seen.append(f"first:{user_id}")

        await bus.consume()

        def second(user_id: int) -> None:
            seen.append(f"second:{user_id}")

        await bus.add_subscriber("user.created", second)
        await emit(app, "user.created", user_id=1)
        assert sorted(seen) == ["first:1", "second:1"]

    async def test_remove_subscriber_specific_handler(self, app: App, bus):
        seen: list[str] = []

        @bus.subscribe("evt")
        def first() -> None:
            seen.append("first")

        h2 = await bus.add_subscriber("evt", lambda: seen.append("second"))
        await bus.consume()
        await emit(app, "evt")
        await bus.remove_subscriber("evt", h2)
        await emit(app, "evt")
        assert seen.count("first") == 2
        assert seen.count("second") == 1

    async def test_remove_subscriber_entire_topic_unbinds_while_consuming(self, app: App, bus):
        seen: list[str] = []

        @bus.subscribe("boot")
        def boot() -> None:
            return None

        await bus.consume()

        await bus.add_subscriber("user.created", lambda: seen.append("hit"))
        await emit(app, "user.created")
        await bus.remove_subscriber("user.created")
        await emit(app, "user.created")
        assert seen == ["hit"]

    async def test_remove_subscriber_emits_topic_unsubscribed_hook(self, app: App, bus):
        events = []

        @app.hook.on_consume_stop
        def on_stop(event):
            events.append(event)

        @bus.subscribe("boot")
        def boot() -> None:
            return None

        await bus.consume()
        await bus.add_subscriber("user.created", lambda: None)
        await bus.remove_subscriber("user.created")

        assert len(events) == 1
        event = events[0]
        assert event.app == "app"
        assert event.mode == "event"
        assert event.queue == bus.event_queue
        assert event.topic == "user.created"
        assert event.reason == "topic_unsubscribed"

    async def test_remove_subscriber_last_handler_not_bound_no_hook(self, app: App, bus):
        events = []

        @app.hook.on_consume_stop
        def on_stop(event):
            events.append(event)

        @bus.subscribe("boot")
        def boot() -> None:
            return None

        await bus.consume()
        # removing non-last handler should not emit topic_unsubscribed
        await bus.add_subscriber("user.created", lambda: None)
        h2 = await bus.add_subscriber("user.created", lambda: None)
        await bus.remove_subscriber("user.created", h2)
        assert events == []

    async def test_remove_subscriber_idle(self, app: App, bus):
        seen: list[int] = []
        events = []

        @app.hook.on_consume_stop
        def on_stop(event):
            events.append(event)

        @bus.subscribe("evt")
        def on_evt(value: int) -> None:
            seen.append(value)

        await bus.remove_subscriber("evt")
        await bus.consume()
        await emit(app, "evt", value=1)
        assert seen == []
        assert events == []

    async def test_add_subscriber_before_consume(self, app: App, bus):
        seen: list[int] = []

        await bus.add_subscriber("evt", lambda value: seen.append(value))
        await bus.consume()
        await emit(app, "evt", value=3)
        assert seen == [3]

    async def test_subscribe_allowed_when_consume_noop_empty(self, app: App, bus):
        # consume() with no subscriptions is a no-op (does not start consuming)
        await bus.consume()
        seen: list[str] = []

        @bus.subscribe("evt")
        def on_evt() -> None:
            seen.append("ok")

        await bus.consume()
        await emit(app, "evt")
        assert seen == ["ok"]

    async def test_remove_subscriber_not_found(self, app: App, bus):
        with pytest.raises(RegistrationNotFoundError) as exc_info:
            await bus.remove_subscriber("missing")
        assert exc_info.value.kind == "subscriber"
        assert exc_info.value.name == "missing"

    async def test_add_subscriber_duplicate_same_handler_and_options_idempotent(self, app: App, bus):
        seen: list[int] = []

        @bus.subscribe("user.created")
        def on_user(user_id: int) -> None:
            seen.append(user_id)

        same = await bus.add_subscriber("user.created", on_user)
        existing = bus.subscriptions["user.created"][0].fn
        assert same is existing
        again = await bus.add_subscriber("user.created", same)
        assert again is same
        await bus.consume()
        await emit(app, "user.created", user_id=1)
        assert seen == [1]

    async def test_add_subscriber_duplicate_same_handler_different_options_raises(self, app: App, bus):
        @bus.subscribe("user.created", validate_types=True)
        def on_user(user_id: int) -> None:
            return None

        with pytest.raises(RegistrationAlreadyExistsError) as exc_info:
            await bus.add_subscriber("user.created", on_user)
        assert exc_info.value.kind == "subscriber"
        assert exc_info.value.name == "user.created"

    async def test_add_subscriber_new_pattern_unsupported_backend(self):
        class NoBindBackend(LocalBackend):
            async def bind_event_topics(self, queue, topics):
                raise NotImplementedError

        app = App("app", backend=NoBindBackend())

        async with app:
            bus = app.bus()

            @bus.subscribe("boot")
            def boot() -> None:
                return None

            await bus.consume()
            with pytest.raises(RuntimeError, match="dynamic event binding"):
                await bus.add_subscriber("late.topic", lambda: None)
            assert "late.topic" not in bus.subscriptions
