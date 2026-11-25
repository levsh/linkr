from __future__ import annotations

import pytest

from linkr import App, ErrorCode, LocalBackend, RPCError, RegistrationAlreadyExistsError, RegistrationNotFoundError
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

        with pytest.raises(RPCError) as exc_info:
            await call(app, "app.ping")
        assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

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


class _RpcService:
    def __init__(self, app: App, rpc) -> None:
        self._app = app
        self._rpc = rpc
        self.calls = 0

    def foo(self, value: int) -> int:
        self.calls += 1
        return value + 1

    def register(self) -> None:
        self._rpc.method("foo")(self.foo)


class _RpcServiceValidated:
    def foo(self, value: int) -> int:
        return value


class _TaskService:
    def __init__(self, app: App, worker) -> None:
        self._app = app
        self._worker = worker
        self.seen: list[str] = []

    def job(self, value: str) -> None:
        self.seen.append(value)

    def register(self) -> None:
        self._worker.task("job")(self.job)


class _BusService:
    def __init__(self, app: App, bus) -> None:
        self._app = app
        self._bus = bus
        self.seen: list[int] = []

    def on_user(self, user_id: int) -> None:
        self.seen.append(user_id)

    def register(self) -> None:
        self._bus.subscribe("user.created")(self.on_user)


class TestDynamicRegistrationBoundMethods:
    async def test_method_decorator_idempotent_for_bound_method(self, app: App, rpc):
        svc = _RpcService(app, rpc)
        svc.register()
        svc.register()  # fresh bound method object
        assert len(rpc.methods) == 1
        await rpc.consume()
        assert await call(app, "app.foo", 1) == 2
        assert svc.calls == 1

    async def test_add_method_idempotent_for_bound_method(self, app: App, rpc):
        svc = _RpcService(app, rpc)
        await rpc.add_method("foo", svc.foo)
        again = await rpc.add_method("foo", svc.foo)  # fresh bound method object
        assert again is rpc.methods["foo"].fn
        assert len(rpc.methods) == 1
        await rpc.consume()
        assert await call(app, "app.foo", 2) == 3
        assert svc.calls == 1

    async def test_add_method_bound_method_same_handler_different_options_raises(self, app: App, rpc):
        svc = _RpcServiceValidated()
        rpc.method("foo", validate_types=True)(svc.foo)
        with pytest.raises(RegistrationAlreadyExistsError):
            await rpc.add_method("foo", svc.foo)

    async def test_task_decorator_idempotent_for_bound_method(self, app: App, worker):
        svc = _TaskService(app, worker)
        svc.register()
        svc.register()
        assert len(worker.tasks) == 1
        await worker.consume()
        await send(app, "app.job", "x")
        assert svc.seen == ["x"]

    async def test_add_task_idempotent_for_bound_method(self, app: App, worker):
        svc = _TaskService(app, worker)
        await worker.add_task("job", svc.job)
        again = await worker.add_task("job", svc.job)
        assert again is worker.tasks["job"].fn
        assert len(worker.tasks) == 1
        await worker.consume()
        await send(app, "app.job", "y")
        assert svc.seen == ["y"]

    async def test_subscribe_decorator_deduplicates_bound_method(self, app: App, bus):
        svc = _BusService(app, bus)
        svc.register()
        svc.register()
        assert len(bus.subscriptions["user.created"]) == 1
        await bus.consume()
        await emit(app, "user.created", user_id=7)
        assert svc.seen == [7]

    async def test_add_subscriber_deduplicates_bound_method(self, app: App, bus):
        svc = _BusService(app, bus)
        await bus.add_subscriber("user.created", svc.on_user)
        again = await bus.add_subscriber("user.created", svc.on_user)
        existing = bus.subscriptions["user.created"][0].fn
        assert again is existing
        assert len(bus.subscriptions["user.created"]) == 1
        await bus.consume()
        await emit(app, "user.created", user_id=9)
        assert svc.seen == [9]

    async def test_remove_subscriber_by_bound_method(self, app: App, bus):
        svc = _BusService(app, bus)
        bus.subscribe("user.created")(svc.on_user)
        await bus.consume()
        await emit(app, "user.created", user_id=1)
        await bus.remove_subscriber("user.created", svc.on_user)
        await emit(app, "user.created", user_id=2)
        assert svc.seen == [1]

    async def test_remove_subscriber_by_fresh_bound_method_while_consuming(self, app: App, bus):
        svc = _BusService(app, bus)
        await bus.add_subscriber("user.created", svc.on_user)
        await bus.consume()
        await emit(app, "user.created", user_id=3)
        await bus.remove_subscriber("user.created", svc.on_user)  # fresh bound method object
        await emit(app, "user.created", user_id=4)
        assert svc.seen == [3]

    async def test_add_subscriber_bound_method_same_handler_different_options_raises(self, app: App, bus):
        class Svc:
            def on_user(self, user_id: int) -> None:
                return None

        svc = Svc()
        bus.subscribe("user.created", validate_types=True)(svc.on_user)
        with pytest.raises(RegistrationAlreadyExistsError):
            await bus.add_subscriber("user.created", svc.on_user)

    async def test_same_function_different_instances_not_deduplicated(self, app: App, bus):
        svc1 = _BusService(app, bus)
        svc2 = _BusService(app, bus)
        await bus.add_subscriber("user.created", svc1.on_user)
        await bus.add_subscriber("user.created", svc2.on_user)
        assert len(bus.subscriptions["user.created"]) == 2
        await bus.consume()
        await emit(app, "user.created", user_id=5)
        assert svc1.seen == [5]
        assert svc2.seen == [5]
