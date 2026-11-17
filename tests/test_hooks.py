from __future__ import annotations

import asyncio

import pytest

from linkr import (
    AfterHandlerEvent,
    App,
    AppHooks,
    BeforeHandlerEvent,
    ConsumeStartEvent,
    ConsumeStopEvent,
    ErrorCode,
    HandlerErrorEvent,
    LocalBackend,
    MessageDroppedEvent,
    RequestPublishedEvent,
    RequestReceivedEvent,
    ResponseReceivedEvent,
    RPCError,
)
from tests.helpers import call, emit, send


class Recorder(AppHooks):
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    async def on_consume_start(self, event: ConsumeStartEvent) -> None:
        self.events.append(("on_consume_start", event))

    async def on_consume_stop(self, event: ConsumeStopEvent) -> None:
        self.events.append(("on_consume_stop", event))

    async def on_request_published(self, event: RequestPublishedEvent) -> None:
        self.events.append(("on_request_published", event))

    async def on_response_received(self, event: ResponseReceivedEvent) -> None:
        self.events.append(("on_response_received", event))

    async def on_request_received(self, event: RequestReceivedEvent) -> None:
        self.events.append(("on_request_received", event))

    async def before_handler(self, event: BeforeHandlerEvent) -> None:
        self.events.append(("before_handler", event))

    async def after_handler(self, event: AfterHandlerEvent) -> None:
        self.events.append(("after_handler", event))

    async def on_handler_error(self, event: HandlerErrorEvent) -> None:
        self.events.append(("on_handler_error", event))

    async def on_message_dropped(self, event: MessageDroppedEvent) -> None:
        self.events.append(("on_message_dropped", event))

    def by_name(self, name: str) -> list[object]:
        return [event for event_name, event in self.events if event_name == name]


class TestHookRegistration:
    async def test_add_remove_hooks_object(self, app: App, rpc):
        recorder = Recorder()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        app.add_hooks(recorder)
        await rpc.consume()
        await call(app, "app.ping")

        assert any(e.queue == "app" and e.mode == "rpc" for e in recorder.by_name("on_consume_start"))
        assert any(e.handler.name == "app.ping" for e in recorder.by_name("after_handler"))

        count = len(recorder.events)
        app.remove_hooks(recorder)
        await call(app, "app.ping")
        assert len(recorder.events) == count

    async def test_subscribe_unsubscribe_and_decorator(self, app: App, rpc):
        seen: list[str] = []

        def listener(event: RequestPublishedEvent) -> None:
            seen.append(event.outcome)

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        app.hooks.on_request_published.subscribe(listener)
        await rpc.consume()
        await call(app, "app.ping")
        app.hooks.on_request_published.unsubscribe(listener)

        decorated: list[str] = []

        @app.hook.after_handler
        async def on_after(event: AfterHandlerEvent) -> None:
            decorated.append(event.handler.name)

        await call(app, "app.ping")

        assert seen == ["ok"]
        assert decorated == ["app.ping"]


class TestHookConsumeEvents:
    async def test_rpc_task_consume_start_stop(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        rpc = app.rpc()
        worker = app.worker()
        recorder = Recorder()
        app.add_hooks(recorder)

        @rpc.method("foo")
        def foo() -> str:
            return "foo"

        @rpc.method("bar")
        def bar() -> str:
            return "bar"

        @worker.task("job")
        def job() -> None:
            return None

        async with app:
            await rpc.consume()
            await worker.consume()

            starts = recorder.by_name("on_consume_start")
            assert ConsumeStartEvent(app="app", mode="rpc", queue="app", topics=()) in starts
            assert ConsumeStartEvent(app="app", mode="task", queue="app", topics=()) in starts

            await rpc.stop_consume()
            await rpc.stop_consume()  # idempotent
            rpc_stops = [e for e in recorder.by_name("on_consume_stop") if e.mode == "rpc"]
            assert rpc_stops == [ConsumeStopEvent(app="app", mode="rpc", queue="app", topic=None, reason="stopped")]

            await worker.stop_consume()
            task_stops = [e for e in recorder.by_name("on_consume_stop") if e.mode == "task"]
            assert task_stops == [ConsumeStopEvent(app="app", mode="task", queue="app", topic=None, reason="stopped")]

    async def test_event_consume_start_stop_reasons(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        bus = app.bus()
        recorder = Recorder()
        app.add_hooks(recorder)

        @bus.subscribe("user.created")
        def on_created(user_id: int) -> None:
            pass

        @bus.subscribe("user.deleted")
        def on_deleted(user_id: int) -> None:
            pass

        async with app:
            await bus.consume()
            queue = bus.event_queue
            assert queue is not None

            starts = [e for e in recorder.by_name("on_consume_start") if e.mode == "event"]
            assert len(starts) == 1
            assert starts[0].queue == queue
            assert starts[0].topics == ("user.created", "user.deleted")

            await bus.stop_consume()
            assert ConsumeStopEvent(
                app="app", mode="event", queue=queue, topic=None, reason="stopped"
            ) in recorder.by_name("on_consume_stop")

    async def test_event_consume_stop_all(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        bus = app.bus()
        recorder = Recorder()
        app.add_hooks(recorder)

        @bus.subscribe("user.created")
        def on_created(user_id: int) -> None:
            pass

        async with app:
            await bus.consume()
            queue = bus.event_queue
            await bus.stop_consume()

        assert ConsumeStopEvent(app="app", mode="event", queue=queue, topic=None, reason="stopped") in recorder.by_name(
            "on_consume_stop"
        )


class TestClientRequestHooks:
    async def test_rpc_published_and_response_ok_error_timeout(self, app: App, rpc):
        published: list[RequestPublishedEvent] = []
        responses: list[ResponseReceivedEvent] = []
        app.hooks.on_request_published.subscribe(published.append)
        app.hooks.on_response_received.subscribe(responses.append)

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        @rpc.method("fail")
        def fail() -> str:
            raise ValueError("boom")

        @rpc.method("slow")
        async def slow() -> str:
            await asyncio.sleep(10)
            return "late"

        await rpc.consume()

        assert await call(app, "app.ping") == "pong"
        with pytest.raises(RPCError):
            await call(app, "app.fail")
        with pytest.raises(RPCError):
            await app.client.call("app.slow", (), {}, client_timeout=0.05)

        assert len(published) == 3
        assert len(responses) == 3

        assert all(
            e.request.type == "rpc" and e.request.data.get("method") in {"ping", "fail", "slow"} for e in published
        )
        assert all(e.outcome == "ok" and e.error is None for e in published)

        assert [e.outcome for e in responses] == ["ok", "error", "timeout"]
        assert responses[0].response.data == "pong"
        assert responses[0].error is None
        assert responses[1].response.type == "error"
        assert responses[2].response.type == "error"
        timeout_data = responses[2].response.data
        timeout_code = timeout_data["error_code"] if isinstance(timeout_data, dict) else timeout_data.error_code
        assert timeout_code == ErrorCode.TIMEOUT
        assert all(e.request is p.request for e, p in zip(responses, published))

    async def test_task_and_event_publish_only(self, app: App, worker, bus):
        published: list[RequestPublishedEvent] = []
        responses: list[ResponseReceivedEvent] = []
        app.hooks.on_request_published.subscribe(published.append)
        app.hooks.on_response_received.subscribe(responses.append)

        @worker.task("job")
        def job(value: int) -> None:
            pass

        @bus.subscribe("topic")
        def on_topic(value: int) -> None:
            pass

        await worker.consume()
        await bus.consume()
        await send(app, "app.job", 1)
        await emit(app, "topic", value=2)

        assert len(published) == 2
        assert responses == []
        assert published[0].request.type == "task"
        assert published[0].request.data.get("task") == "job"
        assert published[1].request.type == "event"
        assert published[1].request.data.get("topic") == "topic"
        assert all(e.outcome == "ok" and e.error is None for e in published)


class TestServerHandlerHooks:
    async def test_request_received_before_after_rpc(self, app: App, rpc):
        recorder = Recorder()
        app.add_hooks(recorder)

        @rpc.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await rpc.consume()
        assert await call(app, "app.add", 2, 3) == 5

        assert len(recorder.by_name("on_request_received")) == 1
        before = recorder.by_name("before_handler")
        after = recorder.by_name("after_handler")
        assert len(before) == 1
        assert len(after) == 1
        assert before[0].handler.name == "app.add"
        assert after[0].handler.name == "app.add"
        assert after[0].result == 5

    async def test_handler_error_rpc_task_event(self, app: App, rpc, worker, bus):
        errors: list[HandlerErrorEvent] = []
        app.hooks.on_handler_error.subscribe(errors.append)

        @rpc.method("fail")
        def fail() -> str:
            raise ValueError("boom")

        @worker.task("fail_task")
        def fail_task() -> None:
            raise ValueError("task boom")

        @bus.subscribe("evt")
        def fail_evt(value: int) -> None:
            raise ValueError("evt boom")

        await rpc.consume()
        await worker.consume()
        await bus.consume()

        with pytest.raises(RPCError):
            await call(app, "app.fail")
        await send(app, "app.fail_task")
        await emit(app, "evt", value=1)
        await asyncio.sleep(0.05)

        assert {(e.mode, e.outcome) for e in errors} == {("rpc", "error"), ("task", "error"), ("event", "error")}
        rpc_err = next(e for e in errors if e.mode == "rpc")
        assert rpc_err.error_code == ErrorCode.INTERNAL_ERROR
        assert rpc_err.handler.name == "app.fail"


class TestDroppedMessageHooks:
    async def test_rpc_drops(self, app: App, rpc):
        drops: list[MessageDroppedEvent] = []
        app.hooks.on_message_dropped.subscribe(drops.append)

        @rpc.method("typed", validate_types=True)
        def typed(x: int) -> int:
            return x

        await rpc.consume()

        with pytest.raises(RPCError):
            await call(app, "app.missing")
        with pytest.raises(RPCError):
            await app.client.call("app.typed", ("bad",), {})

        reasons = [d.reason for d in drops]
        assert "handler_not_found" in reasons
        assert "validation_failed" in reasons
        assert any(
            d.handler and d.handler.name in ("app.typed", "typed") for d in drops if d.reason == "validation_failed"
        )

    async def test_task_and_event_drops(self, app: App, worker, bus):
        drops: list[MessageDroppedEvent] = []
        app.hooks.on_message_dropped.subscribe(drops.append)

        @worker.task("typed", validate_types=True)
        def typed(x: int) -> None:
            pass

        @bus.subscribe("evt", validate_types=True)
        def on_evt(x: int) -> None:
            pass

        await worker.consume()
        await bus.consume()

        await send(app, "app.missing")
        await send(app, "app.typed", "bad")
        await emit(app, "evt", x="bad")
        await asyncio.sleep(0.05)

        reasons = [(d.mode, d.reason) for d in drops]
        assert ("task", "handler_not_found") in reasons
        assert ("task", "validation_failed") in reasons
        assert ("event", "validation_failed") in reasons


class TestHookListenerBehavior:
    async def test_multiple_listeners_and_exception_isolation(self, app: App, rpc):
        seen: list[str] = []

        def bad(_event: RequestPublishedEvent) -> None:
            raise RuntimeError("boom")

        def good(_event: RequestPublishedEvent) -> None:
            seen.append("ok")

        app.hooks.on_request_published.subscribe(bad)
        app.hooks.on_request_published.subscribe(good)

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await rpc.consume()
        assert await call(app, "app.ping") == "pong"
        assert seen == ["ok"]

    async def test_async_listener(self, app: App, rpc):
        seen = asyncio.Event()

        async def listener(event: AfterHandlerEvent) -> None:
            if event.handler.name == "app.ping":
                seen.set()

        app.hooks.after_handler.subscribe(listener)

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await rpc.consume()
        await call(app, "app.ping")
        await asyncio.wait_for(seen.wait(), timeout=1.0)
