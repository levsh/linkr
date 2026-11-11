from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from typing import Any

import pytest

from linkr import App, ErrorCode, RPCError
from linkr.backends.rmq import RmqBackend
from tests.helpers import call, emit, send
from tests.utils import get_ip


async def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 10.0,
    interval: float = 0.05,
    message: str = "condition not met within timeout",
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    if predicate():
        return
    raise AssertionError(message)


def uid(prefix: str = "t") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def make_backend(rabbitmq) -> RmqBackend:
    return RmqBackend(host=get_ip(rabbitmq), port=5672)


@pytest.mark.integration
class TestRmqBackend:
    async def test_rpc_call_between_apps_args_and_kwds(self, rabbitmq):
        suffix = uid("srv")
        server_name = f"server_{suffix}"
        client_name = f"client_{suffix}"

        server = App(server_name, backend=make_backend(rabbitmq))
        client = App(client_name, backend=make_backend(rabbitmq))

        @server.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        @server.method("echo")
        def echo(value: Any) -> Any:
            return value

        async with server, client:
            await server.consume()
            assert await call(client, f"{server_name}.add", 2, 3) == 5
            assert await client.call(f"{server_name}.add", (), {"x": 10, "y": 7}) == 17
            assert await call(client, f"{server_name}.echo", {"a": [1, 2], "b": None}) == {
                "a": [1, 2],
                "b": None,
            }

    async def test_rpc_default_queue_shared_by_methods_and_dispatch(self, rabbitmq):
        name = uid("app")
        app = App(name, backend=make_backend(rabbitmq))
        client = App(uid("client"), backend=make_backend(rabbitmq))
        seen: list[str] = []

        @app.method("foo")
        def foo(value: str) -> str:
            seen.append(f"foo:{value}")
            return f"foo:{value}"

        @app.method("bar")
        def bar(value: str) -> str:
            seen.append(f"bar:{value}")
            return f"bar:{value}"

        async with app, client:
            await app.consume()
            calls = [call(client, f"{name}.foo", "1"), call(client, f"{name}.bar", "2")]
            results = await asyncio.gather(*calls)
            assert sorted(results) == ["bar:2", "foo:1"]
            await wait_until(lambda: len(seen) == 2, timeout=10, message=f"handlers not called: {seen}")
            assert sorted(seen) == ["bar:2", "foo:1"]

    async def test_rpc_grouped_queues_isolated(self, rabbitmq):
        name = uid("app")
        app = App(name, backend=make_backend(rabbitmq))
        client = App(uid("client"), backend=make_backend(rabbitmq))

        @app.method("serviceA.foo")
        def foo() -> str:
            return "foo"

        @app.method("serviceB.bar")
        def bar() -> str:
            return "bar"

        async with app, client:
            await app.consume()
            assert await call(client, f"{name}.serviceA.foo") == "foo"
            assert await call(client, f"{name}.serviceB.bar") == "bar"

            # stop only serviceA queue: serviceB must continue working
            await app._backend.stop_consume_rpc(f"{name}.serviceA")
            assert await call(client, f"{name}.serviceB.bar") == "bar"
            with pytest.raises(RPCError) as exc_info:
                await client.call(f"{name}.serviceA.foo", (), {}, timeout=1.0, ttl=1.0)
            assert exc_info.value.error_code == ErrorCode.TIMEOUT

    async def test_rpc_errors_and_custom_exception(self, rabbitmq):
        name = uid("app")
        app = App(name, backend=make_backend(rabbitmq), validate_types=True)
        client = App(uid("client"), backend=make_backend(rabbitmq))

        @app.method("fail")
        def fail() -> str:
            raise ValueError("boom")

        @app.method("typed_add")
        def typed_add(x: int, y: int) -> int:
            return x + y

        @app.exception_handler(KeyError)
        def handle_key_error(exc: Exception):
            return ("BadKey", str(exc))

        @app.method("need_key")
        def need_key() -> str:
            raise KeyError("missing")

        async with app, client:
            await app.consume()

            with pytest.raises(RPCError) as exc_info:
                await call(client, f"{name}.unknown")
            assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

            with pytest.raises(RPCError) as exc_info:
                await call(client, f"{name}.fail")
            assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR

            with pytest.raises(RPCError) as exc_info:
                await client.call(f"{name}.typed_add", ("x", 2), {})
            assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR

            with pytest.raises(RPCError) as exc_info:
                await call(client, f"{name}.need_key")
            assert exc_info.value.error_code == "BadKey"
            assert "missing" in exc_info.value.error_message

    async def test_task_competing_consumers_no_duplicates(self, rabbitmq):
        suffix = uid("w")
        worker_name = f"worker_{suffix}"
        task_name = f"job_{suffix}"
        qualified_task = f"{worker_name}.{task_name}"

        backend1 = make_backend(rabbitmq)
        backend2 = make_backend(rabbitmq)
        client_backend = make_backend(rabbitmq)

        app1 = App(worker_name, backend=backend1)
        app2 = App(worker_name, backend=backend2)
        client = App(uid("client"), backend=client_backend)

        received: list[str] = []
        lock = asyncio.Lock()

        @app1.task(task_name)
        async def task1(item: int) -> None:
            async with lock:
                received.append(f"1:{item}")

        @app2.task(task_name)
        async def task2(item: int) -> None:
            async with lock:
                received.append(f"2:{item}")

        async with app1, app2, client:
            await app1.consume()
            await app2.consume()

            total = 20
            for index in range(total):
                await send(client, qualified_task, index)

            await wait_until(
                lambda: len(received) == total,
                timeout=15,
                message=f"expected {total} task deliveries, got {len(received)}: {sorted(received)}",
            )

            assert len(received) == total
            assert len(set(received)) == total
            items = sorted(int(value.split(":", 1)[1]) for value in received)
            assert items == list(range(total))
            # both consumers should participate (non-deterministic split, but with
            # prefetch=10 and 20 messages both should get work)
            assert any(value.startswith("1:") for value in received)
            assert any(value.startswith("2:") for value in received)

    async def test_task_send_returns_none_and_handler_executes(self, rabbitmq):
        name = uid("app")
        task_name = f"work_{uid('t')}"
        app = App(name, backend=make_backend(rabbitmq))
        client = App(uid("client"), backend=make_backend(rabbitmq))
        done = asyncio.Event()
        payloads: list[int] = []

        @app.task(task_name)
        def work(value: int) -> int:
            payloads.append(value)
            done.set()
            return value * 2

        async with app, client:
            await app.consume()
            result = await client.send(f"{name}.{task_name}", (21,), {})
            assert result is None
            await asyncio.wait_for(done.wait(), timeout=10)
            assert payloads == [21]

    async def test_stop_consume_rpc_queue_backend(self, rabbitmq):
        name = uid("app")
        backend = make_backend(rabbitmq)
        app = App(name, backend=backend)
        client = App(uid("client"), backend=make_backend(rabbitmq))

        @app.method("group.ping")
        def ping() -> str:
            return "pong"

        async with app, client:
            await app.consume()
            assert await call(client, f"{name}.group.ping") == "pong"
            await backend.stop_consume_rpc(f"{name}.group")
            with pytest.raises(RPCError) as exc_info:
                await client.call(f"{name}.group.ping", (), {}, timeout=1.0, ttl=1.0)
            assert exc_info.value.error_code == ErrorCode.TIMEOUT

    async def test_stop_consume_rpc_specific_method_keeps_shared_queue(self, rabbitmq):
        name = uid("app")
        app = App(name, backend=make_backend(rabbitmq))
        client = App(uid("client"), backend=make_backend(rabbitmq))

        @app.method("foo")
        def foo() -> str:
            return "foo"

        @app.method("bar")
        def bar() -> str:
            return "bar"

        async with app, client:
            await app.consume()
            assert await call(client, f"{name}.foo") == "foo"
            assert await call(client, f"{name}.bar") == "bar"

            await app.stop_consume_rpc("foo")

            with pytest.raises(RPCError) as exc_info:
                await call(client, f"{name}.foo")
            assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND
            assert await call(client, f"{name}.bar") == "bar"

    async def test_stop_consume_tasks_queue_backend(self, rabbitmq):
        worker_name = uid("worker")
        group = f"email_{uid('g')}"
        task_name = f"{group}.send"
        qualified = f"{worker_name}.{task_name}"

        backend = make_backend(rabbitmq)
        app = App(worker_name, backend=backend)
        client = App(uid("client"), backend=make_backend(rabbitmq))
        received: list[str] = []

        @app.task(task_name)
        def handle(value: str) -> None:
            received.append(value)

        async with app, client:
            await app.consume()
            await send(client, qualified, "a")
            await wait_until(lambda: received == ["a"], timeout=10, message=f"task not received: {received}")

            await backend.stop_consume_tasks(f"{worker_name}.{group}")
            await send(client, qualified, "b")
            await asyncio.sleep(1.0)
            assert received == ["a"]

    async def test_event_fanout_and_topic_filtering(self, rabbitmq):
        suffix = uid("ev")
        topic = f"user.created.{suffix}"
        other_topic = f"user.deleted.{suffix}"

        app1 = App(uid("app1"), backend=make_backend(rabbitmq))
        app2 = App(uid("app2"), backend=make_backend(rabbitmq))
        app3 = App(uid("app3"), backend=make_backend(rabbitmq))
        publisher = App(uid("pub"), backend=make_backend(rabbitmq))

        events1: list[int] = []
        events2: list[int] = []
        events3: list[int] = []

        @app1.subscribe(topic)
        def on_created_1(user_id: int) -> None:
            events1.append(user_id)

        @app2.subscribe(topic)
        def on_created_2(user_id: int) -> None:
            events2.append(user_id)

        @app3.subscribe(other_topic)
        def on_deleted(user_id: int) -> None:
            events3.append(user_id)

        async with app1, app2, app3, publisher:
            await app1.consume()
            await app2.consume()
            await app3.consume()

            await emit(publisher, topic, user_id=42)
            await wait_until(lambda: events1 == [42] and events2 == [42], timeout=10, message="fanout not delivered")
            await asyncio.sleep(0.5)
            assert events3 == []

    async def test_event_wildcards_and_multiple_subscribers(self, rabbitmq):
        suffix = uid("ev")
        base = f"acct.{suffix}"
        app = App(uid("app"), backend=make_backend(rabbitmq))
        publisher = App(uid("pub"), backend=make_backend(rabbitmq))
        seen: list[str] = []

        @app.subscribe(f"{base}.*")
        def on_star(name: str) -> None:
            seen.append(f"star:{name}")

        @app.subscribe(f"{base}.#")
        def on_hash(name: str) -> None:
            seen.append(f"hash:{name}")

        @app.subscribe(f"{base}.created")
        def on_exact(name: str) -> None:
            seen.append(f"exact:{name}")

        async with app, publisher:
            await app.consume()
            await emit(publisher, f"{base}.created", name="created")
            await emit(publisher, f"{base}.profile.updated", name="updated")

            expected = sorted(["exact:created", "hash:created", "hash:updated", "star:created"])
            await wait_until(
                lambda: sorted(seen) == expected,
                timeout=15,
                message=f"expected wildcard deliveries {expected}, got {sorted(seen)}",
            )
            assert sorted(seen) == expected

    async def test_stop_consume_events_specific_topic_app(self, rabbitmq):
        suffix = uid("ev")
        topic_a = f"user.created.{suffix}"
        topic_b = f"user.deleted.{suffix}"
        app = App(uid("app"), backend=make_backend(rabbitmq))
        publisher = App(uid("pub"), backend=make_backend(rabbitmq))
        seen: list[str] = []

        @app.subscribe(topic_a)
        def on_a(value: str) -> None:
            seen.append(f"a:{value}")

        @app.subscribe(topic_b)
        def on_b(value: str) -> None:
            seen.append(f"b:{value}")

        async with app, publisher:
            await app.consume()
            await emit(publisher, topic_a, value="1")
            await emit(publisher, topic_b, value="1")
            await wait_until(
                lambda: sorted(seen) == ["a:1", "b:1"],
                timeout=10,
                message=f"initial events missing: {seen}",
            )

            await app.stop_consume_events(topic_a)

            await emit(publisher, topic_a, value="2")
            await emit(publisher, topic_b, value="2")
            await wait_until(lambda: "b:2" in seen, timeout=10, message=f"remaining topic not delivered: {seen}")
            await asyncio.sleep(0.7)
            assert sorted(seen) == ["a:1", "b:1", "b:2"]

    async def test_stop_consume_events_specific_topic_backend(self, rabbitmq):
        suffix = uid("ev")
        topic_a = f"user.created.{suffix}"
        topic_b = f"user.deleted.{suffix}"
        backend = make_backend(rabbitmq)
        app = App(uid("app"), backend=backend)
        publisher = App(uid("pub"), backend=make_backend(rabbitmq))
        seen: list[str] = []

        @app.subscribe(topic_a)
        def on_a(value: str) -> None:
            seen.append(f"a:{value}")

        @app.subscribe(topic_b)
        def on_b(value: str) -> None:
            seen.append(f"b:{value}")

        async with app, publisher:
            await app.consume()
            queue = app._event_queue
            assert queue is not None

            await backend.stop_consume_events(queue, topic_a)

            await emit(publisher, topic_a, value="1")
            await emit(publisher, topic_b, value="1")
            await wait_until(lambda: "b:1" in seen, timeout=10, message=f"remaining topic not delivered: {seen}")
            await asyncio.sleep(0.7)
            assert seen == ["b:1"]

    async def test_stop_consume_events_queue_backend(self, rabbitmq):
        suffix = uid("ev")
        topic = f"user.created.{suffix}"
        backend = make_backend(rabbitmq)
        app = App(uid("app"), backend=backend)
        publisher = App(uid("pub"), backend=make_backend(rabbitmq))
        seen: list[str] = []

        @app.subscribe(topic)
        def on_event(value: str) -> None:
            seen.append(value)

        async with app, publisher:
            await app.consume()
            queue = app._event_queue
            assert queue is not None
            await emit(publisher, topic, value="before")
            await wait_until(lambda: seen == ["before"], timeout=10, message=f"event not delivered: {seen}")

            await backend.stop_consume_events(queue)
            await emit(publisher, topic, value="after")
            await asyncio.sleep(1.0)
            assert seen == ["before"]

    async def test_backend_mass_stop_consume(self, rabbitmq):
        suffix = uid("app")
        name = f"app_{suffix}"
        topic = f"evt.{suffix}"
        backend = make_backend(rabbitmq)
        app = App(name, backend=backend)
        client = App(uid("client"), backend=make_backend(rabbitmq))
        events: list[str] = []

        @app.method("ping")
        def ping() -> str:
            return "pong"

        @app.task("work")
        def work(value: str) -> None:
            events.append(f"task:{value}")

        @app.subscribe(topic)
        def on_event(value: str) -> None:
            events.append(f"event:{value}")

        async with app, client:
            await app.consume()
            assert await call(client, f"{name}.ping") == "pong"

            await backend.stop_consume_rpc()
            await backend.stop_consume_tasks()
            await backend.stop_consume_events()

            with pytest.raises(RPCError) as exc_info:
                await client.call(f"{name}.ping", (), {}, timeout=1.0, ttl=1.0)
            assert exc_info.value.error_code == ErrorCode.TIMEOUT

            await client.send(f"{name}.work", ("x",), {})
            await client.emit(topic, {"value": "y"})
            await asyncio.sleep(1.0)
            assert events == []

    async def test_close_makes_operations_unavailable(self, rabbitmq):
        backend = make_backend(rabbitmq)
        await backend.init()
        await backend.close()

        assert backend._connection is None
        assert backend._channel is None
        with pytest.raises(RuntimeError):
            # request requires initialized channel/resources
            from linkr.models import Message

            await backend.request("q", Message(data=b"{}", headers={}))
