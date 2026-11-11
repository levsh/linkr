from __future__ import annotations

import pytest

from linkr import App, ErrorCode, LocalBackend, RPCError
from tests.helpers import call, emit, send


class TestLocalBackend:
    async def test_rpc_call_between_apps(self):
        server_backend = LocalBackend()
        client_backend = LocalBackend()

        server = App("server", backend=server_backend)
        client = App("client", backend=client_backend)

        @server.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        async with server, client:
            await server.consume()
            assert await call(client, "server.add", 2, 3) == 5

    async def test_rpc_default_queue_shared_by_methods(self):
        app = App("app", backend=LocalBackend())

        @app.method("foo")
        def foo() -> str:
            return "foo"

        @app.method("bar")
        def bar() -> str:
            return "bar"

        async with app:
            await app.consume()
            assert await call(app, "app.foo") == "foo"
            assert await call(app, "app.bar") == "bar"

    async def test_rpc_grouped_queues(self):
        app = App("app", backend=LocalBackend())

        @app.method("serviceA.foo")
        def foo() -> str:
            return "foo"

        @app.method("serviceB.bar")
        def bar() -> str:
            return "bar"

        async with app:
            await app.consume()
            assert await call(app, "app.serviceA.foo") == "foo"
            assert await call(app, "app.serviceB.bar") == "bar"

    async def test_rpc_method_not_found(self):
        app = App("app", backend=LocalBackend())

        @app.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await app.consume()
            with pytest.raises(RPCError) as exc_info:
                await call(app, "app.unknown")
            assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

    async def test_task_delivered_to_exactly_one_consumer(self):
        backend1 = LocalBackend()
        backend2 = LocalBackend()
        client_backend = LocalBackend()

        app1 = App("worker", backend=backend1)
        app2 = App("worker", backend=backend2)
        client = App("client", backend=client_backend)

        received: list[str] = []

        @app1.task("email.send")
        def send1(email: str) -> None:
            received.append(f"1:{email}")

        @app2.task("email.send")
        def send2(email: str) -> None:
            received.append(f"2:{email}")

        async with app1, app2, client:
            await app1.consume()
            await app2.consume()

            for index in range(10):
                await send(client, "worker.email.send", f"user{index}@example.com")

            assert len(received) == 10
            assert len(set(received)) == 10
            assert sum(value.startswith("1:") for value in received) == 5
            assert sum(value.startswith("2:") for value in received) == 5

    async def test_task_no_consumer_is_noop(self):
        client = App("client", backend=LocalBackend())

        async with client:
            await send(client, "worker.email.send", "user@example.com")

    async def test_task_does_not_return_result(self):
        app = App("app", backend=LocalBackend())

        @app.task("work")
        def work(value: int) -> int:
            return value * 2

        async with app:
            await app.consume()
            assert await app.send("app.work", (21,), {}) is None

    async def test_event_delivered_to_all_subscribers(self):
        backend1 = LocalBackend()
        backend2 = LocalBackend()
        backend3 = LocalBackend()

        app1 = App("app1", backend=backend1)
        app2 = App("app2", backend=backend2)
        app3 = App("app3", backend=backend3)

        events1: list[int] = []
        events2: list[int] = []
        events3: list[int] = []

        @app1.subscribe("user.created")
        def on_user_created1(user_id: int) -> None:
            events1.append(user_id)

        @app2.subscribe("user.created")
        def on_user_created2(user_id: int) -> None:
            events2.append(user_id)

        @app3.subscribe("user.deleted")
        def on_user_deleted(user_id: int) -> None:
            events3.append(user_id)

        async with app1, app2, app3:
            await app1.consume()
            await app2.consume()
            await app3.consume()

            await emit(app1, "user.created", user_id=42)

            assert events1 == [42]
            assert events2 == [42]
            assert events3 == []

    async def test_event_wildcards(self):
        app = App("app", backend=LocalBackend())
        seen: list[str] = []

        @app.subscribe("user.*")
        def on_user(event: str) -> None:
            seen.append(f"star:{event}")

        @app.subscribe("user.#")
        def on_user_any(event: str) -> None:
            seen.append(f"hash:{event}")

        async with app:
            await app.consume()
            await emit(app, "user.created", event="created")
            await emit(app, "user.profile.updated", event="updated")

            assert "star:created" in seen
            assert "hash:created" in seen
            assert "hash:updated" in seen
            assert seen.count("star:updated") == 0

    async def test_multiple_subscribers_in_one_app(self):
        app = App("app", backend=LocalBackend())
        seen: list[str] = []

        @app.subscribe("user.created")
        def first(user_id: int) -> None:
            seen.append(f"first:{user_id}")

        @app.subscribe("user.created")
        def second(user_id: int) -> None:
            seen.append(f"second:{user_id}")

        async with app:
            await app.consume()
            await emit(app, "user.created", user_id=1)

            assert sorted(seen) == ["first:1", "second:1"]

    async def test_event_no_subscribers_is_noop(self):
        app = App("app", backend=LocalBackend())

        async with app:
            await emit(app, "nobody.listens")

    async def test_stop_consume_rpc_queue(self):
        backend = LocalBackend()
        app = App("app", backend=backend)

        @app.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await app.consume()
            assert await call(app, "app.ping") == "pong"
            await backend.stop_consume_rpc("app")
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.ping")

    async def test_stop_consume_rpc_all(self):
        backend = LocalBackend()
        app = App("app", backend=backend)

        @app.method("group.ping")
        def ping() -> str:
            return "pong"

        async with app:
            await app.consume()
            assert await call(app, "app.group.ping") == "pong"
            await backend.stop_consume_rpc()
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.group.ping")

    async def test_stop_consume_tasks_queue(self):
        backend = LocalBackend()
        client_backend = LocalBackend()
        app = App("worker", backend=backend)
        client = App("client", backend=client_backend)
        received: list[str] = []

        @app.task("email.send")
        def send_email(email: str) -> None:
            received.append(email)

        async with app, client:
            await app.consume()
            await send(client, "worker.email.send", "a@example.com")
            await backend.stop_consume_tasks("worker.email")
            await send(client, "worker.email.send", "b@example.com")

        assert received == ["a@example.com"]

    async def test_stop_consume_events_topic_unbind(self):
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
            queue = app._event_queue
            assert queue is not None
            await backend.stop_consume_events(queue, "user.created")
            await emit(app, "user.created", user_id=1)
            await emit(app, "user.deleted", user_id=2)

        assert seen == ["deleted:2"]

    async def test_stop_consume_events_queue(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        seen: list[int] = []

        @app.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(user_id)

        async with app:
            await app.consume()
            queue = app._event_queue
            assert queue is not None
            await backend.stop_consume_events(queue)
            await emit(app, "user.created", user_id=1)

        assert seen == []
