from __future__ import annotations

import pytest

from linkr import App, ErrorCode, HealthStatus, LocalBackend, RPCError
from tests.helpers import call, emit, send


class TestLocalBackend:
    async def test_rpc_call_between_apps(self):
        server_backend = LocalBackend()
        client_backend = LocalBackend()

        server_app = App("server", backend=server_backend)
        client = App("client", backend=client_backend)
        server_srv = server_app.rpc()

        @server_srv.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        async with server_app, client:
            await server_srv.consume()
            assert await call(client, "server.add", 2, 3) == 5

    async def test_rpc_default_queue_shared_by_methods(self):
        app = App("app", backend=LocalBackend())
        server = app.rpc()

        @server.method("foo")
        def foo() -> str:
            return "foo"

        @server.method("bar")
        def bar() -> str:
            return "bar"

        async with app:
            await server.consume()
            assert await call(app, "app.foo") == "foo"
            assert await call(app, "app.bar") == "bar"

    async def test_rpc_grouped_queues(self):
        app = App("app", backend=LocalBackend())
        sa = app.rpc("serviceA")
        sb = app.rpc("serviceB")

        @sa.method("foo")
        def foo() -> str:
            return "foo"

        @sb.method("bar")
        def bar() -> str:
            return "bar"

        async with app:
            await sa.consume()
            await sb.consume()
            assert await call(app, "app.serviceA.foo") == "foo"
            assert await call(app, "app.serviceB.bar") == "bar"

    async def test_rpc_method_not_found(self):
        app = App("app", backend=LocalBackend())
        server = app.rpc()

        @server.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await server.consume()
            with pytest.raises(RPCError) as exc_info:
                await call(app, "app.unknown")
            assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

    async def test_task_delivered_to_exactly_one_consumer(self):
        backend1 = LocalBackend()
        backend2 = LocalBackend()
        client_backend = LocalBackend()

        app1 = App("worker", backend=backend1)
        server1 = app1.worker("email")
        app2 = App("worker", backend=backend2)
        server2 = app2.worker("email")
        client = App("client", backend=client_backend)

        received: list[str] = []

        @server1.task("send")
        def send1(email: str) -> None:
            received.append(f"1:{email}")

        @server2.task("send")
        def send2(email: str) -> None:
            received.append(f"2:{email}")

        async with app1, app2, client:
            await server1.consume()
            await server2.consume()

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
        server = app.worker()

        @server.task("work")
        def work(value: int) -> int:
            return value * 2

        async with app:
            await server.consume()
            assert await app.client.send("app.work", (21,), {}) is None

    async def test_event_delivered_to_all_subscribers(self):
        backend1 = LocalBackend()
        backend2 = LocalBackend()
        backend3 = LocalBackend()

        app1 = App("app1", backend=backend1)
        server1 = app1.bus()
        app2 = App("app2", backend=backend2)
        server2 = app2.bus()
        app3 = App("app3", backend=backend3)
        server3 = app3.bus()

        events1: list[int] = []
        events2: list[int] = []
        events3: list[int] = []

        @server1.subscribe("user.created")
        def on_user_created1(user_id: int) -> None:
            events1.append(user_id)

        @server2.subscribe("user.created")
        def on_user_created2(user_id: int) -> None:
            events2.append(user_id)

        @server3.subscribe("user.deleted")
        def on_user_deleted(user_id: int) -> None:
            events3.append(user_id)

        async with app1, app2, app3:
            await server1.consume()
            await server2.consume()
            await server3.consume()

            await emit(app1, "user.created", user_id=42)

            assert events1 == [42]
            assert events2 == [42]
            assert events3 == []

    async def test_event_wildcards(self):
        app = App("app", backend=LocalBackend())
        server = app.bus()
        seen: list[str] = []

        @server.subscribe("user.*")
        def on_user(event: str) -> None:
            seen.append(f"star:{event}")

        @server.subscribe("user.#")
        def on_user_any(event: str) -> None:
            seen.append(f"hash:{event}")

        async with app:
            await server.consume()
            await emit(app, "user.created", event="created")
            await emit(app, "user.profile.updated", event="updated")

            assert "star:created" in seen
            assert "hash:created" in seen
            assert "hash:updated" in seen
            assert seen.count("star:updated") == 0

    async def test_multiple_subscribers_in_one_app(self):
        app = App("app", backend=LocalBackend())
        server = app.bus()
        seen: list[str] = []

        @server.subscribe("user.created")
        def first(user_id: int) -> None:
            seen.append(f"first:{user_id}")

        @server.subscribe("user.created")
        def second(user_id: int) -> None:
            seen.append(f"second:{user_id}")

        async with app:
            await server.consume()
            await emit(app, "user.created", user_id=1)

            assert sorted(seen) == ["first:1", "second:1"]

    async def test_event_no_subscribers_is_noop(self):
        app = App("app", backend=LocalBackend())
        server = app.bus()

        async with app:
            await emit(app, "nobody.listens")

    async def test_stop_consume_rpc_queue(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        server = app.rpc()

        @server.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await server.consume()
            assert await call(app, "app.ping") == "pong"
            await backend.stop_consume_rpc("app")
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.ping")

    async def test_stop_consume_rpc_all(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        grp = app.rpc("group")

        @grp.method("ping")
        def ping() -> str:
            return "pong"

        async with app:
            await grp.consume()
            assert await call(app, "app.group.ping") == "pong"
            await backend.stop_consume_rpc()
            with pytest.raises(RuntimeError, match="no RPC consumer registered"):
                await call(app, "app.group.ping")

    async def test_stop_consume_tasks_queue(self):
        backend = LocalBackend()
        client_backend = LocalBackend()
        app = App("worker", backend=backend)
        email = app.worker("email")
        client = App("client", backend=client_backend)
        received: list[str] = []

        @email.task("send")
        def send_email(addr: str) -> None:
            received.append(addr)

        async with app, client:
            await email.consume()
            await send(client, "worker.email.send", "a@example.com")
            await backend.stop_consume_tasks("worker.email")
            await send(client, "worker.email.send", "b@example.com")

        assert received == ["a@example.com"]

    async def test_stop_consume_events_topic_unbind(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        server = app.bus()
        seen: list[str] = []

        @server.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(f"created:{user_id}")

        @server.subscribe("user.deleted")
        def on_deleted(user_id: int) -> None:
            seen.append(f"deleted:{user_id}")

        async with app:
            await server.consume()
            queue = server.event_queue
            assert queue is not None
            await backend.stop_consume_events(queue, "user.created")
            await emit(app, "user.created", user_id=1)
            await emit(app, "user.deleted", user_id=2)

        assert seen == ["deleted:2"]

    async def test_stop_consume_events_queue(self):
        backend = LocalBackend()
        app = App("app", backend=backend)
        server = app.bus()
        seen: list[int] = []

        @server.subscribe("user.created")
        def on_created(user_id: int) -> None:
            seen.append(user_id)

        async with app:
            await server.consume()
            queue = server.event_queue
            assert queue is not None
            await backend.stop_consume_events(queue)
            await emit(app, "user.created", user_id=1)

        assert seen == []

    async def test_healthcheck_not_initialized(self):
        backend = LocalBackend()

        assert await backend.healthcheck() == HealthStatus(ready=False, health=None)

    async def test_healthcheck_ready(self):
        backend = LocalBackend()
        await backend.init()

        assert await backend.healthcheck() == HealthStatus(ready=True, health=True)
        await backend.close()

    async def test_healthcheck_after_close(self):
        backend = LocalBackend()
        await backend.init()
        await backend.close()

        assert await backend.healthcheck() == HealthStatus(ready=False, health=None)
