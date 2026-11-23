from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from typing import Any

import pytest

from linkr import App, ErrorCode, HealthStatus, RPCError
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
    return RmqBackend(
        host=get_ip(rabbitmq),
        port=5672,
    )


@pytest.mark.integration
class TestRmqBackend:
    async def test_rpc_call_between_apps_args_and_kwds(self, rabbitmq):
        suffix = uid("srv")
        server_name = f"server_{suffix}"
        client_name = f"client_{suffix}"

        server_app = App(server_name, backend=make_backend(rabbitmq))
        server_srv = server_app.rpc()
        client_app = App(client_name, backend=make_backend(rabbitmq))

        @server_srv.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        @server_srv.method("echo")
        def echo(value: Any) -> Any:
            return value

        async with server_app, client_app:
            await server_srv.consume()
            assert await call(client_app, f"{server_name}.add", 2, 3) == 5
            assert await client_app.client.call(f"{server_name}.add", (), {"x": 10, "y": 7}) == 17
            assert await call(client_app, f"{server_name}.echo", {"a": [1, 2], "b": None}) == {
                "a": [1, 2],
                "b": None,
            }

    async def test_rpc_default_queue_shared_by_methods_and_dispatch(self, rabbitmq):
        name = uid("app")
        app = App(name, backend=make_backend(rabbitmq))
        srv = app.rpc()
        client = App(uid("client"), backend=make_backend(rabbitmq))
        seen: list[str] = []

        @srv.method("foo")
        def foo(value: str) -> str:
            seen.append(f"foo:{value}")
            return f"foo:{value}"

        @srv.method("bar")
        def bar(value: str) -> str:
            seen.append(f"bar:{value}")
            return f"bar:{value}"

        async with app, client:
            await srv.consume()
            calls = [call(client, f"{name}.foo", "1"), call(client, f"{name}.bar", "2")]
            results = await asyncio.gather(*calls)
            assert sorted(results) == ["bar:2", "foo:1"]
            await wait_until(lambda: len(seen) == 2, timeout=10, message=f"handlers not called: {seen}")
            assert sorted(seen) == ["bar:2", "foo:1"]

    async def test_rpc_grouped_queues_isolated(self, rabbitmq):
        name = uid("app")
        app = App(name, backend=make_backend(rabbitmq))
        sa = app.rpc("serviceA")
        sb = app.rpc("serviceB")
        client = App(uid("client"), backend=make_backend(rabbitmq))

        @sa.method("foo")
        def foo() -> str:
            return "foo"

        @sb.method("bar")
        def bar() -> str:
            return "bar"

        async with app, client:
            await sa.consume()
            await sb.consume()
            assert await call(client, f"{name}.serviceA.foo") == "foo"
            assert await call(client, f"{name}.serviceB.bar") == "bar"

            # stop only serviceA queue at backend level: serviceB must continue working
            await app._backend.stop_consume_rpc(f"{name}.serviceA")
            assert await call(client, f"{name}.serviceB.bar") == "bar"
            with pytest.raises(RPCError) as exc_info:
                await client.client.call(f"{name}.serviceA.foo", (), {}, client_timeout=1.0, ttl=1.0)
            assert exc_info.value.error_code == ErrorCode.TIMEOUT

    async def test_rpc_errors_and_custom_exception(self, rabbitmq):
        name = uid("app")
        app = App(name, backend=make_backend(rabbitmq), validate_types=True)
        srv = app.rpc()
        client = App(uid("client"), backend=make_backend(rabbitmq))

        @srv.method("fail")
        def fail() -> str:
            raise ValueError("boom")

        @srv.method("typed_add")
        def typed_add(x: int, y: int) -> int:
            return x + y

        @app.exception_handler(KeyError)
        def handle_key_error(exc: Exception):
            return ("BadKey", str(exc))

        @srv.method("need_key")
        def need_key() -> str:
            raise KeyError("missing")

        async with app, client:
            await srv.consume()

            with pytest.raises(RPCError) as exc_info:
                await call(client, f"{name}.unknown")
            assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND

            with pytest.raises(RPCError) as exc_info:
                await call(client, f"{name}.fail")
            assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR

            with pytest.raises(RPCError) as exc_info:
                await client.client.call(f"{name}.typed_add", ("x", 2), {})
            assert exc_info.value.error_code == ErrorCode.VALIDATION_ERROR

            with pytest.raises(RPCError) as exc_info:
                await call(client, f"{name}.need_key")
            assert exc_info.value.error_code == "BadKey"
            assert "missing" in exc_info.value.error_message

    async def test_task_competing_consumers_no_duplicates(self, rabbitmq):
        suffix = uid("w")
        worker_name = f"worker_{suffix}"

        backend1 = make_backend(rabbitmq)
        backend2 = make_backend(rabbitmq)
        client_backend = make_backend(rabbitmq)

        app1 = App(worker_name, backend=backend1)
        app2 = App(worker_name, backend=backend2)
        srv1 = app1.worker("email")
        srv2 = app2.worker("email")
        client = App(uid("client"), backend=client_backend)

        received: list[str] = []
        lock = asyncio.Lock()

        @srv1.task("send")
        async def task1(item: int) -> None:
            async with lock:
                received.append(f"1:{item}")

        @srv2.task("send")
        async def task2(item: int) -> None:
            async with lock:
                received.append(f"2:{item}")

        async with app1, app2, client:
            await srv1.consume()
            await srv2.consume()

            total = 20
            qualified_task = f"{worker_name}.email.send"
            for index in range(total):
                await send(client, qualified_task, index)

            await wait_until(
                lambda: len(received) == total,
                timeout=15,
                message=f"expected {total} task deliveries, got {len(received)}: {sorted(received)}",
            )

            assert len(received) == total
            assert len(set(received)) == total

    async def test_event_fanout_and_topic_filtering(self, rabbitmq):
        suffix = uid("pub")
        app1 = App(f"app1_{suffix}", backend=make_backend(rabbitmq))
        app2 = App(f"app2_{suffix}", backend=make_backend(rabbitmq))
        app3 = App(f"app3_{suffix}", backend=make_backend(rabbitmq))
        publisher = App(f"pub_{suffix}", backend=make_backend(rabbitmq))
        s1 = app1.bus()
        s2 = app2.bus()
        s3 = app3.bus()

        events1: list[int] = []
        events2: list[int] = []
        events3: list[int] = []

        @s1.subscribe("user.created")
        def on_user_created1(user_id: int) -> None:
            events1.append(user_id)

        @s2.subscribe("user.created")
        def on_user_created2(user_id: int) -> None:
            events2.append(user_id)

        @s3.subscribe("user.deleted")
        def on_user_deleted(user_id: int) -> None:
            events3.append(user_id)

        async with app1, app2, app3, publisher:
            await s1.consume()
            await s2.consume()
            await s3.consume()

            await emit(publisher, "user.created", user_id=42)
            await emit(publisher, "user.deleted", user_id=7)

            await wait_until(lambda: events1 == [42] and events2 == [42] and events3 == [7], timeout=10)

        assert events1 == [42]
        assert events2 == [42]
        assert events3 == [7]

    async def test_backend_close_uninitialized_safe(self, rabbitmq):
        backend = make_backend(rabbitmq)
        await backend.close()
        await backend.close()

    async def test_healthcheck(self, rabbitmq):
        backend = make_backend(rabbitmq)
        app = App(uid("health"), backend=backend)
        srv = app.rpc()

        @srv.method("ping")
        def ping() -> str:
            return "pong"

        assert await backend.healthcheck() == HealthStatus(
            ready=False,
            health=None,
            details={"connection": "error"},
        )

        async with app:
            assert await app.healthcheck(fast=True) == HealthStatus(
                ready=True,
                health=True,
                details={"connection": "ok"},
            )
            assert await app.healthcheck() == HealthStatus(
                ready=True,
                health=True,
                details={"connection": "ok", "queues": "ok"},
            )

            await srv.consume()
            assert await app.healthcheck() == HealthStatus(
                ready=True,
                health=True,
                details={"connection": "ok", "queues": "ok"},
            )

        assert await app.healthcheck() == HealthStatus(ready=False, health=None)
