from __future__ import annotations

import asyncio

import pytest

from linkr.app import App
from linkr.transports.local import LocalTransport


class TestLocalTransport:
    @pytest.fixture(autouse=True)
    def reset_transport(self):
        LocalTransport.reset()
        yield
        LocalTransport.reset()

    async def test_local_app_call_and_publish(self):
        transport1 = LocalTransport()
        transport2 = LocalTransport()

        app1 = App(transport=transport1)
        app2 = App(transport=transport2)

        @app1.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        published_messages = []

        @app1.method("notify")
        def notify(msg: str) -> None:
            published_messages.append(msg)

        async with app1, app2:
            await app1.consume()

            # Test RPC call from app2 using app1's transport
            result = await app2.make("add", 2, 3).invoke()
            assert result == 5

            # Test publish from app2 using app1's transport
            await app2.publish(app2.make("notify", msg="hello"))
            assert published_messages == ["hello"]

    async def test_local_routing_queue(self):
        transport = LocalTransport()
        app = App(transport=transport)

        @app.method("service/ping")
        def ping() -> str:
            return "pong-service"

        async with app:
            await app.consume()
            result = await app.make("service/ping").invoke()
            assert result == "pong-service"

    async def test_local_pub_sub(self):
        transport1 = LocalTransport()
        transport2 = LocalTransport()
        transport3 = LocalTransport()

        app1 = App(transport=transport1)
        app2 = App(transport=transport2)
        app3 = App(transport=transport3)

        events1 = []
        events2 = []

        @app1.subscribe("user.created")
        def on_user_created1(user_id: int):
            events1.append(user_id)

        @app2.subscribe("user.created")
        def on_user_created2(user_id: int):
            events2.append(user_id)

        async with app1, app2, app3:
            await app1.consume()
            await app2.consume()

            await app3.publish(app3.make("user.created", user_id=42))

            await asyncio.sleep(0.05)

            assert events1 == [42]
            assert events2 == [42]
