import asyncio

import pytest
from linkr.app import RpcApp
from linkr.exceptions import RpcError
from linkr.transports.rmq import RmqTransport


class TestRMQ:
    async def test_base(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("add")
        def add(a, b):
            return a + b

        await rpc.init()
        try:
            await rpc.consume()

            result = await rpc.make("add", 1, 2).call()

            assert result == 3
        finally:
            await rpc.close()

    async def test_path(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("service/add")
        def add(a, b):
            return a + b

        await rpc.init()
        try:
            await rpc.consume()

            result = await rpc.make("service/add", 1, 2).call()

            assert result == 3
        finally:
            await rpc.close()

    async def test_timeout(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("service/add")
        async def add(a, b):
            await asyncio.sleep(5)
            return a + b

        await rpc.init()
        try:
            await rpc.consume()

            with pytest.raises(RpcError) as exc_info:
                await rpc.make("service/add", 1, 2).call(timeout=1)
            assert exc_info.value.error_code == "Timeout"
        finally:
            await rpc.close()
