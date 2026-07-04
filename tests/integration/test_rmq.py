import asyncio

import pytest
from linkr.app import RpcApp
from linkr.exceptions import ErrorCode, RpcError
from linkr.serializer import JsonRpcSerializer, JsonSerializer
from linkr.transports.rmq import RmqTransport, ThreadSafeRmqTransport


@pytest.mark.integration
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
            assert exc_info.value.error_code == ErrorCode.TIMEOUT
        finally:
            await rpc.close()


@pytest.mark.integration
class TestThreadSafeRMQ:
    async def test_request_from_owner_loop(self, rabbitmq):
        transport = ThreadSafeRmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await rpc.init()
        try:
            await rpc.consume()
            result = await rpc.make("ping").call()
            assert result == "pong"
        finally:
            await rpc.close()

    async def test_request_from_different_loop(self, rabbitmq):
        transport = ThreadSafeRmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("add")
        def add(x: int, y: int) -> int:
            return x + y

        await rpc.init()
        try:
            await rpc.consume()

            def make_call():
                return asyncio.run(rpc.make("add", 2, 3).call())

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(None, make_call)
            assert result == 5
        finally:
            await rpc.close()

    async def test_publish_from_different_loop(self, rabbitmq):
        transport = ThreadSafeRmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await rpc.init()
        try:
            await rpc.consume()

            def do_publish():
                async def publish():
                    req = rpc.make("ping")
                    await rpc.publish(req.request)

                asyncio.run(publish())

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, do_publish)
        finally:
            await rpc.close()

    async def test_nested_rpc_call_from_handler(self, rabbitmq):
        transport = ThreadSafeRmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("inner")
        def inner(x: int) -> int:
            return x * 2

        @rpc.method("outer")
        async def outer(value: int) -> int:
            result = await asyncio.to_thread(
                lambda: asyncio.run(rpc.make("inner", value).call()),
            )
            return result

        await rpc.init()
        try:
            await rpc.consume()

            result = await rpc.make("outer", 21).call()
            assert result == 42
        finally:
            await rpc.close()

    async def test_concurrent_requests_from_multiple_loops(self, rabbitmq):
        transport = ThreadSafeRmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport)

        @rpc.method("echo")
        def echo(value: str) -> str:
            return value

        await rpc.init()
        try:
            await rpc.consume()

            async def call_in_thread(value: str) -> str:
                return await asyncio.to_thread(
                    lambda: asyncio.run(rpc.make("echo", value=value).call()),
                )

            results = await asyncio.gather(
                *(call_in_thread(f"msg-{i}") for i in range(5)),
            )

            assert sorted(results) == [f"msg-{i}" for i in range(5)]
        finally:
            await rpc.close()


@pytest.mark.integration
class TestJsonRpcOverRMQ:
    async def test_positional_args(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport, serializer=JsonRpcSerializer())

        @rpc.method("add")
        def add(a, b):
            return a + b

        await rpc.init()
        try:
            await rpc.consume()
            result = await rpc.make("add", 2, 3).call()
            assert result == 5
        finally:
            await rpc.close()

    async def test_keyword_args(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport, serializer=JsonRpcSerializer())

        @rpc.method("greet")
        def greet(name: str) -> str:
            return f"Hello, {name}!"

        await rpc.init()
        try:
            await rpc.consume()
            result = await rpc.make("greet", name="World").call()
            assert result == "Hello, World!"
        finally:
            await rpc.close()

    async def test_error_method_not_found(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport, serializer=JsonRpcSerializer())

        await rpc.init()
        try:
            await rpc.consume()
            with pytest.raises(RpcError) as exc_info:
                await rpc.make("nonexistent").call()
            assert exc_info.value.error_code == ErrorCode.METHOD_NOT_FOUND
        finally:
            await rpc.close()

    async def test_error_handler_raises(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport, serializer=JsonRpcSerializer())

        @rpc.method("fail")
        def fail() -> str:
            msg = "something went wrong"
            raise ValueError(msg)

        await rpc.init()
        try:
            await rpc.consume()
            with pytest.raises(RpcError) as exc_info:
                await rpc.make("fail").call()
            assert exc_info.value.error_code == ErrorCode.INTERNAL_ERROR
        finally:
            await rpc.close()

    async def test_auto_detect(self, rabbitmq):
        transport = RmqTransport(f"amqp://{rabbitmq['ip']}")
        rpc = RpcApp(transport, serializer=[JsonRpcSerializer(), JsonSerializer()])

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        await rpc.init()
        try:
            await rpc.consume()
            result = await rpc.make("ping").call(serializer="jsonrpc2.0")
            assert result == "pong"
        finally:
            await rpc.close()
