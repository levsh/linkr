# linkr

Async RPC framework.

## Install

```
pip install linkr
```

## Quickstart

```python
from linkr import MockTransport, RpcApp

transport = MockTransport()
app = RpcApp(transport)

@app.method("add")
def add(x: int, y: int) -> int:
    return x + y

await app.init()
await app.consume()

result = await app.make("add", 2, 3).call()
print(result)  # 5

await app.close()
```

## Features

- Decorator-based handler registration
- Timeout, TTL, RTTL per call
- Fire-and-forget via publish()
- App-level middleware (`AppMiddleware`)
- Wire-level middleware (`WireMiddleware`) — compression, encryption
- Gzip compression via `GzipMiddleware`
- Dependency injection with `Depends[T]`
- Pydantic serialization
- JSON-RPC 2.0 support via `JsonRpcSerializer`
- Multi-serializer with auto-detection
- Mock transport for testing (no broker needed)
- RabbitMQ transport

## App-level Middleware

```python
import logging

from typing import Any

from linkr import AppMiddleware
from linkr.models import RpcRequest, RpcResponse


class LoggingMiddleware(AppMiddleware):
    async def dispatch_client(
        self,
        call_next,
        request: RpcRequest,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RpcResponse | None:
        logging.info("[%s] Calling %s", request.id, request.method)
        response = await call_next()
        if response:
            logging.info("[%s] Done", request.id)
        return response

    async def dispatch_server(
        self,
        call_next,
        request: RpcRequest,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RpcResponse | None:
        logging.info("[%s] Calling %s", request.id, request.method)
        response = await call_next()
        if response:
            logging.info("[%s] Done", request.id)
        return response


app.add_middleware(LoggingMiddleware())
```

## Wire-level Middleware

Compression, encryption and other wire transformations use `WireMiddleware`:

```python
from linkr import GzipMiddleware

app.add_middleware(GzipMiddleware())
```

Custom wire-level middleware inherits from `WireMiddleware` and works with raw bytes and wire headers:

```python
import gzip

from typing import Any

from linkr import WireMiddleware
from linkr.models import RawMessage, RpcRequest, RpcResponse


class CustomCompression(WireMiddleware):
    async def dispatch_client(
        self,
        call_next,
        request_raw_message: RawMessage,
        request: RpcRequest,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> RawMessage | None:
        if len(request_raw_message.data) >= 1024:
            request_raw_message.data = gzip.compress(request_raw_message.data)
        raw_response = await call_next()
        if raw_response and raw_response.headers.get("content_encoding") == "gzip":
            raw_response.data = gzip.decompress(raw_response.data)
        return raw_response

    async def dispatch_server(
        self,
        call_next,
        request_raw_message: RawMessage,
        *,
        kwds: dict[str, Any] | None = None,
    ) -> tuple[RawMessage, RpcResponse] | tuple[None, None]:
        if request_raw_message.headers.get("content_encoding") == "gzip":
            request_raw_message.data = gzip.decompress(request_raw_message.data)
        result = await call_next()
        if result is None or result[0] is None:
            return None, None
        raw_response, response = result
        if len(raw_response.data) >= 1024:
            raw_response.data = gzip.compress(raw_response.data)
        return raw_response, response
```

## Dependency Injection

```python
from linkr import Depends, MockTransport, RpcApp


class Database:
    def __init__(self, url: str) -> None:
        self.url = url


transport = MockTransport()
async with RpcApp(transport) as app:
    app.dependencies.add_singleton(Database, lambda: Database("postgres://..."))

    @app.method("ping")
    def ping(db: Depends[Database]) -> str:
        return db.url

    await app.consume()
    result = await app.make("ping").call()
    print(result)  # postgres://...
```

## Error Handling

Type validation is enabled via ``validate_types=True``:

```python
from linkr import MockTransport, RpcApp, RpcError

transport = MockTransport()
async with RpcApp(transport) as app:
    @app.method("add", validate_types=True)
    def add(x: int, y: int) -> int:
        return x + y

    await app.consume()
    try:
        await app.make("add", x="not", y=3).call()
    except RpcError as e:
        print(e.error_code)     # ValidationError
        print(e.error_message)  # x: Input should be a valid integer
```

## Publish (Fire-and-Forget)

Send a message without waiting for a response:

```python
from linkr import MockTransport, RpcApp

transport = MockTransport()
async with RpcApp(transport) as app:
    req = app.make("event", text="hello")
    await app.publish(req.request)
```

## Transports

| Transport             | When to use                |
|-----------------------|----------------------------|
| MockTransport         | Unit tests, local dev      |
| RmqTransport          | Production (RabbitMQ)      |
| ThreadSafeRmqTransport| Cross-event-loop usage     |

## License

MIT
