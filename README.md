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
- Mock transport for testing (no broker needed)
- RabbitMQ transport

## App-level Middleware

```python
import logging

from linkr import AppMiddleware
from linkr.models import RpcRequest, RpcResponse


class LoggingMiddleware(AppMiddleware):
    async def process_request(self, request: RpcRequest) -> RpcRequest:
        if request.data:
            logging.info("[%s] Calling %s", request.id, request.data.get("method"))
        return request

    async def process_response(self, request: RpcRequest, response: RpcResponse) -> RpcResponse:
        if response.data:
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

from linkr import WireMiddleware
from linkr.models import RpcRequest, RpcResponse


class CustomCompression(WireMiddleware):
    async def send(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
        response: RpcResponse | None = None,
    ) -> tuple[bytes, dict[str, str]]:
        if len(data) >= 1024:
            data = gzip.compress(data)
            existing = headers.get("content_encoding", "")
            headers["content_encoding"] = f"{existing},gzip" if existing else "gzip"
        return data, headers

    async def receive(
        self,
        data: bytes,
        headers: dict[str, str],
        request: RpcRequest,
    ) -> tuple[bytes, dict[str, str]]:
        if "gzip" in headers.get("content_encoding", ""):
            data = gzip.decompress(data)
        return data, headers
```

## Dependency Injection

```python
from linkr import Depends, MockTransport, RpcApp


class Database:
    ...


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
