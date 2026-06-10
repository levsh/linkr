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


class LoggingMiddleware(AppMiddleware):
    async def dispatch(self, ctx, call_next):
        if ctx.direction == "request" and ctx.role == "server":
            logging.info("[%s] Calling", ctx.request.id)
        result = await call_next(ctx)
        if ctx.direction == "response" and ctx.role == "server" and ctx.response:
            logging.info("[%s] Done", ctx.request.id)
        return result


app.add_middleware(LoggingMiddleware())
```

## Wire-level Middleware

Compression, encryption and other wire transformations use `WireMiddleware`:

```python
from linkr import GzipMiddleware

app.add_middleware(GzipMiddleware())
```

Custom wire-level middleware inherits from `WireMiddleware` and works with `ctx.body`:

```python
import gzip
from linkr import WireMiddleware
from linkr.models import RpcContext


class CustomCompression(WireMiddleware):
    async def dispatch(self, ctx: RpcContext, call_next):
        if ctx.direction == "request" and ctx.role == "client":
            ctx.body = gzip.compress(ctx.body)
        elif ctx.direction == "request" and ctx.role == "server":
            ctx.body = gzip.decompress(ctx.body)

        ctx = await call_next(ctx)

        if ctx.direction == "response" and ctx.role == "server":
            ctx.body = gzip.compress(ctx.body)
        elif ctx.direction == "response" and ctx.role == "client":
            ctx.body = gzip.decompress(ctx.body)

        return ctx
```

## Dependency Injection

```python
from linkr import Depends


class Database:
    ...


app.dependencies.add_singleton(Database, lambda: Database("postgres://..."))

@app.method("ping")
def ping(db: Depends[Database]) -> str:
    return db.url
```

## Transports

| Transport     | When to use                |
|---------------|----------------------------|
| MockTransport | Unit tests, local dev      |
| RmqTransport  | Production (RabbitMQ)      |

## License

MIT
