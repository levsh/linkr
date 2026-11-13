# Middleware

Middleware wraps client and/or server pipelines using an onion (`call_next`) pattern. `App` supports two layers: **app-level** (deserialized objects) and **wire-level** (raw bytes/headers).

## Adding middleware

```python
from linkr import App, LocalBackend
from linkr.middleware.gzip import GzipMiddleware

app = App("orders", backend=LocalBackend())
app.add_middleware(GzipMiddleware())
app.add_middleware(MyAppMiddleware())
```

- `add_middleware` accepts `AppMiddleware` or `WireMiddleware`.
- Execution follows registration order (outermost to innermost on the way in, reverse on the way out).

## App-level middleware

Subclass `AppMiddleware` to work with `Request`/`Response` objects.

```python
from collections.abc import Awaitable, Callable
from typing import Any

from linkr import AppMiddleware
from linkr.models import Request, Response


class LoggingMiddleware(AppMiddleware):
    async def dispatch_client(
        self,
        call_next: Callable[[], Awaitable[Response | None]],
        request: Request,
        *,
        context: dict[str, Any],
    ) -> Response | None:
        # before send
        response = await call_next()
        # after response (RPC) / completion
        return response

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[Response | None]],
        request: Request,
    ) -> Response | None:
        # before dispatch
        response = await call_next()
        # after dispatch
        return response
```

- `dispatch_client`: wraps the client pipeline around backend call; `context` receives call context.
  - `dispatch_server`: wraps server-side dispatch for the request (only `Request` is available at this layer).
  - For task/event flows there is no response returned to a remote caller.

## Wire-level middleware

Subclass `WireMiddleware` to transform serialized `Message.data`/`Message.headers` (e.g. compression, encryption).

```python
from collections.abc import Awaitable, Callable
from typing import Any

from linkr import WireMiddleware
from linkr.models import Message, Request, Response


class CustomWireMiddleware(WireMiddleware):
    async def dispatch_client(
        self,
        call_next: Callable[[], Awaitable[Message | None]],
        request: Request,
        request_message: Message,
        *,
        context: dict[str, Any],
    ) -> Message | None:
        # mutate request_message before send
        response_message = await call_next()
        # mutate response_message before deserialize
        return response_message

    async def dispatch_server(
        self,
        call_next: Callable[[], Awaitable[tuple[Message, Response] | tuple[None, None]]],
        request_message: Message,
    ) -> tuple[Message, Response] | tuple[None, None]:
        # mutate incoming request_message
        result = await call_next()
        if result is None or result[0] is None:
            return None, None
        response_message, response = result
        # mutate outgoing response_message
        return response_message, response
```

Mutate `request_message`/`response_message` in place to affect wire data and headers.

## Built-in: `GzipMiddleware`

```python
from linkr.middleware.gzip import GzipMiddleware

app.add_middleware(GzipMiddleware(min_size=1024))
```

- Compresses request/response payloads when size is at least `min_size` (default 1024 bytes).
- Signals compression via the `content_encoding` wire header (`gzip`) and decompresses incoming compressed payloads.

## Lifecycle hooks

`BaseMiddleware` provides optional `init()` and `close()` methods called during `App.init()`/`App.close()` for resource setup/cleanup.

```python
from linkr import BaseMiddleware

class MyMiddleware(BaseMiddleware):
    async def init(self) -> None:
        ...

    async def close(self) -> None:
        ...
```
