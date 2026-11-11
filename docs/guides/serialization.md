# Serialization

Serializers convert `Request`/`Response` objects to and from wire `Message` payloads (bytes + headers). `App` handles serialization on client and server pipelines.

## Built-in serializers

### `JsonSerializer` (`linkr1.0`)

Default serializer for all delivery modes.

```python
from linkr import App, JsonSerializer, LocalBackend

app = App("orders", backend=LocalBackend(), serializer=JsonSerializer())
```

- `name`: `linkr1.0`
- Sets wire headers like `content_type: application/json` and `serializer: linkr1.0`
- Supports RPC, tasks, and events

### `JsonRpcSerializer` (`jsonrpc2.0`)

JSON-RPC 2.0 encoding for RPC.

```python
from linkr import App, JsonRpcSerializer, LocalBackend

app = App("orders", backend=LocalBackend(), serializer=JsonRpcSerializer())
```

- `name`: `jsonrpc2.0`
- **RPC only** — task/event requests are not supported by this serializer
- Maps error codes via `ERROR_CODE_MAP` (subclass to extend):

```python
class MyJsonRpcSerializer(JsonRpcSerializer):
    ERROR_CODE_MAP = {
        **JsonRpcSerializer.ERROR_CODE_MAP,
        "Unauthorized": -32001,
    }
```

## Multiple serializers and auto-detection

You can provide a list of serializers:

```python
from linkr import App, JsonRpcSerializer, JsonSerializer, LocalBackend

app = App(
    "orders",
    backend=LocalBackend(),
    serializer=[JsonRpcSerializer(), JsonSerializer()],
)
```

- The **first** serializer is the default for outgoing messages.
- The server can **auto-detect** the serializer for incoming messages from the configured set.
- Select a specific serializer per call with `serializer="<name>"`:

```python
await app.call("orders.ping", (), {}, serializer="jsonrpc2.0")
```

!!! warning "Mode compatibility"
    When using JSON-RPC in a mixed configuration, ensure tasks and events are serialized with a compatible serializer (e.g. `JsonSerializer`).

## Custom serializers

Implement the `Serializer` interface:

- `name: str`
- `dumps_request(request: Request) -> Message`
- `loads_request(message: Message) -> Request`
- `dumps_response(response: Response) -> Message`
- `loads_response(message: Message) -> Response`

Wire-level concerns such as content type or encoding metadata belong in `Message.headers`.
