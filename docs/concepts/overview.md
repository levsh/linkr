# Overview

`linkr` centers around an **application** (`App`) that registers handlers, runs a consume loop, and sends messages through a **backend**. Messages are serialized, optionally transformed by middleware, and dispatched according to a delivery mode.

## Core building blocks

| Component | Role |
|---|---|
| `App` | Registers handlers, manages lifecycle/consume, builds requests, runs client/server pipelines |
| `Backend` | Transport abstraction (`request`/`send`/`emit`, consume/stop operations) |
| Serializer | Converts `Request`/`Response` to/from wire `Message` bytes |
| Middleware | App-level (objects) and wire-level (bytes/headers) transformations around the pipeline |
| Hooks | Observer notifications for lifecycle and message processing events |
| Models | `Request`, `Response`, `Message`, `ErrorInfo`, `HandlerInfo` |

## Message flow

### Client side

1. Build a `Request` (`rpc`/`task`/`event`) with id, payload, and headers (queue, timeout/ttl/rttl as applicable).
2. Run **app middleware** (client).
3. Serialize to a wire `Message`.
4. Run **wire middleware** (client).
5. Deliver via backend:
   - RPC: `await backend.request(queue, message)` returns a `Future`; await it for the response.
   - Task: `await backend.send(queue, message)`.
   - Event: `await backend.emit(topic, message)`.
6. Deserialize/interpret response (RPC) and return result or raise `RPCError`.

### Server side

1. Backend delivers a raw `Message` to the app handler for a queue/mode.
2. Detect/select serializer and run **wire middleware** (server).
3. Deserialize to `Request` and run **app middleware** (server).
4. Dispatch by request type:
   - RPC: resolve handler, validate args, run handler, return `Response(result|error)`.
   - Task: resolve handler, validate/run; no response to caller.
   - Event: find matching subscribers, validate/run each; no response.
5. For RPC, serialize `Response` (respecting `rttl` header) and return it to backend.

## Delivery modes at a glance

| Mode | Registration | Client | Consumers | Response |
|---|---|---|---|---|
| RPC | `@app.method` | `app.call` | One (competing) | Yes |
| Task | `@app.task` | `app.send` | One (competing) | No |
| Event | `@app.subscribe` | `app.emit` | All matching subscribers (fan-out) | No |

See [Delivery modes](delivery-modes.md) and [Routing and naming](routing-and-naming.md) for details.

## Public API surface

Common exports from `linkr`:

- Core: `App`, `get_current_request`, `Depends`
- Backends/models: `Backend`, `LocalBackend`, `Mode`, `Message`, `Request`, `Response`, `RequestType`, `ResponseType`, `HandlerInfo`
- Middleware: `BaseMiddleware`, `AppMiddleware`, `WireMiddleware`
- Serialization: `Serializer`, `JsonSerializer`, `JsonRpcSerializer`
- Errors: `ErrorCode`, `RPCError`, `ErrorInfo`
- Hooks: `AppHooks`, `HookRegistry`, `HookPoint` and event types

!!! note
    `RmqBackend` is provided in `linkr.backends.rmq` and requires the `rabbitmq` extra (`aio-pika`).
