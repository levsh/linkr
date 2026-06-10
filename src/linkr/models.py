from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from linkr.app import RpcApp

RpcData = dict[str, Any]
RpcHeaders = dict[str, Any]


class RpcRequest(BaseModel):
    """
    RPC request message.

    Attributes:
        id: Unique request identifier.
        headers: Arbitrary metadata (routing_key, ttl, timeout, rttl).
        data: Payload, typically a dict with method, args, kwds.
    """

    id: UUID = Field(default_factory=uuid4)
    headers: RpcHeaders = Field(default_factory=dict)
    data: RpcData | None = None


class RpcResponse(BaseModel):
    """
    RPC response message.

    Attributes:
        id: Mirrors the corresponding request id.
        headers: Arbitrary metadata (rttl, etc.).
        data: Result payload or error fields (error_code, error_message, error_details).
    """

    id: UUID
    headers: RpcHeaders = Field(default_factory=dict)
    data: RpcData | None = None


@dataclass
class HandlerInfo:
    """
    Metadata for a registered RPC method handler.

    Attributes:
        name: Method name as registered via @app.method().
        fn: Callable implementing the handler.
        signature: String representation of the handler signature.
        options: Arbitrary keyword options passed to @app.method().
        dep_types: Mapping of parameter name to dependency type resolved
            from Depends[T] annotations.
    """

    name: str
    fn: Callable[..., Any]
    signature: str
    options: dict[str, Any] = field(default_factory=dict)
    dep_types: dict[str, type] = field(default_factory=dict)


@dataclass
class RpcContext:
    """
    Mutable context passed through the middleware chain.

    Attributes:
        app: The RpcApp instance.
        direction: Whether this is a request or response phase.
        role: Whether the current side is client or server.
        request: The incoming/outgoing RpcRequest.
        response: Optional RpcResponse set by middleware.
        body: Raw message bytes, populated by the transport.
        wire_headers: Wire-level metadata (e.g. content_encoding).
        state: Arbitrary key-value store shared across middleware.
    """

    app: RpcApp | Any
    direction: Literal["request", "response"]
    role: Literal["client", "server"]
    request: RpcRequest
    response: RpcResponse | None = None
    body: bytes = b""
    wire_headers: dict[str, str] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)
