from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class RpcRequest(BaseModel):
    """
    RPC request message.

    Attributes:
        id: Unique request identifier.
        headers: Arbitrary metadata.
        data: Payload, typically a dict with method, args, kwds.
    """

    id: UUID = Field(default_factory=uuid4)
    headers: dict[str, Any] = Field(default_factory=dict)
    data: dict[str, Any] | None = None


class RpcResponse(BaseModel):
    """
    RPC response message.

    Attributes:
        id: Mirrors the corresponding request id.
        headers: Arbitrary metadata.
        data: Result payload or error fields (error_code, error_message, error_details).
    """

    id: UUID
    headers: dict[str, Any] = Field(default_factory=dict)
    data: dict[str, Any] | None = None


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
