from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class ErrorInfo(BaseModel):
    error_code: str = ""
    error_message: str = ""
    error_details: dict[str, Any] | None = None


class RpcRequest(BaseModel):
    """
    RPC request message.

    Attributes:
        id: Unique request identifier.
        method: Method name to call.
        args: Positional arguments for the handler.
        kwds: Keyword arguments for the handler.
        headers: Arbitrary metadata.
    """

    id: UUID = Field(default_factory=uuid4)
    method: str
    args: tuple[Any, ...]
    kwds: dict[str, Any]
    headers: dict[str, Any] = Field(default_factory=dict)


class RpcResponse(BaseModel):
    """
    RPC response message.

    Attributes:
        id: Mirrors the corresponding request id.
        data: Result payload or error fields (error_code, error_message, error_details).
        headers: Arbitrary metadata.
    """

    id: UUID
    type: Literal["result", "error"]
    data: Any
    headers: dict[str, Any] = Field(default_factory=dict)


@dataclass
class HandlerInfo:
    """
    Metadata for a registered RPC method handler.

    Attributes:
        name: Method name as registered via @app.method().
        func: Callable implementing the handler.
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
class RawMessage:
    data: bytes
    headers: dict[str, Any]
