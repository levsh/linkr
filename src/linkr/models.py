from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

RequestType = Literal["rpc", "task", "event"]
ResponseType = Literal["result", "error"]


class ErrorInfo(BaseModel):
    """Structured error information returned with error responses."""

    error_code: str = ""
    error_message: str = ""
    error_details: dict[str, Any] | None = None


class Request(BaseModel):
    """
    Incoming request message.

    Carries the request id, delivery mode, payload data and headers.
    """

    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    type: RequestType
    data: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, Any] = Field(default_factory=dict)


class Response(BaseModel):
    """
    Outgoing response message.

    Carries the response id, type (result/error), payload data and headers.
    """

    model_config = ConfigDict(frozen=True)

    id: UUID
    type: ResponseType
    data: Any
    headers: dict[str, Any] = Field(default_factory=dict)


@dataclass
class Message:
    """Raw wire message with byte payload and headers."""

    data: bytes
    headers: dict[str, Any] = field(default_factory=dict)


@dataclass
class HandlerInfo:
    """Metadata about a registered handler."""

    name: str
    request_type: RequestType
    fn: Callable[..., Any]
    signature: str
    options: dict[str, Any] = field(default_factory=dict)
    dep_types: dict[str, tuple[type, Callable[[], Any] | None]] = field(default_factory=dict)
    queue: str | None = None
    validate_args: bool = False
    validation_adapters: dict[str, Any] = field(default_factory=dict)
    validation_param_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class HandlerContext:
    """Read-only handler metadata exposed to handler middleware."""

    name: str
    request_type: RequestType
    options: Mapping[str, Any]
    queue: str | None

    def __post_init__(self) -> None:
        options = self.options
        if not isinstance(options, MappingProxyType):
            options = MappingProxyType(options)
        object.__setattr__(self, "options", options)


@dataclass(frozen=True)
class HealthStatus:
    """Readiness and health status."""

    ready: bool
    health: bool | None
    details: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.details is None:
            return
        details = dict(self.details)
        queues = details.get("queues")
        if isinstance(queues, (list, tuple)):
            normalized_queues = tuple(
                MappingProxyType(dict(item)) if isinstance(item, Mapping) else item for item in queues
            )
            details["queues"] = normalized_queues
        object.__setattr__(self, "details", MappingProxyType(details))


class QueueOptions:
    """Marker base class for backend-specific queue/consumer options.

    Concrete backends define their own option types (for example
    ``RmqQueueOptions`` in ``linkr.backends.rmq``). ``App`` stores and passes
    options without interpreting them; the active backend selects options it
    understands and ignores the rest.
    """
