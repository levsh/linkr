from __future__ import annotations

from abc import ABC, abstractmethod

from linkr.models import RpcRequest, RpcResponse


class Serializer(ABC):
    """Abstract serializer for RPC messages."""

    @abstractmethod
    def dumps_request(self, message: RpcRequest) -> bytes:
        """Serialize RpcRequest to bytes."""

    @abstractmethod
    def loads_request(self, data: bytes) -> RpcRequest:
        """Deserialize bytes to RpcRequest."""

    @abstractmethod
    def dumps_response(self, message: RpcResponse) -> bytes:
        """Serialize RpcResponse to bytes."""

    @abstractmethod
    def loads_response(self, data: bytes) -> RpcResponse:
        """Deserialize bytes to RpcResponse."""


class JsonSerializer(Serializer):
    """JSON serializer for RPC messages using Pydantic model_dump_json / model_validate_json."""

    def dumps_request(self, message: RpcRequest) -> bytes:
        return message.model_dump_json().encode()

    def loads_request(self, data: bytes) -> RpcRequest:
        return RpcRequest.model_validate_json(data)

    def dumps_response(self, message: RpcResponse) -> bytes:
        return message.model_dump_json().encode()

    def loads_response(self, data: bytes) -> RpcResponse:
        return RpcResponse.model_validate_json(data)
