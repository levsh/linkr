from __future__ import annotations

from abc import ABC, abstractmethod

from linkr.models import RpcRequest, RpcResponse


class Serializer(ABC):
    """Abstract serializer for RPC messages."""

    @abstractmethod
    def dumps_request(self, request: RpcRequest) -> tuple[bytes, dict[str, str]]:
        """Serialize RpcRequest to bytes. Returns (data, wire_headers)."""

    @abstractmethod
    def loads_request(self, data: bytes, wire_headers: dict[str, str]) -> RpcRequest:
        """Deserialize bytes to RpcRequest."""

    @abstractmethod
    def dumps_response(self, response: RpcResponse) -> tuple[bytes, dict[str, str]]:
        """Serialize RpcResponse to bytes. Returns (data, wire_headers)."""

    @abstractmethod
    def loads_response(self, data: bytes, wire_headers: dict[str, str]) -> RpcResponse:
        """Deserialize bytes to RpcResponse."""


class JsonSerializer(Serializer):
    """JSON serializer for RPC messages using Pydantic model_dump_json / model_validate_json."""

    def dumps_request(self, request: RpcRequest) -> tuple[bytes, dict[str, str]]:
        return (request.model_dump_json().encode(), {"content_type": "application/json"})

    def loads_request(self, data: bytes, wire_headers: dict[str, str]) -> RpcRequest:
        return RpcRequest.model_validate_json(data)

    def dumps_response(self, response: RpcResponse) -> tuple[bytes, dict[str, str]]:
        return (response.model_dump_json().encode(), {"content_type": "application/json"})

    def loads_response(self, data: bytes, wire_headers: dict[str, str]) -> RpcResponse:
        return RpcResponse.model_validate_json(data)
