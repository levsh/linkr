from __future__ import annotations

from abc import ABC, abstractmethod

from linkr.models import RpcRequest, RpcResponse


class Serializer(ABC):
    """
    Abstract serializer for RPC messages.

    Implementations convert :class:`RpcRequest` and :class:`RpcResponse`
    objects to and from bytes for transmission over the wire.
    Each serialize method returns a ``(data, wire_headers)`` tuple so that
    wire-level metadata (e.g. content type, encoding) can be carried alongside
    the payload.
    """

    @abstractmethod
    def dumps_request(self, request: RpcRequest) -> tuple[bytes, dict[str, str]]:
        """
        Serialize an RPC request to bytes.

        Args:
            request: The request to serialize.

        Returns:
            A ``(data, wire_headers)`` tuple where *data* is the encoded
            payload and *wire_headers* contains wire-level metadata.
        """

    @abstractmethod
    def loads_request(self, data: bytes, wire_headers: dict[str, str]) -> RpcRequest:
        """
        Deserialize bytes into an RPC request.

        Args:
            data: Raw bytes to deserialize.
            wire_headers: Wire-level headers that may influence deserialization.

        Returns:
            The deserialized RpcRequest.
        """

    @abstractmethod
    def dumps_response(self, response: RpcResponse) -> tuple[bytes, dict[str, str]]:
        """
        Serialize an RPC response to bytes.

        Args:
            response: The response to serialize.

        Returns:
            A ``(data, wire_headers)`` tuple.
        """

    @abstractmethod
    def loads_response(self, data: bytes, wire_headers: dict[str, str]) -> RpcResponse:
        """
        Deserialize bytes into an RPC response.

        Args:
            data: Raw bytes to deserialize.
            wire_headers: Wire-level headers that may influence deserialization.

        Returns:
            The deserialized RpcResponse.
        """


class JsonSerializer(Serializer):
    """
    JSON serializer for RPC messages.

    Uses Pydantic's ``model_dump_json()`` and ``model_validate_json()``
    under the hood. Sets ``content_type: application/json`` on every
    serialized message.
    """

    def dumps_request(self, request: RpcRequest) -> tuple[bytes, dict[str, str]]:
        """
        Serialize request as JSON.

        Args:
            request: The request to serialize.

        Returns:
            A ``(utf-8 encoded bytes, content_type header)`` tuple.

        Raises:
            ValidationError: If the request data is invalid.
        """
        return (request.model_dump_json().encode(), {"content_type": "application/json"})

    def loads_request(self, data: bytes, wire_headers: dict[str, str]) -> RpcRequest:
        """
        Deserialize JSON bytes into an RpcRequest.

        Args:
            data: UTF-8 encoded JSON bytes.
            wire_headers: Wire-level headers (currently unused by this implementation).

        Returns:
            The deserialized RpcRequest.

        Raises:
            ValidationError: If the payload is not valid JSON or does not
                match the RpcRequest schema.
        """
        return RpcRequest.model_validate_json(data)

    def dumps_response(self, response: RpcResponse) -> tuple[bytes, dict[str, str]]:
        """
        Serialize response as JSON.

        Args:
            response: The response to serialize.

        Returns:
            A ``(utf-8 encoded bytes, content_type header)`` tuple.

        Raises:
            ValidationError: If the response data is invalid.
        """
        return (response.model_dump_json().encode(), {"content_type": "application/json"})

    def loads_response(self, data: bytes, wire_headers: dict[str, str]) -> RpcResponse:
        """
        Deserialize JSON bytes into an RpcResponse.

        Args:
            data: UTF-8 encoded JSON bytes.
            wire_headers: Wire-level headers (currently unused by this implementation).

        Returns:
            The deserialized RpcResponse.

        Raises:
            ValidationError: If the payload is not valid JSON or does not
                match the RpcResponse schema.
        """
        return RpcResponse.model_validate_json(data)
