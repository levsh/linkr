from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, ClassVar
from uuid import UUID, uuid4

from .models import ErrorInfo, RawMessage, RpcRequest, RpcResponse


class Serializer(ABC):
    """
    Abstract serializer for RPC messages.

    Implementations convert :class:`RpcRequest` and :class:`RpcResponse`
    objects to and from :class:`RawMessage` for transmission over the wire.
    Wire-level metadata (e.g. content type, encoding) is carried in
    ``RawMessage.headers``.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable format name (e.g. ``"linkr1.0"``, ``"jsonrpc2.0"``)."""

    @abstractmethod
    def dumps_request(self, request: RpcRequest) -> RawMessage:
        """
        Serialize an RPC request.

        Args:
            request: The request to serialize.

        Returns:
            A :class:`RawMessage` with the encoded payload and wire-level
            metadata in its ``headers`` dict.
        """

    @abstractmethod
    def loads_request(self, raw: RawMessage) -> RpcRequest:
        """
        Deserialize a :class:`RawMessage` into an RPC request.

        Args:
            raw: The raw message to deserialize.

        Returns:
            The deserialized RpcRequest.
        """

    @abstractmethod
    def dumps_response(self, response: RpcResponse) -> RawMessage:
        """
        Serialize an RPC response.

        Args:
            response: The response to serialize.

        Returns:
            A :class:`RawMessage`.
        """

    @abstractmethod
    def loads_response(self, raw: RawMessage) -> RpcResponse:
        """
        Deserialize a :class:`RawMessage` into an RPC response.

        Args:
            raw: The raw message to deserialize.

        Returns:
            The deserialized RpcResponse.
        """


class JsonSerializer(Serializer):
    """
    JSON serializer for RPC messages.

    Uses Pydantic's ``model_dump_json()`` and ``model_validate_json()``
    under the hood. Sets ``content_type: application/json`` and
    ``serializer: linkr1.0`` on every serialized message.
    """

    @property
    def name(self) -> str:
        return "linkr1.0"

    def dumps_request(self, request: RpcRequest) -> RawMessage:
        data = request.model_dump_json().encode()
        headers = {"content_type": "application/json", "serializer": self.name}
        return RawMessage(data=data, headers=headers)

    def loads_request(self, raw: RawMessage) -> RpcRequest:
        return RpcRequest.model_validate_json(raw.data)

    def dumps_response(self, response: RpcResponse) -> RawMessage:
        data = response.model_dump_json().encode()
        headers = {"content_type": "application/json", "serializer": self.name}
        return RawMessage(data=data, headers=headers)

    def loads_response(self, raw: RawMessage) -> RpcResponse:
        return RpcResponse.model_validate_json(raw.data)


class JsonRpcSerializer(Serializer):
    """
    JSON-RPC 2.0 serializer for RPC messages.

    Serializes to and from the JSON-RPC 2.0 specification.  On output
    ``RawMessage.headers`` always carries ``serializer: jsonrpc2.0`` so that
    a receiver can pick the correct deserializer without guessing.

    On input (``loads_*``) the format is **strict**: only payloads with
    ``"jsonrpc": "2.0"`` are accepted.  Use the auto-detection logic in
    :class:`RpcApp` when multiple serializers are configured.

    To extend the set of known error codes, subclass and override
    ``ERROR_CODE_MAP``::

        class MyJsonRpcSerializer(JsonRpcSerializer):
            ERROR_CODE_MAP = {
                **JsonRpcSerializer.ERROR_CODE_MAP,
                "Unauthorized": -32001,
            }
    """

    ERROR_CODE_MAP: ClassVar[dict[str, int]] = {
        "InternalError": -32603,
        "MethodNotFound": -32601,
        "Timeout": -32000,
        "ValidationError": -32602,
    }

    @property
    def name(self) -> str:
        return "jsonrpc2.0"

    def dumps_request(self, request: RpcRequest) -> RawMessage:
        obj: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": request.method,
            "id": str(request.id),
        }

        if request.args and request.kwds:
            obj["params"] = {"args": list(request.args), "kwds": request.kwds}
        elif request.args:
            obj["params"] = list(request.args)
        elif request.kwds:
            obj["params"] = dict(request.kwds)

        if request.headers:
            obj["headers"] = dict(request.headers)

        data = json.dumps(obj, separators=(",", ":")).encode()
        return RawMessage(data=data, headers={"content_type": "application/json", "serializer": self.name})

    def loads_request(self, raw: RawMessage) -> RpcRequest:
        obj: dict[str, Any] = json.loads(raw.data)
        if not isinstance(obj, dict) or obj.get("jsonrpc") != "2.0":
            raise ValueError("Not a JSON-RPC 2.0 request")

        method: str = obj.get("method", "")
        params = obj.get("params")
        raw_id = obj.get("id")
        headers: dict[str, Any] = obj.get("headers", {})

        if isinstance(params, list):
            args = tuple(params)
            kwds: dict[str, Any] = {}
        elif isinstance(params, dict):
            if "args" in params and "kwds" in params:
                args = tuple(params["args"])
                kwds = dict(params["kwds"])
            else:
                args = ()
                kwds = dict(params)
        else:
            args = ()
            kwds = {}

        req_id = UUID(raw_id) if raw_id is not None else uuid4()
        return RpcRequest(id=req_id, method=method, args=args, kwds=kwds, headers=headers)

    def dumps_response(self, response: RpcResponse) -> RawMessage:
        obj: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": str(response.id),
        }

        if response.type == "result":
            if isinstance(response.data, dict):
                result = response.data.get("result")
            else:
                result = response.data
            obj["result"] = result
        else:
            if isinstance(response.data, ErrorInfo):
                err_data = response.data.model_dump()
            else:
                err_data = response.data
            if isinstance(err_data, dict):
                ec = err_data.get("error_code", "")
                code = type(self).ERROR_CODE_MAP.get(ec, -32603)
            else:
                err_data = {}
                code = -32603
            err: dict[str, Any] = {
                "code": code,
                "message": err_data.get("error_message", ""),
            }
            details = err_data.get("error_details")
            if details:
                err["data"] = details
            obj["error"] = err

        if response.headers:
            obj["headers"] = dict(response.headers)

        data = json.dumps(obj, separators=(",", ":")).encode()
        return RawMessage(data=data, headers={"content_type": "application/json", "serializer": self.name})

    def loads_response(self, raw: RawMessage) -> RpcResponse:
        obj: dict[str, Any] = json.loads(raw.data)
        if not isinstance(obj, dict) or obj.get("jsonrpc") != "2.0":
            raise ValueError("Not a JSON-RPC 2.0 response")

        raw_id = obj.get("id")
        resp_id = UUID(raw_id) if raw_id is not None else uuid4()
        headers: dict[str, Any] = obj.get("headers", {})

        if "error" in obj:
            err = obj["error"]
            code = err.get("code", -32603)
            rev = {v: k for k, v in type(self).ERROR_CODE_MAP.items()}
            ec = rev.get(code, "InternalError")
            data_dict: dict[str, Any] = {
                "error_code": ec,
                "error_message": err.get("message", ""),
            }
            details = err.get("data")
            if details is not None:
                data_dict["error_details"] = details
            return RpcResponse(id=resp_id, type="error", data=data_dict, headers=headers)

        return RpcResponse(id=resp_id, type="result", data={"result": obj.get("result")}, headers=headers)
