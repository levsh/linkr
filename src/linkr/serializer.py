from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, ClassVar
from uuid import UUID, uuid4

from .models import ErrorInfo, Message, Request, Response


class Serializer(ABC):
    """
    Abstract serializer.

    Implementations convert :class:`Request` and :class:`Response` objects to
    and from wire :class:`Message` for transmission over the wire.
    Wire-level metadata (e.g. content type, encoding) is carried in
    ``Message.headers``.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable format name (e.g. ``"linkr1.0"``, ``"jsonrpc2.0"``)."""

    @abstractmethod
    def dumps_request(self, request: Request) -> Message:
        """Serialize a :class:`Request` into a message."""

    @abstractmethod
    def loads_request(self, message: Message) -> Request:
        """Deserialize a :class:`Message` into a request."""

    @abstractmethod
    def dumps_response(self, response: Response) -> Message:
        """Serialize a :class:`Response` into a raw message."""

    @abstractmethod
    def loads_response(self, message: Message) -> Response:
        """Deserialize a :class:`Message` into a response."""


class JsonSerializer(Serializer):
    """JSON serializer for linkr messages."""

    __slots__ = ("_headers",)

    def __init__(self) -> None:
        """Create a JSON serializer for linkr 1.0 messages."""
        self._headers = {"content_type": "application/json", "serializer": "linkr1.0"}

    @property
    def name(self) -> str:
        """Human-readable format name (``"linkr1.0"``)."""
        return "linkr1.0"

    def dumps_request(self, request: Request) -> Message:
        """Serialize a :class:`Request` into a message."""
        data = request.model_dump_json().encode()
        return Message(data=data, headers=self._headers)

    def loads_request(self, message: Message) -> Request:
        """Deserialize a :class:`Message` into a request."""
        return Request.model_validate_json(message.data)

    def dumps_response(self, response: Response) -> Message:
        """Serialize a :class:`Response` into a raw message."""
        data = response.model_dump_json().encode()
        return Message(data=data, headers=self._headers)

    def loads_response(self, message: Message) -> Response:
        """Deserialize a :class:`Message` into a response."""
        return Response.model_validate_json(message.data)


class JsonRpcSerializer(Serializer):
    """
    JSON-RPC 2.0 serializer.

    To extend the set of known error codes, subclass and override ``ERROR_CODE_MAP``::

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

    __slots__ = ("_headers",)

    def __init__(self) -> None:
        """Create a JSON-RPC 2.0 serializer."""
        self._headers = {"content_type": "application/json", "serializer": "jsonrpc2.0"}

    @property
    def name(self) -> str:
        """Human-readable format name (``"jsonrpc2.0"``)."""
        return "jsonrpc2.0"

    def dumps_request(self, request: Request) -> Message:
        """Serialize an RPC :class:`Request` into a JSON-RPC 2.0 message."""
        if request.type != "rpc":
            raise ValueError("JsonRpcSerializer supports only RPC requests")

        data = request.data

        obj: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": data["method"],
            "id": str(request.id),
        }

        args = data["args"]
        kwds = data["kwds"]

        if args and kwds:
            obj["params"] = {"args": list(args), "kwds": kwds}
        elif args:
            obj["params"] = list(args)
        elif kwds:
            obj["params"] = dict(kwds)

        if request.headers:
            obj["headers"] = dict(request.headers)

        payload = json.dumps(obj, separators=(",", ":")).encode()
        return Message(data=payload, headers=self._headers)

    def loads_request(self, message: Message) -> Request:
        """Deserialize a :class:`Message` into a JSON-RPC 2.0 request."""
        obj: dict[str, Any] = json.loads(message.data)

        if not isinstance(obj, dict) or obj.get("jsonrpc") != "2.0":
            raise ValueError("not a JSON-RPC 2.0 request")

        method: str = obj.get("method", "")
        params = obj.get("params")
        raw_id = obj.get("id")
        headers: dict[str, Any] = obj.get("headers", {})

        if isinstance(params, list):
            args = tuple(params)
            kwds = {}
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
        return Request.model_validate(
            {
                "id": req_id,
                "type": "rpc",
                "data": {
                    "method": method,
                    "args": args,
                    "kwds": kwds,
                },
                "headers": headers,
            }
        )

    def dumps_response(self, response: Response) -> Message:
        """Serialize a :class:`Response` into a JSON-RPC 2.0 message."""
        obj: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": str(response.id),
        }

        if response.type == "result":
            obj["result"] = response.data
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
        return Message(data=data, headers=self._headers)

    def loads_response(self, message: Message) -> Response:
        """Deserialize a :class:`Message` into a JSON-RPC 2.0 response."""
        obj: dict[str, Any] = json.loads(message.data)

        if not isinstance(obj, dict) or obj.get("jsonrpc") != "2.0":
            raise ValueError("not a JSON-RPC 2.0 response")

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
            return Response(id=resp_id, type="error", data=data_dict, headers=headers)

        return Response(id=resp_id, type="result", data=obj.get("result"), headers=headers)
