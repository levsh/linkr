from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from linkr.exceptions import ErrorCode
from linkr.models import RawMessage, RpcRequest, RpcResponse
from linkr.serializer import JsonRpcSerializer, JsonSerializer


class TestJsonSerializer:
    @pytest.fixture
    def serializer(self) -> JsonSerializer:
        return JsonSerializer()

    def test_roundtrip_request(self, serializer: JsonSerializer):
        original = RpcRequest(method="test", args=(1, 2), kwds={})
        raw = serializer.dumps_request(original)
        assert raw.headers == {"content_type": "application/json", "serializer": "linkr1.0"}
        restored = serializer.loads_request(raw)
        assert restored.id == original.id
        assert restored.method == original.method

    def test_roundtrip_response(self, serializer: JsonSerializer):
        req = RpcRequest(method="ping", args=(), kwds={})
        original = RpcResponse(id=req.id, type="result", data={"result": 42})
        raw = serializer.dumps_response(original)
        assert raw.headers == {"content_type": "application/json", "serializer": "linkr1.0"}
        restored = serializer.loads_response(raw)
        assert restored.id == original.id
        assert restored.data == original.data

    def test_roundtrip_response_error(self, serializer: JsonSerializer):
        req = RpcRequest(method="ping", args=(), kwds={})
        original = RpcResponse(
            id=req.id,
            type="error",
            data={"error_code": "InternalError", "error_message": "oops", "error_details": {"trace": "..."}},
        )
        raw = serializer.dumps_response(original)
        restored = serializer.loads_response(raw)
        assert restored.data["error_code"] == ErrorCode.INTERNAL_ERROR
        assert restored.data["error_message"] == "oops"

    def test_invalid_data(self, serializer: JsonSerializer):
        with pytest.raises(ValidationError):
            serializer.loads_request(RawMessage(data=b"not json", headers={}))
        with pytest.raises(ValidationError):
            serializer.loads_response(RawMessage(data=b"not json", headers={}))

    def test_dumps_request_returns_headers(self, serializer: JsonSerializer):
        req = RpcRequest(method="ping", args=(), kwds={})
        raw = serializer.dumps_request(req)
        assert raw.headers == {"content_type": "application/json", "serializer": "linkr1.0"}
        assert isinstance(raw.data, bytes)
        assert len(raw.data) > 0

    def test_dumps_response_returns_headers(self, serializer: JsonSerializer):
        req = RpcRequest(method="ping", args=(), kwds={})
        resp = RpcResponse(id=req.id, type="result", data={"result": "ok"})
        raw = serializer.dumps_response(resp)
        assert raw.headers == {"content_type": "application/json", "serializer": "linkr1.0"}
        assert isinstance(raw.data, bytes)
        assert len(raw.data) > 0


class TestJsonRpcSerializer:
    @pytest.fixture
    def serializer(self) -> JsonRpcSerializer:
        return JsonRpcSerializer()

    def test_name(self, serializer: JsonRpcSerializer):
        assert serializer.name == "jsonrpc2.0"

    def test_request_roundtrip_args(self, serializer: JsonRpcSerializer):
        original = RpcRequest(method="add", args=(1, 2), kwds={})
        raw = serializer.dumps_request(original)
        assert raw.headers["serializer"] == "jsonrpc2.0"
        restored = serializer.loads_request(raw)
        assert restored.id == original.id
        assert restored.method == "add"
        assert restored.args == (1, 2)

    def test_request_roundtrip_kwds(self, serializer: JsonRpcSerializer):
        original = RpcRequest(method="greet", args=(), kwds={"name": "world"})
        raw = serializer.dumps_request(original)
        restored = serializer.loads_request(raw)
        assert restored.args == ()
        assert restored.kwds == {"name": "world"}

    def test_request_roundtrip_both(self, serializer: JsonRpcSerializer):
        original = RpcRequest(method="mixed", args=(1,), kwds={"x": 2})
        raw = serializer.dumps_request(original)
        restored = serializer.loads_request(raw)
        assert restored.args == (1,)
        assert restored.kwds == {"x": 2}

    def test_request_roundtrip_no_params(self, serializer: JsonRpcSerializer):
        original = RpcRequest(method="ping", args=(), kwds={})
        raw = serializer.dumps_request(original)
        restored = serializer.loads_request(raw)
        assert restored.method == "ping"

    def test_response_result(self, serializer: JsonRpcSerializer):
        original = RpcResponse(id=UUID("00000000-0000-0000-0000-000000000001"), type="result", data={"result": 42})
        raw = serializer.dumps_response(original)
        assert raw.headers["serializer"] == "jsonrpc2.0"
        restored = serializer.loads_response(raw)
        assert restored.id == original.id
        assert restored.data["result"] == 42

    def test_response_error(self, serializer: JsonRpcSerializer):
        req_id = UUID("00000000-0000-0000-0000-000000000001")
        original = RpcResponse(id=req_id, type="error", data={"error_code": "MethodNotFound", "error_message": "not found"})
        raw = serializer.dumps_response(original)
        restored = serializer.loads_response(raw)
        assert restored.data["error_code"] == ErrorCode.METHOD_NOT_FOUND
        assert restored.data["error_message"] == "not found"

    def test_response_error_with_details(self, serializer: JsonRpcSerializer):
        req_id = UUID("00000000-0000-0000-0000-000000000001")
        original = RpcResponse(
            id=req_id,
            type="error",
            data={"error_code": "InternalError", "error_message": "fail", "error_details": {"exc_type": "ValueError"}},
        )
        raw = serializer.dumps_response(original)
        restored = serializer.loads_response(raw)
        assert restored.data["error_code"] == ErrorCode.INTERNAL_ERROR
        assert restored.data["error_details"]["exc_type"] == "ValueError"

    def test_loads_request_raises_on_native(self, serializer: JsonRpcSerializer):
        native_raw = JsonSerializer().dumps_request(RpcRequest(method="test", args=(), kwds={}))
        with pytest.raises(ValueError, match="Not a JSON-RPC 2.0 request"):
            serializer.loads_request(native_raw)

    def test_loads_response_raises_on_native(self, serializer: JsonRpcSerializer):
        resp = RpcResponse(id=UUID("00000000-0000-0000-0000-000000000001"), type="result", data={"result": 42})
        native_raw = JsonSerializer().dumps_response(resp)
        with pytest.raises(ValueError, match="Not a JSON-RPC 2.0 response"):
            serializer.loads_response(native_raw)

    def test_subclass_with_custom_error_codes(self):
        class AppSerializer(JsonRpcSerializer):
            ERROR_CODE_MAP = {
                **JsonRpcSerializer.ERROR_CODE_MAP,
                "Unauthorized": -32001,
            }

        ser = AppSerializer()
        req_id = UUID("00000000-0000-0000-0000-000000000001")

        raw = ser.dumps_response(
            RpcResponse(
                id=req_id,
                type="error",
                data={"error_code": "Unauthorized", "error_message": "access denied"},
            )
        )
        restored = ser.loads_response(raw)
        assert restored.data["error_code"] == "Unauthorized"
        assert restored.data["error_message"] == "access denied"

        raw2 = ser.dumps_response(
            RpcResponse(
                id=req_id,
                type="error",
                data={"error_code": "InternalError", "error_message": "oops"},
            )
        )
        restored2 = ser.loads_response(raw2)
        assert restored2.data["error_code"] == "InternalError"
