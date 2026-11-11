from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from linkr.exceptions import ErrorCode
from linkr.models import ErrorInfo, Message, Request, Response
from linkr.serializer import JsonRpcSerializer, JsonSerializer


def make_request(
    request_type: str = "rpc",
    method: str = "test",
    args: tuple[object, ...] = (),
    kwds: dict[str, object] | None = None,
) -> Request:
    if request_type == "event":
        data = {"topic": method, "event": kwds or {}}
    else:
        data = {"method": method, "args": args, "kwds": kwds or {}}
    return Request(type=request_type, data=data)  # type: ignore[arg-type]


class TestJsonSerializer:
    @pytest.fixture
    def serializer(self) -> JsonSerializer:
        return JsonSerializer()

    def test_name(self, serializer: JsonSerializer):
        assert serializer.name == "linkr1.0"

    def test_roundtrip_request(self, serializer: JsonSerializer):
        original = make_request(args=(1, 2))
        raw = serializer.dumps_request(original)

        assert raw.headers == {"content_type": "application/json", "serializer": "linkr1.0"}
        restored = serializer.loads_request(raw)
        assert restored.id == original.id
        assert restored.type == "rpc"
        assert restored.data["method"] == "test"
        assert list(restored.data["args"]) == [1, 2]

    def test_roundtrip_task_request(self, serializer: JsonSerializer):
        original = make_request("task", "send", (), {"x": 1})
        restored = serializer.loads_request(serializer.dumps_request(original))

        assert restored.type == "task"
        assert restored.data["method"] == "send"
        assert restored.data["kwds"] == {"x": 1}

    def test_roundtrip_event_request(self, serializer: JsonSerializer):
        original = make_request("event", "user.created", (), {"user_id": 1})
        restored = serializer.loads_request(serializer.dumps_request(original))

        assert restored.type == "event"
        assert restored.data["topic"] == "user.created"
        assert restored.data["event"] == {"user_id": 1}

    def test_roundtrip_response(self, serializer: JsonSerializer):
        original = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="result",
            data={"result": 42},
        )
        raw = serializer.dumps_response(original)

        assert raw.headers == {"content_type": "application/json", "serializer": "linkr1.0"}
        restored = serializer.loads_response(raw)
        assert restored.id == original.id
        assert restored.data == original.data

    def test_roundtrip_error_response(self, serializer: JsonSerializer):
        original = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="error",
            data=ErrorInfo(
                error_code=ErrorCode.INTERNAL_ERROR,
                error_message="oops",
                error_details={"trace": "..."},
            ),
        )
        restored = serializer.loads_response(serializer.dumps_response(original))

        assert restored.type == "error"
        assert restored.data["error_code"] == ErrorCode.INTERNAL_ERROR
        assert restored.data["error_message"] == "oops"
        assert restored.data["error_details"] == {"trace": "..."}

    def test_invalid_data(self, serializer: JsonSerializer):
        with pytest.raises(ValidationError):
            serializer.loads_request(Message(data=b"not json", headers={}))
        with pytest.raises(ValidationError):
            serializer.loads_response(Message(data=b"not json", headers={}))

    def test_dumps_returns_bytes(self, serializer: JsonSerializer):
        raw = serializer.dumps_request(make_request())
        assert isinstance(raw.data, bytes)
        assert raw.data


class TestJsonRpcSerializer:
    @pytest.fixture
    def serializer(self) -> JsonRpcSerializer:
        return JsonRpcSerializer()

    def test_name(self, serializer: JsonRpcSerializer):
        assert serializer.name == "jsonrpc2.0"

    def test_roundtrip_args(self, serializer: JsonRpcSerializer):
        original = make_request("rpc", "add", (1, 2))
        raw = serializer.dumps_request(original)

        assert raw.headers["serializer"] == "jsonrpc2.0"
        restored = serializer.loads_request(raw)
        assert restored.id == original.id
        assert restored.type == "rpc"
        assert restored.data["method"] == "add"
        assert restored.data["args"] == (1, 2)

    def test_roundtrip_kwds(self, serializer: JsonRpcSerializer):
        original = make_request("rpc", "greet", (), {"name": "world"})
        restored = serializer.loads_request(serializer.dumps_request(original))

        assert restored.data["args"] == ()
        assert restored.data["kwds"] == {"name": "world"}

    def test_roundtrip_both(self, serializer: JsonRpcSerializer):
        original = make_request("rpc", "mixed", (1,), {"x": 2})
        restored = serializer.loads_request(serializer.dumps_request(original))

        assert restored.data["args"] == (1,)
        assert restored.data["kwds"] == {"x": 2}

    def test_roundtrip_no_params(self, serializer: JsonRpcSerializer):
        original = make_request("rpc", "ping")
        restored = serializer.loads_request(serializer.dumps_request(original))

        assert restored.data["method"] == "ping"
        assert restored.data["args"] == ()
        assert restored.data["kwds"] == {}

    def test_response_result(self, serializer: JsonRpcSerializer):
        original = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="result",
            data={"result": 42},
        )
        raw = serializer.dumps_response(original)

        assert raw.headers["serializer"] == "jsonrpc2.0"
        restored = serializer.loads_response(raw)
        assert restored.id == original.id
        assert restored.data["result"] == 42

    def test_response_error(self, serializer: JsonRpcSerializer):
        original = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="error",
            data=ErrorInfo(error_code=ErrorCode.METHOD_NOT_FOUND, error_message="not found"),
        )
        restored = serializer.loads_response(serializer.dumps_response(original))

        assert restored.type == "error"
        assert restored.data["error_code"] == ErrorCode.METHOD_NOT_FOUND
        assert restored.data["error_message"] == "not found"

    def test_response_error_with_details(self, serializer: JsonRpcSerializer):
        original = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="error",
            data=ErrorInfo(
                error_code=ErrorCode.INTERNAL_ERROR,
                error_message="fail",
                error_details={"exc_type": "ValueError"},
            ),
        )
        restored = serializer.loads_response(serializer.dumps_response(original))

        assert restored.data["error_code"] == ErrorCode.INTERNAL_ERROR
        assert restored.data["error_details"]["exc_type"] == "ValueError"

    def test_rejects_native_request(self, serializer: JsonRpcSerializer):
        native_raw = JsonSerializer().dumps_request(make_request())
        with pytest.raises(ValueError, match="not a JSON-RPC 2.0 request"):
            serializer.loads_request(native_raw)

    def test_rejects_native_response(self, serializer: JsonRpcSerializer):
        response = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="result",
            data={"result": 42},
        )
        native_raw = JsonSerializer().dumps_response(response)
        with pytest.raises(ValueError, match="not a JSON-RPC 2.0 response"):
            serializer.loads_response(native_raw)

    def test_rejects_non_rpc_request(self, serializer: JsonRpcSerializer):
        with pytest.raises(ValueError, match="only RPC"):
            serializer.dumps_request(make_request("task", "send"))

    def test_subclass_with_custom_error_codes(self):
        class AppSerializer(JsonRpcSerializer):
            ERROR_CODE_MAP = {
                **JsonRpcSerializer.ERROR_CODE_MAP,
                "Unauthorized": -32001,
            }

        ser = AppSerializer()
        req_id = UUID("00000000-0000-0000-0000-000000000001")

        raw = ser.dumps_response(
            Response(
                id=req_id,
                type="error",
                data=ErrorInfo(error_code="Unauthorized", error_message="access denied"),
            )
        )
        restored = ser.loads_response(raw)
        assert restored.data["error_code"] == "Unauthorized"
        assert restored.data["error_message"] == "access denied"

        raw2 = ser.dumps_response(
            Response(
                id=req_id,
                type="error",
                data=ErrorInfo(error_code=ErrorCode.INTERNAL_ERROR, error_message="oops"),
            )
        )
        restored2 = ser.loads_response(raw2)
        assert restored2.data["error_code"] == ErrorCode.INTERNAL_ERROR
