from __future__ import annotations

import pytest
from pydantic import ValidationError

from linkr.models import RpcRequest, RpcResponse
from linkr.serializer import JsonSerializer


def test_roundtrip_request():
    serializer = JsonSerializer()
    original = RpcRequest(data={"method": "test", "args": [1, 2], "kwds": {}})
    data, wire = serializer.dumps_request(original)
    assert wire == {"content_type": "application/json"}
    restored = serializer.loads_request(data, wire)
    assert restored.id == original.id
    assert restored.data == original.data


def test_roundtrip_response():
    serializer = JsonSerializer()
    req = RpcRequest()
    original = RpcResponse(id=req.id, data={"result": 42})
    data, wire = serializer.dumps_response(original)
    assert wire == {"content_type": "application/json"}
    restored = serializer.loads_response(data, wire)
    assert restored.id == original.id
    assert restored.data == original.data


def test_roundtrip_response_error():
    serializer = JsonSerializer()
    req = RpcRequest()
    original = RpcResponse(
        id=req.id,
        data={"error_code": "InternalError", "error_message": "oops", "error_details": {"trace": "..."}},
    )
    data, wire = serializer.dumps_response(original)
    restored = serializer.loads_response(data, wire)
    assert restored.data["error_code"] == "InternalError"
    assert restored.data["error_message"] == "oops"


def test_serializer_invalid_data():
    serializer = JsonSerializer()
    with pytest.raises(ValidationError):
        serializer.loads_request(b"not json", {"content_type": "application/json"})
    with pytest.raises(ValidationError):
        serializer.loads_response(b"not json", {"content_type": "application/json"})


def test_dumps_request_returns_wire_headers():
    serializer = JsonSerializer()
    req = RpcRequest(data={"method": "ping"})
    data, wire = serializer.dumps_request(req)
    assert wire == {"content_type": "application/json"}
    assert isinstance(data, bytes)
    assert len(data) > 0


def test_dumps_response_returns_wire_headers():
    serializer = JsonSerializer()
    req = RpcRequest()
    resp = RpcResponse(id=req.id, data={"result": "ok"})
    data, wire = serializer.dumps_response(resp)
    assert wire == {"content_type": "application/json"}
    assert isinstance(data, bytes)
    assert len(data) > 0
