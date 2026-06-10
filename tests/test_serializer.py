from __future__ import annotations

import pytest
from pydantic import ValidationError

from linkr.models import RpcRequest, RpcResponse
from linkr.serializer import JsonSerializer


def test_roundtrip_request():
    serializer = JsonSerializer()
    original = RpcRequest(data={"method": "test", "args": [1, 2], "kwds": {}})
    data = serializer.dumps_request(original)
    restored = serializer.loads_request(data)
    assert restored.id == original.id
    assert restored.data == original.data


def test_roundtrip_response():
    serializer = JsonSerializer()
    req = RpcRequest()
    original = RpcResponse(id=req.id, data={"result": 42})
    data = serializer.dumps_response(original)
    restored = serializer.loads_response(data)
    assert restored.id == original.id
    assert restored.data == original.data


def test_roundtrip_response_error():
    serializer = JsonSerializer()
    req = RpcRequest()
    original = RpcResponse(
        id=req.id,
        data={"error_code": "InternalError", "error_message": "oops", "error_details": {"trace": "..."}},
    )
    data = serializer.dumps_response(original)
    restored = serializer.loads_response(data)
    assert restored.data["error_code"] == "InternalError"
    assert restored.data["error_message"] == "oops"


def test_serializer_invalid_data():
    serializer = JsonSerializer()
    with pytest.raises(ValidationError):
        serializer.loads_request(b"not json")
    with pytest.raises(ValidationError):
        serializer.loads_response(b"not json")
