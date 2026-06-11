from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from linkr.models import RpcRequest, RpcResponse


def test_rpc_request_defaults():
    req = RpcRequest()
    assert isinstance(req.id, UUID)
    assert req.headers == {}
    assert req.data is None


def test_rpc_request_with_data():
    req = RpcRequest(data={"method": "foo", "args": [1], "kwds": {"x": 2}})
    assert req.data == {"method": "foo", "args": [1], "kwds": {"x": 2}}


def test_rpc_request_with_headers():
    req = RpcRequest(headers={"routing_key": "test", "timeout": 5.0})
    assert req.headers["routing_key"] == "test"
    assert req.headers["timeout"] == 5.0


def test_rpc_request_serialization():
    req = RpcRequest(data={"key": "value"})
    data = req.model_dump_json()
    restored = RpcRequest.model_validate_json(data)
    assert restored.id == req.id
    assert restored.data == req.data
    assert restored.headers == req.headers


def test_rpc_response_defaults():
    req = RpcRequest()
    resp = RpcResponse(id=req.id)
    assert resp.id == req.id
    assert resp.headers == {}
    assert resp.data is None


def test_rpc_response_serialization():
    req = RpcRequest()
    resp = RpcResponse(id=req.id, data={"result": 42})
    data = resp.model_dump_json()
    restored = RpcResponse.model_validate_json(data)
    assert restored.id == resp.id
    assert restored.data == resp.data


def test_rpc_response_headersonly():
    resp = RpcResponse(id=UUID("00000000-0000-0000-0000-000000000001"), headers={"x-custom": "val"})
    assert resp.headers["x-custom"] == "val"


def test_rpc_response_invalid_id():
    with pytest.raises(ValidationError):
        RpcResponse(id="not-a-uuid")  # type: ignore[arg-type]

