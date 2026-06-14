from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from linkr.models import RpcRequest, RpcResponse


class TestRpcModels:
    def test_request_defaults(self):
        req = RpcRequest(method="ping", args=(), kwds={})
        assert isinstance(req.id, UUID)
        assert req.headers == {}

    def test_request_with_args(self):
        req = RpcRequest(method="foo", args=(1,), kwds={"x": 2})
        assert req.method == "foo"
        assert req.args == (1,)
        assert req.kwds == {"x": 2}

    def test_request_with_headers(self):
        req = RpcRequest(method="test", args=(), kwds={}, headers={"routing_key": "test", "timeout": 5.0})
        assert req.headers["routing_key"] == "test"
        assert req.headers["timeout"] == 5.0

    def test_request_serialization(self):
        req = RpcRequest(method="test", args=(), kwds={"key": "value"})
        data = req.model_dump_json()
        restored = RpcRequest.model_validate_json(data)
        assert restored.id == req.id
        assert restored.method == req.method
        assert restored.kwds == req.kwds
        assert restored.headers == req.headers

    def test_response_defaults(self):
        req = RpcRequest(method="ping", args=(), kwds={})
        resp = RpcResponse(id=req.id, type="result", data=None)
        assert resp.id == req.id
        assert resp.headers == {}
        assert resp.data is None

    def test_response_serialization(self):
        req = RpcRequest(method="ping", args=(), kwds={})
        resp = RpcResponse(id=req.id, type="result", data={"result": 42})
        data = resp.model_dump_json()
        restored = RpcResponse.model_validate_json(data)
        assert restored.id == resp.id
        assert restored.data == resp.data

    def test_response_headersonly(self):
        resp = RpcResponse(id=UUID("00000000-0000-0000-0000-000000000001"), type="result", data=None, headers={"x-custom": "val"})
        assert resp.headers["x-custom"] == "val"

    def test_response_invalid_id(self):
        with pytest.raises(ValidationError):
            RpcResponse(id="not-a-uuid")  # type: ignore[arg-type]
