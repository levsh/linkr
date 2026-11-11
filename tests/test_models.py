from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from linkr.models import ErrorInfo, HandlerInfo, Message, Request, Response


class TestRequestModel:
    def test_request_defaults(self):
        req = Request(type="rpc", data={"method": "ping", "args": (), "kwds": {}})

        assert isinstance(req.id, UUID)
        assert req.type == "rpc"
        assert req.data["method"] == "ping"
        assert req.data["args"] == ()
        assert req.data["kwds"] == {}
        assert req.headers == {}

    def test_request_with_data_and_headers(self):
        req = Request(
            type="task",
            data={"method": "foo", "args": (1,), "kwds": {"x": 2}},
            headers={"timeout": 5.0},
        )

        assert req.type == "task"
        assert req.data["args"] == (1,)
        assert req.data["kwds"] == {"x": 2}
        assert req.headers["timeout"] == 5.0

    def test_request_requires_type(self):
        with pytest.raises(ValidationError):
            Request.model_validate({"data": {"method": "ping"}})

    def test_request_invalid_type(self):
        with pytest.raises(ValidationError):
            Request(type="invalid", data={})  # type: ignore[arg-type]

    def test_request_serialization(self):
        req = Request(type="event", data={"topic": "user.created", "event": {"user_id": 1}})
        restored = Request.model_validate_json(req.model_dump_json())

        assert restored.id == req.id
        assert restored.type == "event"
        assert restored.data == req.data
        assert restored.headers == req.headers


class TestResponseModel:
    def test_response_defaults(self):
        resp = Response(id=UUID("00000000-0000-0000-0000-000000000001"), type="result", data=None)

        assert resp.type == "result"
        assert resp.headers == {}
        assert resp.data is None

    def test_response_serialization(self):
        resp = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="result",
            data={"result": 42},
        )
        restored = Response.model_validate_json(resp.model_dump_json())

        assert restored.id == resp.id
        assert restored.data == resp.data

    def test_response_headers(self):
        resp = Response(
            id=UUID("00000000-0000-0000-0000-000000000001"),
            type="result",
            data=None,
            headers={"x-custom": "val"},
        )

        assert resp.headers["x-custom"] == "val"

    def test_response_invalid_id(self):
        with pytest.raises(ValidationError):
            Response(id="not-a-uuid", type="result", data=None)  # type: ignore[arg-type]


class TestMessageAndInfoModels:
    def test_message_defaults(self):
        message = Message(data=b"payload")

        assert message.data == b"payload"
        assert message.headers == {}

    def test_message_headers_are_independent(self):
        first = Message(data=b"a")
        second = Message(data=b"b")
        first.headers["x"] = "1"

        assert second.headers == {}

    def test_error_info_defaults(self):
        info = ErrorInfo()

        assert info.error_code == ""
        assert info.error_message == ""
        assert info.error_details is None

    def test_handler_info_defaults(self):
        info = HandlerInfo(name="ping", request_type="rpc", fn=lambda: None, signature="()")

        assert info.options == {}
        assert info.dep_types == {}
        assert info.queue is None
