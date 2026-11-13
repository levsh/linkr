from __future__ import annotations

import json

from linkr import App, Depends, LocalBackend
from linkr.contracts import AppContract, RpcMethodContract, TaskMethodContract
from tests.helpers import call


class TestDescribeContracts:
    async def test_describe_basic_rpc_task_event(self, app: App):
        @app.method("math.add")
        def add(x: int, y: int) -> int:
            """Add two numbers."""
            return x + y

        @app.task("email.send")
        def send_email(user_id: int, template: str = "welcome") -> None:
            return None

        @app.subscribe("user.*")
        def on_user(user_id: int) -> None:
            return None

        contract = app.describe()
        assert isinstance(contract, AppContract)
        rpc_model = contract.methods["app.math.add"]
        task_model = contract.tasks["app.email.send"]
        assert isinstance(rpc_model, RpcMethodContract)
        assert isinstance(task_model, TaskMethodContract)
        assert rpc_model.call == "app.math.add"
        assert not hasattr(rpc_model, "send") or getattr(rpc_model, "send", None) is None
        assert task_model.send == "app.email.send"
        assert rpc_model.model_dump().get("send") is None or "send" not in rpc_model.model_dump()
        assert "call" not in task_model.model_dump()
        spec = contract.to_dict()

        assert spec["contract"] == "linkr/1.0-alpha"
        assert spec["app"] == "app"

        rpc = spec["methods"]["app.math.add"]
        assert rpc["kind"] == "rpc"
        assert rpc["name"] == "add"
        assert rpc["queue"] == "app.math"
        assert rpc["call"] == "app.math.add"
        assert rpc["description"] == "Add two numbers."
        assert rpc["output"]["schema"]["type"] == "integer"
        assert [p["name"] for p in rpc["input"]["params"]] == ["x", "y"]
        assert rpc["input"]["args_schema"]["type"] == "array"
        assert rpc["input"]["args_schema"]["prefixItems"][0]["type"] == "integer"
        assert rpc["input"]["kwds_schema"]["type"] == "object"
        assert "x" in rpc["input"]["kwds_schema"]["properties"]

        task = spec["tasks"]["app.email.send"]
        assert task["kind"] == "task"
        assert task["name"] == "send"
        assert task["queue"] == "app.email"
        assert task["send"] == "app.email.send"
        assert task["output"] is None
        params = {p["name"]: p for p in task["input"]["params"]}
        assert params["template"]["required"] is False
        assert params["template"]["default"] == "welcome"

        assert "user.*" in spec["events"]
        event = spec["events"]["user.*"]
        assert event["kind"] == "event"
        assert event["topic"] == "user.*"
        assert len(event["subscribers"]) == 1
        sub = event["subscribers"][0]
        assert sub["handler"] == "on_user"
        assert sub["queue_template"] == "app.<instance_id>"
        assert sub["payload"]["kwds_schema"]["type"] == "object"
        assert "user_id" in sub["payload"]["kwds_schema"]["properties"]

    async def test_describe_excludes_depends_params(self, app: App):
        @app.method("with_dep")
        def handler(x: int, d: Depends[int, lambda: 1]) -> int:
            return x + d

        spec = app.describe().to_dict()
        rpc = spec["methods"]["app.with_dep"]
        names = [p["name"] for p in rpc["input"]["params"]]
        assert names == ["x"]
        assert "d" not in rpc["input"]["kwds_schema"]["properties"]

    async def test_describe_include_filters(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        @app.task("work")
        def work() -> None:
            return None

        @app.subscribe("evt")
        def on_evt() -> None:
            return None

        methods_only = app.describe(include={"methods"}).to_dict()
        assert "app.ping" in methods_only["methods"]
        assert methods_only["tasks"] == {}
        assert methods_only["events"] == {}

    async def test_describe_json(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        text = app.describe_json(indent=2)
        data = json.loads(text)
        assert data["app"] == "app"
        assert "app.ping" in data["methods"]

    async def test_subscriptions_property(self, app: App):
        @app.subscribe("a.*")
        def on_a(x: int) -> None:
            return None

        @app.subscribe("b.#")
        def on_b(y: str) -> None:
            return None

        subs = app.subscriptions
        assert set(subs.keys()) == {"a.*", "b.#"}
        assert subs["a.*"][0].request_type == "event"

    async def test_expose_describe_rpc(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        app.expose_describe("system.describe")
        await app.consume()

        spec = await call(app, "app.system.describe")
        assert isinstance(spec, dict)
        assert spec["contract"] == "linkr/1.0-alpha"
        assert "app.ping" in spec["methods"]
        assert "app.system.describe" in spec["methods"]

    async def test_expose_describe_default_name(self, app: App):
        app.expose_describe()
        await app.consume()

        spec = await call(app, "app.linkr.describe")
        assert isinstance(spec, dict)
        assert spec["contract"] == "linkr/1.0-alpha"
        assert "app.linkr.describe" in spec["methods"]


class TestDescribeCrossApp:
    async def test_cross_app_rpc_describe(self):
        backend = LocalBackend()
        server = App("server", backend=backend)
        client = App("client", backend=backend)

        @server.method("echo")
        def echo(value: str) -> str:
            return value

        server.expose_describe("contracts")

        async with server, client:
            await server.consume()
            spec = await client.call("server.contracts", (), {})
            assert spec["app"] == "server"
            assert "server.echo" in spec["methods"]
            assert "server.contracts" in spec["methods"]


class TestDescribeHtml:
    async def test_app_describe_html(self, app: App):
        @app.method("math.add")
        def add(x: int, y: int) -> int:
            """Add two numbers."""
            return x + y

        @app.task("email.send")
        def send_email(user_id: int) -> None:
            return None

        @app.subscribe("user.*")
        def on_user(user_id: int) -> None:
            return None

        html_doc = app.describe_html()

        assert html_doc.startswith("<!doctype html>")
        assert "<title>" in html_doc
        assert "linkr/1.0-alpha" in html_doc
        assert "app.math.add" in html_doc
        assert "app.email.send" in html_doc
        assert "user.*" in html_doc
        assert "Add two numbers." in html_doc
        assert "Methods (RPC)" in html_doc
        assert "Tasks" in html_doc
        assert "Events" in html_doc
        assert "Components (schemas)" in html_doc

    async def test_contract_to_html_and_renderer(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        contract = app.describe()
        html_from_model = contract.to_html()
        assert "<!doctype html>" in html_from_model
        assert "app.ping" in html_from_model

        from linkr.contracts import HtmlContractRenderer, render_contract_html

        html_from_renderer = HtmlContractRenderer(contract).render()
        html_from_helper = render_contract_html(contract.to_dict(), title="Custom")
        assert "app.ping" in html_from_renderer
        assert "<title>Custom</title>" in html_from_helper

    async def test_describe_html_filter(self, app: App):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        @app.task("work")
        def work() -> None:
            return None

        html_doc = app.describe_html(include={"methods"})
        assert "app.ping" in html_doc
        assert "app.work" not in html_doc
