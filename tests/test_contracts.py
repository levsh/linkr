from __future__ import annotations

import json

from linkr import App, Depends, LocalBackend
from linkr.contracts import (
    AppContract,
    RpcMethodContract,
    TaskMethodContract,
    describe_app,
    describe_app_html,
    describe_app_json,
    expose_describe,
)
from linkr.middleware import AppMiddleware
from linkr.middleware.gzip import GzipMiddleware
from tests.helpers import call


class TestDescribeContracts:
    async def test_describe_basic_rpc_task_event(self, app: App):
        math = app.rpc("math")
        email = app.worker("email")
        bus = app.bus()

        @math.method("add")
        def add(x: int, y: int) -> int:
            """Add two numbers."""
            return x + y

        @email.task("send")
        def send_email(user_id: int, template: str = "welcome") -> None:
            return None

        @bus.subscribe("user.*")
        def on_user(user_id: int) -> None:
            return None

        contract = describe_app(app)
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
        assert rpc["features"] == []
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
        assert task["features"] == []
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
        assert sub["features"] == []
        assert sub["payload"]["kwds_schema"]["type"] == "object"
        assert "user_id" in sub["payload"]["kwds_schema"]["properties"]

    async def test_describe_excludes_depends_params(self, app: App):
        server = app.rpc()

        @server.method("with_dep")
        def handler(x: int, d: Depends[int, lambda: 1]) -> int:
            return x + d

        spec = describe_app(app).to_dict()
        rpc = spec["methods"]["app.with_dep"]
        names = [p["name"] for p in rpc["input"]["params"]]
        assert names == ["x"]
        assert "d" not in rpc["input"]["kwds_schema"]["properties"]

    async def test_describe_include_filters(self, app: App):
        rpc = app.rpc()
        worker = app.worker()
        bus = app.bus()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        @worker.task("work")
        def work() -> None:
            return None

        @bus.subscribe("evt")
        def on_evt() -> None:
            return None

        methods_only = describe_app(app, include={"methods"}).to_dict()
        assert "app.ping" in methods_only["methods"]
        assert methods_only["tasks"] == {}
        assert methods_only["events"] == {}

    async def test_describe_json(self, app: App):
        server = app.rpc()

        @server.method("ping")
        def ping() -> str:
            return "pong"

        text = describe_app_json(app, indent=2)
        data = json.loads(text)
        assert data["app"] == "app"
        assert "app.ping" in data["methods"]

    async def test_subscriptions_property(self, app: App):
        bus = app.bus()

        @bus.subscribe("a.*")
        def on_a(x: int) -> None:
            return None

        @bus.subscribe("b.#")
        def on_b(y: str) -> None:
            return None

        subs = app.subscriptions
        assert set(subs.keys()) == {"a.*", "b.#"}
        assert subs["a.*"][0].request_type == "event"

    async def test_expose_describe_rpc(self, app: App):
        root = app.rpc()
        sys_srv = app.rpc("system")

        @root.method("ping")
        def ping() -> str:
            return "pong"

        expose_describe(app, "describe", server=sys_srv)
        await root.consume()
        await sys_srv.consume()

        spec = await call(app, "app.system.describe")
        assert isinstance(spec, dict)
        assert spec["contract"] == "linkr/1.0-alpha"
        assert "app.ping" in spec["methods"]
        assert "app.system.describe" in spec["methods"]

    async def test_expose_describe_default_name(self, app: App):
        root = app.rpc()
        expose_describe(app, server=root)
        await root.consume()

        spec = await call(app, "app.describe")
        assert isinstance(spec, dict)
        assert spec["contract"] == "linkr/1.0-alpha"
        assert "app.describe" in spec["methods"]


class TestDescribeCrossApp:
    async def test_cross_app_rpc_describe(self):
        backend = LocalBackend()
        server_app = App("server", backend=backend)
        client = App("client", backend=backend)
        srv = server_app.rpc()

        @srv.method("echo")
        def echo(value: str) -> str:
            return value

        expose_describe(server_app, "contracts", server=srv)

        async with server_app, client:
            await srv.consume()
            spec = await client.client.call("server.contracts", (), {})
            assert spec["app"] == "server"
            assert "server.echo" in spec["methods"]
            assert "server.contracts" in spec["methods"]


class TestDescribeHtml:
    async def test_app_describe_html(self, app: App):
        math = app.rpc("math")
        email = app.worker("email")
        bus = app.bus()

        @math.method("add")
        def add(x: int, y: int) -> int:
            """Add two numbers."""
            return x + y

        @email.task("send")
        def send_email(user_id: int) -> None:
            return None

        @bus.subscribe("user.*")
        def on_user(user_id: int) -> None:
            return None

        html_doc = describe_app_html(app)

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
        server = app.rpc()

        @server.method("ping")
        def ping() -> str:
            return "pong"

        contract = describe_app(app)
        html_from_model = contract.to_html()
        assert "<!doctype html>" in html_from_model
        assert "app.ping" in html_from_model

        from linkr.contracts import HtmlContractRenderer, render_contract_html

        html_from_renderer = HtmlContractRenderer(contract).render()
        html_from_helper = render_contract_html(contract.to_dict(), title="Custom")
        assert "app.ping" in html_from_renderer
        assert "<title>Custom</title>" in html_from_helper

    async def test_describe_html_filter(self, app: App):
        rpc = app.rpc()
        worker = app.worker()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        @worker.task("work")
        def work() -> None:
            return None

        html_doc = describe_app_html(app, include={"methods"})
        assert "app.ping" in html_doc
        assert "app.work" not in html_doc


class _TracingMiddleware(AppMiddleware):
    features = ("tracing", "gzip", "tracing", "")


class _DynamicFeatureMiddleware(AppMiddleware):
    def features(self):
        return ["auth", "auth"]


class TestDescribeFeatures:
    async def test_middleware_features_in_contract(self):
        app = App(
            "app",
            backend=LocalBackend(),
            middlewares=[GzipMiddleware(), _TracingMiddleware(), _DynamicFeatureMiddleware()],
        )
        rpc = app.rpc()
        worker = app.worker("jobs")
        bus = app.bus()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        @worker.task("run")
        def run() -> None:
            return None

        @bus.subscribe("evt")
        def on_evt() -> None:
            return None

        spec = describe_app(app).to_dict()
        # deduplicated, stable order (app middlewares then wire middlewares, as applied)
        expected = ["tracing", "gzip", "auth"]
        assert spec["methods"]["app.ping"]["features"] == expected
        assert spec["tasks"]["app.jobs.run"]["features"] == expected
        assert spec["events"]["evt"]["subscribers"][0]["features"] == expected

        html_doc = describe_app_html(app)
        assert "feature: gzip" in html_doc
        assert "badge-feature" in html_doc

    async def test_default_no_features(self, app: App):
        rpc = app.rpc()

        @rpc.method("ping")
        def ping() -> str:
            return "pong"

        assert describe_app(app).to_dict()["methods"]["app.ping"]["features"] == []

    async def test_features_per_bus_server(self):
        app = App("app", backend=LocalBackend(), middlewares=[GzipMiddleware()])
        bus_a = app.bus("a")
        bus_b = app.bus("b")

        @bus_a.subscribe("same")
        def on_a() -> None:
            return None

        @bus_b.subscribe("same")
        def on_b() -> None:
            return None

        spec = describe_app(app).to_dict()
        subs = spec["events"]["same"]["subscribers"]
        assert len(subs) == 2
        assert all(sub["features"] == ["gzip"] for sub in subs)
