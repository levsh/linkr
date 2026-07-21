from __future__ import annotations

from typing import get_args, get_origin

import pytest

from linkr import Depends, DiContainer, get_current_request


class Database:
    def __init__(self, url: str) -> None:
        self.url = url


class Config:
    def __init__(self, env: str) -> None:
        self.env = env


class TestDiContainer:
    def test_depends_marker(self) -> None:
        ann: object = Depends[Database]
        assert get_origin(ann) is Depends
        assert get_args(ann) == (Database,)

    def test_singleton(self) -> None:
        c = DiContainer()
        c.add_singleton(Database, lambda: Database("postgres://test"))
        assert c.resolve(Database) is c.resolve(Database)

    def test_transient(self) -> None:
        c = DiContainer()
        c.add_transient(Config, lambda: Config("dev"))
        assert c.resolve(Config) is not c.resolve(Config)

    def test_unknown(self) -> None:
        c = DiContainer()
        with pytest.raises(KeyError, match="no dependency registered"):
            c.resolve(str)


class TestDiWithApp:
    async def test_resolves_singleton(self, app, transport):
        app.dependencies.add_singleton(Database, lambda: Database("postgres://db"))

        @app.method("ping")
        def ping(db: Depends[Database]) -> str:
            return db.url  # type: ignore[attr-defined]

        await app.consume()
        result = await app.make("ping").invoke()
        assert result == "postgres://db"

    async def test_with_rpc_args(self, app, transport):
        app.dependencies.add_singleton(Database, lambda: Database("postgres://db"))

        @app.method("greet")
        def greet(name: str, db: Depends[Database]) -> str:
            return f"Hello {name} from {db.url}"  # type: ignore[attr-defined]

        await app.consume()
        result = await app.make("greet", name="World").invoke()
        assert result == "Hello World from postgres://db"

    async def test_kwds_override(self, app, transport):
        app.dependencies.add_singleton(Database, lambda: Database("preview"))

        @app.method("check")
        def check(db: Depends[Database]) -> str:
            if isinstance(db, Database):
                return f"DI:{db.url}"
            return f"override:{db}"

        await app.consume()

        result_di = await app.make("check").invoke()
        assert result_di == "DI:preview"

        result_override = await app.make("check", db="manual").invoke()
        assert result_override == "override:manual"

    async def test_no_deps_still_works(self, app, transport):

        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        result = await app.make("ping").invoke()
        assert result == "pong"

    async def test_handler_without_annotation(self, app, transport):
        app.dependencies.add_singleton(Database, lambda: Database("postgres://db"))

        @app.method("echo")
        def echo(value: str) -> str:
            return value

        await app.consume()
        result = await app.make("echo", value="hello").invoke()
        assert result == "hello"

    async def test_multiple_deps(self, app, transport):
        app.dependencies.add_singleton(Database, lambda: Database("postgres://db"))
        app.dependencies.add_singleton(Config, lambda: Config("production"))

        @app.method("status")
        def status(db: Depends[Database], cfg: Depends[Config]) -> str:
            return f"{db.url}/{cfg.env}"  # type: ignore[attr-defined]

        await app.consume()
        result = await app.make("status").invoke()
        assert result == "postgres://db/production"


class RequestContext:
    def __init__(self, req_id: str) -> None:
        self.req_id = req_id


class TestDiScoped:
    def test_scoped_outside_request_raises(self) -> None:
        c = DiContainer()
        c.add_scoped(RequestContext, lambda: RequestContext("123"))
        with pytest.raises(RuntimeError, match="Cannot resolve scoped dependency"):
            c.resolve(RequestContext)

    async def test_scoped_dependency_in_app(self, app, transport):
        app.dependencies.add_scoped(RequestContext, lambda: RequestContext(str(get_current_request().id)))

        @app.method("get_id")
        def get_id(ctx: Depends[RequestContext]) -> str:
            return ctx.req_id  # type: ignore[attr-defined]

        await app.consume()
        req = app.make("get_id")
        result = await req.invoke()
        assert result == str(req.request.id)
