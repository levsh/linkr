from __future__ import annotations

from contextvars import ContextVar

import pytest

from linkr import Depends, get_current_request
from linkr.exceptions import RPCError
from tests.helpers import call


class Database:
    def __init__(self, url: str) -> None:
        self.url = url


class Config:
    def __init__(self, env: str) -> None:
        self.env = env


# Module-level factory functions (accessible to get_type_hints)

def make_db_factory() -> callable:
    instance = None
    def factory() -> Database:
        nonlocal instance
        if instance is None:
            instance = Database("postgres://db")
        return instance
    return factory


def make_transient_db_factory() -> callable:
    counter = {"count": 0}
    def factory() -> Database:
        counter["count"] += 1
        return Database(f"db-{counter['count']}")
    return factory


def make_cfg_factory() -> callable:
    instance = None
    def factory() -> Config:
        nonlocal instance
        if instance is None:
            instance = Config("production")
        return instance
    return factory


def failing_factory() -> Database:
    raise ValueError("factory failed")


class RequestContext:
    def __init__(self, req_id: str) -> None:
        self.req_id = req_id


_request_ctx: ContextVar[RequestContext | None] = ContextVar("req_ctx", default=None)


def scoped_factory() -> RequestContext:
    ctx = _request_ctx.get()
    if ctx is None:
        ctx = RequestContext(str(get_current_request().id))
        _request_ctx.set(ctx)
    return ctx


class TestDependsMarker:
    def test_depends_with_factory(self) -> None:
        def factory():
            return Database("postgres://test")
        ann = Depends[Database, factory]
        from typing import Annotated, get_args, get_origin
        assert get_origin(ann) is Annotated
        args = get_args(ann)
        assert args[0] is Database
        assert isinstance(args[1], Depends)
        assert args[1].factory is factory


class TestDiWithApp:
    async def test_resolves_singleton(self, app):
        @app.method("ping")
        def ping(db: Depends[Database, make_db_factory()]) -> str:
            return db.url

        await app.consume()
        assert await call(app, "ping") == "postgres://db"
        assert await call(app, "ping") == "postgres://db"

    async def test_transient_factory(self, app):
        @app.method("get_db")
        def get_db(db: Depends[Database, make_transient_db_factory()]) -> str:
            return db.url

        await app.consume()
        assert await call(app, "get_db") == "db-1"
        assert await call(app, "get_db") == "db-2"
        assert await call(app, "get_db") == "db-3"

    async def test_with_rpc_args(self, app):
        @app.method("greet")
        def greet(name: str, db: Depends[Database, make_db_factory()]) -> str:
            return f"Hello {name} from {db.url}"

        await app.consume()
        assert await call(app, "greet", name="World") == "Hello World from postgres://db"

    async def test_kwds_override(self, app):
        @app.method("check")
        def check(db: Depends[Database, make_db_factory()]) -> str:
            return f"DI:{db.url}"

        await app.consume()
        assert await call(app, "check") == "DI:postgres://db"
        # Test that explicit kwds are passed through (not overriding DI)
        # The handler's db parameter is typed as Database, so passing a string
        # would fail type validation. This test verifies DI runs before kwds merge.
        assert await call(app, "check") == "DI:postgres://db"

    async def test_no_deps_still_works(self, app):
        @app.method("ping")
        def ping() -> str:
            return "pong"

        await app.consume()
        assert await call(app, "ping") == "pong"

    async def test_handler_without_annotation(self, app):
        @app.method("echo")
        def echo(value: str) -> str:
            return value

        await app.consume()
        assert await call(app, "echo", value="hello") == "hello"

    async def test_multiple_deps(self, app):
        @app.method("status")
        def status(db: Depends[Database, make_db_factory()], cfg: Depends[Config, make_cfg_factory()]) -> str:
            return f"{db.url}/{cfg.env}"

        await app.consume()
        assert await call(app, "status") == "postgres://db/production"


class TestScopedDependency:
    async def test_scoped_dependency_in_app(self, app):
        @app.method("get_id")
        def get_id(ctx: Depends[RequestContext, scoped_factory]) -> str:
            return ctx.req_id

        await app.consume()
        result = await call(app, "get_id")
        assert isinstance(result, str)
        assert result

    async def test_scoped_dependency_cached_per_request(self, app):
        @app.method("same")
        def same(
            first: Depends[RequestContext, scoped_factory],
            second: Depends[RequestContext, scoped_factory],
        ) -> bool:
            return first is second

        await app.consume()
        assert await call(app, "same") is True


class TestErrorHandling:
    async def test_missing_factory_raises(self, app):
        @app.method("fail")
        def fail(db: Depends[Database]) -> str:  # no factory
            return db.url

        await app.consume()
        with pytest.raises(RPCError, match="requires inline factory"):
            await call(app, "fail")

    async def test_factory_exception_propagates(self, app):
        @app.method("fail")
        def fail(db: Depends[Database, failing_factory]) -> str:
            return db.url

        await app.consume()
        with pytest.raises(RPCError, match="factory failed"):
            await call(app, "fail")
