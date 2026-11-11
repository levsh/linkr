from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Any, Generic, TypeVar

T = TypeVar("T")


class Depends(Generic[T]):
    """
    Dependency injection marker with optional inline factory.

    Use ``Depends[Type, factory]`` as a type hint on handler parameters.
    The factory is called to produce the dependency instance.

    Examples::

        # Transient - new instance each call
        def handler(db: Depends[Database, lambda: Database("postgres://...")]) -> None: ...

        # Singleton - factory with internal cache
        def make_singleton() -> Callable[..., Database]:
            instance = None
            def factory():
                nonlocal instance
                if instance is None:
                    instance = Database("postgres://...")
                return instance
            return factory
        def handler(db: Depends[Database, make_singleton()]) -> None: ...

        # Class factory (callable object)
        class DatabaseFactory:
            def __init__(self, url: str):
                self.url = url
                self._instance = None
            def __call__(self) -> Database:
                if self._instance is None:
                    self._instance = Database(self.url)
                return self._instance
        def handler(db: Depends[Database, DatabaseFactory("postgres://...")]) -> None: ...
    """

    def __init__(self, factory: Callable[[], T] | None = None) -> None:
        """
        Args:
            factory: Optional callable that produces the dependency instance.
                When None, the dependency must be resolved another way
                (e.g. via an external container).
        """
        self.factory = factory

    def __class_getitem__(cls, item: type | tuple[type, Callable[[], Any] | None]) -> Annotated[Any, Any]:
        if isinstance(item, tuple):
            type_ = item[0]
            factory = item[1] if len(item) > 1 else None
            return Annotated[type_, cls(factory)]
        return Annotated[item, cls(None)]

    def __repr__(self) -> str:
        return f"Depends[factory={self.factory is not None}]"
