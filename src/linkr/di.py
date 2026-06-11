from __future__ import annotations

from collections.abc import Callable
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Depends(Generic[T]):
    """
    Marker for dependency injection in handler parameters.

    Use ``Depends[T]`` as a type hint on handler parameters to signal
    that the value should be resolved from the :class:`DiContainer`
    rather than from the RPC arguments.

    Example::

        def get_user(user_id: int, db: Depends[Database]) -> dict:
            ...
    """


class DiContainer:
    """
    Registry of dependency types.

    Stores factories for singleton and transient dependencies.
    Singleton factories are called once and the result is cached.
    Transient factories produce a new instance on every resolution.

    Example::

        container = DiContainer()
        container.add_singleton(Database, lambda: Database("postgres://..."))
        container.add_transient(RequestContext, create_ctx)
    """

    def __init__(self) -> None:
        """
        Initialize empty container.

        Attributes:
            _singletons: Cached singleton instances.
            _singleton_factories: Factories for singletons not yet resolved.
            _transients: Factories for transient dependencies.
        """
        self._singletons: dict[type, Any] = {}
        self._singleton_factories: dict[type, Callable[[], Any]] = {}
        self._transients: dict[type, Callable[[], Any]] = {}

    def add_singleton(self, type_: type, factory: Callable[[], Any]) -> None:
        """
        Register a singleton dependency.

        The factory is called once on first resolve and the result is
        cached for all future resolutions.

        Args:
            type_: The dependency type used in Depends[T].
            factory: A zero-argument callable that produces the instance.
        """
        self._singleton_factories[type_] = factory

    def add_transient(self, type_: type, factory: Callable[[], Any]) -> None:
        """
        Register a transient dependency.

        A new instance is created by calling the factory every time the
        dependency is resolved.

        Args:
            type_: The dependency type used in Depends[T].
            factory: A zero-argument callable that produces the instance.
        """
        self._transients[type_] = factory

    def resolve(self, type_: type) -> Any:
        """
        Resolve a dependency by type.

        Args:
            type_: The type to resolve.

        Returns:
            The resolved dependency instance.

        Raises:
            KeyError: If no dependency is registered for *type_*.
        """
        if type_ in self._singletons:
            return self._singletons[type_]

        if type_ in self._singleton_factories:
            instance = self._singleton_factories[type_]()
            self._singletons[type_] = instance
            del self._singleton_factories[type_]
            return instance

        factory = self._transients.get(type_)
        if factory is None:
            raise KeyError(f"No dependency registered for {type_}")
        return factory()
