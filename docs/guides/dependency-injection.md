# Dependency injection

`linkr` supports injecting dependencies into handlers (RPC, task, event) via `Depends` annotations and provides access to the current request via `get_current_request()`.

## `Depends`

`Depends[T, factory]` is used in handler parameter annotations (as `Annotated[T, Depends(factory)]` sugar). The `factory` is a callable invoked to produce the dependency value.

```python
from linkr import App, Depends, LocalBackend


class Database:
    def __init__(self, url: str) -> None:
        self.url = url


db_factory = lambda: Database("postgres://localhost/app")

app = App("orders", backend=LocalBackend())

@app.method("ping")
def ping(db: Depends[Database, db_factory]) -> str:
    return db.url
```

### Resolution behavior

- Dependencies are resolved when a handler is executed.
- The provided factory is called to obtain the value for each injection point (per handler invocation) as defined by the app's dependency resolution.
- `Depends` without a factory is representable, but a usable factory should be provided for automatic resolution.

### Factory patterns

**Transient (new instance per use, depending on factory):**

```python
@app.method("op")
def op(db: Depends[Database, lambda: Database("postgres://...")]) -> str:
    return db.url
```

**Singleton-like (factory-managed cache):**

```python
from collections.abc import Callable

def singleton_db_factory() -> Callable[[], Database]:
    instance: Database | None = None

    def factory() -> Database:
        nonlocal instance
        if instance is None:
            instance = Database("postgres://...")
        return instance

    return factory

get_db = singleton_db_factory()

@app.method("op")
def op(db: Depends[Database, get_db]) -> str:
    return db.url
```

**Callable object factory:**

```python
class DatabaseFactory:
    def __init__(self, url: str) -> None:
        self.url = url
        self._instance: Database | None = None

    def __call__(self) -> Database:
        if self._instance is None:
            self._instance = Database(self.url)
        return self._instance

db_factory = DatabaseFactory("postgres://...")

@app.task("job")
async def job(db: Depends[Database, db_factory]) -> None:
    ...
```

## `get_current_request()`

Returns the `Request` currently being dispatched in the active async context.

```python
from linkr import get_current_request

@app.method("whoami")
def whoami() -> str:
    req = get_current_request()
    return str(req.id)
```

- Available inside handler execution (RPC/task/event) while a request is active.
- Raises `RuntimeError` if called outside an active request context.

## Works across modes

Dependencies and request context are available to:

- `@app.method(...)` handlers
- `@app.task(...)` handlers
- `@app.subscribe(...)` handlers

## Notes

- Dependency injection is driven by type annotations (`Annotated[..., Depends(...)]`); only annotated dependency parameters are treated as injected.
- For argument validation of handler inputs, see [Error handling](error-handling.md) (`validate_types`).
