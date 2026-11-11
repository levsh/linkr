from __future__ import annotations

from typing import Any

from linkr import App


async def call(app: App, method: str, *args: Any, **kwds: Any) -> Any:
    """Call an RPC method using the canonical ``args``/``kwds`` API."""
    return await app.call(method, args, kwds)


async def send(app: App, task: str, *args: Any, **kwds: Any) -> None:
    """Send a task using the canonical ``args``/``kwds`` API."""
    await app.send(task, args, kwds)


async def emit(app: App, topic: str, **event: Any) -> None:
    """Emit an event using a mapping payload."""
    await app.emit(topic, event)
