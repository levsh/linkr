# Contract demo

This page shows a generated contract for a demo `linkr` application with
2 RPC methods, 2 tasks, and 2 event subscriptions.

> Source of truth: `docs/examples/contract_demo_app.py`.
> This page and `contract-demo.html` are generated automatically by
> `scripts/generate_contract_docs.py` when running `make docs`.
> To update the demo or the contract, edit the source file and run `make docs`.

## Demo application

```python
"""Demo application used to generate a contract example for the documentation.

Update this file and run ``make docs`` to regenerate:
- docs/reference/contract-demo.md
- docs/reference/contract-demo.html
"""

from __future__ import annotations

from linkr import App, LocalBackend


def build_demo_app() -> App:
    """Build and return the demo app (2 RPC methods, 2 tasks, 2 event subscriptions)."""
    app = App("demo", backend=LocalBackend())

    @app.method("math.add")
    def add(x: int, y: int) -> int:
        """Add two integers and return the sum."""
        return x + y

    @app.method("users.get")
    def get_user(user_id: int) -> dict[str, object]:
        """Return user information by identifier."""
        return {"id": user_id, "name": "alice"}

    @app.task("email.send")
    def send_email(to: str, subject: str, body: str) -> None:
        """Send an email message (fire-and-forget task)."""

    @app.task("reports.generate")
    def generate_report(report_id: int, full: bool = False) -> None:
        """Generate a report by id."""

    @app.subscribe("user.created")
    def on_user_created(user_id: int, email: str) -> None:
        """Handle a newly created user event."""

    @app.subscribe("order.#")
    def on_order_event(order_id: int, status: str) -> None:
        """Handle order lifecycle events."""

    # Expose contract over RPC as part of the demo (optional but illustrative).
    app.expose_describe("contracts")

    return app
```

## Generated contract

<iframe
  src="contract-demo.html"
  title="Contract demo"
  style="width:100%; height:1000px; border:1px solid #e4e7ec; border-radius:8px; background:#fff;"
  loading="lazy">
</iframe>

If the embedded view does not load, open [contract-demo.html](contract-demo.html) directly.
