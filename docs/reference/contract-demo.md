# Contract demo

This page shows a generated contract for a demo `linkr` application with
2 RPC methods, 2 tasks, and 2 event subscriptions.

> Source of truth: `docs/examples/contract_demo_app.py`.
> This page and `contract-demo.html` are generated automatically by
> `scripts/generate_contract_docs.py` when running `make docs`.
> To update the demo or the contract, edit the source file and run `make docs`.

## Demo application

```python
from __future__ import annotations

import json

from linkr import App, LocalBackend
from linkr.contracts import describe_app, expose_describe
from linkr.middleware.gzip import GzipMiddleware


def build_demo_app() -> App:
    app = App("demo", backend=LocalBackend())
    math = app.rpc("math")
    users = app.rpc("users")
    email = app.worker("email")
    reports = app.worker("reports", validate_types=True, middlewares=[GzipMiddleware()])
    bus = app.bus()

    @math.method("add")
    def add(x: int, y: int) -> int:
        """Add two numbers and return the sum."""
        return x + y

    @users.method("get")
    def get_user(user_id: int, include_deleted: bool = False) -> dict[str, int | bool]:
        """Return a user payload by id."""
        return {"user_id": user_id, "include_deleted": include_deleted}

    @email.task("send")
    def send_email(user_id: int, template: str = "welcome") -> None:
        """Send an email to a user (fire-and-forget)."""
        return None

    @reports.task("generate")
    def generate_report(report_id: str, force: bool = False) -> None:
        """Generate a report asynchronously."""
        return None

    @bus.subscribe("user.created")
    def on_user_created(user_id: int, email: str) -> None:
        """Handle user creation events."""
        return None

    @bus.subscribe("order.#")
    def on_order_event(order_id: str, amount: float | None = None) -> None:
        """Handle all order-related events."""
        return None

    expose_describe(app)
    return app


def main() -> None:
    app = build_demo_app()
    contract = describe_app(app)
    print(json.dumps(contract.to_dict(), indent=2, ensure_ascii=False))

    html_doc = contract.to_html()
    with open("contract-demo.html", "w", encoding="utf-8") as f:
        f.write(html_doc)
    print("Wrote contract-demo.html")


if __name__ == "__main__":
    main()
```

## Generated contract

<iframe
  src="contract-demo.html"
  title="Contract demo"
  style="width:100%; height:1000px; border:1px solid #e4e7ec; border-radius:8px; background:#fff;"
  loading="lazy">
</iframe>

If the embedded view does not load, open [contract-demo.html](contract-demo.html) directly.
