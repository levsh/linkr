from __future__ import annotations

import json

from linkr import App, LocalBackend
from linkr.contracts import render_contract_html


def build_demo_app() -> App:
    app = App("demo", backend=LocalBackend())
    root = app.server()
    math = app.server("math")
    users = app.server("users")
    email = app.server("email")
    reports = app.server("reports")

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

    @root.subscribe("user.created")
    def on_user_created(user_id: int, email: str) -> None:
        """Handle user creation events."""
        return None

    @root.subscribe("order.#")
    def on_order_event(order_id: str, amount: float | None = None) -> None:
        """Handle all order-related events."""
        return None

    app.expose_describe(server=root)
    return app


def main() -> None:
    app = build_demo_app()
    contract = app.describe()
    print(json.dumps(contract.to_dict(), indent=2, ensure_ascii=False))

    html_doc = contract.to_html()
    with open("contract-demo.html", "w", encoding="utf-8") as f:
        f.write(html_doc)
    # equivalent: render_contract_html(contract)
    print("Wrote contract-demo.html")


if __name__ == "__main__":
    main()
