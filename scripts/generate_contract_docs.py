#!/usr/bin/env python3
"""Generate contract demo documentation artifacts.

This script:
1. Loads the demo app from docs/examples/contract_demo_app.py (editable source).
2. Renders its contract as docs/reference/contract-demo.html.
3. Generates docs/reference/contract-demo.md embedding the demo source and the
   rendered contract (used by `make docs`).

Run via: PYTHONPATH=src python scripts/generate_contract_docs.py
(or `make docs`, which runs this script automatically).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEMO_APP_PATH = ROOT / "docs" / "examples" / "contract_demo_app.py"
HTML_OUT_PATH = ROOT / "docs" / "reference" / "contract-demo.html"
MD_OUT_PATH = ROOT / "docs" / "reference" / "contract-demo.md"


def load_demo_app():
    """Load and build the demo app from the editable documentation example."""
    src_path = ROOT / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    spec = importlib.util.spec_from_file_location("contract_demo_app", DEMO_APP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load demo app module: {DEMO_APP_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "build_demo_app"):
        raise RuntimeError("Demo app module must define build_demo_app()")
    return module.build_demo_app()


def read_demo_source() -> str:
    """Return demo app source as text for embedding in documentation."""
    return DEMO_APP_PATH.read_text(encoding="utf-8").rstrip() + "\n"


def generate_markdown(source: str) -> str:
    """Build the contract demo markdown page content."""
    return f"""# Contract demo

This page shows a generated contract for a demo `linkr` application with
2 RPC methods, 2 tasks, and 2 event subscriptions.

> Source of truth: `docs/examples/contract_demo_app.py`.
> This page and `contract-demo.html` are generated automatically by
> `scripts/generate_contract_docs.py` when running `make docs`.
> To update the demo or the contract, edit the source file and run `make docs`.

## Demo application

```python
{source}```

## Generated contract

<iframe
  src="contract-demo.html"
  title="Contract demo"
  style="width:100%; height:1000px; border:1px solid #e4e7ec; border-radius:8px; background:#fff;"
  loading="lazy">
</iframe>

If the embedded view does not load, open [contract-demo.html](contract-demo.html) directly.
"""


def main() -> int:
    app = load_demo_app()
    contract_html = app.describe_html(title="Contract — demo (example)")

    HTML_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    MD_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    HTML_OUT_PATH.write_text(contract_html, encoding="utf-8")
    MD_OUT_PATH.write_text(generate_markdown(read_demo_source()), encoding="utf-8")

    rel_html = HTML_OUT_PATH.relative_to(ROOT)
    rel_md = MD_OUT_PATH.relative_to(ROOT)
    print(f"Generated {rel_html}")
    print(f"Generated {rel_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
