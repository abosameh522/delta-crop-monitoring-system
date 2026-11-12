"""Console entry point: ``delta-crops-dashboard``.

Thin wrapper that hands Streamlit the app file and the user's extra arguments,
so the dashboard is one command from the repository root:

    delta-crops-dashboard              # http://localhost:8501
    delta-crops-dashboard --server.port 8600
"""

from __future__ import annotations

import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    from streamlit.web import cli as streamlit_cli

    app = Path(__file__).with_name("app.py")
    extra = list(sys.argv[1:] if argv is None else argv)
    sys.argv = ["streamlit", "run", str(app), "--server.headless=true", *extra]
    return int(streamlit_cli.main() or 0)


if __name__ == "__main__":
    raise SystemExit(main())
