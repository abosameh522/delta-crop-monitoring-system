"""Container healthcheck: the Prefect API answers and PREFECT_HOME is writable.

Used by ``docker compose`` as ``python -m delta_crops.orchestration.healthcheck``.
Exit 0 = healthy, 1 = unhealthy, and the reason is printed either way.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_API_URL = "http://127.0.0.1:4200/api"
DEFAULT_HOME = "/var/lib/prefect"

#: Indirection so tests can replace the HTTP call without touching urllib.
_open = urllib.request.urlopen


def check(api_url: str | None = None, home: str | None = None,
          timeout: float = 5.0) -> tuple[bool, str]:
    """Return (healthy, human-readable detail)."""
    home_path = Path(home or os.environ.get("PREFECT_HOME", DEFAULT_HOME))
    if not home_path.is_dir() or not os.access(home_path, os.W_OK):
        return False, f"PREFECT_HOME not writable: {home_path}"

    url = (api_url or os.environ.get("PREFECT_API_URL") or DEFAULT_API_URL).rstrip("/") + "/health"
    try:
        with _open(url, timeout=timeout) as response:
            body = response.read().decode("utf-8", "replace").strip()
            ok = response.status == 200 and body.lower().startswith("true")
            return ok, f"{url} -> {response.status} {body[:40]}"
    except (urllib.error.URLError, OSError, ValueError) as error:
        return False, f"{url} unreachable: {error}"


def main() -> int:
    healthy, detail = check()
    print(("healthy: " if healthy else "unhealthy: ") + detail)
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
