"""Container healthcheck: the Prefect API answers, and the deployment is live.

Two modes, both with ``--api-url``:

  server        --home /var/lib/prefect
                API reachable and its state directory writable.
  orchestrator  --deployment <flow-name>/<deployment-name>
                API reachable and the deployment exists and is not paused.

What this proves and does not prove is documented in
``docs/phase6b-docker.md``: it is not a heartbeat. A wedged runner that still
has a registered, unpaused deployment would report healthy.
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_API_URL = "http://127.0.0.1:4200/api"

#: Indirection so tests can replace the HTTP call without touching urllib.
_open = urllib.request.urlopen


def _get(url: str, timeout: float) -> tuple[int, bytes]:
    with _open(url, timeout=timeout) as response:
        return response.status, response.read()


def check(api_url: str | None = None, home: str | None = None,
          deployment: str | None = None, timeout: float = 5.0) -> tuple[bool, str]:
    """Return (healthy, human-readable detail)."""
    if home is not None:
        path = Path(home)
        if not path.is_dir() or not os.access(path, os.W_OK):
            return False, f"PREFECT_HOME not writable: {path}"

    base = (api_url or os.environ.get("PREFECT_API_URL") or DEFAULT_API_URL).rstrip("/")
    try:
        status, body = _get(f"{base}/health", timeout)
    except (urllib.error.URLError, OSError, ValueError) as error:
        return False, f"{base}/health unreachable: {error}"
    text = body.decode("utf-8", "replace").strip()
    if status != 200 or not text.lower().startswith("true"):
        return False, f"{base}/health -> {status} {text[:40]}"

    if not deployment:
        return True, f"{base}/health -> {status} {text[:20]}"

    try:
        status, body = _get(f"{base}/deployments/name/{deployment}", timeout)
    except (urllib.error.URLError, OSError, ValueError) as error:
        return False, f"deployment {deployment} unreadable: {error}"
    if status != 200:
        return False, f"deployment {deployment} -> {status} (not registered yet)"
    try:
        payload: dict[str, Any] = json.loads(body)
    except json.JSONDecodeError as error:
        return False, f"deployment {deployment} returned invalid JSON: {error}"
    if payload.get("paused"):
        return False, f"deployment {deployment} is paused"
    return True, f"{base} healthy; deployment {deployment} registered and active"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=None,
                        help="Prefect API base URL (default: $PREFECT_API_URL)")
    parser.add_argument("--home", default=None, help="require this PREFECT_HOME to be writable")
    parser.add_argument("--deployment", default=None,
                        help="require <FLOW_NAME>/<DEPLOYMENT_NAME> to exist and be unpaused")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args(argv)

    healthy, detail = check(args.api_url, args.home, args.deployment, args.timeout)
    print(("healthy: " if healthy else "unhealthy: ") + detail)
    return 0 if healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
