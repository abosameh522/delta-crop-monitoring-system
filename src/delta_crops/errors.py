"""Stable command exit contract exceptions.

Exit codes:
  0  success
  1  unexpected error
  2  argparse usage error (built-in)
  3  transient failure (network, STAC API, DuckDB lock) — retryable
  4  asset/date failure (ingestion asset, silver date) — not retryable
  10 critical quality failure (schema, NDVI range, dup keys, row count, missing partition) — not retryable
  11 WeeklyGateFailure (Gold gate exceeded) — not retryable
"""

class TransientFailure(RuntimeError):
    """Truly transient errors: network, STAC API timeout, DuckDB lock."""
    exit_code = 3

class AssetFailure(RuntimeError):
    """One or more Bronze assets failed to download after retries."""
    exit_code = 4

class SilverDateFailure(RuntimeError):
    """One or more Silver dates failed to process."""
    exit_code = 4

class QualityFailure(RuntimeError):
    """Critical Silver quality check failed."""
    exit_code = 10

class WeeklyGateFailure(RuntimeError):
    """Gold weekly NULL share exceeded the configured cap."""
    exit_code = 11


def to_exit_code(exc: BaseException) -> int:
    """Map an exception to its contracted exit code."""
    for cls in type(exc).__mro__:
        code = getattr(cls, "exit_code", None)
        if code is not None:
            return code
    return 1
