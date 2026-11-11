"""Run the scheduled pipeline once: ``python -m delta_crops.orchestration``.

This is the manual/scheduled entry point. By default it processes only
configured years whose season is still in progress; with none it logs a no-op.
A complete year is processed only with an explicit ``--reprocess-year``.
"""

from __future__ import annotations

import argparse

from delta_crops.config import data_dir
from delta_crops.orchestration.flow import DEFAULT_RETRIES, scheduled_flow


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--config", default=None)
    parser.add_argument("--reprocess-year", type=int, default=None)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=1.0, help="base exponential delay in seconds")
    args = parser.parse_args(argv)

    delays = [args.retry_delay * (2 ** attempt) for attempt in range(args.retries)]
    result = scheduled_flow(
        data_dir=str(args.data_dir or data_dir()),
        config_path=args.config,
        reprocess_year=args.reprocess_year,
        retries=args.retries,
        retry_delays=delays,
    )
    print(f"Pipeline run: {result['status']}")
    return 0 if result["status"] in {"ok", "no_op"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
