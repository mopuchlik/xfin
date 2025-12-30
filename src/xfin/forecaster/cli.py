from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from xfin.data_engine.logging_config import setup_logging
from xfin.forecaster.diagnostics import mae
from xfin.forecaster.registry import list_models
from xfin.forecaster.runner import RunConfig, run_forecast

logger = logging.getLogger(__name__)


def _parse_tickers(s: str | None) -> list[str] | None:
    if not s:
        return None
    return [t.strip() for t in s.split(",") if t.strip()]


def main() -> int:
    p = argparse.ArgumentParser(prog="xfin-forecast")
    p.add_argument("--data", default="data/processed/bars", help="Processed dataset root (partitioned parquet)")
    p.add_argument("--model", default="naive_last_close", choices=list_models())
    p.add_argument("--mode", default="auto", choices=["auto", "global", "per_ticker"])
    p.add_argument("--tickers", default=None, help="Comma-separated tickers, e.g. AAA,BBB (optional)")
    p.add_argument("--horizon", type=int, default=1)

    p.add_argument("--log-level", default="INFO")
    p.add_argument("--log-file", default=None)
    args = p.parse_args()

    try:
        setup_logging(level=args.log_level, log_file=args.log_file)
    except ValueError as e:
        print(f"Invalid log level: {e}", file=sys.stderr)
        return 2

    data_root = Path(args.data)
    if not data_root.exists():
        logger.error("Data path does not exist: %s", data_root)
        return 2

    logger.info("Loading dataset: %s", data_root)
    df = pd.read_parquet(data_root)
    df["dt"] = pd.to_datetime(df["dt"])

    cfg = RunConfig(
        model=args.model,
        mode=args.mode,
        tickers=_parse_tickers(args.tickers),
        horizon=args.horizon,
    )

    result = run_forecast(df, cfg)

    # Print a minimal metric
    if isinstance(result, dict):
        for tkr, r in result.items():
            # Note: y_true is in the supervised df inside runner; for now keep CLI minimal.
            logger.info("Predicted %s rows for ticker=%s", len(r.y_pred), tkr)
        return 0
    else:
        # Same note as above: metric wiring comes next (we can return y_true too).
        logger.info("Predicted %s rows (global)", len(result.y_pred))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
