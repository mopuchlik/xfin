from __future__ import annotations

import argparse
import asyncio
import logging  # NEW
import sys
from pathlib import Path

from fin_forecast.core.pipeline import BuildDatasetPipeline
from fin_forecast.logging_config import setup_logging  # NEW

logger = logging.getLogger(__name__)  # NEW


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--folder",
        type=str,
        default=None,
        help="Folder with .mst files (default: data/raw)",
    )
    p.add_argument("--pattern", type=str, default="*.mst")
    p.add_argument(
        "--out",
        type=str,
        default="data/processed/bars",
        help="Output directory for partitioned Parquet dataset (partitioned by ticker)",
    )

    # NEW: logging args
    p.add_argument(
        "--log-level",
        default="INFO",
        help="DEBUG, INFO, WARNING, ERROR",
    )
    p.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path, e.g. logs/run.log",
    )

    args = p.parse_args()

    # NEW: configure logging once, early
    try:
        setup_logging(level=args.log_level, log_file=args.log_file)
    except ValueError as e:
        print(f"Invalid log level: {e}", file=sys.stderr)
        return 2

    try:
        pipeline = BuildDatasetPipeline()

        folder = Path(args.folder) if args.folder else None
        out_path = Path(args.out)

        # Safety: for partitioned parquet, --out must be a directory, not a *.parquet file
        if out_path.suffix.lower() == ".parquet":
            raise ValueError(
                "--out must be a directory when writing a partitioned Parquet dataset. "
                "Example: --out data/processed/bars"
            )

        out_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Starting dataset build (folder=%s, pattern=%s, out=%s)",
            folder if folder else "data/raw (default in pipeline?)",
            args.pattern,
            out_path,
        )

        df = asyncio.run(pipeline.run(folder=folder, pattern=args.pattern))

        logger.info("Built dataframe: rows=%s, cols=%s", f"{len(df):,}", df.shape[1])

        # Write partitioned dataset
        df.to_parquet(
            out_path,
            index=False,
            partition_cols=["ticker"],
        )

        logger.info(
            "Saved partitioned dataset to %s (rows=%s, tickers=%s)",
            out_path,
            f"{len(df):,}",
            df["ticker"].nunique() if "ticker" in df.columns else "N/A",
        )
        return 0

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        return 130
    except FileNotFoundError as e:
        logger.error("File/folder not found: %s", e)
        return 2
    except ValueError as e:
        logger.error("Invalid arguments/data: %s", e)
        return 2
    except ImportError as e:
        # Common case: missing parquet engine (pyarrow/fastparquet)
        logger.error(
            "Import error: %s. If this is about Parquet, install an engine, e.g.: uv add pyarrow",
            e,
        )
        return 1
    except Exception:
        # NEW: logs stack trace automatically
        logger.exception("Unexpected error")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
