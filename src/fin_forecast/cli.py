from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from fin_forecast.core.pipeline import BuildDatasetPipeline


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
    args = p.parse_args()

    try:
        pipeline = BuildDatasetPipeline()

        folder = Path(args.folder) if args.folder else None
        df = asyncio.run(pipeline.run(folder=folder, pattern=args.pattern))

        out_path = Path(args.out)

        # Safety: for partitioned parquet, --out must be a directory, not a *.parquet file
        if out_path.suffix.lower() == ".parquet":
            raise ValueError(
                "--out must be a directory when writing a partitioned Parquet dataset. "
                "Example: --out data/processed/bars"
            )

        out_path.mkdir(parents=True, exist_ok=True)

        # Write partitioned dataset
        df.to_parquet(
            out_path,
            index=False,
            partition_cols=["ticker"],
        )

        print(
            f"Saved partitioned dataset to {out_path} "
            f"(rows={len(df):,}, tickers={df['ticker'].nunique()})"
        )
        return 0

    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    except FileNotFoundError as e:
        print(f"File/folder not found: {e}", file=sys.stderr)
        return 2
    except ValueError as e:
        print(f"Invalid arguments/data: {e}", file=sys.stderr)
        return 2
    except ImportError as e:
        # Common case: missing parquet engine (pyarrow/fastparquet)
        print(
            f"Import error: {e}\n"
            "If this is about Parquet, install a Parquet engine, e.g.: uv add pyarrow",
            file=sys.stderr,
        )
        return 1
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
