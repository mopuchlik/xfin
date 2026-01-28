from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds


def _ensure_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Path not found: {path}")


def _parse_columns(columns_csv: str | None) -> list[str] | None:
    if not columns_csv:
        return None
    cols = [c.strip() for c in columns_csv.split(",") if c.strip()]
    return cols or None


def _discover_tickers(dataset: ds.Dataset) -> list[str]:
    """
    Discover unique tickers from a hive-partitioned dataset.

    With partitioning="hive", partition keys like ticker=MBANK are materialized as a
    virtual column 'ticker' during scanning, so we can read only that column cheaply.
    """
    tbl = dataset.to_table(columns=["ticker"])
    vals = pc.unique(tbl["ticker"]).to_pylist()
    return sorted([v for v in vals if v is not None])


def load_partitioned_bars_arrow(
    root: Path,
    *,
    tickers: Sequence[str] | None = None,
    max_tickers: int | None = None,
    columns: Iterable[str] | None = None,
    date_min: str | None = None,
    date_max: str | None = None,
) -> pd.DataFrame:
    """
    Load hive-partitioned Parquet bars dataset using pyarrow.dataset.
    """
    _ensure_exists(root)

    dataset = ds.dataset(str(root), format="parquet", partitioning="hive")

    if tickers:
        chosen = list(tickers)
    else:
        chosen = _discover_tickers(dataset)
        if max_tickers is not None:
            chosen = chosen[:max_tickers]

    filt = None

    if chosen:
        expr = ds.field("ticker").isin(chosen)
        filt = expr if filt is None else (filt & expr)

    if date_min:
        dt_min = pd.to_datetime(date_min)
        expr = ds.field("dt") >= pa.scalar(dt_min.to_datetime64())
        filt = expr if filt is None else (filt & expr)

    if date_max:
        dt_max = pd.to_datetime(date_max)
        expr = ds.field("dt") <= pa.scalar(dt_max.to_datetime64())
        filt = expr if filt is None else (filt & expr)

    if columns is not None:
        cols = list(dict.fromkeys(list(columns) + ["ticker"]))
        if (date_min or date_max) and "dt" not in cols:
            cols.append("dt")
    else:
        cols = None

    table = (
        dataset.to_table(filter=filt, columns=cols)
        if filt is not None
        else dataset.to_table(columns=cols)
    )
    df = table.to_pandas()

    if "dt" in df.columns:
        df["dt"] = pd.to_datetime(df["dt"])

    required = {"ticker", "dt"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    return df


def save_stacked_parquet(df: pd.DataFrame, out: Path, *, index: bool = False) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=index)


def save_stacked_csv(df: pd.DataFrame, out: Path, *, index: bool = False) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=index)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Load partitioned bars dataset (pyarrow) and optionally save stacked parquet/csv."
    )
    p.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Root of hive-partitioned dataset, e.g. data/processed/bars",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output parquet file path (optional)",
    )
    p.add_argument(
        "--out-csv",
        type=Path,
        default=None,
        help="Output CSV file path (optional)",
    )
    p.add_argument(
        "--date-min",
        type=str,
        default=None,
        help="Inclusive min date for dt, e.g. 2024-01-01",
    )
    p.add_argument(
        "--date-max",
        type=str,
        default=None,
        help="Inclusive max date for dt, e.g. 2024-12-31",
    )
    p.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers, e.g. MBANK,PKN",
    )
    p.add_argument(
        "--max-tickers",
        type=int,
        default=None,
        help="Load only first N tickers (lexicographic)",
    )
    p.add_argument(
        "--columns",
        type=str,
        default=None,
        help="Comma-separated columns to load, e.g. dt,close,vol",
    )
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)

    tickers = None
    if args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]

    columns = _parse_columns(args.columns)

    df = load_partitioned_bars_arrow(
        args.root,
        tickers=tickers,
        max_tickers=args.max_tickers,
        columns=columns,
        date_min=args.date_min,
        date_max=args.date_max,
    )

    print("Loaded:", df.shape)
    print(df.head())

    if args.out is not None:
        save_stacked_parquet(df, args.out)
        print(f"Saved parquet to: {args.out}")

    if args.out_csv is not None:
        save_stacked_csv(df, args.out_csv)
        print(f"Saved CSV to: {args.out_csv}")


if __name__ == "__main__":
    main()
