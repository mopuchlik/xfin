from pathlib import Path
from typing import Optional, Iterable

import pyarrow.dataset as ds
import pyarrow.compute as pc
import pandas as pd


def load_partitioned_bars_arrow(
    root: Path,
    *,
    max_tickers: Optional[int] = None,
    columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Load a partitioned Parquet dataset using pyarrow.dataset.

    Parameters
    ----------
    root : Path
        Root directory of the dataset (partitioned by ticker).
    max_tickers : int | None
        If provided, load only the first N tickers (sorted lexicographically).
    columns : iterable[str] | None
        Columns to load (partition columns like 'ticker' are auto-included).

    Returns
    -------
    pd.DataFrame
    """
    if not root.exists():
        raise FileNotFoundError(root)

    dataset = ds.dataset(
        root,
        format="parquet",
        partitioning="hive",  # crucial: understands ticker=AAA
    )

    # Determine available tickers (from partition metadata)
    tickers = sorted(
        pc.unique(dataset.to_table(columns=["ticker"])["ticker"]).to_pylist()
    )

    if max_tickers is not None:
        tickers = tickers[:max_tickers]

    # Build a filter: ticker in {AAA, BBB, ...}
    filter_expr = pc.field("ticker").isin(tickers)

    table = dataset.to_table(
        filter=filter_expr,
        columns=columns,
    )

    return table.to_pandas()


def main():
    root = Path("data/processed/bars")

    df = load_partitioned_bars_arrow(
        root,
        max_tickers=10,
        columns=["ticker", "dt", "close"],  # ticker is auto-added
    )

    df["dt"] = pd.to_datetime(df["dt"])

    date_range_df = (
        df.groupby("ticker", as_index=False)
        .agg(
            min_date=("dt", "min"),
            max_date=("dt", "max"),
            n_obs=("dt", "count"),
        )
        .sort_values("ticker")
    )
    print("---------------\n")
    print(df.head())
    print("---------------\n")
    print(date_range_df.head())
    print("---------------\n")
    print(df.info())


if __name__ == "__main__":
    main()
