import pandas as pd
from pathlib import Path


def main():
    path = Path("data/processed/bars.parquet")

    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_parquet(path)

    # ensure datetime dtype (should already be, but safe)
    df["date"] = pd.to_datetime(df["date"])

    # aggregate min / max date per ticker
    date_range_df = (
        df.groupby("ticker", as_index=False)
        .agg(
            min_date=("date", "min"),
            max_date=("date", "max"),
            n_obs=("date", "count"),
        )
        .sort_values("ticker")
    )

    print("\n----------------")
    print(df.head())
    print("\n----------------")
    print(date_range_df.head())
    print("\n----------------")
    print(df.info())

if __name__ == "__main__":
    main()
