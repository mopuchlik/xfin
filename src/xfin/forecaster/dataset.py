from __future__ import annotations

from typing import Sequence

import pandas as pd


def select_tickers(df: pd.DataFrame, tickers: Sequence[str] | None) -> pd.DataFrame:
    """
    Filter dataset to a subset of tickers. If tickers is None/empty, return df as-is.
    """
    if not tickers:
        return df
    return df[df["ticker"].isin(list(tickers))].copy()


def prepare_supervised(
    df: pd.DataFrame,
    *,
    target_col: str = "close",
    horizon: int = 1,
) -> pd.DataFrame:
    """
    Create supervised target y = target_col shifted by -horizon per ticker.
    """
    required = {"ticker", "dt", target_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df = df.sort_values(["ticker", "dt"]).copy()
    df["y"] = df.groupby("ticker")[target_col].shift(-horizon)
    df = df.dropna(subset=["y"])
    return df
