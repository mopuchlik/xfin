from __future__ import annotations

import pandas as pd

from xfin.forecaster.models.base import ForecastResult


class NaiveLastClose:
    """
    Baseline persistence model: predict y as the current close.
    Useful as a sanity check and benchmark.

    Works in both modes; we mark it 'global' by default.
    """
    name = "naive_last_close"
    scope = "global"

    def fit(self, df: pd.DataFrame) -> "NaiveLastClose":
        return self  # no training

    def predict(self, df: pd.DataFrame) -> ForecastResult:
        if "close" not in df.columns:
            raise ValueError("Expected column 'close' in df")
        y_pred = df["close"].astype(float)
        return ForecastResult(y_pred=y_pred, meta={})
