from __future__ import annotations

import pandas as pd


def mae(y_true: pd.Series, y_pred: pd.Series) -> float:
    return float((y_true.astype(float) - y_pred.astype(float)).abs().mean())
