from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

import pandas as pd

Scope = Literal["global", "per_ticker"]


@dataclass(frozen=True)
class ForecastResult:
    """
    Standard output of any forecasting model.

    y_pred: predictions aligned with df rows used for prediction
    meta: additional artifacts/diagnostics (optional)
    """
    y_pred: pd.Series
    meta: dict


class Forecaster(Protocol):
    """
    Minimal interface every forecaster must implement.
    """
    name: str
    scope: Scope  # how this model is intended to be trained/run

    def fit(self, df: pd.DataFrame) -> "Forecaster": ...
    def predict(self, df: pd.DataFrame) -> ForecastResult: ...
