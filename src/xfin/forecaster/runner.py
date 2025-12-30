from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

import pandas as pd

from xfin.forecaster.dataset import prepare_supervised, select_tickers
from xfin.forecaster.models.base import ForecastResult
from xfin.forecaster.registry import create_model

Mode = Literal["auto", "global", "per_ticker"]


@dataclass(frozen=True)
class RunConfig:
    model: str
    mode: Mode = "auto"
    tickers: Sequence[str] | None = None
    target_col: str = "close"
    horizon: int = 1


def run_forecast(df: pd.DataFrame, cfg: RunConfig):
    """
    Run forecasting in chosen mode.

    Returns:
      - ForecastResult (global)
      - dict[ticker, ForecastResult] (per_ticker)
    """
    df = select_tickers(df, cfg.tickers)
    df = prepare_supervised(df, target_col=cfg.target_col, horizon=cfg.horizon)

    model = create_model(cfg.model)

    mode: Mode = cfg.mode
    if mode == "auto":
        mode = "per_ticker" if getattr(model, "scope", "global") == "per_ticker" else "global"

    if mode == "global":
        model.fit(df)
        return model.predict(df)

    if mode == "per_ticker":
        out: dict[str, ForecastResult] = {}
        for tkr, g in df.groupby("ticker", sort=True):
            m = create_model(cfg.model)
            m.fit(g)
            out[tkr] = m.predict(g)
        return out

    raise ValueError(f"Unknown mode: {cfg.mode!r}")
