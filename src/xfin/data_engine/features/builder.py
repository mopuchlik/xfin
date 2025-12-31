from __future__ import annotations

import numpy as np
import pandas as pd
from dataclasses import dataclass

from xfin.data_engine.domain.models import BarField, OHLCVBar


@dataclass
class BarDatasetBuilder:
    add_basic_features: bool = True

    def to_dataframe(self, bars: list[OHLCVBar]) -> pd.DataFrame:
        rows = [
            {
                BarField.TICKER.value: b.ticker,
                BarField.DATE.value: b.dt,
                BarField.OPEN.value: b.open,
                BarField.HIGH.value: b.high,
                BarField.LOW.value: b.low,
                BarField.CLOSE.value: b.close,
                BarField.VOL.value: b.vol,
                BarField.OPENINT.value: getattr(b, "openint", float("nan")),
            }
            for b in bars
        ]
        df = pd.DataFrame(rows)
        if df.empty:
            return df

        df[BarField.DATE.value] = pd.to_datetime(df[BarField.DATE.value])
        df = df.sort_values([BarField.TICKER.value, BarField.DATE.value]).reset_index(drop=True)

        if self.add_basic_features:
            df = self._add_generic_features(df)

        return df

    def _add_generic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        t = BarField.TICKER.value
        d = BarField.DATE.value
        c = BarField.CLOSE.value
        v = BarField.VOL.value

        # # logdiff and pct change (example)
        # close = df[c]
        # prev_close = df.groupby(t)[c].shift(1)
        # df["log_ret_1d"] = np.log(close) - np.log(prev_close)
        # df["ret_1d"] = close / prev_close - 1.0
        # bad_price = (close <= 0) | (prev_close <= 0)
        # df.loc[bad_price, ["ret_1d", "log_ret_1d"]] = np.nan

        # other examples
        df["vol_chg_1d"] = df.groupby(t)[v].pct_change()
        df["month"] = df[d].dt.month
        df["weekday"] = df[d].dt.weekday
        return df
