from datetime import date

import pandas as pd

from xfin.data_engine.domain.models import OHLCVBar
from xfin.data_engine.features.builder import BarDatasetBuilder


def test_builder_creates_dataframe():
    bars = [
        OHLCVBar("AAA", date(2024, 1, 1), 10, 11, 9, 10, 100),
        OHLCVBar("AAA", date(2024, 1, 2), 10, 12, 9, 11, 150),
    ]

    builder = BarDatasetBuilder(add_basic_features=False)
    df = builder.to_dataframe(bars)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df["ticker"].unique()) == ["AAA"]
    assert df.iloc[0]["close"] == 10


def test_builder_adds_features():
    bars = [
        OHLCVBar("AAA", date(2024, 1, 1), 10, 11, 9, 10, 100),
        OHLCVBar("AAA", date(2024, 1, 2), 10, 12, 9, 11, 150),
    ]

    builder = BarDatasetBuilder(add_basic_features=True)
    df = builder.to_dataframe(bars)

    # assert "ret_1d" in df.columns
    # assert "log_ret_1d" in df.columns
    assert "vol_chg_1d" in df.columns
    # assert pd.isna(df.loc[0, "ret_1d"])
    # assert df.loc[1, "ret_1d"] > 0
