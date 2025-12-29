import asyncio
from pathlib import Path

import pandas as pd

from fin_forecast.core.pipeline import BuildDatasetPipeline


def test_pipeline_smoke(tmp_path: Path):
    # arrange: create tiny mst file
    mst = tmp_path / "test.mst"
    mst.write_text(
        "AAA,20240102,10,11,9,10.5,100\n"
        "AAA,20240103,10.5,12,10,11,150\n"
        "BBB,20240102,10,11,9,10.5,100,1\n"
        "BBB,20240103,10.5,12,10,11,150,2\n"
    )

    pipeline = BuildDatasetPipeline()

    # act
    df = asyncio.run(pipeline.run(folder=tmp_path))

    # assert
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 4
    assert set(df.columns).issuperset(
        {"ticker", "dt", "open", "high", "low", "close", "vol", "openint"}
    )
    assert df["openint"].isna().any()
