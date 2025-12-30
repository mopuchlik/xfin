import asyncio

from xfin.data_engine.domain.models import BarField
from xfin.data_engine.core.pipeline import BuildDatasetPipeline


def test_barfield_values():
    """
    Ensure BarField defines the canonical set of core OHLCV(+OPENINT) column names.

    This test protects the project-wide DataFrame schema by failing if a column
    is added, removed, or renamed without explicitly updating BarField.
    """

    expected = {
        "ticker",
        "dt",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "openint",
    }

    actual = {f.value for f in BarField}
    assert actual == expected


def test_barfield_matches_dataframe_columns(tmp_path):
    """
    Verify that all BarField-defined columns are present in the DataFrame
    produced by the dataset pipeline.

    This ensures that feature builders and downstream consumers relying on
    BarField can safely access their expected columns, even as additional
    derived features are added to the DataFrame.
    """

    # minimal MST
    mst = tmp_path / "t.mst"
    mst.write_text("AAA,20240102,1,2,0.5,1.5,100\n")

    df = asyncio.run(BuildDatasetPipeline().run(folder=tmp_path))

    # All BarField columns must exist in df
    for field in BarField:
        assert field.value in df.columns
