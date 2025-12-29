import asyncio
from pathlib import Path

from fin_forecast.core.pipeline import BuildDatasetPipeline


def test_pipeline_async_deterministic(tmp_path: Path):
    """
    Ensure that running the asynchronous pipeline multiple times on the same
    input produces identical results.

    This test protects against concurrency-related bugs (e.g. race conditions,
    non-deterministic ordering) that could arise from async file loading or
    iteration.
    """
    mst = tmp_path / "a.mst"
    mst.write_text(
        "AAA,20240101,1,2,0.5,1.5,10\n"
        "AAA,20240102,1.5,2.5,1,2,20\n"
    )

    pipeline = BuildDatasetPipeline()

    df1 = asyncio.run(pipeline.run(folder=tmp_path))
    df2 = asyncio.run(pipeline.run(folder=tmp_path))

    assert df1.equals(df2)
