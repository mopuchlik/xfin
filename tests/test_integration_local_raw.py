import asyncio
from pathlib import Path
import shutil
import pytest

from fin_forecast.core.pipeline import BuildDatasetPipeline


# @pytest.mark.slow
def test_pipeline_on_real_raw_data_subset(tmp_path: Path):
    """
    Integration test on a small, fixed subset of real raw MST files.

    Limits the scope to the first N files to keep runtime predictable
    while still exercising the full pipeline on real data.
    """
    raw_dir = Path("data/raw")
    assert raw_dir.exists()

    # take only first N files (sorted for determinism)
    N = 10
    files = sorted(raw_dir.glob("*.mst"))[:N]
    assert len(files) > 0

    # copy (or symlink) into temp dir
    subset_dir = tmp_path / "raw_subset"
    subset_dir.mkdir()

    for f in files:
        shutil.copy(f, subset_dir / f.name)
        # or: os.symlink(f, subset_dir / f.name)

    pipeline = BuildDatasetPipeline()
    df = asyncio.run(pipeline.run(folder=subset_dir, pattern="*.mst"))

    assert len(df) > 0
    assert {"ticker", "dt", "open", "high", "low", "close", "vol", "openint"}.issubset(df.columns)
