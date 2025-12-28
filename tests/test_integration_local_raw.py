# import asyncio
# from pathlib import Path
# import pytest

# from fin_forecast.core.pipeline import BuildDatasetPipeline

# @pytest.mark.slow
# def test_pipeline_on_real_raw_data():
#     raw_dir = Path("data/raw")
#     assert raw_dir.exists()

#     pipeline = BuildDatasetPipeline()
#     df = asyncio.run(pipeline.run(folder=raw_dir, pattern="*.mst"))

#     assert len(df) > 0
#     assert {"ticker", "date", "close"}.issubset(df.columns)
