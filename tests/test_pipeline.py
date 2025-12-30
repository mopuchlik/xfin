import asyncio
from pathlib import Path
import pytest

from xfin.data_engine.core.pipeline import BuildDatasetPipeline


def test_pipeline_missing_folder():
    """
    Ensure the pipeline fails loudly when the input folder does not exist.

    This prevents silent success with empty outputs and documents the expected
    failure behavior for callers (CLI, tests, integrations).
    """
    pipeline = BuildDatasetPipeline()

    with pytest.raises(FileNotFoundError):
        asyncio.run(pipeline.run(folder=Path("does_not_exist")))
