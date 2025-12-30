import asyncio
from pathlib import Path

from xfin.data_engine.core.pipeline import BuildDatasetPipeline


def test_parser_logs_skipped_rows(tmp_path: Path, caplog):
    """
    Ensure the parser emits debug-level log messages when malformed or empty
    rows are skipped.

    This test protects logging instrumentation, ensuring diagnostics remain
    available when data quality issues occur.
    """
    mst = tmp_path / "a.mst"
    mst.write_text(
        "AAA,20240101,1,2,0.5,1.5,10\n"
        "\n"  # malformed / empty line
    )

    with caplog.at_level("DEBUG"):
        asyncio.run(BuildDatasetPipeline().run(folder=tmp_path))

    assert any("Skipped" in rec.message for rec in caplog.records)
