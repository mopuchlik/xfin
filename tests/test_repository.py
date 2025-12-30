import asyncio
from pathlib import Path

from xfin.data_engine.io.repository import MstRepository
from xfin.data_engine.io.parsers import MstBarParser
from xfin.data_engine.io.sources import LocalFileSource


def test_repository_iter_bars(tmp_path: Path):
    """
    Verify that MstRepository.iter_bars() yields all parsed bars exactly once.

    This test validates the async iterator contract:
    - files are read correctly
    - parser output is forwarded
    - no bars are dropped or duplicated
    """
    mst = tmp_path / "a.mst"
    mst.write_text("AAA,20240101,1,2,0.5,1.5,10\n")

    repo = MstRepository(
        locators=[str(mst)],
        source=LocalFileSource(),
        parser=MstBarParser(),
        max_concurrency=2,
    )

    async def collect():
        return [bar async for bar in repo.iter_bars()]

    bars = asyncio.run(collect())

    assert len(bars) == 1
    assert bars[0].ticker == "AAA"
