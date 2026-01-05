from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from xfin.config import AppConfig
from xfin.data_engine.features.builder import BarDatasetBuilder
from xfin.data_engine.io.parsers import MstBarParser
from xfin.data_engine.io.repository import MstRepository
from xfin.data_engine.io.sources import LocalFileSource

logger = logging.getLogger(__name__)


@dataclass
class BuildDatasetPipeline:
    cfg: AppConfig = AppConfig()

    async def run(
        self, folder: Path | None = None, pattern: str = "*.mst"
    ) -> pd.DataFrame:
        t0 = time.perf_counter()

        folder = folder or self.cfg.data_engine.raw_dir
        logger.info("Building dataset from folder=%s pattern=%s", folder, pattern)

        paths = sorted(str(p) for p in Path(folder).glob(pattern))
        logger.info("Found %d files", len(paths))

        if not paths:
            raise FileNotFoundError(
                f"No files matched pattern={pattern!r} in folder={str(folder)!r}"
            )

        repo = MstRepository(
            locators=paths,
            source=LocalFileSource(),
            parser=MstBarParser(delimiter=",", has_header=True),
            max_concurrency=self.cfg.data_engine.max_concurrency,
        )

        bars = []
        count = 0

        async for bar in repo.iter_bars():
            bars.append(bar)
            count += 1

            if count % 100_000 == 0:
                logger.debug("Parsed %d bars so far", count)

        logger.info("Parsed %d bars in %.2fs", count, time.perf_counter() - t0)

        t1 = time.perf_counter()
        df = BarDatasetBuilder(add_basic_features=True).to_dataframe(bars)

        logger.info(
            "Built dataframe in %.2fs (rows=%d cols=%d)",
            time.perf_counter() - t1,
            df.shape[0],
            df.shape[1],
        )

        return df
