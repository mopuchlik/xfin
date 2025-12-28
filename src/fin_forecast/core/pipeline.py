from __future__ import annotations

from pathlib import Path

import pandas as pd
from dataclasses import dataclass

from fin_forecast.config import AppConfig
from fin_forecast.features.builder import BarDatasetBuilder
from fin_forecast.io.parsers import MstBarParser
from fin_forecast.io.repository import MstRepository
from fin_forecast.io.sources import LocalFileSource


@dataclass
class BuildDatasetPipeline:
    cfg: AppConfig = AppConfig()

    async def run(self, folder: Path | None = None, pattern: str = "*.mst") -> pd.DataFrame:
        folder = folder or self.cfg.raw_dir
        paths = sorted(str(p) for p in Path(folder).glob(pattern))

        repo = MstRepository(
            locators=paths,
            source=LocalFileSource(),
            parser=MstBarParser(delimiter=",", has_header=True),
            max_concurrency=self.cfg.max_concurrency,
        )

        bars = []
        async for bar in repo.iter_bars():
            bars.append(bar)

        df = BarDatasetBuilder(add_basic_features=True).to_dataframe(bars)
        return df
