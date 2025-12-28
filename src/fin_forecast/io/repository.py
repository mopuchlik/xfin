from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterator, Sequence

from fin_forecast.domain.models import OHLCVBar
from fin_forecast.domain.types import BarParser, BarRepository, ByteSource


@dataclass
class MstRepository(BarRepository):
    locators: Sequence[str]
    source: ByteSource
    parser: BarParser
    encoding: str = "utf-8"
    max_concurrency: int = 50

    async def iter_bars(self) -> Iterator[OHLCVBar]:
        sem = asyncio.Semaphore(self.max_concurrency)

        async def load_one(loc: str) -> list[OHLCVBar]:
            async with sem:
                text = await self.source.read_text(loc, encoding=self.encoding)
                return list(self.parser.parse_lines(text.splitlines()))

        tasks = [asyncio.create_task(load_one(loc)) for loc in self.locators]

        for fut in asyncio.as_completed(tasks):
            for bar in await fut:
                yield bar

