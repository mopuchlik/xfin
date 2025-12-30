from __future__ import annotations

from typing import Iterable, Iterator, Protocol, Sequence

from .models import OHLCVBar


class BarParser(Protocol):
    def parse_lines(self, lines: Iterable[str]) -> Iterator[OHLCVBar]:
        ...


class ByteSource(Protocol):
    async def read_text(self, locator: str, encoding: str = "utf-8") -> str:
        ...


class BarRepository(Protocol):
    async def iter_bars(self) -> Iterator[OHLCVBar]:
        ...
