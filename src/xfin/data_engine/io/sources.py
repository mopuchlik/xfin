from __future__ import annotations

import asyncio
from pathlib import Path

from dataclasses import dataclass

from fin_forecast.domain.types import ByteSource


@dataclass
class LocalFileSource(ByteSource):
    errors: str = "replace"

    async def read_text(self, locator: str, encoding: str = "utf-8") -> str:
        path = Path(locator)

        def _read() -> str:
            return path.read_text(encoding=encoding, errors=self.errors)

        return await asyncio.to_thread(_read)
