from __future__ import annotations

from typing import Iterable, Iterator
import logging
from dataclasses import dataclass

from fin_forecast.domain.models import OHLCVBar
from fin_forecast.domain.types import BarParser

_base_logger = logging.getLogger(__name__)


@dataclass
class MstBarParser(BarParser):
    delimiter: str = ","
    has_header: bool = False

    def parse_lines(
        self,
        lines: Iterable[str],
        *,
        filename: str | None = None,
    ) -> Iterator[OHLCVBar]:
        # Create adapter ONCE per file
        logger = logging.LoggerAdapter(
            _base_logger,
            {"data_file": filename or "<unknown>"},
        )

        skipped_empty = 0
        skipped_badcols = 0
        skipped_other = 0

        for line in lines:
            line = line.strip()

            # skip empty lines
            if not line:
                skipped_empty += 1
                continue

            # handle BOM (Byte Order Mark)
            line_no_bom = line.lstrip("\ufeff")
            if self.has_header and line_no_bom.lower().startswith("<ticker>"):
                continue

            parts = [p.strip() for p in line.split(self.delimiter)]
            if len(parts) not in (7, 8):
                skipped_badcols += 1
                continue

            try:
                yield OHLCVBar.from_fields(parts)
            except Exception:
                skipped_other += 1

        # Log only if anything interesting happened
        if skipped_empty or skipped_badcols or skipped_other:
            logger.debug(
                "Skipped rows: empty=%d badcols=%d other=%d",
                skipped_empty,
                skipped_badcols,
                skipped_other,
            )
