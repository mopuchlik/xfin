from __future__ import annotations

from typing import Iterable, Iterator

from dataclasses import dataclass

from fin_forecast.domain.models import OHLCVBar
from fin_forecast.domain.types import BarParser


@dataclass
class MstBarParser(BarParser):
    delimiter: str = ","
    has_header: bool = False

    def parse_lines(self, lines: Iterable[str]) -> Iterator[OHLCVBar]:

        skipped = 0

        for line in lines:
            line = line.strip()
            
            # skip empty lines
            if not line:
                skipped += 1
                continue

            # handle BOM (Byte Order Mark) -- zero width no-break space 
            # + be less strict about position
            line_no_bom = line.lstrip("\ufeff")
            if self.has_header and line_no_bom.lower().startswith("<ticker>"):
                continue

            parts = [p.strip() for p in line.split(self.delimiter)]
            if len(parts) != 7:
                skipped += 1
                continue  # defensive: skip malformed rows
            
            try:
                yield OHLCVBar.from_fields(parts)
            except Exception:
                # defensive: skip rows with bad values (dates, floats, etc.)
                skipped += 1


        if skipped:
            print(f"[MstBarParser] Skipped {skipped} malformed/empty rows")
