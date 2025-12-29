from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Sequence, Optional

from pydantic.dataclasses import dataclass


class BarField(str, Enum):
    TICKER = "ticker"
    DATE = "dt"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOL = "vol"
    OPENINT = "openint"


@dataclass(frozen=True)
class OHLCVBar:
    ticker: str
    dt: date
    open: float
    high: float
    low: float
    close: float
    vol: float
    openint: float | None = None  # allow missing logically

    @staticmethod
    def from_fields(fields: Sequence[str]) -> "OHLCVBar":
        if len(fields) not in (7, 8):
            raise ValueError(f"Expected 7 or 8 fields, got {len(fields)}: {fields}")

        ticker = fields[0].strip()
        dt = datetime.strptime(fields[1].strip(), "%Y%m%d").date()

        o = float(fields[2])
        h = float(fields[3])
        l = float(fields[4])
        c = float(fields[5])
        v = float(fields[6])

        # NEW: open interest (optional 8th column)
        if len(fields) == 8 and fields[7].strip() != "":
            oi: float | None = float(fields[7])
        else:
            oi = float("nan")  # best for pandas/parquet

        return OHLCVBar(
            ticker=ticker, dt=dt, open=o, high=h, low=l, close=c, vol=v, openint=oi
        )
