from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Sequence

from pydantic.dataclasses import dataclass


class BarField(str, Enum):
    TICKER = "ticker"
    DATE = "date"
    OPEN = "open"
    HIGH = "high"
    LOW = "low"
    CLOSE = "close"
    VOL = "vol"


@dataclass(frozen=True)
class OHLCVBar:
    ticker: str
    dt: date
    open: float
    high: float
    low: float
    close: float
    vol: float

    @staticmethod
    def from_fields(fields: Sequence[str]) -> "OHLCVBar":
        if len(fields) != 7:
            raise ValueError(f"Expected 7 fields, got {len(fields)}: {fields}")

        ticker = fields[0].strip()
        dt = datetime.strptime(fields[1].strip(), "%Y%m%d").date()

        o = float(fields[2]); h = float(fields[3]); l = float(fields[4])
        c = float(fields[5]); v = float(fields[6])

        return OHLCVBar(ticker=ticker, dt=dt, open=o, high=h, low=l, close=c, vol=v)
