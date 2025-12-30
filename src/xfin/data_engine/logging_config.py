from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional


class DefaultContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "data_file"):
            record.data_file = "-"
        return True


def setup_logging(*, level: str = "INFO", log_file: Optional[str] = None) -> None:
    """
    Configure root logging once for the app.
    Console -> stderr
    Optional file -> log_file
    """
    root = logging.getLogger()
    root.handlers.clear()

    numeric = getattr(logging, level.upper(), None)
    if not isinstance(numeric, int):
        raise ValueError(f"Invalid log level: {level!r}")

    root.setLevel(numeric)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | src=%(filename)s | data=%(data_file)s | %(message)s"

    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    context_filter = DefaultContextFilter()  # NEW

    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(numeric)
    ch.setFormatter(formatter)
    ch.addFilter(context_filter)  # NEW
    root.addHandler(ch)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setLevel(numeric)
        fh.setFormatter(formatter)
        fh.addFilter(context_filter)  # NEW
        root.addHandler(fh)
