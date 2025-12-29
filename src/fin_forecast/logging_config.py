from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional


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

    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    ch = logging.StreamHandler(sys.stderr)
    ch.setLevel(numeric)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setLevel(numeric)
        fh.setFormatter(formatter)
        root.addHandler(fh)
