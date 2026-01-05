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
    Configure root logging for the app.

    - Console -> stderr
    - Optional file -> log_file
    - Safe to call multiple times (won't add duplicate handlers)
    """
    root = logging.getLogger()

    numeric = getattr(logging, level.upper(), None)
    if not isinstance(numeric, int):
        raise ValueError(f"Invalid log level: {level!r}")

    # If already configured, just update levels and optionally add a new file handler.
    already = getattr(root, "_xfin_configured", False)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | src=%(filename)s | data=%(data_file)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    context_filter = DefaultContextFilter()

    def _apply_common(handler: logging.Handler) -> None:
        handler.setLevel(numeric)
        handler.setFormatter(formatter)
        # Avoid adding the same filter instance multiple times
        if not any(isinstance(f, DefaultContextFilter) for f in handler.filters):
            handler.addFilter(context_filter)

    root.setLevel(numeric)

    if not already:
        # Console handler (stderr)
        ch = logging.StreamHandler(sys.stderr)
        _apply_common(ch)
        root.addHandler(ch)

        # Mark configured after base handlers are added
        root._xfin_configured = True  # type: ignore[attr-defined]
    else:
        # Update levels/formatters on existing handlers
        for h in root.handlers:
            _apply_common(h)

    # Optional file handler: add it if requested and not already present
    if log_file:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        wanted = str(path.resolve())

        def _is_same_file_handler(h: logging.Handler) -> bool:
            if not isinstance(h, logging.FileHandler):
                return False
            try:
                return str(Path(h.baseFilename).resolve()) == wanted  # type: ignore[attr-defined]
            except Exception:
                return False

        if not any(_is_same_file_handler(h) for h in root.handlers):
            fh = logging.FileHandler(path, encoding="utf-8")
            _apply_common(fh)
            root.addHandler(fh)
