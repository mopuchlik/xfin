from __future__ import annotations

from pathlib import Path
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class AppConfig:
    project_root: Path = Path(__file__).resolve().parents[2]  # repo root
    data_dir: Path = project_root / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"
    max_concurrency: int = 50
