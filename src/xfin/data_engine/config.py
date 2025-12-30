from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def find_project_root(start: Path | None = None) -> Path:
    """
    Find repo/project root by walking upward until pyproject.toml is found.
    Falls back to current working directory if not found.
    """
    p = (start or Path.cwd()).resolve()
    for parent in (p, *p.parents):
        if (parent / "pyproject.toml").is_file():
            return parent
    return Path.cwd().resolve()


@dataclass(frozen=True)
class AppConfig:
    project_root: Path = find_project_root()
    data_dir: Path = project_root / "data"
    raw_dir: Path = data_dir / "raw"
    processed_dir: Path = data_dir / "processed"
    max_concurrency: int = 50
