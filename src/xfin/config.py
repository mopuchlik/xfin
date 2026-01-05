from __future__ import annotations

from dataclasses import dataclass, field
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


# -------------------------
# Data Engine configuration
# -------------------------
@dataclass(frozen=True)
class DataEngineConfig:
    project_root: Path = field(default_factory=find_project_root)

    # derived paths (computed in __post_init__)
    data_dir: Path = field(init=False)
    raw_dir: Path = field(init=False)
    processed_dir: Path = field(init=False)

    # runtime
    max_concurrency: int = 50

    def __post_init__(self) -> None:
        root = self.project_root
        object.__setattr__(self, "data_dir", root / "data")
        object.__setattr__(self, "raw_dir", self.data_dir / "raw")
        object.__setattr__(self, "processed_dir", self.data_dir / "processed")


# -------------------------
# Forecaster configuration
# -------------------------
@dataclass(frozen=True)
class ForecasterConfig:
    """
    Placeholder for forecaster-related defaults.
    Intentionally empty for now.
    """
    pass


# -------------------------
# Application-wide config
# -------------------------
@dataclass(frozen=True)
class AppConfig:
    data_engine: DataEngineConfig = field(default_factory=DataEngineConfig)
    forecaster: ForecasterConfig = field(default_factory=ForecasterConfig)
