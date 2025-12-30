from __future__ import annotations

from typing import Callable

from xfin.forecaster.models.naive import NaiveLastClose

_REGISTRY: dict[str, Callable[[], object]] = {
    NaiveLastClose.name: NaiveLastClose,
}


def list_models() -> list[str]:
    """List available model names."""
    return sorted(_REGISTRY.keys())


def create_model(name: str):
    """Instantiate a model by name."""
    try:
        return _REGISTRY[name]()
    except KeyError:
        raise ValueError(f"Unknown model {name!r}. Available: {', '.join(list_models())}")
