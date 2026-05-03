"""Runtime context passed from the Flask entrypoint into route modules."""

from __future__ import annotations

from collections.abc import MutableMapping
from dataclasses import dataclass
from typing import Any


@dataclass
class AppRuntimeContext:
    """Thin dynamic view over app runtime globals.

    Phase 1 keeps runtime ownership in ``app.py``. The context lets extracted
    route modules read the current values without copying mutable or reassigned
    state.
    """

    namespace: MutableMapping[str, Any]

    def __getattr__(self, name: str) -> Any:
        try:
            return self.namespace[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "namespace":
            super().__setattr__(name, value)
            return
        self.namespace[name] = value
