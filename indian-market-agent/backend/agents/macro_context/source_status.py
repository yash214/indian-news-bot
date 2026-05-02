"""Provider/source status helpers for macro snapshots."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass
class SourceStatus:
    provider: str
    enabled: bool
    configured: bool
    last_success_at: datetime | None = None
    last_error: str = ""
    using_fallback: bool = False
    stale: bool = False

    def mark_success(self, at: datetime | None = None, *, stale: bool = False) -> "SourceStatus":
        self.last_success_at = at
        self.last_error = ""
        self.stale = stale
        return self

    def mark_error(self, error: str, *, stale: bool = False, using_fallback: bool = False) -> "SourceStatus":
        self.last_error = str(error or "")[:240]
        self.using_fallback = using_fallback
        self.stale = stale
        return self

    def to_dict(self) -> dict:
        payload = asdict(self)
        if self.last_success_at:
            payload["last_success_at"] = self.last_success_at.isoformat()
        else:
            payload["last_success_at"] = None
        return payload
