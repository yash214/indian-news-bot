"""Shared storage wrapper for future agent outputs."""

from __future__ import annotations

try:
    from backend.core.persistence import db_get_json, db_set_json
except ModuleNotFoundError:
    from core.persistence import db_get_json, db_set_json


def load_agent_output(key: str, default=None):
    return db_get_json(f"agent_output:{key}", default=default)


def save_agent_output(key: str, value) -> None:
    db_set_json(f"agent_output:{key}", value)

