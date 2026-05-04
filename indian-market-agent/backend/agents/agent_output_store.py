"""Shared latest + historical storage for agent outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
import json

try:
    from backend.core.persistence import STATE_DB_PATH, db_connect, db_get_json, db_set_json, init_state_db
    from backend.shared.serialization import to_jsonable
except ModuleNotFoundError:
    from core.persistence import STATE_DB_PATH, db_connect, db_get_json, db_set_json, init_state_db
    try:
        from shared.serialization import to_jsonable
    except ModuleNotFoundError:
        to_jsonable = None


LATEST_PREFIX = "agent_output:"


def build_agent_output_key(agent_name: str, symbol: str, report_type: str) -> str:
    return f"{LATEST_PREFIX}{_clean_key_part(agent_name)}:{_clean_key_part(symbol)}:{_clean_key_part(report_type)}"


def load_agent_output(key: str, default=None, path: Path | None = None):
    return db_get_json(_latest_key(key), default=default, path=path or STATE_DB_PATH)


def save_agent_output(key: str, value, path: Path | None = None) -> None:
    db_set_json(_latest_key(key), _to_jsonable(value), path=path or STATE_DB_PATH)


def save_agent_report(
    agent_name: str,
    symbol: str,
    report_type: str,
    payload: dict,
    generated_at: str | None = None,
    valid_until: str | None = None,
    stale_after_seconds: int | None = None,
    bias: str | None = None,
    confidence: float | None = None,
    input_hash: str | None = None,
    ruleset_version: str | None = None,
    agent_version: str | None = None,
    path: Path | None = None,
) -> None:
    db_path = path or STATE_DB_PATH
    payload_dict = _payload_dict(payload)
    latest_key = build_agent_output_key(agent_name, symbol, report_type)

    try:
        db_set_json(latest_key, payload_dict, path=db_path)
    except Exception as exc:
        print(f"[!] agent output latest persist error: {exc}")

    generated_at_value = generated_at or payload_dict.get("generated_at")
    generated_dt = _parse_datetime(generated_at_value) or datetime.now(timezone.utc)
    generated_at_text = generated_dt.isoformat()
    valid_until_text = _iso_or_none(valid_until if valid_until is not None else payload_dict.get("valid_until"))
    stale_value = _safe_int(stale_after_seconds if stale_after_seconds is not None else payload_dict.get("stale_after_seconds"))
    bias_value = bias or _derive_bias(payload_dict)
    confidence_value = _safe_float(confidence if confidence is not None else payload_dict.get("confidence"))
    ruleset_value = ruleset_version or _derive_ruleset_version(payload_dict)
    created_at = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(payload_dict, ensure_ascii=False, sort_keys=True)

    try:
        init_state_db(db_path)
        with db_connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO agent_outputs(
                    agent_name, symbol, report_type,
                    generated_at, generated_ts, valid_until, stale_after_seconds,
                    bias, confidence, payload_json, input_hash, ruleset_version,
                    agent_version, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _clean_key_part(agent_name),
                    _clean_key_part(symbol),
                    _clean_key_part(report_type),
                    generated_at_text,
                    generated_dt.timestamp(),
                    valid_until_text,
                    stale_value,
                    bias_value,
                    confidence_value,
                    payload_json,
                    input_hash,
                    ruleset_value,
                    agent_version,
                    created_at,
                ),
            )
            conn.commit()
    except Exception as exc:
        print(f"[!] agent output history persist error: {exc}")


def load_latest_agent_report(
    agent_name: str,
    symbol: str,
    report_type: str,
    default=None,
    path: Path | None = None,
) -> dict | None:
    db_path = path or STATE_DB_PATH
    latest = db_get_json(build_agent_output_key(agent_name, symbol, report_type), default=None, path=db_path)
    if latest is not None:
        return latest
    history = load_agent_report_history(agent_name, symbol, report_type, limit=1, path=db_path)
    if history:
        return history[0].get("payload")
    return default


def load_agent_report_history(
    agent_name: str,
    symbol: str,
    report_type: str,
    limit: int = 100,
    path: Path | None = None,
) -> list[dict]:
    db_path = path or STATE_DB_PATH
    init_state_db(db_path)
    with db_connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM agent_outputs
            WHERE agent_name = ? AND symbol = ? AND report_type = ?
            ORDER BY generated_ts DESC, id DESC
            LIMIT ?
            """,
            (_clean_key_part(agent_name), _clean_key_part(symbol), _clean_key_part(report_type), _safe_limit(limit, 100)),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def load_agent_reports_since(
    agent_name: str,
    symbol: str,
    report_type: str,
    since_ts: float,
    limit: int = 500,
    path: Path | None = None,
) -> list[dict]:
    db_path = path or STATE_DB_PATH
    init_state_db(db_path)
    with db_connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM agent_outputs
            WHERE agent_name = ? AND symbol = ? AND report_type = ? AND generated_ts >= ?
            ORDER BY generated_ts DESC, id DESC
            LIMIT ?
            """,
            (
                _clean_key_part(agent_name),
                _clean_key_part(symbol),
                _clean_key_part(report_type),
                _safe_float(since_ts, 0.0) or 0.0,
                _safe_limit(limit, 500),
            ),
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _to_jsonable(value):
    if to_jsonable is not None:
        return to_jsonable(value)
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _to_jsonable(value.to_dict())
    if is_dataclass(value):
        return _to_jsonable(asdict(value))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]
    return value


def _payload_dict(payload) -> dict:
    value = _to_jsonable(payload)
    return value if isinstance(value, dict) else {"value": value}


def _latest_key(key: str) -> str:
    text = str(key or "").strip()
    return text if text.startswith(LATEST_PREFIX) else f"{LATEST_PREFIX}{text}"


def _clean_key_part(value) -> str:
    return str(value or "").strip() if str(value or "").strip() else "UNKNOWN"


def _derive_bias(payload: dict) -> str | None:
    for key in ("bias", "macro_bias", "primary_regime", "sentiment", "overall_sentiment"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _derive_ruleset_version(payload: dict) -> str | None:
    value = payload.get("ruleset_version")
    if value:
        return str(value)
    key_levels = payload.get("key_levels")
    if isinstance(key_levels, dict) and key_levels.get("ruleset_version"):
        return str(key_levels.get("ruleset_version"))
    return None


def _parse_datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _iso_or_none(value) -> str | None:
    if value in (None, ""):
        return None
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value)


def _safe_int(value) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _safe_float(value, default=None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_limit(value, default: int) -> int:
    try:
        return max(1, min(5000, int(value)))
    except (TypeError, ValueError):
        return default


def _row_to_dict(row) -> dict:
    payload = {}
    try:
        payload = json.loads(row["payload_json"])
    except (TypeError, json.JSONDecodeError):
        payload = {}
    return {
        "id": row["id"],
        "agent_name": row["agent_name"],
        "symbol": row["symbol"],
        "report_type": row["report_type"],
        "generated_at": row["generated_at"],
        "generated_ts": row["generated_ts"],
        "valid_until": row["valid_until"],
        "stale_after_seconds": row["stale_after_seconds"],
        "bias": row["bias"],
        "confidence": row["confidence"],
        "input_hash": row["input_hash"],
        "ruleset_version": row["ruleset_version"],
        "agent_version": row["agent_version"],
        "created_at": row["created_at"],
        "payload": payload,
    }
