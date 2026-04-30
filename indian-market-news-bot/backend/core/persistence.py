"""SQLite persistence and app-state sanitization helpers."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

try:
    from backend.core.settings import (
        AI_ARTICLE_ANALYSIS_PROMPT_VERSION,
        AI_SUMMARY_PROMPT_VERSION,
        ALLOWED_REFRESH_WINDOWS,
        DATA_DIR,
        DEFAULT_APP_STATE,
        STATE_DB_PATH,
        WATCHLIST_SYMBOL_LIMIT,
    )
except ModuleNotFoundError:
    from core.settings import (
        AI_ARTICLE_ANALYSIS_PROMPT_VERSION,
        AI_SUMMARY_PROMPT_VERSION,
        ALLOWED_REFRESH_WINDOWS,
        DATA_DIR,
        DEFAULT_APP_STATE,
        STATE_DB_PATH,
        WATCHLIST_SYMBOL_LIMIT,
    )


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def default_app_state() -> dict:
    return {
        "tickerSelections": list(DEFAULT_APP_STATE["tickerSelections"]),
        "watchlist": list(DEFAULT_APP_STATE["watchlist"]),
        "bookmarks": list(DEFAULT_APP_STATE["bookmarks"]),
        "portfolio": dict(DEFAULT_APP_STATE["portfolio"]),
    }


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def db_connect(path: Path = STATE_DB_PATH) -> sqlite3.Connection:
    ensure_data_dir()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_state_db(path: Path = STATE_DB_PATH) -> None:
    with db_connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_news_summaries (
                cache_key TEXT PRIMARY KEY,
                article_id TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                source TEXT NOT NULL,
                published TEXT NOT NULL,
                prompt_version TEXT NOT NULL,
                summary TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_news_analysis (
                cache_key TEXT PRIMARY KEY,
                article_id TEXT NOT NULL,
                title TEXT NOT NULL,
                link TEXT NOT NULL,
                source TEXT NOT NULL,
                published TEXT NOT NULL,
                prompt_version TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def _db_get_record(key: str, path: Path = STATE_DB_PATH) -> tuple[bool, dict | list | str | int | float | None]:
    init_state_db(path)
    with db_connect(path) as conn:
        row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    if row is None:
        return False, None
    return True, json.loads(row["value"])


def db_get_json(key: str, default=None, path: Path = STATE_DB_PATH):
    exists, value = _db_get_record(key, path)
    return value if exists else default


def db_set_json(key: str, value, path: Path = STATE_DB_PATH) -> None:
    init_state_db(path)
    with db_connect(path) as conn:
        conn.execute(
            """
            INSERT INTO app_state(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value = excluded.value,
                updated_at = excluded.updated_at
            """,
            (key, json.dumps(value), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()


def load_persisted_ai_news_summary(cache_key: str, path: Path = STATE_DB_PATH) -> str:
    if not cache_key:
        return ""
    init_state_db(path)
    with db_connect(path) as conn:
        row = conn.execute(
            """
            SELECT summary
            FROM ai_news_summaries
            WHERE cache_key = ? AND prompt_version = ?
            """,
            (cache_key, AI_SUMMARY_PROMPT_VERSION),
        ).fetchone()
    return str(row["summary"] or "").strip() if row else ""


def persist_ai_news_summary(cache_key: str, article: dict, summary: str, path: Path = STATE_DB_PATH) -> None:
    summary = str(summary or "").strip()
    if not cache_key or not summary:
        return
    init_state_db(path)
    now = datetime.now(timezone.utc).isoformat()
    with db_connect(path) as conn:
        conn.execute(
            """
            INSERT INTO ai_news_summaries(
                cache_key, article_id, title, link, source, published, prompt_version,
                summary, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                article_id = excluded.article_id,
                title = excluded.title,
                link = excluded.link,
                source = excluded.source,
                published = excluded.published,
                prompt_version = excluded.prompt_version,
                summary = excluded.summary,
                updated_at = excluded.updated_at
            """,
            (
                cache_key,
                str(article.get("id") or ""),
                str(article.get("title") or ""),
                str(article.get("link") or ""),
                str(article.get("source") or ""),
                str(article.get("published") or ""),
                AI_SUMMARY_PROMPT_VERSION,
                summary,
                now,
                now,
            ),
        )
        conn.commit()


def load_persisted_ai_news_analysis(cache_key: str, path: Path = STATE_DB_PATH) -> dict:
    if not cache_key:
        return {}
    init_state_db(path)
    with db_connect(path) as conn:
        row = conn.execute(
            """
            SELECT analysis_json
            FROM ai_news_analysis
            WHERE cache_key = ? AND prompt_version = ?
            """,
            (cache_key, AI_ARTICLE_ANALYSIS_PROMPT_VERSION),
        ).fetchone()
    if not row:
        return {}
    try:
        payload = json.loads(row["analysis_json"])
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def persist_ai_news_analysis(cache_key: str, article: dict, analysis: dict, path: Path = STATE_DB_PATH) -> None:
    if not cache_key or not isinstance(analysis, dict):
        return
    init_state_db(path)
    now = datetime.now(timezone.utc).isoformat()
    with db_connect(path) as conn:
        conn.execute(
            """
            INSERT INTO ai_news_analysis(
                cache_key, article_id, title, link, source, published, prompt_version,
                analysis_json, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                article_id = excluded.article_id,
                title = excluded.title,
                link = excluded.link,
                source = excluded.source,
                published = excluded.published,
                prompt_version = excluded.prompt_version,
                analysis_json = excluded.analysis_json,
                updated_at = excluded.updated_at
            """,
            (
                cache_key,
                str(article.get("id") or ""),
                str(article.get("title") or ""),
                str(article.get("link") or ""),
                str(article.get("source") or ""),
                str(article.get("published") or ""),
                AI_ARTICLE_ANALYSIS_PROMPT_VERSION,
                json.dumps(analysis, ensure_ascii=False, sort_keys=True),
                now,
                now,
            ),
        )
        conn.commit()


def sanitize_symbols(value, limit: int = WATCHLIST_SYMBOL_LIMIT) -> list[str]:
    if isinstance(value, list):
        pieces = value
    else:
        pieces = str(value or "").split(",")

    seen, out = set(), []
    for piece in pieces:
        sym = re.sub(r"[^A-Z0-9&.-]", "", str(piece).upper().strip())
        if sym and sym not in seen:
            seen.add(sym)
            out.append(sym)
        if len(out) >= limit:
            break
    return out


def sanitize_bookmarks(value) -> list[str]:
    if not isinstance(value, list):
        return []
    seen, out = set(), []
    for piece in value:
        cleaned = re.sub(r"[^A-Za-z0-9_-]", "", str(piece).strip())[:64]
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
        if len(out) >= 500:
            break
    return out


def sanitize_portfolio(value) -> dict[str, dict[str, float]]:
    if not isinstance(value, dict):
        return {}
    out = {}
    for raw_symbol, raw_entry in value.items():
        symbol = sanitize_symbols([raw_symbol], limit=1)
        if not symbol or not isinstance(raw_entry, dict):
            continue
        qty = _safe_float(raw_entry.get("qty"), default=0.0)
        buy_price = _safe_float(raw_entry.get("buyPrice"), default=0.0)
        if qty <= 0 or buy_price <= 0:
            continue
        out[symbol[0]] = {
            "qty": round(qty, 4),
            "buyPrice": round(buy_price, 2),
        }
        if len(out) >= WATCHLIST_SYMBOL_LIMIT:
            break
    return out


def sanitize_state_patch(payload: dict | None) -> dict:
    payload = payload or {}
    clean = {}
    if "tickerSelections" in payload:
        clean["tickerSelections"] = sanitize_symbols(payload.get("tickerSelections"), WATCHLIST_SYMBOL_LIMIT)
    if "watchlist" in payload:
        clean["watchlist"] = sanitize_symbols(payload.get("watchlist"), WATCHLIST_SYMBOL_LIMIT)
    if "bookmarks" in payload:
        clean["bookmarks"] = sanitize_bookmarks(payload.get("bookmarks"))
    if "portfolio" in payload:
        clean["portfolio"] = sanitize_portfolio(payload.get("portfolio"))
    return clean


def load_persisted_app_state(path: Path = STATE_DB_PATH) -> tuple[dict, bool]:
    state = default_app_state()
    has_stored_state = False
    for key in DEFAULT_APP_STATE:
        exists, value = _db_get_record(key, path)
        if not exists:
            continue
        has_stored_state = True
        clean = sanitize_state_patch({key: value})
        if key in clean:
            state[key] = clean[key]
    return state, has_stored_state


def load_refresh_settings(path: Path = STATE_DB_PATH) -> int:
    settings = db_get_json("settings", default={}, path=path)
    if not isinstance(settings, dict):
        return 300
    seconds = int(settings.get("refreshInterval", 300) or 300)
    return seconds if seconds in ALLOWED_REFRESH_WINDOWS else 300


def persist_refresh_settings(seconds: int, path: Path = STATE_DB_PATH) -> None:
    db_set_json("settings", {"refreshInterval": seconds}, path=path)
