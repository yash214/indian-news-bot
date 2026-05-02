"""Shared runtime settings for the India Market Desk backend."""

from __future__ import annotations

import os
from datetime import time as dt_time, timezone, timedelta
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))

BACKEND_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = BACKEND_DIR / "data"
DATA_DIR = Path(os.environ.get("MARKET_DESK_DATA_DIR", str(DEFAULT_DATA_DIR))).expanduser()
STATE_DB_PATH = DATA_DIR / "market_desk.db"
HOLIDAY_CALENDAR_PATH = Path(
    os.environ.get("MARKET_DESK_HOLIDAY_FILE", str(DEFAULT_DATA_DIR / "nse_holidays.json"))
).expanduser()

MARKET_OPEN_TIME = dt_time(hour=9, minute=15)
MARKET_CLOSE_TIME = dt_time(hour=15, minute=30)

WATCHLIST_SYMBOL_LIMIT = 20
DEFAULT_TRACKED_TICKERS = ["INFY", "HCLTECH", "WIPRO", "TCS", "RELIANCE"]
DEFAULT_WATCHLIST = ["INFY", "HCLTECH", "WIPRO", "RELIANCE"]
DEFAULT_APP_STATE = {
    "tickerSelections": DEFAULT_TRACKED_TICKERS,
    "watchlist": DEFAULT_WATCHLIST,
    "bookmarks": [],
    "portfolio": {},
}

ALLOWED_REFRESH_WINDOWS = [60, 120, 300, 600, 900]

AI_SUMMARY_PROMPT_VERSION = "brief-v8-material-facts-market-impact"
AI_ARTICLE_ANALYSIS_PROMPT_VERSION = "article-analysis-v5-news-agent-index-schema"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_time(name: str, default: str) -> dt_time:
    raw = (os.environ.get(name, default) or default).strip()
    try:
        hour_str, minute_str = raw.split(":", 1)
        hour = max(0, min(23, int(hour_str)))
        minute = max(0, min(59, int(minute_str)))
    except (TypeError, ValueError):
        hour, minute = (8, 35)
    return dt_time(hour=hour, minute=minute)


FMP_ENABLED = _env_bool("FMP_ENABLED", False)
FMP_TIMEOUT_SECONDS = int(os.environ.get("FMP_TIMEOUT_SECONDS", "8") or 8)
FMP_CACHE_TTL_SECONDS = int(os.environ.get("FMP_CACHE_TTL_SECONDS", "3600") or 3600)

MACRO_AGENT_ENABLED = _env_bool("MACRO_AGENT_ENABLED", True)
MACRO_AGENT_REFRESH_MODE = (os.environ.get("MACRO_AGENT_REFRESH_MODE", "scheduled") or "scheduled").strip().lower()
MACRO_AGENT_PREMARKET_TIME = _env_time("MACRO_AGENT_PREMARKET_TIME", "08:35")
MACRO_AGENT_OPEN_CHECK_TIME = _env_time("MACRO_AGENT_OPEN_CHECK_TIME", "09:25")
MACRO_AGENT_MIDDAY_TIME = _env_time("MACRO_AGENT_MIDDAY_TIME", "12:30")
MACRO_AGENT_PRE_CLOSE_TIME = _env_time("MACRO_AGENT_PRE_CLOSE_TIME", "15:00")
MACRO_AGENT_POSTMARKET_TIME = _env_time("MACRO_AGENT_POSTMARKET_TIME", "15:45")
MACRO_AGENT_TIMEZONE = (os.environ.get("MACRO_AGENT_TIMEZONE", "Asia/Kolkata") or "Asia/Kolkata").strip()
MACRO_AGENT_SNAPSHOT_TTL_SECONDS = int(os.environ.get("MACRO_AGENT_SNAPSHOT_TTL_SECONDS", "5400") or 5400)
