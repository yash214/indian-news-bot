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

WATCHLIST_SYMBOL_LIMIT = 12
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
AI_ARTICLE_ANALYSIS_PROMPT_VERSION = "article-analysis-v4-material-facts-index-impact"

UPSTOX_AUTH_STATE_KEY = "upstox_oauth_state"
UPSTOX_AUTH_TOKEN_KEY = "upstox_auth_token"
