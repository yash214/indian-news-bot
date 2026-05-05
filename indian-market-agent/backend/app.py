#!/usr/bin/env python3
"""
Indian Market Agent backend.

Project layout:
    indian-market-agent/
      backend/
        app.py
        requirements.txt
      frontend/
        index.html
        assets/
          styles.css
          app.js

Run from the project root:
    pip install -r requirements.txt
    python backend/app.py
"""

from __future__ import annotations

import json
import os
import queue
import re
import secrets
import subprocess
import sys
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dt_time, timezone, timedelta
from urllib.parse import quote

from flask import Flask, Response, jsonify, request, stream_with_context

try:
    from backend.core.settings import (
        ALLOWED_REFRESH_WINDOWS,
        BACKEND_DIR,
        DATA_DIR,
        DEFAULT_APP_STATE,
        DEFAULT_TRACKED_TICKERS,
        DEFAULT_WATCHLIST,
        HOLIDAY_CALENDAR_PATH,
        IST,
        MARKET_CLOSE_TIME,
        MARKET_OPEN_TIME,
        STATE_DB_PATH,
        WATCHLIST_SYMBOL_LIMIT,
    )
    from backend.core.persistence import (
        db_connect,
        db_get_json,
        db_set_json,
        default_app_state,
        ensure_data_dir,
        init_state_db,
        load_persisted_ai_news_analysis,
        load_persisted_ai_news_summary,
        load_persisted_app_state,
        load_refresh_settings,
        persist_ai_news_analysis,
        persist_ai_news_summary,
        persist_refresh_settings,
        sanitize_bookmarks,
        sanitize_portfolio,
        sanitize_state_patch,
        sanitize_symbols,
    )
    from backend.agents.news.sources import GLOBAL_SCOPE, LOCAL_SCOPE, RSS_FEEDS, google_news_search_rss, news_feed
    from backend.market.catalog import (
        ANALYTICS_INDEX_NAMES,
        NSE_INDICES_WANTED,
        NSE_STOCKS,
        PRIMARY_LEVEL_LABELS,
        SECTOR_TO_INDEX,
        SYMBOL_SUGGESTIONS,
        UPSTOX_DEFAULT_INSTRUMENT_KEYS,
        UPSTOX_INDEX_INSTRUMENT_KEYS,
        UPSTOX_OPTION_UNDERLYINGS,
        sanitize_symbol_list,
        search_symbols,
        symbol_directory_entry,
    )
    from backend.market.math import (
        bias_from_score,
        clamp,
        conviction_from_score,
        day_type_from_context,
        format_level,
        implied_move_points,
        intraday_range_pct,
        intraday_return,
        pct_return,
        realized_vol,
        relative_gap,
        round_or_none,
        rsi,
        safe_float,
        score_band,
        setup_label,
        sma,
        trend_label,
    )
    from backend.agents.news.scoring import (
        BEARISH,
        BULLISH,
        IMPACT_EVENTS,
        IMPACT_KW,
        LOW_SIGNAL_HEADLINES,
        MARKET_CONTEXT,
        SECTOR_KW,
        SOURCE_IMPACT,
        TITLE_SHOCK,
        build_sector_news_scores,
        classify,
        impact,
        impact_details,
        low_signal_penalty,
        recency_impact_score,
        sector_bias_label,
        sentiment,
        source_impact_score,
        weighted_keyword_hits,
    )
    from backend.agents.news.ai import NewsAiSummaryService
    from backend.agents.news.analysis import build_article_analysis_prompt, extract_json_object, normalize_article_analysis
    from backend.agents.news.report_aggregator import NewsReportAggregator
    from backend.agents.news.report_store import (
        load_recent_article_ai_analyses,
        save_article_ai_analysis,
        save_index_news_report,
    )
    from backend.agents.news.summaries import (
        build_news_summary_prompt,
        extract_ollama_response_text,
        normalize_ai_summary,
        summary_needs_ai,
    )
    from backend.agents.news.article_extract import article_text_is_useful, extract_article_text
    from backend.providers.ai import (
        AiProviderConfigurationError,
        ai_model_name_from_env,
        ai_provider_name_from_env,
        create_ai_text_provider,
    )
    from backend.providers.upstox.market_data import (
        option_underlying_key,
        parse_upstox_instrument_overrides,
        parse_upstox_timestamp,
        summarize_upstox_option_chain,
        upstox_instrument_key_for_symbol,
        upstox_quote_from_payload,
        upstox_token_preview,
    )
    from backend.providers.upstox.v3_proto import decode_feed_response
    from backend.providers.upstox.live import build_stream_request, stream_authorize_url, stream_quote_from_feed
    from backend.routes.derivatives_routes import register_derivatives_routes
    from backend.routes.fo_agent_routes import register_fo_agent_routes
    from backend.routes.frontend_routes import register_frontend_routes
    from backend.routes.health_routes import register_health_routes
    from backend.routes.macro_agent_routes import register_macro_agent_routes
    from backend.routes.market_regime_routes import register_market_regime_routes
    from backend.routes.market_routes import register_market_routes
    from backend.routes.news_agent_routes import register_news_agent_routes
    from backend.routes.news_routes import register_news_routes
    from backend.routes.upstox_routes import register_upstox_routes
    from backend.services import ai_runtime, analytics_runtime, background_runtime, market_runtime, news_runtime, provider_status, upstox_runtime
    from backend.services.fo_runtime import (
        build_fo_snapshot,
        fo_runtime_status,
        get_latest_fo_structure_report,
        run_fo_structure_cycle,
    )
    from backend.services.macro_runtime import (
        build_macro_snapshot,
        get_latest_macro_context_report,
        macro_refresh_due,
        macro_runtime_status,
        next_macro_refresh_time,
        run_macro_context_cycle,
    )
    from backend.services.market_regime_runtime import (
        build_market_regime_snapshot,
        get_latest_market_regime_report,
        market_regime_runtime_status,
        run_market_regime_cycle,
    )
    from backend.services.runtime_context import build_runtime_context
    from backend.agents.news.text import (
        build_article_preview,
        clean_headline,
        clean_summary,
        feed_publisher_label,
        keyword_re,
        normalized_headline,
        strip_html,
        url_hash,
    )
except ModuleNotFoundError:
    backend_module_dir = Path(__file__).resolve().parent
    if str(backend_module_dir) not in sys.path:
        sys.path.insert(0, str(backend_module_dir))
    from core.settings import (
        ALLOWED_REFRESH_WINDOWS,
        BACKEND_DIR,
        DATA_DIR,
        DEFAULT_APP_STATE,
        DEFAULT_TRACKED_TICKERS,
        DEFAULT_WATCHLIST,
        HOLIDAY_CALENDAR_PATH,
        IST,
        MARKET_CLOSE_TIME,
        MARKET_OPEN_TIME,
        STATE_DB_PATH,
        WATCHLIST_SYMBOL_LIMIT,
    )
    from core.persistence import (
        db_connect,
        db_get_json,
        db_set_json,
        default_app_state,
        ensure_data_dir,
        init_state_db,
        load_persisted_ai_news_analysis,
        load_persisted_ai_news_summary,
        load_persisted_app_state,
        load_refresh_settings,
        persist_ai_news_analysis,
        persist_ai_news_summary,
        persist_refresh_settings,
        sanitize_bookmarks,
        sanitize_portfolio,
        sanitize_state_patch,
        sanitize_symbols,
    )
    from agents.news.sources import GLOBAL_SCOPE, LOCAL_SCOPE, RSS_FEEDS, google_news_search_rss, news_feed
    from market.catalog import (
        ANALYTICS_INDEX_NAMES,
        NSE_INDICES_WANTED,
        NSE_STOCKS,
        PRIMARY_LEVEL_LABELS,
        SECTOR_TO_INDEX,
        SYMBOL_SUGGESTIONS,
        UPSTOX_DEFAULT_INSTRUMENT_KEYS,
        UPSTOX_INDEX_INSTRUMENT_KEYS,
        UPSTOX_OPTION_UNDERLYINGS,
        sanitize_symbol_list,
        search_symbols,
        symbol_directory_entry,
    )
    from market.math import (
        bias_from_score,
        clamp,
        conviction_from_score,
        day_type_from_context,
        format_level,
        implied_move_points,
        intraday_range_pct,
        intraday_return,
        pct_return,
        realized_vol,
        relative_gap,
        round_or_none,
        rsi,
        safe_float,
        score_band,
        setup_label,
        sma,
        trend_label,
    )
    from agents.news.scoring import (
        BEARISH,
        BULLISH,
        IMPACT_EVENTS,
        IMPACT_KW,
        LOW_SIGNAL_HEADLINES,
        MARKET_CONTEXT,
        SECTOR_KW,
        SOURCE_IMPACT,
        TITLE_SHOCK,
        build_sector_news_scores,
        classify,
        impact,
        impact_details,
        low_signal_penalty,
        recency_impact_score,
        sector_bias_label,
        sentiment,
        source_impact_score,
        weighted_keyword_hits,
    )
    from agents.news.ai import NewsAiSummaryService
    from agents.news.analysis import build_article_analysis_prompt, extract_json_object, normalize_article_analysis
    from agents.news.report_aggregator import NewsReportAggregator
    from agents.news.report_store import (
        load_recent_article_ai_analyses,
        save_article_ai_analysis,
        save_index_news_report,
    )
    from agents.news.summaries import (
        build_news_summary_prompt,
        extract_ollama_response_text,
        normalize_ai_summary,
        summary_needs_ai,
    )
    from agents.news.article_extract import article_text_is_useful, extract_article_text
    from providers.ai import (
        AiProviderConfigurationError,
        ai_model_name_from_env,
        ai_provider_name_from_env,
        create_ai_text_provider,
    )
    from providers.upstox.market_data import (
        option_underlying_key,
        parse_upstox_instrument_overrides,
        parse_upstox_timestamp,
        summarize_upstox_option_chain,
        upstox_instrument_key_for_symbol,
        upstox_quote_from_payload,
        upstox_token_preview,
    )
    from providers.upstox.v3_proto import decode_feed_response
    from providers.upstox.live import build_stream_request, stream_authorize_url, stream_quote_from_feed
    from routes.derivatives_routes import register_derivatives_routes
    from routes.fo_agent_routes import register_fo_agent_routes
    from routes.frontend_routes import register_frontend_routes
    from routes.health_routes import register_health_routes
    from routes.macro_agent_routes import register_macro_agent_routes
    from routes.market_regime_routes import register_market_regime_routes
    from routes.market_routes import register_market_routes
    from routes.news_agent_routes import register_news_agent_routes
    from routes.news_routes import register_news_routes
    from routes.upstox_routes import register_upstox_routes
    from services import ai_runtime, analytics_runtime, background_runtime, market_runtime, news_runtime, provider_status, upstox_runtime
    from services.fo_runtime import (
        build_fo_snapshot,
        fo_runtime_status,
        get_latest_fo_structure_report,
        run_fo_structure_cycle,
    )
    from services.macro_runtime import (
        build_macro_snapshot,
        get_latest_macro_context_report,
        macro_refresh_due,
        macro_runtime_status,
        next_macro_refresh_time,
        run_macro_context_cycle,
    )
    from services.market_regime_runtime import (
        build_market_regime_snapshot,
        get_latest_market_regime_report,
        market_regime_runtime_status,
        run_market_regime_cycle,
    )
    from services.runtime_context import build_runtime_context
    from agents.news.text import (
        build_article_preview,
        clean_headline,
        clean_summary,
        feed_publisher_label,
        keyword_re,
        normalized_headline,
        strip_html,
        url_hash,
    )

try:
    import certifi
    import requests
except ImportError:
    sys.exit("Missing: pip install requests certifi")

try:
    import feedparser
except ImportError:
    sys.exit("Missing: pip install feedparser")

# ── Config ─────────────────────────────────────────────────────────────────
TRACKED_QUOTE_LIMIT = 20

PREOPEN_TICK_INTERVAL_SECONDS = 10
INTRADAY_TICK_INTERVAL_SECONDS = 10
AFTER_HOURS_TICK_INTERVAL_SECONDS = 60
INTRADAY_TICK_STALE_SECONDS = 30
AFTER_HOURS_TICK_STALE_SECONDS = 180
STREAM_UI_BROADCAST_SECONDS = max(0.25, float(os.environ.get("STREAM_UI_BROADCAST_SECONDS", "1.0") or "1.0"))
GLOBAL_QUOTE_REFRESH_SECONDS = max(2.0, float(os.environ.get("GLOBAL_QUOTE_REFRESH_SECONDS", "5.0") or "5.0"))
MIN_NEWS_STALE_SECONDS = 600
LIVE_NSE_QUOTE_CACHE_TTL = 8.0
CLOSED_NSE_QUOTE_CACHE_TTL = 45.0
NSE_SESSION_REFRESH_SECONDS = 900
MAX_QUOTE_WORKERS = 8
MAX_NEWS_WORKERS = 8
NSE_PROVIDER_NAME = "nse"
UPSTOX_PROVIDER_NAME = "upstox"
UPSTOX_DEFAULT_API_BASE = "https://api.upstox.com/v2"
UPSTOX_DEFAULT_V3_API_BASE = "https://api.upstox.com/v3"
UPSTOX_QUOTE_BATCH_LIMIT = 500
UPSTOX_INSTRUMENT_SEARCH_TTL = 86400.0
UPSTOX_STREAM_MODE = "full"
UPSTOX_STREAM_RECONNECT_SECONDS = 5
UPSTOX_STREAM_OPEN_STALE_SECONDS = 12.0
UPSTOX_STREAM_CLOSED_STALE_SECONDS = 180.0
AI_CHAT_MAX_HISTORY = max(0, int(os.environ.get("AI_CHAT_MAX_HISTORY", "8") or "8"))
AI_CHAT_MAX_CONTEXT_ARTICLES = max(1, int(os.environ.get("AI_CHAT_MAX_CONTEXT_ARTICLES", "10") or "10"))
AI_CHAT_MAX_WEB_RESULTS = max(0, int(os.environ.get("AI_CHAT_MAX_WEB_RESULTS", "6") or "6"))
AI_CHAT_WEB_CACHE_TTL = max(30.0, float(os.environ.get("AI_CHAT_WEB_CACHE_TTL", "180") or "180"))
AI_CHAT_CONTEXT_CHAR_LIMIT = max(4000, int(os.environ.get("AI_CHAT_CONTEXT_CHAR_LIMIT", "14000") or "14000"))
AI_CHAT_MAX_TOKENS = max(200, int(os.environ.get("AI_CHAT_MAX_TOKENS", "750") or "750"))
RUNTIME_NEWS_STATE_KEY = "runtime_news_payload"
RUNTIME_SNAPSHOT_STATE_KEY = "runtime_market_snapshot"

EMPTY_ANALYTICS_PAYLOAD = {
    "generatedAt": None,
    "overviewCards": [],
    "alerts": [],
    "sectorBoard": [],
    "sectorMap": {},
    "keyLevels": [],
    "watchlistSignals": [],
    "symbolMap": {},
    "regime": None,
    "primary": [],
}

EMPTY_DERIVATIVES_PAYLOAD = {
    "generatedAt": None,
    "overviewCards": [],
    "predictionCards": [],
    "contextNotes": [],
    "riskFlags": [],
    "crossAssetRows": [],
    "relativeValueRows": [],
    "scoreBreakdown": [],
    "tradeScenarios": [],
    "signalMatrix": [],
    "triggerMap": [],
}

NSE_SESSION = requests.Session()
NSE_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept":           "application/json,text/html,*/*",
    "Accept-Language":  "en-IN,en;q=0.9",
    "Referer":          "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}
_sess = requests.Session()
_sess.verify = certifi.where()
_sess.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/html,*/*",
    "Accept-Language": "en-IN,en;q=0.9",
})
NSE_SESSION.verify = certifi.where()
NSE_SESSION.headers.update(NSE_HEADERS)

# ── State ──────────────────────────────────────────────────────────────────
_lock = threading.Lock()
_sse_lock = threading.Lock()
_sse_queues: list[queue.Queue[str]] = []

_arts: list[dict] = []
_ticks: dict = {}
_index_snapshot: dict = {}
_feed_status: dict = {}
_updated: str = "loading..."
_price_history: dict[str, list[float]] = {}
MAX_HIST = 40
NSE_HOLIDAY_CALENDAR: dict[str, dict[str, str]] = {}
_app_state: dict = {
    "tickerSelections": list(DEFAULT_TRACKED_TICKERS),
    "watchlist": list(DEFAULT_WATCHLIST),
    "bookmarks": [],
    "portfolio": {},
}
_has_persisted_state = False
_analytics_payload: dict = dict(EMPTY_ANALYTICS_PAYLOAD)
_derivatives_payload: dict = dict(EMPTY_DERIVATIVES_PAYLOAD)
_tracked_symbol_quotes: dict[str, dict] = {}
_last_news_refresh_ts: float | None = None
_last_tick_refresh_ts: float | None = None
_last_analytics_refresh_ts: float | None = None
_last_derivatives_refresh_ts: float | None = None
_last_macro_context_run_at: datetime | None = None
_last_fast_stream_broadcast_ts = 0.0

_nse_quote_cache: dict[str, tuple[dict, float]] = {}
_upstox_quote_cache: dict[str, tuple[dict, float]] = {}
_ai_summary_service: NewsAiSummaryService | None = None
_ai_chat_web_cache: dict[str, tuple[list[dict], float]] = {}
_upstox_stream_quote_cache: dict[str, tuple[dict, float]] = {}
_upstox_instrument_search_cache: dict[str, tuple[list[dict], float]] = {}
_upstox_curl_preferred_until = 0.0
_news_refresh_seconds = 300
_refresh_wakeup = threading.Event()
_upstox_stream_wakeup = threading.Event()
_thread_local = threading.local()
_upstox_stream_status = {
    "connected": False,
    "lastConnectAt": None,
    "lastDisconnectAt": None,
    "lastMessageAt": None,
    "lastError": None,
    "mode": UPSTOX_STREAM_MODE,
    "desiredSubscriptions": 0,
    "activeSubscriptions": 0,
    "segmentStatus": {},
    "dependencyReady": False,
}


# ── Helpers ────────────────────────────────────────────────────────────────
def ist_now() -> datetime:
    return datetime.now(IST)


def current_india_vix_quote() -> dict | None:
    with _lock:
        index_vix = dict(_index_snapshot.get("India VIX") or {}) if isinstance(_index_snapshot.get("India VIX"), dict) else {}
        tick_vix = dict(_ticks.get("VIX") or _ticks.get("India VIX") or {}) if isinstance(_ticks.get("VIX") or _ticks.get("India VIX"), dict) else {}
    payload = index_vix or tick_vix
    return payload or None


def _runtime_context_or_none():
    return globals().get("runtime_context")


def is_market_open() -> bool:
    return bool(get_market_status()["isMarketOpen"])


def _build_http_session() -> requests.Session:
    sess = requests.Session()
    sess.verify = certifi.where()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 Chrome/124 Safari/537.36",
        "Accept": "application/json,text/html,*/*",
        "Accept-Language": "en-IN,en;q=0.9",
    })
    return sess


def http_session() -> requests.Session:
    sess = getattr(_thread_local, "http_session", None)
    if sess is None:
        sess = _build_http_session()
        _thread_local.http_session = sess
    return sess


def _build_nse_session() -> requests.Session:
    sess = requests.Session()
    sess.verify = certifi.where()
    sess.headers.update(NSE_HEADERS)
    sess._market_desk_ready_at = 0.0
    return sess


def nse_session() -> requests.Session:
    sess = getattr(_thread_local, "nse_session", None)
    if sess is None:
        sess = _build_nse_session()
        _thread_local.nse_session = sess
    return sess


def requested_market_data_provider() -> str:
    return provider_status.requested_market_data_provider()


def upstox_analytics_token() -> str:
    return provider_status.upstox_analytics_token()


def upstox_token_source() -> str:
    return provider_status.upstox_token_source()


def upstox_configured() -> bool:
    return provider_status.upstox_configured()


def upstox_api_base() -> str:
    return upstox_runtime.upstox_api_base()


def upstox_v3_api_base() -> str:
    return upstox_runtime.upstox_v3_api_base()


def upstox_fallback_enabled() -> bool:
    return provider_status.upstox_fallback_enabled()


def upstox_debug_enabled() -> bool:
    return upstox_runtime.upstox_debug_enabled()


def upstox_http_transport() -> str:
    return upstox_runtime.upstox_http_transport()


def upstox_user_agent() -> str:
    return upstox_runtime.upstox_user_agent()


def upstox_stream_stale_after(status: dict | None = None) -> float:
    return provider_status.upstox_stream_stale_after(status, context=_runtime_context_or_none())


def market_data_provider_status() -> dict:
    return provider_status.market_data_provider_status(context=_runtime_context_or_none())


def active_market_data_provider() -> str:
    return provider_status.active_market_data_provider(context=_runtime_context_or_none())


def ticker_refresh_interval(status: dict | None = None) -> int:
    return provider_status.ticker_refresh_interval(status, context=_runtime_context_or_none())


def nse_quote_cache_ttl(status: dict | None = None) -> float:
    return provider_status.nse_quote_cache_ttl(status, context=_runtime_context_or_none())


def quote_age_seconds(quote: dict | None, now_ts: float | None = None) -> float | None:
    return provider_status.quote_age_seconds(quote, now_ts)


def upstox_stream_runtime_status() -> dict:
    return provider_status.upstox_stream_runtime_status(context=_runtime_context_or_none())


def upstox_rest_runtime_status() -> dict:
    return provider_status.upstox_rest_runtime_status(context=_runtime_context_or_none())


def _set_upstox_rest_status(**patch) -> None:
    provider_status.patch_upstox_rest_status(**patch)


def set_upstox_rest_status(ok: bool, error: str = "") -> None:
    provider_status.set_upstox_rest_status(ok, error)


def _short_error(exc: Exception | str, limit: int = 180) -> str:
    return upstox_runtime._short_error(exc, limit)


def upstox_stream_dependencies_ready() -> bool:
    return upstox_runtime.upstox_stream_dependencies_ready()


def upstox_stream_authorized_redirect_uri() -> str:
    return upstox_runtime.upstox_stream_authorized_redirect_uri(context=_runtime_context_or_none())


def upstox_integration_status() -> dict:
    return upstox_runtime.upstox_integration_status(context=_runtime_context_or_none())


def upstox_runtime_status() -> dict:
    return upstox_runtime.upstox_runtime_status(context=_runtime_context_or_none())


def load_holiday_calendar(path: Path = HOLIDAY_CALENDAR_PATH) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    calendar: dict[str, dict[str, str]] = {}
    for year, items in data.items():
        year_key = str(year)
        calendar[year_key] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            date_str = str(item.get("date", "")).strip()
            name = str(item.get("name") or item.get("description") or "").strip()
            if date_str and name:
                calendar[year_key][date_str] = name
    return calendar


def holiday_name_on(day: date) -> str | None:
    return NSE_HOLIDAY_CALENDAR.get(str(day.year), {}).get(day.isoformat())


def is_trading_day(day: date) -> tuple[bool, str | None]:
    if day.weekday() >= 5:
        return False, None
    holiday_name = holiday_name_on(day)
    if holiday_name:
        return False, holiday_name
    return True, None


def combine_ist(day: date, value: dt_time) -> datetime:
    return datetime.combine(day, value, tzinfo=IST)


def next_trading_open(now: datetime) -> datetime | None:
    for offset in range(0, 370):
        candidate_day = now.date() + timedelta(days=offset)
        is_open_day, _ = is_trading_day(candidate_day)
        if not is_open_day:
            continue
        session_open = combine_ist(candidate_day, MARKET_OPEN_TIME)
        if offset == 0 and now >= session_open:
            continue
        return session_open
    return None


def get_market_status(now: datetime | None = None) -> dict:
    now = now or ist_now()
    today = now.date()
    is_open_day, holiday_name = is_trading_day(today)
    session_open = combine_ist(today, MARKET_OPEN_TIME)
    session_close = combine_ist(today, MARKET_CLOSE_TIME)

    if not is_open_day:
        if holiday_name:
            session = "holiday"
            reason = f"Exchange holiday: {holiday_name}"
        else:
            session = "weekend"
            reason = f"Weekend closure ({now.strftime('%A')})"
        is_market_open_now = False
    elif now < session_open:
        session = "preopen"
        reason = f"Pre-open. Regular trading starts at {MARKET_OPEN_TIME.strftime('%H:%M')} IST."
        is_market_open_now = False
    elif now <= session_close:
        session = "open"
        reason = "Regular cash-market trading session."
        is_market_open_now = True
    else:
        session = "postclose"
        reason = f"Post-close. Regular trading ended at {MARKET_CLOSE_TIME.strftime('%H:%M')} IST."
        is_market_open_now = False

    with _lock:
        news_ts = _last_news_refresh_ts
        tick_ts = _last_tick_refresh_ts

    news_age = round(now.timestamp() - news_ts, 1) if news_ts else None
    ticker_age = round(now.timestamp() - tick_ts, 1) if tick_ts else None
    news_stale_after = max(get_news_refresh_seconds() * 2, MIN_NEWS_STALE_SECONDS)
    tickers_stale_after = INTRADAY_TICK_STALE_SECONDS if is_market_open_now else AFTER_HOURS_TICK_STALE_SECONDS
    news_stale = news_age is None or news_age > news_stale_after
    tickers_stale = ticker_age is None or ticker_age > tickers_stale_after

    session_labels = {
        "holiday": "Exchange holiday",
        "weekend": "Weekend",
        "preopen": "Pre-open",
        "open": "Market open",
        "postclose": "Post-close",
    }
    next_open = next_trading_open(now)
    return {
        "asOf": now.isoformat(),
        "isTradingDay": bool(is_open_day),
        "isMarketOpen": bool(is_market_open_now),
        "session": session,
        "sessionLabel": session_labels[session],
        "reason": reason,
        "holiday": holiday_name,
        "nextOpen": next_open.isoformat() if next_open else None,
        "newsAgeSeconds": news_age,
        "tickerAgeSeconds": ticker_age,
        "newsStale": news_stale,
        "tickersStale": tickers_stale,
        "staleData": news_stale or tickers_stale,
        "tickIntervalSeconds": ticker_refresh_interval({"session": session, "isMarketOpen": is_market_open_now}),
        "tickStaleAfterSeconds": tickers_stale_after,
    }


def tracked_symbols_for_state(state: dict) -> list[str]:
    symbols = []
    for sym in state.get("tickerSelections", []):
        if sym not in symbols:
            symbols.append(sym)
    for sym in state.get("watchlist", []):
        if sym not in symbols:
            symbols.append(sym)
    for sym in state.get("portfolio", {}).keys():
        if sym not in symbols:
            symbols.append(sym)
        if len(symbols) >= TRACKED_QUOTE_LIMIT:
            break
    return symbols[:TRACKED_QUOTE_LIMIT]


def analytics_symbols_for_state(state: dict) -> list[str]:
    symbols = []
    for sym in state.get("watchlist", []):
        if sym not in symbols:
            symbols.append(sym)
    for sym in state.get("portfolio", {}).keys():
        if sym not in symbols:
            symbols.append(sym)
        if len(symbols) >= WATCHLIST_SYMBOL_LIMIT:
            break
    return symbols[:WATCHLIST_SYMBOL_LIMIT]


def get_news_refresh_seconds() -> int:
    return news_runtime.get_news_refresh_seconds()


def set_news_refresh_seconds(seconds: int) -> int:
    global _news_refresh_seconds
    current = news_runtime.set_news_refresh_seconds(seconds)
    with _lock:
        _news_refresh_seconds = current
    return current


def get_app_state_copy() -> dict:
    with _lock:
        return {
            "tickerSelections": list(_app_state["tickerSelections"]),
            "watchlist": list(_app_state["watchlist"]),
            "bookmarks": list(_app_state["bookmarks"]),
            "portfolio": dict(_app_state["portfolio"]),
        }


def persist_app_state(state: dict) -> None:
    for key in DEFAULT_APP_STATE:
        db_set_json(key, state[key])


def update_app_state(payload: dict | None) -> dict:
    global _app_state, _has_persisted_state
    patch = sanitize_state_patch(payload)
    if not patch:
        return get_app_state_copy()

    with _lock:
        merged = {
            "tickerSelections": list(_app_state["tickerSelections"]),
            "watchlist": list(_app_state["watchlist"]),
            "bookmarks": list(_app_state["bookmarks"]),
            "portfolio": dict(_app_state["portfolio"]),
        }
        merged.update(patch)
        _app_state = merged
        _has_persisted_state = True

    persist_app_state(merged)
    _upstox_stream_wakeup.set()
    try:
        rebuild_computed_payloads()
        persist_runtime_snapshot_payload()
        broadcast_market_snapshot()
    except Exception:
        pass
    return get_app_state_copy()


def format_quote_for_client(sym: str, quote: dict | None, status: dict | None = None) -> dict:
    return market_runtime.format_quote_for_client(sym, quote, context=_runtime_context_or_none(), status=status)


def format_quotes_for_client(quotes: dict[str, dict], status: dict | None = None) -> dict[str, dict]:
    return market_runtime.format_quotes_for_client(quotes, context=_runtime_context_or_none(), status=status)


def refresh_quote_cache_for_symbols(symbols: list[str]) -> dict[str, dict]:
    return market_runtime.refresh_quote_cache_for_symbols(symbols, context=_runtime_context_or_none())


def refresh_tracked_symbol_quotes(state: dict | None = None) -> dict[str, dict]:
    return market_runtime.refresh_tracked_symbol_quotes(context=_runtime_context_or_none(), state=state)


def rebuild_computed_payloads() -> None:
    market_runtime.rebuild_computed_payloads(context=_runtime_context_or_none())


def market_data_snapshot(include_history: bool = False) -> dict:
    return market_runtime.market_data_snapshot(context=_runtime_context_or_none(), include_history=include_history)


def initialize_runtime_state() -> None:
    return background_runtime.initialize_runtime_state(context=_runtime_context_or_none())


_background_threads_started = False
_background_threads_lock = threading.Lock()


def background_threads_enabled() -> bool:
    return background_runtime.background_threads_enabled()


def external_worker_mode() -> bool:
    return background_runtime.external_worker_mode()


def macro_background_thread_enabled() -> bool:
    return background_runtime.macro_background_thread_enabled()


def start_background_workers() -> bool:
    return background_runtime.start_background_workers(context=_runtime_context_or_none())


def background_runtime_status() -> dict:
    return background_runtime.background_runtime_status(context=_runtime_context_or_none())


def persist_runtime_news_payload(articles: list[dict], feed_status: dict, updated: str, refreshed_at: float) -> None:
    news_runtime.persist_runtime_news_payload(articles, feed_status, updated, refreshed_at, context=_runtime_context_or_none())


def persist_runtime_snapshot_payload() -> None:
    market_runtime.persist_runtime_snapshot_payload(context=_runtime_context_or_none())


def runtime_news_payload_from_db() -> dict | None:
    return news_runtime.runtime_news_payload_from_db()


def runtime_snapshot_from_db(include_history: bool = False) -> dict | None:
    return market_runtime.runtime_snapshot_from_db(include_history=include_history)


# ── AI market chat helpers ─────────────────────────────────────────────────
def ai_chat_provider_name() -> str:
    return ai_runtime.ai_chat_provider_name()


def _trim_text(value, limit: int = 420) -> str:
    return ai_runtime._trim_text(value, limit)


def _compact_chat_history(history) -> list[dict]:
    return ai_runtime._compact_chat_history(history)


def _price_momentum_for_chat(history: list | tuple | None) -> dict | None:
    return ai_runtime._price_momentum_for_chat(history)


def _compact_quote_for_chat(label: str, quote: dict | None, history: list | tuple | None = None) -> dict | None:
    return ai_runtime._compact_quote_for_chat(label, quote, history, context=_runtime_context_or_none())


def _chat_query_terms(question: str) -> set[str]:
    return ai_runtime._chat_query_terms(question)


def _article_relevance_score(article: dict, query_terms: set[str]) -> tuple[int, float]:
    return ai_runtime._article_relevance_score(article, query_terms)


def _articles_for_ai_chat() -> list[dict]:
    return ai_runtime._articles_for_ai_chat(context=_runtime_context_or_none())


def _article_ai_context(article: dict, *, summary_limit: int = 700) -> dict:
    return ai_runtime._article_ai_context(article, summary_limit=summary_limit)


def _topic_ai_summaries_for_ai_chat(question: str, limit: int = 8) -> list[dict]:
    return ai_runtime._topic_ai_summaries_for_ai_chat(question, limit=limit, context=_runtime_context_or_none())


def _recent_articles_for_ai_chat(question: str, limit: int = AI_CHAT_MAX_CONTEXT_ARTICLES) -> list[dict]:
    return ai_runtime._recent_articles_for_ai_chat(question, limit=limit, context=_runtime_context_or_none())


def _ai_chat_web_query(question: str) -> str:
    return ai_runtime._ai_chat_web_query(question)


def _internet_results_for_ai_chat(question: str) -> list[dict]:
    return ai_runtime._internet_results_for_ai_chat(
        question,
        context=_runtime_context_or_none(),
        feed_fetcher=_get_feed,
        web_cache=_ai_chat_web_cache,
    )


def build_ai_chat_context(question: str) -> dict:
    return ai_runtime.build_ai_chat_context(
        question,
        context=_runtime_context_or_none(),
        feed_fetcher=_get_feed,
        web_cache=_ai_chat_web_cache,
    )


def build_ai_chat_prompt(question: str, history, context: dict) -> str:
    return ai_runtime.build_ai_chat_prompt(question, context, history=history)


def generate_ai_chat_response(question: str, history) -> tuple[str, str, str]:
    return ai_runtime.generate_ai_chat_response(
        question,
        history=history,
        context=_runtime_context_or_none(),
        provider_factory=create_ai_text_provider,
        context_builder=build_ai_chat_context,
        http_session_factory=http_session,
    )


# ── Network helpers ────────────────────────────────────────────────────────
def _get_feed(url: str) -> bytes:
    return news_runtime._get_feed(url)


def handle_ai_article_analysis_applied(article: dict) -> None:
    ai_runtime.handle_ai_article_analysis_applied(article, context=_runtime_context_or_none())


def persist_news_article_analysis(cache_key: str, article: dict, analysis: dict, path: Path = STATE_DB_PATH) -> None:
    ai_runtime.persist_news_article_analysis(cache_key, article, analysis, path)


def ai_summary_service() -> NewsAiSummaryService:
    global _ai_summary_service
    if _ai_summary_service is None:
        _ai_summary_service = ai_runtime.ai_summary_service(context=_runtime_context_or_none())
    return _ai_summary_service


def ollama_api_base() -> str:
    return ai_summary_service().ollama_api_base()


def ai_news_summary_model() -> str:
    return ai_summary_service().ai_news_summary_model()


def ai_news_summaries_enabled() -> bool:
    return ai_summary_service().ai_news_summaries_enabled()


def article_extraction_enabled() -> bool:
    return ai_summary_service().article_extraction_enabled()


def article_link_supports_direct_extraction(link: str) -> bool:
    return ai_summary_service().article_link_supports_direct_extraction(link)


def fetch_accessible_article_text(article: dict) -> str:
    return ai_summary_service().fetch_accessible_article_text(article)


def prepare_article_for_ai_summary(article: dict) -> dict:
    return ai_summary_service().prepare_article_for_ai_summary(article)


def truncate_ai_summary_input(text: str) -> str:
    return ai_summary_service().truncate_ai_summary_input(text)


def ai_summary_cache_key(article: dict) -> str:
    return ai_summary_service().ai_summary_cache_key(article)


def ai_analysis_cache_key(article: dict) -> str:
    return ai_summary_service().ai_analysis_cache_key(article)


def get_cached_ai_news_summary(cache_key: str) -> str:
    return ai_summary_service().get_cached_ai_news_summary(cache_key)


def article_is_in_ai_summary_window(article: dict, now: float | None = None) -> bool:
    return ai_summary_service().article_is_in_ai_summary_window(article, now)


def article_has_ai_summary(article: dict) -> bool:
    return ai_summary_service().article_has_ai_summary(article)


def ai_summary_update_payload(article: dict) -> dict:
    return ai_summary_service().ai_summary_update_payload(article)


def hydrate_article_from_ai_cache(article: dict) -> bool:
    return ai_summary_service().hydrate_article_from_ai_cache(article)


def ai_summary_progress_for_articles(articles: list[dict], now: float | None = None) -> dict:
    return ai_summary_service().ai_summary_progress_for_articles(articles, now)


def ai_summary_executor() -> ThreadPoolExecutor:
    return ai_summary_service().ai_summary_executor_instance()


def apply_ai_summary_to_article(article: dict, summary: str) -> None:
    ai_summary_service().apply_ai_summary_to_article(article, summary)


def generate_ai_news_summary(article: dict) -> str:
    return ai_summary_service().generate_ai_news_summary(article)


def queue_ai_news_summary(article: dict) -> None:
    ai_summary_service().queue_ai_news_summary(article)


def enrich_articles_with_ai_summaries(articles: list[dict]) -> None:
    ai_summary_service().enrich_articles_with_ai_summaries(articles)


def ai_runtime_status() -> dict:
    return ai_runtime.ai_runtime_status()


def _nse_init_session(force: bool = False) -> requests.Session:
    sess = nse_session()
    now = time.time()
    ready_at = float(getattr(sess, "_market_desk_ready_at", 0.0) or 0.0)
    if not force and (now - ready_at) < NSE_SESSION_REFRESH_SECONDS:
        return sess
    try:
        sess.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=6, verify=certifi.where())
        sess._market_desk_ready_at = now
    except Exception as e:
        print(f"[!] NSE session init: {e}")
    return sess


def _is_missing_number(value) -> bool:
    return value is None or value != value


def _clean_general_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9&.^-]", "", str(symbol or "").upper().strip())


def fetch_cross_asset_quotes() -> dict[str, dict]:
    return {}


def upstox_stream_subscription_map(state: dict | None = None) -> dict[str, str]:
    return upstox_runtime.upstox_stream_subscription_map(state=state, context=_runtime_context_or_none())


def upstox_headers() -> dict[str, str]:
    return upstox_runtime.upstox_headers()


def upstox_response_error(response) -> str:
    return upstox_runtime.upstox_response_error(response)


UpstoxEdgeBlockedError = upstox_runtime.UpstoxEdgeBlockedError


def upstox_response_is_html_block(response) -> bool:
    return upstox_runtime.upstox_response_is_html_block(response)


def _upstox_http_status_code(response) -> int:
    return upstox_runtime._upstox_http_status_code(response)


def _prefer_upstox_curl() -> bool:
    return upstox_runtime._prefer_upstox_curl(context=_runtime_context_or_none())


def _mark_upstox_curl_preferred(seconds: float = 900.0) -> None:
    upstox_runtime._mark_upstox_curl_preferred(seconds, context=_runtime_context_or_none())


def _upstox_request_json_with_requests(url: str, timeout: int) -> dict:
    return upstox_runtime._upstox_request_json_with_requests(url, timeout, context=_runtime_context_or_none())


def _curl_config_value(value: str) -> str:
    return upstox_runtime._curl_config_value(value)


def _upstox_request_json_with_curl(url: str, timeout: int) -> dict:
    return upstox_runtime._upstox_request_json_with_curl(url, timeout, context=_runtime_context_or_none())


def upstox_request_json(url: str, timeout: int = 8) -> dict:
    return upstox_runtime.upstox_request_json(url, timeout=timeout, context=_runtime_context_or_none())


def _parse_upstox_quote_payload(label_to_key: dict[str, str], payload: dict, received_at: float) -> dict[str, dict]:
    return upstox_runtime._parse_upstox_quote_payload(label_to_key, payload, received_at, context=_runtime_context_or_none())


def upstox_quotes_url(instrument_keys: list[str]) -> str:
    return upstox_runtime.upstox_quotes_url(instrument_keys)


def upstox_instrument_search_url(
    query: str,
    exchanges: str = "NSE",
    segments: str = "EQ,INDEX",
    records: int = 12,
) -> str:
    return upstox_runtime.upstox_instrument_search_url(query, exchanges=exchanges, segments=segments, records=records)


def _clean_market_symbol(symbol: str) -> str:
    return upstox_runtime._clean_market_symbol(symbol)


def _upstox_search_cache_key(query: str, exchanges: str, segments: str, records: int) -> str:
    return upstox_runtime._upstox_search_cache_key(query, exchanges, segments, records)


def upstox_search_instruments(
    query: str,
    exchanges: str = "NSE",
    segments: str = "EQ,INDEX",
    records: int = 12,
) -> list[dict]:
    return upstox_runtime.upstox_search_instruments(
        query,
        exchanges=exchanges,
        segments=segments,
        records=records,
        context=_runtime_context_or_none(),
    )


def _symbol_from_upstox_instrument(row: dict) -> str:
    return upstox_runtime._symbol_from_upstox_instrument(row)


def _sector_for_upstox_instrument(row: dict) -> str:
    return upstox_runtime._sector_for_upstox_instrument(row)


def _upstox_instrument_to_suggestion(row: dict) -> dict | None:
    return upstox_runtime._upstox_instrument_to_suggestion(row)


def upstox_symbol_search_results(query: str, limit: int = 8) -> list[dict]:
    return upstox_runtime.upstox_symbol_search_results(query, limit=limit, context=_runtime_context_or_none())


def resolve_upstox_instrument_key(symbol: str) -> str | None:
    return upstox_runtime.resolve_upstox_instrument_key(symbol, context=_runtime_context_or_none())


def upstox_option_chain_url(underlying_key: str, expiry_date: str) -> str:
    return upstox_runtime.upstox_option_chain_url(underlying_key, expiry_date)


def fetch_upstox_quote_batch(label_to_key: dict[str, str], received_at: float | None = None) -> dict[str, dict]:
    return upstox_runtime.fetch_upstox_quote_batch(label_to_key, received_at=received_at, context=_runtime_context_or_none())


def fetch_upstox_stream_quotes_by_label(label_to_key: dict[str, str]) -> dict[str, dict]:
    return upstox_runtime.fetch_upstox_stream_quotes_by_label(label_to_key, context=_runtime_context_or_none())


def fetch_upstox_quotes_by_label(label_to_key: dict[str, str]) -> dict[str, dict]:
    return upstox_runtime.fetch_upstox_quotes_by_label(label_to_key, context=_runtime_context_or_none())


def _fetch_upstox_quote(symbol: str) -> dict | None:
    return upstox_runtime._fetch_upstox_quote(symbol, context=_runtime_context_or_none())


def fetch_upstox_index_quotes() -> dict[str, dict]:
    return upstox_runtime.fetch_upstox_index_quotes(context=_runtime_context_or_none())


def ticker_payload_from_quote(quote: dict, default_sym: str = "") -> dict:
    return upstox_runtime.ticker_payload_from_quote(quote, default_sym=default_sym, context=_runtime_context_or_none())


def apply_quote_update_to_runtime(label: str, quote: dict, *, update_indexes: bool = True) -> None:
    upstox_runtime.apply_quote_update_to_runtime(
        label,
        quote,
        update_indexes=update_indexes,
        context=_runtime_context_or_none(),
    )


def maybe_broadcast_fast_market_snapshot(force: bool = False) -> None:
    upstox_runtime.maybe_broadcast_fast_market_snapshot(force=force, context=_runtime_context_or_none())


def _set_upstox_stream_status(**patch) -> None:
    upstox_runtime._set_upstox_stream_status(context=_runtime_context_or_none(), **patch)


def _send_upstox_stream_request(ws, method: str, instrument_keys: list[str], mode: str = UPSTOX_STREAM_MODE) -> None:
    upstox_runtime._send_upstox_stream_request(ws, method, instrument_keys, mode=mode)


def _apply_upstox_stream_payload(payload: dict, label_by_key: dict[str, str]) -> None:
    upstox_runtime._apply_upstox_stream_payload(payload, label_by_key, context=_runtime_context_or_none())


def upstox_stream_loop() -> None:
    upstox_runtime.upstox_stream_loop(context=_runtime_context_or_none())


def fetch_live_quote(symbol: str) -> dict | None:
    return market_runtime.fetch_live_quote(symbol, context=_runtime_context_or_none())


def fetch_upstox_option_chain(underlying: str, expiry_date: str, max_rows: int = 80) -> dict:
    if not upstox_configured():
        raise RuntimeError("UPSTOX_ANALYTICS_TOKEN is not configured")
    payload = upstox_runtime.fetch_upstox_option_chain(
        underlying,
        expiry_date=expiry_date,
        max_rows=max_rows,
        context=_runtime_context_or_none(),
    )
    if payload is None:
        raise RuntimeError("UPSTOX_ANALYTICS_TOKEN is not configured")
    return payload


def _fetch_nse_quote(symbol: str) -> dict | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    if not clean:
        return None
    now = time.time()
    cached = _nse_quote_cache.get(clean)
    if cached and (now - cached[1] < nse_quote_cache_ttl()):
        return cached[0]

    session = _nse_init_session()
    r = session.get(
        "https://www.nseindia.com/api/quote-equity",
        params={"symbol": clean},
        headers=NSE_HEADERS,
        timeout=6,
        verify=certifi.where(),
    )
    if r.status_code in {401, 403}:
        session = _nse_init_session(force=True)
        r = session.get(
            "https://www.nseindia.com/api/quote-equity",
            params={"symbol": clean},
            headers=NSE_HEADERS,
            timeout=6,
            verify=certifi.where(),
        )
    r.raise_for_status()
    payload = r.json()
    pi = payload.get("priceInfo", {})
    last = safe_float(pi.get("lastPrice"))
    prev = safe_float(pi.get("previousClose"), last)
    if not last:
        return None
    quote = {
        "symbol": clean,
        "name": payload.get("info", {}).get("companyName") or clean,
        "price": round(last, 2),
        "previous_close": round(prev, 2),
        "change": round(last - prev, 2),
        "pct": round(((last - prev) / prev * 100) if prev else 0, 2),
        "day_high": round(safe_float(pi.get("intraDayHighLow", {}).get("max"), last), 2),
        "day_low": round(safe_float(pi.get("intraDayHighLow", {}).get("min"), last), 2),
        "fetchedAt": now,
        "source": "NSE",
    }
    _nse_quote_cache[clean] = (quote, now)
    return quote


def _history_candidates(label_or_symbol: str, is_index: bool = False) -> list[str]:
    return []


def build_live_only_signal(symbol: str, live_quote: dict) -> dict:
    return analytics_runtime.build_live_only_signal(symbol, live_quote)


def build_symbol_signal(symbol: str, live_quote: dict | None = None, is_index: bool = False) -> dict | None:
    return analytics_runtime.build_symbol_signal(
        symbol,
        live_quote=live_quote,
        is_index=is_index,
        context=_runtime_context_or_none(),
    )


# ── Data fetchers ──────────────────────────────────────────────────────────
def fetch_feed_articles(feed_cfg: dict[str, str]) -> tuple[str, dict, list[dict]]:
    return news_runtime.fetch_feed_articles(feed_cfg)


def fetch_news() -> tuple[list[dict], dict]:
    return news_runtime.fetch_news(context=_runtime_context_or_none())


def refresh_news_now() -> list[dict]:
    return news_runtime.refresh_news_now(context=_runtime_context_or_none())


def get_latest_news_items() -> list[dict]:
    return news_runtime.get_latest_news_items(context=_runtime_context_or_none())


def news_runtime_status() -> dict:
    return news_runtime.news_runtime_status()


def fetch_tickers() -> tuple[dict, dict]:
    return market_runtime.fetch_tickers(context=_runtime_context_or_none())


def build_market_analytics_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    symbols: list[str],
    quote_map: dict[str, dict] | None = None,
) -> dict:
    return analytics_runtime.build_market_analytics_payload(
        articles,
        ticks,
        index_snapshot,
        symbols,
        quote_map=quote_map,
        context=_runtime_context_or_none(),
    )


def build_derivatives_analysis_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    option_chain: dict | None = None,
    price_history: dict[str, list[float]] | None = None,
    market_status: dict | None = None,
) -> dict:
    return analytics_runtime.build_derivatives_analysis_payload(
        articles,
        ticks,
        index_snapshot,
        option_chain=option_chain,
        price_history=price_history,
        market_status=market_status,
        context=_runtime_context_or_none(),
    )


def analytics_runtime_status() -> dict:
    return analytics_runtime.analytics_runtime_status(context=_runtime_context_or_none())


# ── Background loops ───────────────────────────────────────────────────────
def broadcast_market_snapshot() -> None:
    market_runtime.broadcast_market_snapshot(context=_runtime_context_or_none())


def broadcast_tickers(data: dict) -> None:
    market_runtime.broadcast_tickers(data, context=_runtime_context_or_none())


def _update_price_history(ticks: dict) -> None:
    market_runtime.update_price_history(ticks, context=_runtime_context_or_none())


def update_price_history(symbol: str | dict, price: float | None = None) -> None:
    market_runtime.update_price_history(symbol, price, context=_runtime_context_or_none())


def market_runtime_status() -> dict:
    return market_runtime.market_runtime_status(context=_runtime_context_or_none())


def refresh_loop() -> None:
    return background_runtime.refresh_loop(context=_runtime_context_or_none())


def ticker_loop() -> None:
    return background_runtime.ticker_loop(context=_runtime_context_or_none())


def macro_context_loop() -> None:
    return background_runtime.macro_context_loop(context=_runtime_context_or_none())


def global_quote_loop() -> None:
    return background_runtime.global_quote_loop(context=_runtime_context_or_none())


# ── Flask ──────────────────────────────────────────────────────────────────
def _call_runtime_global(name: str, *args, **kwargs):
    return globals()[name](*args, **kwargs)


FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
runtime_context = build_runtime_context(
    current_india_vix_quote=current_india_vix_quote,
    build_macro_snapshot=build_macro_snapshot,
    run_macro_context_cycle=run_macro_context_cycle,
    get_latest_macro_context_report=get_latest_macro_context_report,
    build_market_regime_snapshot=build_market_regime_snapshot,
    run_market_regime_cycle=run_market_regime_cycle,
    get_latest_market_regime_report=get_latest_market_regime_report,
    build_fo_snapshot=build_fo_snapshot,
    run_fo_structure_cycle=run_fo_structure_cycle,
    get_latest_fo_structure_report=get_latest_fo_structure_report,
    refresh_news_now=refresh_news_now,
    get_latest_news_items=get_latest_news_items,
    fetch_news=fetch_news,
    get_news_refresh_seconds=get_news_refresh_seconds,
    set_news_refresh_seconds=set_news_refresh_seconds,
    runtime_news_payload_from_db=runtime_news_payload_from_db,
    persist_runtime_news_payload=persist_runtime_news_payload,
    generate_ai_chat_response=generate_ai_chat_response,
    ai_summary_progress_for_articles=ai_summary_progress_for_articles,
    ai_summary_update_payload=ai_summary_update_payload,
    hydrate_article_from_ai_cache=hydrate_article_from_ai_cache,
    article_has_ai_summary=article_has_ai_summary,
    build_live_only_signal=build_live_only_signal,
    build_symbol_signal=build_symbol_signal,
    build_market_analytics_payload=build_market_analytics_payload,
    build_derivatives_analysis_payload=build_derivatives_analysis_payload,
    analytics_runtime_status=analytics_runtime_status,
    format_quote_for_client=format_quote_for_client,
    format_quotes_for_client=format_quotes_for_client,
    refresh_quote_cache_for_symbols=refresh_quote_cache_for_symbols,
    refresh_tracked_symbol_quotes=refresh_tracked_symbol_quotes,
    rebuild_computed_payloads=rebuild_computed_payloads,
    market_data_snapshot=market_data_snapshot,
    fetch_live_quote=fetch_live_quote,
    fetch_tickers=fetch_tickers,
    broadcast_market_snapshot=broadcast_market_snapshot,
    broadcast_tickers=broadcast_tickers,
    update_price_history=update_price_history,
    runtime_snapshot_from_db=runtime_snapshot_from_db,
    persist_runtime_snapshot_payload=persist_runtime_snapshot_payload,
    market_runtime_status=market_runtime_status,
    provider_status=market_data_provider_status,
    market_data_provider_status=market_data_provider_status,
    active_market_data_provider=active_market_data_provider,
    upstox_stream_runtime_status=upstox_stream_runtime_status,
    upstox_rest_runtime_status=upstox_rest_runtime_status,
    ticker_refresh_interval=ticker_refresh_interval,
    nse_quote_cache_ttl=nse_quote_cache_ttl,
    quote_age_seconds=quote_age_seconds,
    upstox_runtime_status=upstox_runtime_status,
    upstox_search_instruments=lambda *args, **kwargs: _call_runtime_global("upstox_search_instruments", *args, **kwargs),
    upstox_symbol_search_results=lambda *args, **kwargs: _call_runtime_global("upstox_symbol_search_results", *args, **kwargs),
    resolve_upstox_instrument_key=lambda *args, **kwargs: _call_runtime_global("resolve_upstox_instrument_key", *args, **kwargs),
    fetch_upstox_quote_batch=lambda *args, **kwargs: _call_runtime_global("fetch_upstox_quote_batch", *args, **kwargs),
    fetch_upstox_quotes_by_label=lambda *args, **kwargs: _call_runtime_global("fetch_upstox_quotes_by_label", *args, **kwargs),
    fetch_upstox_stream_quotes_by_label=lambda *args, **kwargs: _call_runtime_global("fetch_upstox_stream_quotes_by_label", *args, **kwargs),
    fetch_upstox_index_quotes=lambda *args, **kwargs: _call_runtime_global("fetch_upstox_index_quotes", *args, **kwargs),
    fetch_upstox_option_chain=lambda *args, **kwargs: _call_runtime_global("fetch_upstox_option_chain", *args, **kwargs),
    upstox_stream_subscription_map=lambda *args, **kwargs: _call_runtime_global("upstox_stream_subscription_map", *args, **kwargs),
    upstox_stream_loop=lambda *args, **kwargs: _call_runtime_global("upstox_stream_loop", *args, **kwargs),
    upstox_integration_status=lambda *args, **kwargs: _call_runtime_global("upstox_integration_status", *args, **kwargs),
    initialize_runtime_state=lambda *args, **kwargs: _call_runtime_global("initialize_runtime_state", *args, **kwargs),
    start_background_workers=lambda *args, **kwargs: _call_runtime_global("start_background_workers", *args, **kwargs),
    background_threads_enabled=lambda *args, **kwargs: _call_runtime_global("background_threads_enabled", *args, **kwargs),
    external_worker_mode=lambda *args, **kwargs: _call_runtime_global("external_worker_mode", *args, **kwargs),
    macro_background_thread_enabled=lambda *args, **kwargs: _call_runtime_global("macro_background_thread_enabled", *args, **kwargs),
    refresh_loop=lambda *args, **kwargs: _call_runtime_global("refresh_loop", *args, **kwargs),
    ticker_loop=lambda *args, **kwargs: _call_runtime_global("ticker_loop", *args, **kwargs),
    macro_context_loop=lambda *args, **kwargs: _call_runtime_global("macro_context_loop", *args, **kwargs),
    global_quote_loop=lambda *args, **kwargs: _call_runtime_global("global_quote_loop", *args, **kwargs),
    background_runtime_status=lambda *args, **kwargs: _call_runtime_global("background_runtime_status", *args, **kwargs),
    runtime_state=globals(),
)
register_frontend_routes(app, runtime_context)
register_health_routes(app, runtime_context)
register_news_routes(app, runtime_context)
register_news_agent_routes(app, runtime_context)
register_macro_agent_routes(app, runtime_context)
register_fo_agent_routes(app, runtime_context)
register_market_regime_routes(app, runtime_context)
register_market_routes(app, runtime_context)
register_derivatives_routes(app, runtime_context)
register_upstox_routes(app, runtime_context)


# ── Frontend entrypoint ────────────────────────────────────────────────────
initialize_runtime_state()
start_background_workers()

if __name__ == "__main__":
    host = os.environ.get("HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = int(os.environ.get("PORT", "9090"))

    print("=" * 60)
    print("  India Market Desk")
    print(f"  http://127.0.0.1:{port}")
    print()
    provider = market_data_provider_status()
    print(f"  Live {provider['active'].upper()} prices via SSE ({INTRADAY_TICK_INTERVAL_SECONDS}s intraday, {AFTER_HOURS_TICK_INTERVAL_SECONDS}s after-hours)")
    if provider["requested"] == UPSTOX_PROVIDER_NAME and provider["active"] != UPSTOX_PROVIDER_NAME:
        print("  Upstox requested but UPSTOX_ANALYTICS_TOKEN is not configured; using NSE fallback")
    print("  Ctrl+C to stop")
    print("=" * 60)
    # Do not auto-open a browser here; macOS may block it with Permission denied.
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
