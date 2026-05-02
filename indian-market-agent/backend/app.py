#!/usr/bin/env python3
"""
Indian Market News Bot backend.

Project layout:
    indian-market-news-bot/
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
        MACRO_AGENT_ENABLED,
        MACRO_AGENT_REFRESH_MODE,
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
    from backend.agents.macro_context import MacroContextAgent
    from backend.agents.macro_context.snapshot_builder import MacroSnapshotBuilder
    from backend.agents.macro_context.schedule import get_next_macro_refresh_time, is_macro_refresh_due
    from backend.providers.india_vix_provider import IndiaVixProvider
    from backend.routes.macro_agent_routes import register_macro_agent_routes
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
        MACRO_AGENT_ENABLED,
        MACRO_AGENT_REFRESH_MODE,
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
    from agents.macro_context import MacroContextAgent
    from agents.macro_context.snapshot_builder import MacroSnapshotBuilder
    from agents.macro_context.schedule import get_next_macro_refresh_time, is_macro_refresh_due
    from providers.india_vix_provider import IndiaVixProvider
    from routes.macro_agent_routes import register_macro_agent_routes
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
_upstox_rest_status = {
    "lastError": None,
    "lastErrorAt": None,
    "lastOkAt": None,
    "failedKeys": [],
}
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


def build_macro_snapshot(*, use_mock: bool = False):
    builder = MacroSnapshotBuilder(india_vix_provider=IndiaVixProvider(fetcher=current_india_vix_quote))
    return builder.build_mock_snapshot() if use_mock else builder.build()


def run_macro_context_cycle(*, force_refresh: bool = False, use_mock: bool = False):
    snapshot = build_macro_snapshot(use_mock=use_mock)
    report = MacroContextAgent().analyze(snapshot)
    if force_refresh:
        print("[*] Macro context refresh forced via API or worker call")
    return report


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
    provider = os.environ.get("MARKET_DATA_PROVIDER", NSE_PROVIDER_NAME).strip().lower()
    return provider if provider in {NSE_PROVIDER_NAME, UPSTOX_PROVIDER_NAME} else NSE_PROVIDER_NAME


def upstox_analytics_token() -> str:
    return os.environ.get("UPSTOX_ANALYTICS_TOKEN", "").strip()


def upstox_token_source() -> str:
    return "analytics_env" if upstox_analytics_token() else "none"


def upstox_configured() -> bool:
    return bool(upstox_analytics_token())


def upstox_api_base() -> str:
    return os.environ.get("UPSTOX_API_BASE", UPSTOX_DEFAULT_API_BASE).strip().rstrip("/")


def upstox_v3_api_base() -> str:
    configured = os.environ.get("UPSTOX_V3_API_BASE", "").strip().rstrip("/")
    if configured:
        return configured
    base = upstox_api_base()
    return re.sub(r"/v2/?$", "/v3", base) if re.search(r"/v2/?$", base) else UPSTOX_DEFAULT_V3_API_BASE


def upstox_fallback_enabled() -> bool:
    raw = os.environ.get("UPSTOX_FALLBACK_TO_NSE", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def upstox_debug_enabled() -> bool:
    raw = os.environ.get("UPSTOX_DEBUG", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def upstox_http_transport() -> str:
    transport = os.environ.get("UPSTOX_HTTP_TRANSPORT", "auto").strip().lower()
    return transport if transport in {"auto", "requests", "curl"} else "auto"


def upstox_user_agent() -> str:
    return os.environ.get("UPSTOX_USER_AGENT", "curl/8.7.1").strip() or "curl/8.7.1"


def upstox_stream_stale_after(status: dict | None = None) -> float:
    status = status or get_market_status()
    return (
        UPSTOX_STREAM_OPEN_STALE_SECONDS
        if status.get("session") in {"preopen", "open"}
        else UPSTOX_STREAM_CLOSED_STALE_SECONDS
    )


def market_data_provider_status() -> dict:
    requested = requested_market_data_provider()
    configured = upstox_configured()
    active = active_market_data_provider()
    stream = upstox_stream_runtime_status()
    rest = upstox_rest_runtime_status()
    token_source = upstox_token_source()
    degraded = requested == UPSTOX_PROVIDER_NAME and configured and bool(rest.get("lastError")) and not stream["connected"]
    return {
        "requested": requested,
        "active": active,
        "upstoxConfigured": configured,
        "upstoxAuthConfigured": configured,
        "upstoxTokenSource": token_source,
        "upstoxTokenMode": "analytics" if configured else "none",
        "fallbackToNse": upstox_fallback_enabled(),
        "streamConnected": stream["connected"],
        "streamDependencyReady": stream["dependencyReady"],
        "degraded": degraded,
        "rest": rest,
        "reason": (
            "Upstox analytics token missing; using NSE fallback"
            if requested == UPSTOX_PROVIDER_NAME and not configured
            else "Upstox V3 live stream enabled"
            if active == UPSTOX_PROVIDER_NAME and stream["connected"]
            else f"Upstox REST issue; NSE fallback active: {rest['lastError']}"
            if degraded and upstox_fallback_enabled()
            else f"Upstox REST issue: {rest['lastError']}"
            if degraded
            else "Upstox REST quotes enabled"
            if active == UPSTOX_PROVIDER_NAME
            else "NSE public endpoints enabled"
        ),
    }


def active_market_data_provider() -> str:
    if requested_market_data_provider() == UPSTOX_PROVIDER_NAME and upstox_configured():
        return UPSTOX_PROVIDER_NAME
    return NSE_PROVIDER_NAME


def ticker_refresh_interval(status: dict | None = None) -> int:
    status = status or get_market_status()
    if status.get("session") == "preopen":
        return PREOPEN_TICK_INTERVAL_SECONDS
    if status.get("isMarketOpen"):
        return INTRADAY_TICK_INTERVAL_SECONDS
    return AFTER_HOURS_TICK_INTERVAL_SECONDS


def nse_quote_cache_ttl(status: dict | None = None) -> float:
    status = status or get_market_status()
    return LIVE_NSE_QUOTE_CACHE_TTL if status.get("session") in {"preopen", "open"} else CLOSED_NSE_QUOTE_CACHE_TTL


def quote_age_seconds(quote: dict | None, now_ts: float | None = None) -> float | None:
    if not quote or quote.get("fetchedAt") is None:
        return None
    now_ts = time.time() if now_ts is None else now_ts
    return round(max(now_ts - float(quote["fetchedAt"]), 0), 1)


def upstox_stream_runtime_status() -> dict:
    with _lock:
        status = dict(_upstox_stream_status)
    last_message_at = status.get("lastMessageAt")
    status["lastMessageAgeSeconds"] = round(max(time.time() - last_message_at, 0), 1) if last_message_at else None
    status["lastConnectAt"] = datetime.fromtimestamp(status["lastConnectAt"], IST).isoformat() if status.get("lastConnectAt") else None
    status["lastDisconnectAt"] = datetime.fromtimestamp(status["lastDisconnectAt"], IST).isoformat() if status.get("lastDisconnectAt") else None
    status["lastMessageAt"] = datetime.fromtimestamp(last_message_at, IST).isoformat() if last_message_at else None
    return status


def upstox_rest_runtime_status() -> dict:
    with _lock:
        status = dict(_upstox_rest_status)
    status["transport"] = upstox_http_transport()
    status["curlPreferred"] = _prefer_upstox_curl()
    if status.get("lastErrorAt"):
        status["lastErrorAt"] = datetime.fromtimestamp(status["lastErrorAt"], IST).isoformat()
    if status.get("lastOkAt"):
        status["lastOkAt"] = datetime.fromtimestamp(status["lastOkAt"], IST).isoformat()
    return status


def _set_upstox_rest_status(**patch) -> None:
    with _lock:
        _upstox_rest_status.update(patch)


def _short_error(exc: Exception | str, limit: int = 180) -> str:
    return str(exc).replace("\n", " ").strip()[:limit]


def upstox_stream_dependencies_ready() -> bool:
    try:
        import websocket  # noqa: F401
    except ImportError:
        return False
    return True


def upstox_stream_authorized_redirect_uri() -> str:
    payload = upstox_request_json(stream_authorize_url(upstox_v3_api_base()), timeout=8)
    uri = str((payload.get("data") or {}).get("authorized_redirect_uri") or "").strip()
    if not uri:
        raise RuntimeError("Upstox V3 authorize response did not include authorized_redirect_uri")
    return uri


def upstox_integration_status() -> dict:
    token = upstox_analytics_token()
    provider = market_data_provider_status()
    return {
        "provider": provider,
        "authConfigured": bool(token),
        "credential": "UPSTOX_ANALYTICS_TOKEN",
        "connected": bool(token),
        "tokenPreview": upstox_token_preview(token) if token else None,
        "tokenSource": provider["upstoxTokenSource"],
        "tokenMode": "analytics" if token else "none",
        "readOnly": True,
        "supportedApis": [
            "Full market quotes",
            "OHLC quotes V3",
            "LTP quotes V3",
            "Historical candle data V3",
            "Market Data Feed V3",
            "Market Data Feed Authorize V3",
            "Market Status",
            "Put/Call Option chain",
            "Option contracts",
            "Option Greeks",
            "Instrument Search",
        ],
        "stream": upstox_stream_runtime_status(),
        "dataDir": str(DATA_DIR),
    }


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
    with _lock:
        return _news_refresh_seconds


def set_news_refresh_seconds(seconds: int) -> int:
    global _news_refresh_seconds
    if seconds not in ALLOWED_REFRESH_WINDOWS:
        raise ValueError("Unsupported refresh interval")
    with _lock:
        _news_refresh_seconds = seconds
    persist_refresh_settings(seconds)
    _refresh_wakeup.set()
    return seconds


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


def format_quote_for_client(sym: str, quote: dict, status: dict | None = None) -> dict:
    status = status or get_market_status()
    entry = symbol_directory_entry(sym)
    age = quote_age_seconds(quote)
    stale_after = nse_quote_cache_ttl(status) * 2
    stale = bool(quote.get("stale")) or age is None or age > stale_after
    name = (entry or {}).get("name") or quote.get("name") or sym
    payload = {
        "symbol": sym,
        "label": sym,
        "name": name,
        "price": quote["price"],
        "change": quote["change"],
        "pct": quote["pct"],
        "live": not stale,
        "sym": quote.get("sym", "Rs"),
        "fetchedAt": quote.get("fetchedAt"),
        "ageSeconds": age,
        "stale": stale,
        "source": quote.get("source", "NSE"),
    }
    for key in ["previous_close", "open", "day_high", "day_low", "providerTimestamp", "sourceDetail"]:
        if quote.get(key) is not None:
            payload[key] = quote.get(key)
    return payload


def format_quotes_for_client(quotes: dict[str, dict], status: dict | None = None) -> dict[str, dict]:
    status = status or get_market_status()
    return {sym: format_quote_for_client(sym, quote, status=status) for sym, quote in quotes.items() if quote}


def refresh_quote_cache_for_symbols(symbols: list[str]) -> dict[str, dict]:
    clean_symbols = []
    for sym in symbols:
        normalized = _clean_general_symbol(sym)
        if normalized and normalized not in clean_symbols:
            clean_symbols.append(normalized)
    if not clean_symbols:
        return {}

    if active_market_data_provider() == UPSTOX_PROVIDER_NAME:
        label_to_key = {
            sym: key
            for sym in clean_symbols
            if (key := resolve_upstox_instrument_key(sym))
        }
        try:
            quotes = fetch_upstox_quotes_by_label(label_to_key) if label_to_key else {}
        except Exception as exc:
            print(f"[!] Upstox quotes failed; falling back to NSE: {exc}")
            quotes = {}
        if not upstox_fallback_enabled():
            return quotes
        pending_symbols = [sym for sym in clean_symbols if sym not in quotes]
    else:
        quotes = {}
        pending_symbols = list(clean_symbols)

    def nse_worker(sym: str) -> tuple[str, dict | None]:
        try:
            return sym, _fetch_nse_quote(sym)
        except Exception:
            return sym, None

    with ThreadPoolExecutor(max_workers=min(MAX_QUOTE_WORKERS, len(pending_symbols) or 1)) as executor:
        futures = [executor.submit(nse_worker, sym) for sym in pending_symbols]
        for future in as_completed(futures):
            sym, quote = future.result()
            if quote:
                quotes[sym] = quote
    return quotes


def refresh_tracked_symbol_quotes(state: dict | None = None) -> dict[str, dict]:
    global _tracked_symbol_quotes
    state = state or get_app_state_copy()
    symbols = tracked_symbols_for_state(state)
    quotes = refresh_quote_cache_for_symbols(symbols)
    with _lock:
        previous = dict(_tracked_symbol_quotes)
        for sym in symbols:
            if sym in quotes:
                previous[sym] = quotes[sym]
        _tracked_symbol_quotes = {sym: previous[sym] for sym in symbols if sym in previous}
        return dict(_tracked_symbol_quotes)


def rebuild_computed_payloads() -> None:
    global _analytics_payload, _derivatives_payload, _last_analytics_refresh_ts, _last_derivatives_refresh_ts
    state = get_app_state_copy()
    with _lock:
        arts = list(_arts)
        ticks = dict(_ticks)
        index_snapshot = dict(_index_snapshot)
        tracked_quotes = dict(_tracked_symbol_quotes)
        history = dict(_price_history)

    analytics_payload = build_market_analytics_payload(
        arts,
        ticks,
        index_snapshot,
        analytics_symbols_for_state(state),
        quote_map=tracked_quotes,
    )
    derivatives_payload = build_derivatives_analysis_payload(
        arts,
        ticks,
        index_snapshot,
        price_history=history,
        market_status=get_market_status(),
    )
    refreshed_at = time.time()
    with _lock:
        _analytics_payload = analytics_payload
        _derivatives_payload = derivatives_payload
        _last_analytics_refresh_ts = refreshed_at
        _last_derivatives_refresh_ts = refreshed_at


def market_data_snapshot(include_history: bool = False) -> dict:
    with _lock:
        ticks = dict(_ticks)
        tracked_quotes = dict(_tracked_symbol_quotes)
        analytics_payload = dict(_analytics_payload)
        derivatives_payload = dict(_derivatives_payload)
        last_tick_ts = _last_tick_refresh_ts
        last_analytics_ts = _last_analytics_refresh_ts
        last_derivatives_ts = _last_derivatives_refresh_ts
        history = {key: list(values) for key, values in _price_history.items()} if include_history else None

    status = get_market_status()
    payload = {
        "serverTime": ist_now().isoformat(),
        "lastTickAt": last_tick_ts,
        "lastAnalyticsAt": last_analytics_ts,
        "lastDerivativesAt": last_derivatives_ts,
        "marketStatus": status,
        "dataProvider": market_data_provider_status(),
        "ticks": ticks,
        "trackedQuotes": format_quotes_for_client(tracked_quotes, status=status),
        "analytics": analytics_payload,
        "derivatives": derivatives_payload,
    }
    if include_history:
        payload["history"] = history or {}
    return payload


def initialize_runtime_state() -> None:
    global NSE_HOLIDAY_CALENDAR, _app_state, _has_persisted_state, _news_refresh_seconds
    NSE_HOLIDAY_CALENDAR = load_holiday_calendar()
    state, has_stored_state = load_persisted_app_state()
    refresh_seconds = load_refresh_settings()
    with _lock:
        _app_state = state
        _has_persisted_state = has_stored_state
        _news_refresh_seconds = refresh_seconds


_background_threads_started = False
_background_threads_lock = threading.Lock()


def background_threads_enabled() -> bool:
    raw = os.environ.get("MARKET_DESK_DISABLE_THREADS", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return False
    return "unittest" not in sys.modules and "pytest" not in sys.modules


def external_worker_mode() -> bool:
    return os.environ.get("MARKET_DESK_DISABLE_THREADS", "").strip().lower() in {"1", "true", "yes", "on"}


def macro_background_thread_enabled() -> bool:
    return "unittest" not in sys.modules and "pytest" not in sys.modules


def start_background_workers() -> bool:
    global _background_threads_started
    if not background_threads_enabled():
        return False
    with _background_threads_lock:
        if _background_threads_started:
            return False
        threading.Thread(target=refresh_loop, daemon=True, name="market-desk-refresh").start()
        threading.Thread(target=ticker_loop, daemon=True, name="market-desk-ticker").start()
        threading.Thread(target=upstox_stream_loop, daemon=True, name="market-desk-upstox-v3").start()
        threading.Thread(target=global_quote_loop, daemon=True, name="market-desk-global-quotes").start()
        if macro_background_thread_enabled():
            threading.Thread(target=macro_context_loop, daemon=True, name="market-desk-macro-context").start()
        _background_threads_started = True
        return True


def persist_runtime_news_payload(articles: list[dict], feed_status: dict, updated: str, refreshed_at: float) -> None:
    try:
        db_set_json(
            RUNTIME_NEWS_STATE_KEY,
            {
                "articles": articles,
                "feedStatus": feed_status,
                "updated": updated,
                "lastNewsRefreshTs": refreshed_at,
                "aiSummaryProgress": ai_summary_progress_for_articles(articles),
            },
        )
    except Exception as exc:
        print(f"[!] runtime news persist error: {exc}")


def persist_runtime_snapshot_payload() -> None:
    try:
        db_set_json(RUNTIME_SNAPSHOT_STATE_KEY, market_data_snapshot(include_history=True))
    except Exception as exc:
        print(f"[!] runtime snapshot persist error: {exc}")


def runtime_news_payload_from_db() -> dict | None:
    payload = db_get_json(RUNTIME_NEWS_STATE_KEY, default=None)
    return payload if isinstance(payload, dict) else None


def runtime_snapshot_from_db(include_history: bool = False) -> dict | None:
    payload = db_get_json(RUNTIME_SNAPSHOT_STATE_KEY, default=None)
    if not isinstance(payload, dict):
        return None
    if not include_history:
        payload = dict(payload)
        payload.pop("history", None)
    return payload


# ── AI market chat helpers ─────────────────────────────────────────────────
def ai_chat_provider_name() -> str:
    configured = os.environ.get("AI_CHAT_PROVIDER", "").strip().lower().replace("-", "_")
    aliases = {
        "ollama": "ollama",
        "local": "ollama",
        "bedrock": "bedrock",
        "bedrock_api_key": "bedrock-api-key",
        "bedrock_responses": "bedrock-api-key",
        "bedrock_openai": "bedrock-api-key",
    }
    if configured:
        return aliases.get(configured, configured)
    if os.environ.get("BEDROCK_API_KEY", "").strip():
        return "bedrock-api-key"
    if os.environ.get("AWS_ACCESS_KEY_ID", "").strip() or os.environ.get("AWS_PROFILE", "").strip():
        return "bedrock"
    return ai_provider_name_from_env()


def _trim_text(value, limit: int = 420) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _compact_chat_history(history) -> list[dict]:
    if not isinstance(history, list):
        return []
    out = []
    for item in history[-AI_CHAT_MAX_HISTORY:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").lower()
        if role not in {"user", "assistant"}:
            continue
        content = _trim_text(item.get("content"), 900)
        if content:
            out.append({"role": role, "content": content})
    return out


def _price_momentum_for_chat(history: list | tuple | None) -> dict | None:
    if not history:
        return None
    values = []
    for item in list(history)[-10:]:
        try:
            values.append(round(float(item), 4))
        except Exception:
            continue
    if not values:
        return None
    start = values[0]
    latest = values[-1]
    change = round(latest - start, 4)
    pct = round((change / start * 100) if start else 0.0, 3)
    return {
        "samples": values,
        "sampleCount": len(values),
        "trailChange": change,
        "trailPct": pct,
    }


def _compact_quote_for_chat(label: str, quote: dict | None, history: list | tuple | None = None) -> dict | None:
    if not quote:
        return None
    price = quote.get("price")
    compact = {
        "label": label,
        "price": round_or_none(price),
        "previousClose": round_or_none(quote.get("previous_close")),
        "open": round_or_none(quote.get("open")),
        "dayHigh": round_or_none(quote.get("day_high")),
        "dayLow": round_or_none(quote.get("day_low")),
        "change": round_or_none(quote.get("change")),
        "pct": round_or_none(quote.get("pct")),
        "source": quote.get("source") or "market feed",
        "providerTimestamp": quote.get("providerTimestamp"),
        "ageSeconds": quote_age_seconds(quote),
        "currencySymbol": quote.get("sym", ""),
    }
    momentum = _price_momentum_for_chat(history)
    if momentum:
        compact["recentMomentum"] = momentum
    return compact


def _chat_query_terms(question: str) -> set[str]:
    aliases = {
        "bent": "brent",
        "xau": "gold",
        "rupee": "inr",
        "dollar": "usd",
        "oil": "crude",
    }
    terms = set()
    for term in re.findall(r"[a-zA-Z0-9/.-]{2,}", str(question or "").lower()):
        clean = term.strip("./-")
        if clean in {"what", "why", "does", "with", "from", "this", "that", "today", "market", "move", "moving"}:
            continue
        terms.add(aliases.get(clean, clean))
    if "usd/inr" in str(question or "").lower():
        terms.update({"usd", "inr"})
    return terms


def _article_relevance_score(article: dict, query_terms: set[str]) -> tuple[int, float]:
    ai_analysis = article.get("aiAnalysis") if isinstance(article.get("aiAnalysis"), dict) else {}
    ai_reasons = " ".join(ai_analysis.get("reasons") or [])
    index_impact = json.dumps(ai_analysis.get("indexImpact") or {}, ensure_ascii=False)
    text = f"{article.get('title', '')} {article.get('summary', '')} {article.get('sector', '')} {ai_reasons} {index_impact}".lower()
    term_hits = sum(1 for term in query_terms if term and term in text)
    if "brent" in query_terms and ("brent" in text or "crude" in text or "oil" in text):
        term_hits += 2
    impact_score = int(article.get("impact") or 0)
    return (term_hits * 10 + impact_score, float(article.get("ts") or 0))


def _articles_for_ai_chat() -> list[dict]:
    if external_worker_mode():
        runtime_payload = runtime_news_payload_from_db() or {}
        raw_articles = runtime_payload.get("articles") if isinstance(runtime_payload.get("articles"), list) else []
    else:
        with _lock:
            raw_articles = list(_arts)
    return raw_articles


def _article_ai_context(article: dict, *, summary_limit: int = 700) -> dict:
    sentiment_payload = article.get("sentiment") if isinstance(article.get("sentiment"), dict) else {}
    impact_meta = article.get("impactMeta") if isinstance(article.get("impactMeta"), dict) else {}
    ai_meta = article.get("aiAnalysis") if isinstance(article.get("aiAnalysis"), dict) else {}
    impact_ai = impact_meta.get("ai") if isinstance(impact_meta.get("ai"), dict) else {}
    reasons = ai_meta.get("reasons") or impact_meta.get("reasons") or []
    return {
        "title": _trim_text(article.get("title"), 180),
        "source": article.get("source"),
        "published": article.get("published"),
        "sector": article.get("sector"),
        "scope": article.get("scope"),
        "impact": article.get("impact"),
        "sentiment": sentiment_payload.get("label") or article.get("sentiment"),
        "summary": _trim_text(article.get("summary"), summary_limit),
        "summarySource": article.get("summarySource") or "feed",
        "analysisSource": article.get("analysisSource") or "",
        "indexImpact": ai_meta.get("indexImpact") or impact_ai.get("indexImpact") or {},
        "reasons": reasons[:5] if isinstance(reasons, list) else [],
        "textSource": ai_meta.get("textSource") or impact_ai.get("textSource") or "",
        "inputChars": ai_meta.get("inputChars") or impact_ai.get("inputChars") or 0,
    }


def _topic_ai_summaries_for_ai_chat(question: str, limit: int = 8) -> list[dict]:
    query_terms = _chat_query_terms(question)
    candidates = []
    for article in _articles_for_ai_chat():
        if article.get("summarySource") != "ai" or not str(article.get("summary") or "").strip():
            continue
        score = _article_relevance_score(article, query_terms)
        if score[0] <= 0 and query_terms:
            continue
        candidates.append((score, article))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return [_article_ai_context(article, summary_limit=760) for _score, article in candidates[:limit]]


def _recent_articles_for_ai_chat(question: str, limit: int = AI_CHAT_MAX_CONTEXT_ARTICLES) -> list[dict]:
    raw_articles = _articles_for_ai_chat()
    query_terms = _chat_query_terms(question)
    scored = sorted(
        raw_articles,
        key=lambda article: _article_relevance_score(article, query_terms),
        reverse=True,
    )
    return [_article_ai_context(article, summary_limit=520) for article in scored[:limit]]


def _ai_chat_web_query(question: str) -> str:
    text = str(question or "").strip()
    lower = text.lower().replace("bent", "brent")
    if "brent" in lower or "crude" in lower or "oil" in lower:
        return "Brent crude oil prices why up today OPEC inventory demand geopolitics"
    if "usd" in lower or "inr" in lower or "rupee" in lower:
        return "USD INR rupee why moving today RBI dollar oil yields"
    if "nifty bank" in lower or "bank nifty" in lower:
        return "Bank Nifty why moving today Indian banks RBI stocks"
    if "nifty" in lower or "sensex" in lower:
        return "Nifty Sensex why moving today Indian stock market"
    return f"{text} market news finance today"


def _internet_results_for_ai_chat(question: str) -> list[dict]:
    if AI_CHAT_MAX_WEB_RESULTS <= 0:
        return []
    query = _ai_chat_web_query(question)
    cache_key = query.lower()
    now = time.time()
    cached = _ai_chat_web_cache.get(cache_key)
    if cached and now - cached[1] < AI_CHAT_WEB_CACHE_TTL:
        return list(cached[0])

    results: list[dict] = []
    seen_links: set[str] = set()
    for scope in [GLOBAL_SCOPE, LOCAL_SCOPE]:
        if len(results) >= AI_CHAT_MAX_WEB_RESULTS:
            break
        try:
            data = _get_feed(google_news_search_rss(query, scope))
            feed = feedparser.parse(data)
        except Exception as exc:
            if upstox_debug_enabled():
                print(f"[!] AI chat web context ({scope}): {exc}")
            continue
        for entry in feed.entries[:AI_CHAT_MAX_WEB_RESULTS * 2]:
            link = str(entry.get("link") or "").strip()
            if link and link in seen_links:
                continue
            seen_links.add(link)
            source_meta = entry.get("source") or {}
            publisher = strip_html(source_meta.get("title", "")) if hasattr(source_meta, "get") else ""
            title = strip_html(entry.get("title", "")).strip()
            summary = clean_summary(strip_html(entry.get("summary", entry.get("description", ""))))
            if not title:
                continue
            try:
                pp = entry.get("published_parsed") or entry.get("updated_parsed")
                published_dt = datetime(*pp[:6], tzinfo=timezone.utc).astimezone(IST) if pp else None
            except Exception:
                published_dt = None
            results.append({
                "title": _trim_text(title, 220),
                "source": publisher or feed_publisher_label("Google News"),
                "scope": scope,
                "published": published_dt.isoformat() if published_dt else None,
                "summary": _trim_text(summary, 480),
                "link": link,
            })
            if len(results) >= AI_CHAT_MAX_WEB_RESULTS:
                break
    _ai_chat_web_cache[cache_key] = (list(results), now)
    return results


def build_ai_chat_context(question: str) -> dict:
    snapshot = None
    if external_worker_mode():
        snapshot = runtime_snapshot_from_db(include_history=True)
    snapshot = snapshot if isinstance(snapshot, dict) else market_data_snapshot(include_history=True)
    analytics = snapshot.get("analytics") if isinstance(snapshot.get("analytics"), dict) else {}
    derivatives = snapshot.get("derivatives") if isinstance(snapshot.get("derivatives"), dict) else {}
    ticks = snapshot.get("ticks") if isinstance(snapshot.get("ticks"), dict) else {}
    tracked_quotes = snapshot.get("trackedQuotes") if isinstance(snapshot.get("trackedQuotes"), dict) else {}
    history_map = snapshot.get("history") if isinstance(snapshot.get("history"), dict) else {}
    status = snapshot.get("marketStatus") if isinstance(snapshot.get("marketStatus"), dict) else get_market_status()
    provider = snapshot.get("dataProvider") if isinstance(snapshot.get("dataProvider"), dict) else market_data_provider_status()

    return {
        "asOf": ist_now().isoformat(),
        "marketStatus": {
            "session": status.get("session"),
            "label": status.get("sessionLabel"),
            "reason": status.get("reason"),
            "tickerAgeSeconds": status.get("tickerAgeSeconds"),
            "newsAgeSeconds": status.get("newsAgeSeconds"),
            "staleData": status.get("staleData"),
        },
        "dataProvider": {
            "active": provider.get("active"),
            "requested": provider.get("requested"),
            "reason": provider.get("reason"),
            "streamConnected": provider.get("streamConnected"),
            "degraded": provider.get("degraded"),
        },
        "tickerTape": {
            label: compact
            for label, quote in ticks.items()
            if (compact := _compact_quote_for_chat(label, quote, history_map.get(label)))
        },
        "trackedQuotes": {
            label: compact
            for label, quote in tracked_quotes.items()
            if (compact := _compact_quote_for_chat(label, quote, history_map.get(label)))
        },
        "analytics": {
            "overviewCards": analytics.get("overviewCards", [])[:7],
            "alerts": analytics.get("alerts", [])[:5],
            "sectorBoard": analytics.get("sectorBoard", [])[:8],
            "keyLevels": analytics.get("keyLevels", [])[:6],
        },
        "derivatives": {
            "predictionCards": derivatives.get("predictionCards", [])[:6],
            "riskFlags": derivatives.get("riskFlags", [])[:6],
            "contextNotes": derivatives.get("contextNotes", [])[:6],
            "crossAssetRows": derivatives.get("crossAssetRows", [])[:6],
            "relativeValueRows": derivatives.get("relativeValueRows", [])[:6],
        },
        "topicAiSummaries": _topic_ai_summaries_for_ai_chat(question),
        "recentNews": _recent_articles_for_ai_chat(question),
        "internetNews": _internet_results_for_ai_chat(question),
    }


def build_ai_chat_prompt(question: str, history, context: dict) -> str:
    compact_history = _compact_chat_history(history)
    context_json = json.dumps(context, ensure_ascii=False, sort_keys=True)
    if len(context_json) > AI_CHAT_CONTEXT_CHAR_LIMIT:
        context_json = context_json[:AI_CHAT_CONTEXT_CHAR_LIMIT].rstrip() + "...<truncated>"
    history_text = "\n".join(f"{item['role']}: {item['content']}" for item in compact_history) or "No previous chat."
    return f"""You are StockTerminal's AI Market Chat for an intraday Indian-market dashboard.

Rules:
- Use only the supplied dashboard context and recent news. If the context is not enough, say exactly what is missing.
- Treat internetNews as fresh web/news evidence fetched by the app at request time. Mention source names when those items drive the answer.
- Treat topicAiSummaries as pre-analyzed article evidence selected for the user's topic. Use them before generic recentNews, but decide whether they actually explain the move.
- If the user writes "bent crude", interpret it as "Brent crude".
- Explain market moves by combining live price change, recentMomentum, source/age of quote, topicAiSummaries, fresh internetNews, macro/sector context, and uncertainty.
- If the evidence is weak or unrelated, say that clearly and give the most likely drivers to verify next.
- Keep the answer concise but useful: start with a one-sentence direct answer, then 3-5 evidence bullets, then one "Watch next" line.
- Do not use markdown headings like ###. Use plain labels if a section label helps.
- Do not invent facts, prices, sources, or breaking news.
- Do not place trades or promise outcomes. You can describe bias, risk, and what to watch next.

Recent chat:
{history_text}

User question:
{question}

Live dashboard context JSON:
{context_json}
"""


def generate_ai_chat_response(question: str, history) -> tuple[str, str, str]:
    provider_name = ai_chat_provider_name()
    provider = create_ai_text_provider(http_session_factory=http_session, provider_name=provider_name)
    if not provider.is_configured():
        raise AiProviderConfigurationError(
            "AI chat is not configured. Set BEDROCK_API_KEY for Bedrock API-key chat, or configure AWS credentials for Bedrock Runtime."
        )
    context = build_ai_chat_context(question)
    prompt = build_ai_chat_prompt(question, history, context)
    answer = provider.generate_text(
        prompt=prompt,
        temperature=0.2,
        max_tokens=AI_CHAT_MAX_TOKENS,
        json_mode=False,
    )
    return answer.strip(), provider_name, ai_model_name_from_env(provider_name)


# ── Network helpers ────────────────────────────────────────────────────────
def _get_feed(url: str) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }
    r = http_session().get(url, headers=headers, timeout=8)
    r.raise_for_status()
    return r.content


def handle_ai_article_analysis_applied(article: dict) -> None:
    article_id = str(article.get("id") or "")
    with _lock:
        article_is_live = bool(article_id) and any(str(current.get("id") or "") == article_id for current in _arts)
        articles = list(_arts)
        feed_status = dict(_feed_status)
        updated = _updated
        refreshed_at = _last_news_refresh_ts or time.time()
    if not article_is_live:
        return
    try:
        rebuild_computed_payloads()
        persist_runtime_news_payload(articles, feed_status, updated, refreshed_at)
        persist_runtime_snapshot_payload()
        broadcast_market_snapshot()
    except Exception as exc:
        print(f"[!] AI analytics refresh error: {exc}")


def persist_news_article_analysis(cache_key: str, article: dict, analysis: dict, path: Path = STATE_DB_PATH) -> None:
    persist_ai_news_analysis(cache_key, article, analysis, path)
    try:
        save_article_ai_analysis(analysis, path)
    except Exception as exc:
        print(f"[!] news agent article analysis persist error: {exc}")


def ai_summary_service() -> NewsAiSummaryService:
    global _ai_summary_service
    if _ai_summary_service is None:
        _ai_summary_service = NewsAiSummaryService(
            http_session_factory=http_session,
            load_persisted_summary=load_persisted_ai_news_summary,
            load_persisted_analysis=load_persisted_ai_news_analysis,
            persist_summary=persist_ai_news_summary,
            persist_analysis=persist_news_article_analysis,
            articles_factory=lambda: _arts,
            articles_lock=_lock,
            on_analysis_applied=handle_ai_article_analysis_applied,
        )
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
    state = state or get_app_state_copy()
    labels = dict(UPSTOX_INDEX_INSTRUMENT_KEYS)
    for sym in NSE_STOCKS.values():
        key = resolve_upstox_instrument_key(sym)
        if key:
            labels[sym] = key
    for sym in tracked_symbols_for_state(state):
        key = resolve_upstox_instrument_key(sym)
        if key:
            labels[sym] = key
    return labels


def upstox_headers() -> dict[str, str]:
    token = upstox_analytics_token()
    if not token:
        raise RuntimeError("UPSTOX_ANALYTICS_TOKEN is not configured")
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": upstox_user_agent(),
    }


def upstox_response_error(response) -> str:
    status_code = getattr(response, "status_code", None)
    prefix = f"HTTP {status_code}" if status_code else "Upstox request failed"
    try:
        payload = response.json()
    except Exception:
        text = str(getattr(response, "text", "") or "").strip()
        return f"{prefix}: {text[:240] or 'no response body'}"

    errors = payload.get("errors") if isinstance(payload, dict) else None
    if isinstance(errors, list) and errors:
        first = errors[0] or {}
        code = first.get("errorCode") or first.get("error_code") or first.get("code")
        message = first.get("message") or first.get("errorMessage") or first.get("error")
        detail = " ".join(str(part) for part in (code, message) if part)
        return f"{prefix}: {detail or str(first)[:240]}"
    if isinstance(payload, dict):
        message = payload.get("message") or payload.get("error") or payload.get("status")
        if message:
            return f"{prefix}: {message}"
    return f"{prefix}: {str(payload)[:240]}"


class UpstoxEdgeBlockedError(RuntimeError):
    """Raised when Upstox's edge returns HTML instead of API JSON."""


def upstox_response_is_html_block(response) -> bool:
    status_code = _upstox_http_status_code(response)
    if status_code != 403:
        return False
    headers = getattr(response, "headers", {}) or {}
    content_type = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
    text = str(getattr(response, "text", "") or "")[:600].lower()
    return "text/html" in content_type or "<!doctype html" in text or "<html" in text or "cloudflare" in text


def _upstox_http_status_code(response) -> int:
    try:
        return int(getattr(response, "status_code", 200) or 200)
    except Exception:
        return 200


def _prefer_upstox_curl() -> bool:
    transport = upstox_http_transport()
    if transport == "curl":
        return True
    if transport == "requests":
        return False
    return time.time() < _upstox_curl_preferred_until


def _mark_upstox_curl_preferred(seconds: float = 900.0) -> None:
    global _upstox_curl_preferred_until
    _upstox_curl_preferred_until = max(_upstox_curl_preferred_until, time.time() + seconds)


def _upstox_request_json_with_requests(url: str, timeout: int) -> dict:
    response = http_session().get(
        url,
        headers=upstox_headers(),
        timeout=timeout,
    )
    status_code = _upstox_http_status_code(response)
    if status_code >= 400:
        error = upstox_response_error(response)
        if upstox_response_is_html_block(response):
            raise UpstoxEdgeBlockedError(error)
        raise RuntimeError(error)
    return response.json()


def _curl_config_value(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _upstox_request_json_with_curl(url: str, timeout: int) -> dict:
    marker = "__MARKET_DESK_HTTP_STATUS__"
    config_lines = [
        f'url = "{_curl_config_value(url)}"',
        'request = "GET"',
        f"max-time = {timeout}",
        "silent",
        "show-error",
        "compressed",
        f'write-out = "{marker}%{{http_code}}"',
    ]
    for name, value in upstox_headers().items():
        config_lines.append(f'header = "{_curl_config_value(f"{name}: {value}")}"')

    try:
        completed = subprocess.run(
            ["curl", "--config", "-"],
            input="\n".join(config_lines) + "\n",
            capture_output=True,
            text=True,
            timeout=timeout + 3,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("curl transport requested, but curl is not installed") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"Upstox curl request timed out after {timeout}s") from exc

    stdout = completed.stdout or ""
    stderr = (completed.stderr or "").strip()
    if marker not in stdout:
        detail = stderr or stdout[:240] or f"curl exited with {completed.returncode}"
        raise RuntimeError(f"Upstox curl request failed: {detail}")
    body, status_text = stdout.rsplit(marker, 1)
    try:
        status_code = int(status_text.strip()[-3:])
    except ValueError as exc:
        raise RuntimeError(f"Upstox curl request returned an unreadable status: {status_text[:80]}") from exc
    if completed.returncode != 0 and not body:
        raise RuntimeError(f"Upstox curl request failed: {stderr or completed.returncode}")
    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code}: {body.strip()[:240] or stderr or 'no response body'}")
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Upstox curl response was not JSON: {body.strip()[:240]}") from exc


def upstox_request_json(url: str, timeout: int = 8) -> dict:
    transport = upstox_http_transport()
    if _prefer_upstox_curl():
        if upstox_debug_enabled():
            print("[*] Upstox transport: curl")
        return _upstox_request_json_with_curl(url, timeout)
    try:
        if upstox_debug_enabled():
            print("[*] Upstox transport: requests")
        return _upstox_request_json_with_requests(url, timeout)
    except UpstoxEdgeBlockedError as exc:
        if transport != "auto":
            raise
        _mark_upstox_curl_preferred()
        if upstox_debug_enabled():
            print("[*] Upstox requests transport got HTML 403; switching to curl transport")
        return _upstox_request_json_with_curl(url, timeout)


def _parse_upstox_quote_payload(label_to_key: dict[str, str], payload: dict, received_at: float) -> dict[str, dict]:
    key_to_label = {key: label for label, key in label_to_key.items()}
    out: dict[str, dict] = {}
    for quote_payload in (payload.get("data") or {}).values():
        instrument_key = quote_payload.get("instrument_token") or quote_payload.get("instrument_key")
        label = key_to_label.get(instrument_key)
        if not label:
            symbol = re.sub(r"[^A-Z0-9&.-]", "", str(quote_payload.get("symbol", "")).upper())
            label = symbol if symbol in label_to_key else None
        if not label:
            continue
        quote = upstox_quote_from_payload(label, quote_payload, received_at)
        if not quote:
            continue
        cache_key = f"{label}|{label_to_key[label]}"
        _upstox_quote_cache[cache_key] = (quote, received_at)
        out[label] = quote
    return out


def upstox_quotes_url(instrument_keys: list[str]) -> str:
    # Upstox documents comma as the raw separator. quote_plus would turn spaces
    # into "+", so use quote to produce "%20" and keep commas unescaped.
    encoded_keys = quote(",".join(instrument_keys), safe=",")
    return f"{upstox_api_base()}/market-quote/quotes?instrument_key={encoded_keys}"


def upstox_instrument_search_url(
    query: str,
    exchanges: str = "NSE",
    segments: str = "EQ,INDEX",
    records: int = 12,
) -> str:
    params = {
        "query": query[:50],
        "exchanges": exchanges,
        "segments": segments,
        "page_number": "1",
        "records": str(min(max(records, 1), 30)),
    }
    return (
        f"{upstox_api_base()}/instruments/search?"
        + "&".join(f"{key}={quote(value, safe=',')}" for key, value in params.items())
    )


def _clean_market_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9&.-]", "", str(symbol or "").upper().strip())


def _upstox_search_cache_key(query: str, exchanges: str, segments: str, records: int) -> str:
    return "|".join([query.upper()[:50], exchanges.upper(), segments.upper(), str(records)])


def upstox_search_instruments(
    query: str,
    exchanges: str = "NSE",
    segments: str = "EQ,INDEX",
    records: int = 12,
) -> list[dict]:
    query = str(query or "").strip()
    if not query or not upstox_configured():
        return []
    cache_key = _upstox_search_cache_key(query, exchanges, segments, records)
    now = time.time()
    cached = _upstox_instrument_search_cache.get(cache_key)
    if cached and now - cached[1] < UPSTOX_INSTRUMENT_SEARCH_TTL:
        return list(cached[0])
    payload = upstox_request_json(upstox_instrument_search_url(query, exchanges, segments, records), timeout=6)
    rows = payload.get("data") if isinstance(payload, dict) else []
    rows = rows if isinstance(rows, list) else []
    _upstox_instrument_search_cache[cache_key] = (rows, now)
    return list(rows)


def _symbol_from_upstox_instrument(row: dict) -> str:
    symbol = row.get("trading_symbol") or row.get("underlying_symbol") or row.get("short_name") or row.get("name")
    return _clean_market_symbol(symbol)


def _sector_for_upstox_instrument(row: dict) -> str:
    segment = str(row.get("segment") or "")
    if segment.endswith("_INDEX") or row.get("instrument_type") == "INDEX":
        return "Index"
    return (symbol_directory_entry(_symbol_from_upstox_instrument(row)) or {}).get("sector") or "General"


def _upstox_instrument_to_suggestion(row: dict) -> dict | None:
    instrument_key = str(row.get("instrument_key") or "").strip()
    symbol = _symbol_from_upstox_instrument(row)
    if not instrument_key or not symbol:
        return None
    name = str(row.get("short_name") or row.get("name") or symbol).strip()
    return {
        "symbol": symbol,
        "name": name,
        "sector": _sector_for_upstox_instrument(row),
        "instrumentKey": instrument_key,
        "source": "Upstox",
    }


def upstox_symbol_search_results(query: str, limit: int = 8) -> list[dict]:
    q = _clean_market_symbol(query)
    if len(q) < 2:
        return []
    try:
        rows = upstox_search_instruments(query, records=min(max(limit * 2, 8), 30))
    except Exception as exc:
        if upstox_debug_enabled():
            print(f"[!] Upstox instrument search failed for {q}: {exc}")
        return []
    suggestions = []
    for row in rows:
        item = _upstox_instrument_to_suggestion(row)
        if item:
            suggestions.append(item)
    suggestions.sort(key=lambda item: (
        0 if item["symbol"] == q else 1 if item["symbol"].startswith(q) else 2,
        0 if item.get("sector") == "Index" else 1,
        item["symbol"],
    ))
    out, seen = [], set()
    for item in suggestions:
        if item["symbol"] in seen:
            continue
        seen.add(item["symbol"])
        out.append(item)
        if len(out) >= limit:
            break
    return out


def resolve_upstox_instrument_key(symbol: str) -> str | None:
    clean = _clean_market_symbol(symbol)
    if not clean:
        return None
    static_key = upstox_instrument_key_for_symbol(clean)
    if static_key:
        return static_key
    for row in upstox_search_instruments(clean, records=12):
        row_symbol = _symbol_from_upstox_instrument(row)
        instrument_key = str(row.get("instrument_key") or "").strip()
        segment = str(row.get("segment") or "")
        instrument_type = str(row.get("instrument_type") or "")
        if instrument_key and row_symbol == clean and (segment == "NSE_EQ" or segment == "NSE_INDEX" or instrument_type == "INDEX"):
            return instrument_key
    return None


def upstox_option_chain_url(underlying_key: str, expiry_date: str) -> str:
    return (
        f"{upstox_api_base()}/option/chain"
        f"?instrument_key={quote(underlying_key, safe='')}"
        f"&expiry_date={quote(expiry_date, safe='')}"
    )


def fetch_upstox_quote_batch(label_to_key: dict[str, str], received_at: float) -> dict[str, dict]:
    url = upstox_quotes_url(list(label_to_key.values()))
    if upstox_debug_enabled():
        print(f"[*] Upstox quotes URL: {url}")
    payload = upstox_request_json(url, timeout=8)
    if payload.get("status") not in {None, "success"}:
        raise RuntimeError(f"Upstox quote request failed: {payload.get('status')}")
    return _parse_upstox_quote_payload(label_to_key, payload, received_at)


def fetch_upstox_stream_quotes_by_label(label_to_key: dict[str, str]) -> dict[str, dict]:
    if not label_to_key:
        return {}
    now = time.time()
    stale_after = upstox_stream_stale_after()
    out = {}
    with _lock:
        for label, key in label_to_key.items():
            cached = _upstox_stream_quote_cache.get(key)
            if not cached:
                continue
            quote = cached[0]
            age = quote_age_seconds(quote, now)
            if age is None or age > stale_after:
                continue
            if quote.get("symbol") != label:
                quote = dict(quote, symbol=label)
            out[label] = quote
    return out


def fetch_upstox_quotes_by_label(label_to_key: dict[str, str]) -> dict[str, dict]:
    if not label_to_key or not upstox_configured():
        return {}

    now = time.time()
    ttl = nse_quote_cache_ttl()
    out: dict[str, dict] = fetch_upstox_stream_quotes_by_label(label_to_key)
    pending: dict[str, str] = {}
    for label, key in label_to_key.items():
        if not key or label in out:
            continue
        cache_key = f"{label}|{key}"
        cached = _upstox_quote_cache.get(cache_key)
        if cached and (now - cached[1] < ttl):
            out[label] = cached[0]
        else:
            pending[label] = key

    while pending:
        labels = list(pending.keys())[:UPSTOX_QUOTE_BATCH_LIMIT]
        batch = {label: pending[label] for label in labels}
        try:
            out.update(fetch_upstox_quote_batch(batch, now))
            _set_upstox_rest_status(lastError=None, lastOkAt=time.time(), failedKeys=[])
        except Exception as exc:
            batch_error = str(exc)
            if len(batch) == 1:
                label, key = next(iter(batch.items()))
                _set_upstox_rest_status(lastError=batch_error[:240], lastErrorAt=time.time(), failedKeys=[key])
                print(f"[!] Upstox {label} ({key}): {batch_error}")
            else:
                print(f"[!] Upstox quote batch rejected; retrying individually: {batch_error}")
                failed_keys: list[str] = []
                successful = False
                for label, key in batch.items():
                    try:
                        single = fetch_upstox_quote_batch({label: key}, now)
                        if single:
                            out.update(single)
                            successful = True
                    except Exception as single_exc:
                        failed_keys.append(key)
                        print(f"[!] Upstox {label} ({key}): {single_exc}")
                _set_upstox_rest_status(
                    lastError=(batch_error[:240] if failed_keys else None),
                    lastErrorAt=time.time() if failed_keys else None,
                    lastOkAt=time.time() if successful else _upstox_rest_status.get("lastOkAt"),
                    failedKeys=failed_keys,
                )
        for label in labels:
            pending.pop(label, None)
    return out


def _fetch_upstox_quote(symbol: str) -> dict | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    key = resolve_upstox_instrument_key(clean)
    if not clean or not key:
        return None
    return fetch_upstox_quotes_by_label({clean: key}).get(clean)


def fetch_upstox_index_quotes() -> dict[str, dict]:
    return fetch_upstox_quotes_by_label(dict(UPSTOX_INDEX_INSTRUMENT_KEYS))


def ticker_payload_from_quote(quote: dict, default_sym: str = "") -> dict:
    age = quote_age_seconds(quote)
    stale_after = nse_quote_cache_ttl() * 2
    stale = bool(quote.get("stale")) or age is None or age > stale_after
    payload = {
        "price": quote["price"],
        "change": quote["change"],
        "pct": quote["pct"],
        "live": not stale,
        "sym": quote.get("sym", default_sym),
        "fetchedAt": quote.get("fetchedAt", time.time()),
        "ageSeconds": age,
        "stale": stale,
        "source": quote.get("source", "Market feed"),
    }
    for key in ["previous_close", "open", "day_high", "day_low", "providerTimestamp", "sourceDetail", "providerError"]:
        if quote.get(key) is not None:
            payload[key] = quote.get(key)
    return payload


def apply_quote_update_to_runtime(label: str, quote: dict, *, update_indexes: bool = True) -> None:
    global _last_tick_refresh_ts
    now = time.time()
    index_label = next(
        (idx_label for idx_label, key in UPSTOX_INDEX_INSTRUMENT_KEYS.items() if key == quote.get("instrumentKey")),
        label if label in UPSTOX_INDEX_INSTRUMENT_KEYS else None,
    )
    ticker_label = "VIX" if index_label == "India VIX" else index_label or label
    ticker_payload = ticker_payload_from_quote(quote, default_sym="" if index_label else "Rs")
    price_history_updates: list[tuple[str, float]] = []
    with _lock:
        tracked_symbols = set(tracked_symbols_for_state(_app_state))
        if update_indexes and index_label:
            _index_snapshot[index_label] = dict(ticker_payload)
            _ticks[ticker_label] = dict(ticker_payload)
            price_history_updates.append((ticker_label, ticker_payload["price"]))
        if label in tracked_symbols:
            _tracked_symbol_quotes[label] = quote
        _last_tick_refresh_ts = now
        for hist_label, price in price_history_updates:
            hist = _price_history.setdefault(hist_label, [])
            if not hist or hist[-1] != price:
                hist.append(price)
            if len(hist) > MAX_HIST:
                _price_history[hist_label] = hist[-MAX_HIST:]


def maybe_broadcast_fast_market_snapshot(force: bool = False) -> None:
    global _last_fast_stream_broadcast_ts
    now = time.time()
    if not force and now - _last_fast_stream_broadcast_ts < STREAM_UI_BROADCAST_SECONDS:
        return
    _last_fast_stream_broadcast_ts = now
    broadcast_market_snapshot()


def _set_upstox_stream_status(**patch) -> None:
    with _lock:
        _upstox_stream_status.update(patch)


def _send_upstox_stream_request(ws, method: str, instrument_keys: list[str], mode: str = UPSTOX_STREAM_MODE) -> None:
    if not instrument_keys:
        return
    ws.send_binary(build_stream_request(method, instrument_keys, guid=secrets.token_hex(12), mode=mode))


def _apply_upstox_stream_payload(payload: dict, label_by_key: dict[str, str]) -> None:
    current_ts = payload.get("currentTs") or int(time.time() * 1000)
    message_type = payload.get("type")
    if message_type == "market_info":
        _set_upstox_stream_status(
            lastMessageAt=time.time(),
            segmentStatus=((payload.get("marketInfo") or {}).get("segmentStatus") or {}),
            lastError=None,
        )
        return
    if message_type not in {"live_feed", "initial_feed"}:
        return

    updates: dict[str, tuple[dict, float]] = {}
    labels_updated = False
    for instrument_key, feed in (payload.get("feeds") or {}).items():
        label = label_by_key.get(instrument_key, instrument_key)
        directory_entry = symbol_directory_entry(label)
        quote = stream_quote_from_feed(
            label,
            instrument_key,
            feed or {},
            current_ts,
            name=(directory_entry or {}).get("name") or label,
        )
        if quote:
            updates[instrument_key] = (quote, time.time())
            apply_quote_update_to_runtime(label, quote)
            labels_updated = True

    if updates:
        with _lock:
            _upstox_stream_quote_cache.update(updates)
            _upstox_stream_status["lastMessageAt"] = time.time()
            _upstox_stream_status["lastError"] = None
    if labels_updated:
        maybe_broadcast_fast_market_snapshot()


def upstox_stream_loop() -> None:
    while True:
        desired = upstox_stream_subscription_map()
        dependency_ready = upstox_stream_dependencies_ready()
        _set_upstox_stream_status(
            dependencyReady=dependency_ready,
            desiredSubscriptions=len({key for key in desired.values() if key}),
            mode=UPSTOX_STREAM_MODE,
        )
        if requested_market_data_provider() != UPSTOX_PROVIDER_NAME or not upstox_configured():
            _set_upstox_stream_status(connected=False, activeSubscriptions=0)
            _upstox_stream_wakeup.wait(timeout=10)
            _upstox_stream_wakeup.clear()
            continue
        if not dependency_ready:
            _set_upstox_stream_status(
                connected=False,
                activeSubscriptions=0,
                lastError="Install websocket-client to enable Upstox V3 streaming",
            )
            _upstox_stream_wakeup.wait(timeout=30)
            _upstox_stream_wakeup.clear()
            continue

        ws = None
        try:
            import websocket
            from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException

            uri = upstox_stream_authorized_redirect_uri()
            ws = websocket.create_connection(
                uri,
                timeout=10,
                enable_multithread=True,
                sslopt={"ca_certs": certifi.where()},
            )
            ws.settimeout(1)
            _set_upstox_stream_status(
                connected=True,
                lastConnectAt=time.time(),
                lastError=None,
                activeSubscriptions=0,
            )
            active_keys: set[str] = set()

            while requested_market_data_provider() == UPSTOX_PROVIDER_NAME and upstox_configured():
                desired = upstox_stream_subscription_map()
                label_by_key = {key: label for label, key in desired.items() if key}
                desired_keys = set(label_by_key.keys())
                additions = sorted(desired_keys - active_keys)
                removals = sorted(active_keys - desired_keys)
                if removals:
                    _send_upstox_stream_request(ws, "unsub", removals)
                    active_keys -= set(removals)
                if additions:
                    _send_upstox_stream_request(ws, "sub", additions)
                    active_keys |= set(additions)
                _set_upstox_stream_status(
                    desiredSubscriptions=len(desired_keys),
                    activeSubscriptions=len(active_keys),
                )

                try:
                    frame = ws.recv()
                except WebSocketTimeoutException:
                    if _upstox_stream_wakeup.wait(timeout=0.2):
                        _upstox_stream_wakeup.clear()
                    continue
                except WebSocketConnectionClosedException as exc:
                    raise RuntimeError(f"Upstox V3 socket closed: {exc}") from exc

                if frame is None:
                    continue
                payload = decode_feed_response(frame.encode("utf-8") if isinstance(frame, str) else frame)
                _apply_upstox_stream_payload(payload, label_by_key)
                if _upstox_stream_wakeup.is_set():
                    _upstox_stream_wakeup.clear()
        except Exception as exc:
            _set_upstox_stream_status(
                connected=False,
                lastDisconnectAt=time.time(),
                activeSubscriptions=0,
                lastError=str(exc)[:240],
            )
            time.sleep(UPSTOX_STREAM_RECONNECT_SECONDS)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
            _set_upstox_stream_status(connected=False, lastDisconnectAt=time.time(), activeSubscriptions=0)


def fetch_live_quote(symbol: str) -> dict | None:
    clean = _clean_general_symbol(symbol)
    if active_market_data_provider() == UPSTOX_PROVIDER_NAME:
        try:
            quote = _fetch_upstox_quote(clean or symbol)
            if quote:
                return quote
        except Exception as exc:
            print(f"[!] Upstox {symbol}: {exc}")
        if not upstox_fallback_enabled():
            return None
    return _fetch_nse_quote(clean or symbol)


def fetch_upstox_option_chain(underlying: str, expiry_date: str, max_rows: int = 80) -> dict:
    if not upstox_configured():
        raise RuntimeError("UPSTOX_ANALYTICS_TOKEN is not configured")
    underlying_key = option_underlying_key(underlying)
    if not underlying_key:
        raise ValueError(f"Unsupported Upstox option underlying: {underlying}")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", expiry_date or ""):
        raise ValueError("expiry must be provided in YYYY-MM-DD format")
    payload = upstox_request_json(upstox_option_chain_url(underlying_key, expiry_date), timeout=10)
    if payload.get("status") not in {None, "success"}:
        raise RuntimeError(f"Upstox option-chain request failed: {payload.get('status')}")
    return summarize_upstox_option_chain(payload.get("data") or [], underlying, expiry_date, max_rows=max_rows)


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
    price = live_quote["price"]
    day_high = live_quote.get("day_high", price)
    day_low = live_quote.get("day_low", price)
    pct = live_quote.get("pct", 0.0)
    if pct >= 1.25:
        trend = "Intraday strength"
    elif pct <= -1.25:
        trend = "Intraday weakness"
    else:
        trend = "Range"

    if day_high and price >= day_high * 0.9975:
        signal = "Near day high"
    elif day_low and price <= day_low * 1.0025:
        signal = "Near day low"
    elif pct >= 0.5:
        signal = "Buyer support"
    elif pct <= -0.5:
        signal = "Seller pressure"
    else:
        signal = "Wait for setup"

    breakout_gap = ((price / day_high) - 1) * 100 if day_high else None
    return {
        "symbol": _clean_general_symbol(symbol),
        "name": live_quote.get("name") or symbol,
        "price": round(price, 2),
        "change": round(live_quote.get("change", 0.0), 2),
        "pct": round(pct, 2),
        "trend": trend,
        "signal": signal,
        "rsi14": None,
        "ret5": None,
        "ret20": None,
        "vol20": None,
        "sma20": None,
        "sma50": None,
        "high20": round_or_none(day_high),
        "low20": round_or_none(day_low),
        "support": round_or_none(day_low),
        "resistance": round_or_none(day_high),
        "volumeRatio": None,
        "breakoutGap": round_or_none(breakout_gap),
        "drawdownFromHigh": round_or_none(breakout_gap),
    }


def build_symbol_signal(symbol: str, live_quote: dict | None = None, is_index: bool = False) -> dict | None:
    return build_live_only_signal(symbol, live_quote) if live_quote else None


# ── Data fetchers ──────────────────────────────────────────────────────────
def fetch_feed_articles(feed_cfg: dict[str, str]) -> tuple[str, dict, list[dict]]:
    src = feed_cfg["name"]
    url = feed_cfg["url"]
    scope = feed_cfg["scope"]
    articles = []
    try:
        data = _get_feed(url)
        feed = feedparser.parse(data)
        for e in feed.entries[:18]:
            source_meta = e.get("source") or {}
            publisher = strip_html(source_meta.get("title", "")) if hasattr(source_meta, "get") else ""
            publisher = publisher or feed_publisher_label(src)
            title = clean_headline(e.get("title", ""), publisher)
            summary = build_article_preview(title, e.get("summary", e.get("description", "")), publisher)
            link = e.get("link", "#")
            h = url_hash(link) if link != "#" else url_hash(title[:60])
            if not title:
                continue

            try:
                pp = e.get("published_parsed") or e.get("updated_parsed")
                dt = datetime(*pp[:6], tzinfo=timezone.utc).astimezone(IST) if pp else ist_now()
            except Exception:
                dt = ist_now()

            sent = sentiment(title, summary)
            impact_score, impact_meta = impact_details(
                title,
                summary,
                sent,
                source=publisher,
                published_dt=dt,
                scope=scope,
            )
            articles.append({
                "id": h,
                "title": title,
                "titleKey": normalized_headline(title),
                "summary": summary,
                "sourceSummary": summary,
                "link": link,
                "source": publisher,
                "feed": src,
                "scope": scope,
                "sector": classify(title + " " + summary),
                "sentiment": sent,
                "impact": impact_score,
                "impactMeta": impact_meta,
                "published": dt.strftime("%d %b %H:%M"),
                "ts": dt.timestamp(),
            })
        return src, {"ok": True, "count": len(articles), "scope": scope}, articles
    except Exception as ex:
        print(f"[!] Feed {src}: {ex}")
        return src, {"ok": False, "error": str(ex)[:120], "scope": scope}, []


def fetch_news() -> tuple[list[dict], dict]:
    seen_hashes, seen_titles, out = set(), set(), []
    status = {}

    with ThreadPoolExecutor(max_workers=min(MAX_NEWS_WORKERS, len(RSS_FEEDS))) as executor:
        futures = [executor.submit(fetch_feed_articles, feed_cfg) for feed_cfg in RSS_FEEDS]
        for future in as_completed(futures):
            src, feed_status, articles = future.result()
            status[src] = feed_status
            for article in articles:
                title_key = article.pop("titleKey", "")
                if article["id"] in seen_hashes or title_key in seen_titles:
                    continue
                seen_hashes.add(article["id"])
                seen_titles.add(title_key)
                out.append(article)

    out.sort(key=lambda x: -x["ts"])
    enrich_articles_with_ai_summaries(out)
    return out, status


def fetch_tickers() -> tuple[dict, dict]:
    out = {}
    analytics_indices = {}
    fetched_at = time.time()

    if active_market_data_provider() == UPSTOX_PROVIDER_NAME:
        try:
            for label, quote in fetch_upstox_index_quotes().items():
                client_quote = {
                    "price": quote["price"],
                    "change": quote["change"],
                    "pct": quote["pct"],
                    "live": True,
                    "sym": "",
                    "fetchedAt": quote.get("fetchedAt", fetched_at),
                    "source": quote.get("source", "Upstox"),
                }
                analytics_indices[label] = dict(client_quote)
                ticker_label = "VIX" if label == "India VIX" else label
                out[ticker_label] = dict(client_quote)
        except Exception as e:
            print(f"[!] Upstox indices: {e}")

    # NSE indices
    try:
        r = _nse_init_session().get(
            "https://www.nseindia.com/api/allIndices",
            headers=NSE_HEADERS, timeout=6, verify=certifi.where()
        )
        if r.status_code in {401, 403}:
            r = _nse_init_session(force=True).get(
                "https://www.nseindia.com/api/allIndices",
                headers=NSE_HEADERS, timeout=6, verify=certifi.where()
            )
        r.raise_for_status()
        for idx in r.json().get("data", []):
            name = idx.get("indexSymbol", "")
            if name in ANALYTICS_INDEX_NAMES:
                label = ANALYTICS_INDEX_NAMES[name]
                last = safe_float(idx.get("last"))
                prev = safe_float(idx.get("previousClose"), last)
                if last:
                    ch = round(last - prev, 2)
                    pct = round((ch / prev * 100) if prev else 0, 2)
                    if label not in analytics_indices:
                        analytics_indices[label] = {
                            "price": round(last, 2),
                            "change": ch,
                            "pct": pct,
                            "live": True,
                            "sym": "",
                            "fetchedAt": fetched_at,
                            "source": "NSE",
                        }
            if name in NSE_INDICES_WANTED:
                label = NSE_INDICES_WANTED[name]
                last = safe_float(idx.get("last"))
                prev = safe_float(idx.get("previousClose"), last)
                ch = round(last - prev, 2)
                pct = round((ch / prev * 100) if prev else 0, 2)
                if label not in out:
                    out[label] = {"price": round(last, 2), "change": ch, "pct": pct, "live": True, "sym": "", "fetchedAt": fetched_at, "source": "NSE"}
    except Exception as e:
        print(f"[!] NSE allIndices: {e}")

    stock_quotes = refresh_quote_cache_for_symbols(list(NSE_STOCKS.values()))
    for label, sym in NSE_STOCKS.items():
        quote = stock_quotes.get(sym)
        if quote:
            out[label] = {
                "price": quote["price"],
                "change": quote["change"],
                "pct": quote["pct"],
                "live": True,
                "sym": "Rs",
                "fetchedAt": quote.get("fetchedAt", fetched_at),
                "source": quote.get("source", "NSE"),
            }

    return out, analytics_indices


def build_market_analytics_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    symbols: list[str],
    quote_map: dict[str, dict] | None = None,
) -> dict:
    sector_news = build_sector_news_scores(articles)
    sector_rows = []
    sector_map = {}

    for sector, label in SECTOR_TO_INDEX.items():
        snap = index_snapshot.get(label)
        news = sector_news.get(sector, {"score": 0.0, "count": 0, "bull": 0, "bear": 0})
        bias, tone = sector_bias_label(news["score"])
        row = {
            "sector": sector,
            "label": label,
            "pct": round_or_none(snap.get("pct") if snap else None),
            "price": round_or_none(snap.get("price") if snap else None),
            "count": news["count"],
            "bull": news["bull"],
            "bear": news["bear"],
            "newsScore": news["score"],
            "newsBias": bias,
            "tone": tone,
        }
        sector_rows.append(row)
        sector_map[sector] = row

    tradable_rows = [row for row in sector_rows if row["pct"] is not None and row["sector"] != "General"]
    leaders = sorted(tradable_rows, key=lambda row: row["pct"], reverse=True)
    positive = sum(1 for row in tradable_rows if row["pct"] > 0)
    negative = sum(1 for row in tradable_rows if row["pct"] < 0)

    primary_signals = []
    primary_map = {}
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            live_quote = index_snapshot.get(label) or ticks.get(label)
            signal = build_symbol_signal(label, live_quote=live_quote, is_index=True)
            if signal:
                primary_signals.append({"label": label, **signal})
                primary_map[label] = signal
            elif live_quote:
                primary_signals.append({
                    "label": label,
                    "symbol": label.upper().replace(" ", ""),
                    "name": label,
                    "price": live_quote["price"],
                    "change": live_quote["change"],
                    "pct": live_quote["pct"],
                    "trend": "Live",
                    "signal": "Live snapshot",
                })

    nifty = (index_snapshot.get("Nifty 50") or ticks.get("Nifty 50") or {})
    bank = (index_snapshot.get("Nifty Bank") or ticks.get("Nifty Bank") or {})
    it_idx = (index_snapshot.get("Nifty IT") or ticks.get("Nifty IT") or {})
    midcap = (index_snapshot.get("Nifty Midcap") or ticks.get("Nifty Midcap") or {})
    smallcap = (index_snapshot.get("Nifty Smallcap") or ticks.get("Nifty Smallcap") or {})
    vix = (index_snapshot.get("India VIX") or ticks.get("VIX") or {})
    crude = ticks.get("Crude Oil") or {}

    risk_score = 0
    nifty_pct = nifty.get("pct")
    bank_pct = bank.get("pct")
    vix_price = vix.get("price")
    vix_chg = vix.get("pct")
    crude_price = crude.get("price")
    crude_pct = crude.get("pct")
    if nifty_pct is not None:
        risk_score += 1 if nifty_pct > 0 else -1
    if bank_pct is not None and nifty_pct is not None:
        risk_score += 1 if bank_pct >= nifty_pct else -1
    if midcap.get("pct") is not None and nifty_pct is not None and midcap["pct"] > nifty_pct:
        risk_score += 1
    if smallcap.get("pct") is not None and nifty_pct is not None and smallcap["pct"] > nifty_pct:
        risk_score += 1
    if positive > negative:
        risk_score += 1
    elif negative > positive:
        risk_score -= 1
    if vix_price:
        risk_score += 1 if vix_price < 15 else -1
    if vix_chg is not None:
        risk_score += 1 if vix_chg < 0 else -1 if vix_chg > 1.5 else 0

    if risk_score >= 4:
        regime = {"label": "Risk-On Trend", "tone": "bull", "detail": "Breadth is supportive and volatility is contained."}
    elif risk_score <= -3:
        regime = {"label": "Risk-Off Tape", "tone": "bear", "detail": "Size down and respect headline risk while VIX is elevated."}
    else:
        regime = {"label": "Rotation Market", "tone": "neutral", "detail": "Leadership is selective, so sector selection matters more than headline index direction."}

    news_score = round(sum(item["score"] for item in sector_news.values()), 2)
    news_tone = "bull" if news_score > 5 else "bear" if news_score < -5 else "neutral"
    breadth_label = f"{positive} sectors up / {negative} down"
    leadership = leaders[0]["sector"] if leaders else "Mixed"
    laggard = leaders[-1]["sector"] if leaders else "Mixed"
    breadth_spread = positive - negative
    smid_gap = relative_gap(smallcap.get("pct"), nifty_pct)
    bank_vs_it = relative_gap(bank_pct, it_idx.get("pct"))
    crude_tone = "bull" if crude_pct and crude_pct >= 1 else "bear" if crude_pct and crude_pct <= -1 else "neutral"
    if crude_price is not None and crude_pct is not None:
        crude_detail = f"{crude_pct:+.2f}% on the day. Useful for tracking energy names, OMCs, and inflation-sensitive moves."
    elif crude_price is not None:
        crude_detail = "Tracking front-month crude oil futures for energy-sensitive setups."
    else:
        crude_detail = "Tracking front-month crude oil futures for energy-sensitive setups."

    overview_cards = [
        {"label": "Regime", "value": regime["label"], "detail": regime["detail"], "tone": regime["tone"]},
        {"label": "Breadth", "value": breadth_label, "detail": f"Spread {breadth_spread:+d} across key sectors", "tone": "bull" if breadth_spread > 0 else "bear" if breadth_spread < 0 else "neutral"},
        {"label": "Volatility", "value": f"{vix_price:.2f} VIX" if vix_price is not None else "Unavailable", "detail": "Calm tape" if vix_price and vix_price < 14 else "Higher hedging demand" if vix_price and vix_price >= 16 else "Middle of the range", "tone": "bull" if vix_price and vix_price < 14 else "bear" if vix_price and vix_price >= 16 else "neutral"},
        {"label": "Crude Oil", "value": f"${crude_price:.2f}" if crude_price is not None else "Unavailable", "detail": crude_detail, "tone": crude_tone},
        {"label": "Leadership", "value": leadership, "detail": f"Weakest pocket: {laggard}", "tone": "bull" if leaders and leaders[0]["pct"] and leaders[0]["pct"] > 0 else "neutral"},
        {"label": "SMID vs Nifty", "value": f"{smid_gap:+.2f}%" if smid_gap is not None else "Unavailable", "detail": "Positive means broader risk appetite is expanding beyond the headline index.", "tone": "bull" if smid_gap and smid_gap > 0 else "bear" if smid_gap and smid_gap < 0 else "neutral"},
        {"label": "News Pulse", "value": f"{news_score:+.1f}", "detail": "Weighted from recent high-impact headlines by sector.", "tone": news_tone},
    ]

    alerts = []
    if leaders:
        leader_row = leaders[0]
        alerts.append(f"{leader_row['sector']} is leading today at {leader_row['pct']:+.2f}% with {leader_row['newsBias'].lower()} news flow.")
    if vix_price is not None:
        if vix_price >= 16:
            alerts.append(f"India VIX is at {vix_price:.2f}. Expect wider intraday swings and demand cleaner entries.")
        elif vix_price <= 13.5:
            alerts.append(f"India VIX is muted at {vix_price:.2f}, which usually favors trend-following over panic hedging.")
    if crude_pct is not None and abs(crude_pct) >= 1.5:
        move = "spiking" if crude_pct > 0 else "sliding"
        alerts.append(f"Crude oil is {move} {abs(crude_pct):.2f}% today. Watch energy names, OMCs, and inflation-sensitive sectors for spillover.")
    if bank_vs_it is not None and abs(bank_vs_it) >= 0.75:
        lead = "Banks" if bank_vs_it > 0 else "IT"
        alerts.append(f"{lead} is outperforming the other leadership pocket by {abs(bank_vs_it):.2f}%, a useful clue for intraday sector rotation.")
    if smid_gap is not None and smid_gap >= 0.75:
        alerts.append(f"Smallcaps are beating Nifty 50 by {smid_gap:.2f}%. That usually signals broader participation and better breakout follow-through.")
    elif smid_gap is not None and smid_gap <= -0.75:
        alerts.append(f"Smallcaps are lagging Nifty 50 by {abs(smid_gap):.2f}%, which is often a warning that risk appetite is narrowing.")

    symbol_signals = []
    symbol_map = {}
    for sym in symbols:
        live_quote = (quote_map or {}).get(sym)
        if live_quote is None and quote_map is None:
            try:
                live_quote = fetch_live_quote(sym)
            except Exception:
                live_quote = None
        try:
            signal = build_symbol_signal(sym, live_quote=live_quote, is_index=False)
        except Exception:
            signal = None
        if signal:
            symbol_signals.append(signal)
            symbol_map[signal["symbol"]] = signal

    key_levels = []
    for item in primary_signals:
        key_levels.append({
            "label": item["label"],
            "price": item.get("price"),
            "pct": item.get("pct"),
            "trend": item.get("trend"),
            "rsi14": item.get("rsi14"),
            "support": item.get("support"),
            "resistance": item.get("resistance"),
            "signal": item.get("signal"),
        })

    return {
        "generatedAt": ist_now().strftime("%H:%M:%S"),
        "overviewCards": overview_cards,
        "alerts": alerts[:5],
        "sectorBoard": leaders,
        "sectorMap": sector_map,
        "keyLevels": key_levels,
        "watchlistSignals": symbol_signals,
        "symbolMap": symbol_map,
        "regime": regime,
        "primary": primary_signals,
    }


def build_derivatives_analysis_payload(
    articles: list[dict],
    ticks: dict,
    index_snapshot: dict,
    price_history: dict[str, list[float]] | None = None,
    market_status: dict | None = None,
) -> dict:
    price_history = price_history or {}
    market_status = market_status or get_market_status()
    sector_news = build_sector_news_scores(articles)

    nifty = (index_snapshot.get("Nifty 50") or ticks.get("Nifty 50") or {})
    bank = (index_snapshot.get("Nifty Bank") or ticks.get("Nifty Bank") or {})
    it_idx = (index_snapshot.get("Nifty IT") or ticks.get("Nifty IT") or {})
    midcap = (index_snapshot.get("Nifty Midcap") or ticks.get("Nifty Midcap") or {})
    smallcap = (index_snapshot.get("Nifty Smallcap") or ticks.get("Nifty Smallcap") or {})
    vix = (index_snapshot.get("India VIX") or ticks.get("VIX") or {})
    crude = ticks.get("Crude Oil") or {}
    brent = ticks.get("Brent Crude") or {}
    usd_inr = ticks.get("USD/INR") or {}
    gold = ticks.get("Gold") or {}

    nifty_pct = nifty.get("pct")
    bank_pct = bank.get("pct")
    it_pct = it_idx.get("pct")
    midcap_pct = midcap.get("pct")
    smallcap_pct = smallcap.get("pct")
    vix_price = vix.get("price")
    vix_pct = vix.get("pct")
    crude_price = crude.get("price")
    crude_pct = crude.get("pct")
    usd_pct = usd_inr.get("pct")

    bank_vs_nifty = relative_gap(bank_pct, nifty_pct)
    it_vs_nifty = relative_gap(it_pct, nifty_pct)
    midcap_vs_nifty = relative_gap(midcap_pct, nifty_pct)
    smallcap_vs_nifty = relative_gap(smallcap_pct, nifty_pct)

    banking_news = sector_news.get("Banking", {"score": 0.0})
    it_news = sector_news.get("IT", {"score": 0.0})
    energy_news = sector_news.get("Energy", {"score": 0.0})
    general_news = sector_news.get("General", {"score": 0.0})
    headline_news_score = round(
        banking_news.get("score", 0.0) * 0.35
        + it_news.get("score", 0.0) * 0.2
        + energy_news.get("score", 0.0) * 0.15
        + general_news.get("score", 0.0) * 0.3,
        2,
    )

    nifty_hist = price_history.get("Nifty 50", [])
    bank_hist = price_history.get("Nifty Bank", [])
    vix_hist = price_history.get("VIX", [])
    nifty_flow_3 = intraday_return(nifty_hist, 3)
    nifty_flow_8 = intraday_return(nifty_hist, 8)
    bank_flow_3 = intraday_return(bank_hist, 3)
    bank_flow_8 = intraday_return(bank_hist, 8)
    vix_flow_3 = intraday_return(vix_hist, 3)
    focus_label = "Nifty Bank" if (bank_vs_nifty or 0) > 0.35 or abs(bank_flow_8 or 0) > abs(nifty_flow_8 or 0) else "Nifty 50"
    focus_display = "Bank Nifty" if focus_label == "Nifty Bank" else focus_label
    focus_hist = bank_hist if focus_label == "Nifty Bank" else nifty_hist
    focus_flow_3 = bank_flow_3 if focus_label == "Nifty Bank" else nifty_flow_3
    focus_flow_8 = bank_flow_8 if focus_label == "Nifty Bank" else nifty_flow_8
    focus_intraday_range = intraday_range_pct(focus_hist, 12)

    trend_component = score_band(nifty_pct, 0.75, 0.2, -0.2, -0.75) + score_band(nifty_flow_8, 0.45, 0.12, -0.12, -0.45)
    leadership_component = score_band(bank_vs_nifty, 0.6, 0.2, -0.2, -0.6) + score_band(smallcap_vs_nifty, 0.5, 0.15, -0.15, -0.5)

    vol_component = 0
    if vix_price is not None:
        vol_component += 1 if vix_price <= 14 else -1 if vix_price >= 16.5 else 0
    if vix_pct is not None:
        vol_component += 1 if vix_pct <= -1.5 else -1 if vix_pct >= 1.5 else 0

    macro_component = 0
    if crude_pct is not None:
        macro_component += 1 if crude_pct <= -0.8 else -1 if crude_pct >= 1 else 0
    if usd_pct is not None:
        macro_component += 1 if usd_pct <= -0.15 else -1 if usd_pct >= 0.15 else 0

    news_component = score_band(headline_news_score, 10, 2.5, -2.5, -10)
    flow_component = score_band(focus_flow_3, 0.35, 0.1, -0.1, -0.35) + score_band(focus_flow_8, 0.55, 0.15, -0.15, -0.55)
    composite_score = trend_component + leadership_component + vol_component + macro_component + news_component + flow_component

    data_points = sum(
        value is not None
        for value in [
            nifty_pct, bank_pct, bank_vs_nifty, smallcap_vs_nifty, vix_price, vix_pct, crude_pct, usd_pct,
            headline_news_score, nifty_flow_3, nifty_flow_8, bank_flow_3, bank_flow_8,
        ]
    )
    bias_label, bias_tone = bias_from_score(composite_score)
    conviction = conviction_from_score(composite_score, data_points)
    bull_prob = int(clamp(50 + composite_score * 5, 18, 82))
    bear_prob = 100 - bull_prob
    day_type, day_type_detail = day_type_from_context(composite_score, vix_price, focus_flow_3, focus_intraday_range)

    primary_signal_map = {}
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            live_quote = index_snapshot.get(label) or ticks.get(label) or (ticks.get("VIX") if label == "India VIX" else None)
            try:
                primary_signal_map[label] = build_symbol_signal(label, live_quote=live_quote, is_index=True)
            except Exception:
                primary_signal_map[label] = None

    focus_live_quote = bank if focus_label == "Nifty Bank" else nifty
    focus_signal = primary_signal_map.get(focus_label)
    focus_price = focus_signal.get("price") if focus_signal else focus_live_quote.get("price")
    expected_move_points, expected_move_pct = implied_move_points(focus_price, vix_price)

    overview_cards = [
        {
            "label": "Index Leader",
            "value": focus_display if focus_price is not None else "Waiting",
            "detail": "This is where the cleaner short-term derivatives expression is currently clustering.",
            "tone": bias_tone,
        },
        {
            "label": "Volatility Regime",
            "value": f"{vix_price:.2f} VIX" if vix_price is not None else "Unavailable",
            "detail": "Higher VIX usually means wider option premiums and faster sentiment flips."
            if vix_price is not None
            else "Waiting for volatility data.",
            "tone": "bear" if vix_price is not None and vix_price >= 16 else "bull" if vix_price is not None and vix_price <= 13.5 else "neutral",
        },
        {
            "label": "Bank vs Nifty",
            "value": f"{bank_vs_nifty:+.2f}%" if bank_vs_nifty is not None else "Unavailable",
            "detail": "Positive means Bank Nifty is outperforming the headline index.",
            "tone": "bull" if bank_vs_nifty is not None and bank_vs_nifty > 0 else "bear" if bank_vs_nifty is not None and bank_vs_nifty < 0 else "neutral",
        },
        {
            "label": "Short-Term Flow",
            "value": f"{focus_flow_8:+.2f}%" if focus_flow_8 is not None else "Unavailable",
            "detail": f"Recent {focus_display} tape over the latest dashboard ticks.",
            "tone": "bull" if focus_flow_8 is not None and focus_flow_8 > 0 else "bear" if focus_flow_8 is not None and focus_flow_8 < 0 else "neutral",
        },
        {
            "label": "Crude Impulse",
            "value": f"${crude_price:.2f}" if crude_price is not None else "Unavailable",
            "detail": "Crude matters for OMCs, inflation expectations, and broad risk tone.",
            "tone": "bull" if crude_pct is not None and crude_pct < 0 else "bear" if crude_pct is not None and crude_pct > 1 else "neutral",
        },
        {
            "label": "Rupee Pulse",
            "value": f"{usd_inr.get('price', 0):.2f}" if usd_inr.get("price") is not None else "Unavailable",
            "detail": "USD/INR pressure often feeds into imported inflation and foreign-flow sentiment.",
            "tone": "bear" if usd_pct is not None and usd_pct > 0.3 else "bull" if usd_pct is not None and usd_pct < -0.3 else "neutral",
        },
    ]

    prediction_cards = [
        {
            "label": "Model Bias",
            "value": bias_label,
            "detail": "Composite directional read from index trend, breadth, intraday tape, volatility, macro, and news.",
            "tone": bias_tone,
        },
        {
            "label": "Conviction",
            "value": f"{conviction} / 100",
            "detail": "Higher means more factors are aligned in the same direction. It is still context, not certainty.",
            "tone": "bull" if conviction >= 68 and bias_tone == "bull" else "bear" if conviction >= 68 and bias_tone == "bear" else "neutral",
        },
        {
            "label": "Bull Path",
            "value": f"{bull_prob}%",
            "detail": "Probability-weighted leaning toward upside continuation from the current composite score.",
            "tone": "bull" if bull_prob > 55 else "neutral",
        },
        {
            "label": "Bear Path",
            "value": f"{bear_prob}%",
            "detail": "Probability-weighted leaning toward downside continuation from the current composite score.",
            "tone": "bear" if bear_prob > 55 else "neutral",
        },
        {
            "label": "Day Type",
            "value": day_type,
            "detail": day_type_detail,
            "tone": "bull" if "Trend" in day_type and bias_tone == "bull" else "bear" if "Trend" in day_type and bias_tone == "bear" else "neutral",
        },
        {
            "label": "Expected Move",
            "value": f"{expected_move_points:,.0f} pts" if expected_move_points is not None else "Unavailable",
            "detail": f"Approx {expected_move_pct:.2f}% 1-day move from India VIX on {focus_display}."
            if expected_move_pct is not None
            else "Waiting for a valid price and VIX snapshot to estimate range.",
            "tone": "neutral",
        },
    ]

    context_notes = []
    context_notes.append(
        f"{focus_display} is the cleaner derivatives focus right now, with composite score {composite_score:+d} and {conviction}/100 conviction."
    )
    if bank_vs_nifty is not None:
        lead = "Banks" if bank_vs_nifty > 0 else "The headline index"
        context_notes.append(f"{lead} are leading by {abs(bank_vs_nifty):.2f}% versus Nifty 50, which matters for where the next clean impulse is most likely to show up.")
    if focus_flow_3 is not None and focus_flow_8 is not None:
        context_notes.append(f"Short-term tape check: {focus_display} is {focus_flow_3:+.2f}% over the last 3 ticks and {focus_flow_8:+.2f}% over the last 8 ticks.")
    if smallcap_vs_nifty is not None and abs(smallcap_vs_nifty) >= 0.5:
        breadth_mood = "broader participation is expanding" if smallcap_vs_nifty > 0 else "risk appetite is narrowing into larger names"
        context_notes.append(f"Smallcaps are {abs(smallcap_vs_nifty):.2f}% {'ahead of' if smallcap_vs_nifty > 0 else 'behind'} Nifty 50, suggesting {breadth_mood}.")
    if vix_price is not None:
        if vix_price >= 16:
            context_notes.append(f"India VIX at {vix_price:.2f} means intraday trend calls need more room and faster invalidation discipline.")
        elif vix_price <= 13.5:
            context_notes.append(f"India VIX at {vix_price:.2f} supports cleaner premium decay and better trend follow-through if price confirms.")
    if headline_news_score:
        context_notes.append(
            f"News pulse snapshot: Banking {banking_news['score']:+.1f}, IT {it_news['score']:+.1f}, Energy {energy_news['score']:+.1f}, Headline market {general_news['score']:+.1f}."
        )

    risk_flags = []
    if market_status.get("staleData"):
        risk_flags.append({"label": "Stale data", "detail": "One or more live feeds are stale. Reduce trust in short-term calls until the tape refreshes.", "tone": "bear"})
    if vix_price is not None and vix_price >= 16:
        risk_flags.append({"label": "Elevated volatility", "detail": "Option premiums are richer and reversals can be sharper than the raw index move suggests.", "tone": "bear"})
    if vix_pct is not None and vix_pct >= 2:
        risk_flags.append({"label": "Volatility repricing", "detail": "VIX is rising fast intraday, which can punish late directional entries.", "tone": "bear"})
    if bank_vs_nifty is not None and abs(bank_vs_nifty) >= 1:
        risk_flags.append({"label": "Leadership narrow", "detail": "A big Bank-vs-Nifty gap can be powerful, but it also means the move is less broad than it looks.", "tone": "neutral"})
    if smallcap_vs_nifty is not None and smallcap_vs_nifty <= -0.75:
        risk_flags.append({"label": "Breadth weak", "detail": "Smallcaps are lagging hard, which often reduces breakout durability.", "tone": "bear"})
    if crude_pct is not None and crude_pct >= 1:
        risk_flags.append({"label": "Crude pressure", "detail": "A sharp crude rise raises inflation sensitivity and can cap upside in rate-sensitive pockets.", "tone": "bear"})
    if usd_pct is not None and usd_pct >= 0.2:
        risk_flags.append({"label": "Rupee weakness", "detail": "A firm USD/INR move can add macro pressure and make equity upside less forgiving.", "tone": "bear"})
    if focus_intraday_range is not None and focus_intraday_range >= 1:
        risk_flags.append({"label": "Range already expanded", "detail": "A large early range means continuation entries need much cleaner structure than usual.", "tone": "neutral"})
    if len(focus_hist) < 6:
        risk_flags.append({"label": "Tape model warming up", "detail": "Short-term momentum signals are based on limited live history so far.", "tone": "neutral"})

    score_breakdown = [
        {"label": "Trend", "score": trend_component, "detail": "Daily index direction plus short-term tape follow-through."},
        {"label": "Leadership", "score": leadership_component, "detail": "Banking leadership and breadth expansion versus the headline index."},
        {"label": "Volatility", "score": vol_component, "detail": "India VIX level and whether volatility is being bid or offered."},
        {"label": "Macro", "score": macro_component, "detail": "Crude and USD/INR pressure or relief."},
        {"label": "News", "score": news_component, "detail": "Weighted pulse from market-sensitive news buckets."},
        {"label": "Flow", "score": flow_component, "detail": "Very short-term live tape direction from recent snapshots."},
    ]

    cross_asset_rows = [
        {
            "label": "Crude Oil",
            "price": crude.get("price"),
            "pct": crude.get("pct"),
            "unit": "$",
            "detail": "Energy sensitivity, inflation expectations, OMC context.",
        },
        {
            "label": "Brent Crude",
            "price": brent.get("price"),
            "pct": brent.get("pct"),
            "unit": "$",
            "detail": "Global oil benchmark and geopolitical risk barometer.",
        },
        {
            "label": "USD/INR",
            "price": usd_inr.get("price"),
            "pct": usd_inr.get("pct"),
            "unit": "",
            "detail": "Rupee pressure, imported inflation, and foreign-flow context.",
        },
        {
            "label": "Gold",
            "price": gold.get("price"),
            "pct": gold.get("pct"),
            "unit": "$",
            "detail": "Safe-haven tone and risk-aversion cross-check.",
        },
    ]

    relative_value_rows = [
        {
            "label": "Bank Nifty vs Nifty",
            "pct": bank_vs_nifty,
            "detail": "Tracks financial leadership against the headline index.",
        },
        {
            "label": "Nifty IT vs Nifty",
            "pct": it_vs_nifty,
            "detail": "Checks whether growth-sensitive tech is confirming or diverging.",
        },
        {
            "label": "Midcap vs Nifty",
            "pct": midcap_vs_nifty,
            "detail": "Useful for judging whether participation is broadening.",
        },
        {
            "label": "Smallcap vs Nifty",
            "pct": smallcap_vs_nifty,
            "detail": "A quick breadth read for risk appetite beyond the headline index.",
        },
    ]

    signal_matrix = []
    if ticks or index_snapshot:
        for label, live_quote in [
            ("Nifty 50", nifty),
            ("Nifty Bank", bank),
            ("Nifty IT", it_idx),
            ("India VIX", vix),
        ]:
            signal = primary_signal_map.get(label)
            short_3 = intraday_return(price_history.get("VIX" if label == "India VIX" else label, []), 3)
            short_8 = intraday_return(price_history.get("VIX" if label == "India VIX" else label, []), 8)
            signal_matrix.append({
                "label": "Bank Nifty" if label == "Nifty Bank" else label,
                "price": (signal or {}).get("price", live_quote.get("price")),
                "pct": (signal or {}).get("pct", live_quote.get("pct")),
                "short3": short_3,
                "short8": short_8,
                "trend": (signal or {}).get("trend", "Live"),
                "signal": (signal or {}).get("signal", "Live snapshot"),
            })

    trigger_map = []
    if ticks or index_snapshot:
        for label in PRIMARY_LEVEL_LABELS:
            signal = primary_signal_map.get(label)
            if not signal:
                continue
            trigger_map.append({
                "label": "Bank Nifty" if label == "Nifty Bank" else label,
                "price": signal.get("price"),
                "pct": signal.get("pct"),
                "trend": signal.get("trend"),
                "signal": signal.get("signal"),
                "support": signal.get("support"),
                "resistance": signal.get("resistance"),
                "rsi14": signal.get("rsi14"),
                "ret5": signal.get("ret5"),
            })

    trade_scenarios = []
    focus_support = (focus_signal or {}).get("support")
    focus_resistance = (focus_signal or {}).get("resistance")
    if focus_price is not None:
        bull_target = round_or_none((focus_resistance or focus_price) + ((expected_move_points or 0) * 0.7))
        bear_target = round_or_none((focus_support or focus_price) - ((expected_move_points or 0) * 0.7))
        fade_anchor = round_or_none(focus_price + ((expected_move_points or 0) * 0.35))
        trade_scenarios = [
            {
                "label": "Bull continuation",
                "tone": "bull",
                "trigger": f"Acceptance above {format_level(focus_resistance)}" if focus_resistance is not None else f"Strength above {format_level(focus_price)}",
                "target": format_level(bull_target),
                "invalidation": format_level(focus_support),
                "note": f"Works best when {focus_display} holds leadership and VIX stops expanding.",
            },
            {
                "label": "Bear continuation",
                "tone": "bear",
                "trigger": f"Break below {format_level(focus_support)}" if focus_support is not None else f"Weakness below {format_level(focus_price)}",
                "target": format_level(bear_target),
                "invalidation": format_level(focus_resistance),
                "note": "Cleaner if breadth narrows, USD/INR stays firm, or VIX keeps getting bid.",
            },
            {
                "label": "Fade / reversal",
                "tone": "neutral",
                "trigger": f"Failed move back inside {format_level(focus_support)} - {format_level(focus_resistance)}" if focus_support is not None and focus_resistance is not None else f"Failed extension around {format_level(fade_anchor)}",
                "target": format_level(focus_price),
                "invalidation": format_level(bull_target if composite_score >= 0 else bear_target),
                "note": "Most relevant when conviction is middling and the day type is rotation or high-gamma two-way.",
            },
        ]

    return {
        "generatedAt": ist_now().strftime("%H:%M:%S"),
        "overviewCards": overview_cards,
        "predictionCards": prediction_cards,
        "contextNotes": context_notes[:6],
        "riskFlags": risk_flags[:6],
        "crossAssetRows": cross_asset_rows,
        "relativeValueRows": relative_value_rows,
        "scoreBreakdown": score_breakdown,
        "tradeScenarios": trade_scenarios,
        "signalMatrix": signal_matrix,
        "triggerMap": trigger_map,
    }


# ── Background loops ───────────────────────────────────────────────────────
def broadcast_market_snapshot() -> None:
    payload = "data:" + json.dumps(market_data_snapshot(include_history=False)) + "\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_queues:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            try:
                _sse_queues.remove(q)
            except ValueError:
                pass


def broadcast_tickers(data: dict) -> None:
    broadcast_market_snapshot()


def _update_price_history(ticks: dict) -> None:
    with _lock:
        for label, data in ticks.items():
            price = data.get("price")
            if price is None:
                continue
            _price_history.setdefault(label, []).append(price)
            if len(_price_history[label]) > MAX_HIST:
                _price_history[label] = _price_history[label][-MAX_HIST:]


def refresh_loop() -> None:
    global _arts, _feed_status, _updated, _last_news_refresh_ts
    _nse_init_session()
    while True:
        try:
            print("[*] Refreshing news...")
            arts, fstatus = fetch_news()
            refreshed_at = time.time()
            with _lock:
                _arts = arts
                _feed_status = fstatus
                _updated = ist_now().strftime("%H:%M:%S")
                _last_news_refresh_ts = refreshed_at
            rebuild_computed_payloads()
            persist_runtime_news_payload(arts, fstatus, _updated, refreshed_at)
            persist_runtime_snapshot_payload()
            print(f"[+] {len(arts)} articles | {_updated}")
        except Exception as e:
            print(f"[!] refresh_loop error: {e}")
        wait_seconds = get_news_refresh_seconds()
        _refresh_wakeup.wait(timeout=wait_seconds)
        _refresh_wakeup.clear()


def ticker_loop() -> None:
    global _ticks, _index_snapshot, _last_tick_refresh_ts
    while True:
        try:
            _nse_init_session()
            ticks, analytics_indices = fetch_tickers()
            refreshed_at = time.time()
            with _lock:
                _ticks = ticks
                _index_snapshot = analytics_indices
                _last_tick_refresh_ts = refreshed_at
            _update_price_history(ticks)
            refresh_tracked_symbol_quotes()
            rebuild_computed_payloads()
            persist_runtime_snapshot_payload()
            broadcast_tickers(ticks)
            print(f"[~] Tickers: {list(ticks.keys())}")
        except Exception as e:
            print(f"[!] ticker_loop error: {e}")
        time.sleep(ticker_refresh_interval())


def macro_context_loop() -> None:
    global _last_macro_context_run_at
    # FMP free-plan access is limited, so this loop only reacts to scheduled macro checkpoints.
    while True:
        try:
            now = ist_now()
            if MACRO_AGENT_ENABLED and MACRO_AGENT_REFRESH_MODE == "scheduled":
                if is_macro_refresh_due(now, _last_macro_context_run_at):
                    run_macro_context_cycle(force_refresh=False, use_mock=False)
                    _last_macro_context_run_at = now
                    next_run = get_next_macro_refresh_time(now)
                    print(f"[~] Macro context refreshed at {now.isoformat()} | next={next_run.isoformat() if next_run else 'n/a'}")
        except Exception as exc:
            print(f"[!] macro_context_loop error: {exc}")
        time.sleep(60)


def global_quote_loop() -> None:
    while True:
        try:
            # Tracked symbols are refreshed through the main NSE/Upstox ticker
            # flow instead of a separate global quote provider loop.
            pass
        except Exception as exc:
            print(f"[!] global_quote_loop error: {exc}")
        time.sleep(GLOBAL_QUOTE_REFRESH_SECONDS)


# ── Flask ──────────────────────────────────────────────────────────────────
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
register_macro_agent_routes(app, run_macro_context_cycle)


@app.route("/api/news")
def api_news():
    if external_worker_mode():
        runtime_payload = runtime_news_payload_from_db()
        if runtime_payload:
            articles = runtime_payload.get("articles") if isinstance(runtime_payload.get("articles"), list) else []
            for article in articles:
                hydrate_article_from_ai_cache(article)
            payload = {
                "articles": articles,
                "updated": runtime_payload.get("updated") or "",
                "feedStatus": runtime_payload.get("feedStatus") if isinstance(runtime_payload.get("feedStatus"), dict) else {},
                "refreshInterval": _news_refresh_seconds,
                "allowedRefreshWindows": ALLOWED_REFRESH_WINDOWS,
                "marketStatus": get_market_status(),
                "aiSummaryProgress": runtime_payload.get("aiSummaryProgress") or ai_summary_progress_for_articles(articles),
            }
            return jsonify(payload)
    with _lock:
        articles = list(_arts)
        payload = {
            "articles": articles,
            "updated": _updated,
            "feedStatus": dict(_feed_status),
            "refreshInterval": _news_refresh_seconds,
            "allowedRefreshWindows": ALLOWED_REFRESH_WINDOWS,
        }
    payload["marketStatus"] = get_market_status()
    payload["aiSummaryProgress"] = ai_summary_progress_for_articles(articles)
    return jsonify(payload)


@app.route("/api/news/ai-summaries")
def api_news_ai_summaries():
    if external_worker_mode():
        runtime_payload = runtime_news_payload_from_db() or {}
        articles = runtime_payload.get("articles") if isinstance(runtime_payload.get("articles"), list) else []
        for article in articles:
            hydrate_article_from_ai_cache(article)
        updates = [ai_summary_update_payload(article) for article in articles if article_has_ai_summary(article)]
        return jsonify({
            "updates": updates,
            "progress": runtime_payload.get("aiSummaryProgress") or ai_summary_progress_for_articles(articles),
            "updated": runtime_payload.get("updated") or "",
        })
    with _lock:
        articles = list(_arts)
        updated = _updated
    updates = [ai_summary_update_payload(article) for article in articles if article_has_ai_summary(article)]
    return jsonify({
        "updates": updates,
        "progress": ai_summary_progress_for_articles(articles),
        "updated": updated,
    })


# TODO: Move these News Agent routes into backend.routes.news_agent_routes
# after the Flask app is converted to blueprints.
@app.route("/api/news/agent/report")
def api_news_agent_report():
    index = request.args.get("index", "NIFTY")
    try:
        lookback_hours = int(request.args.get("lookback_hours", "24") or 24)
    except (TypeError, ValueError):
        lookback_hours = 24
    lookback_hours = int(clamp(lookback_hours, 1, 168))
    analyses = load_recent_article_ai_analyses(lookback_hours=lookback_hours)
    report = NewsReportAggregator(analyses).build_report(index=index, lookback_hours=lookback_hours)
    try:
        save_index_news_report(report)
    except Exception as exc:
        print(f"[!] news agent report persist error: {exc}")
    return jsonify(report.to_dict())


@app.route("/api/news/agent/articles")
def api_news_agent_articles():
    try:
        lookback_hours = int(request.args.get("lookback_hours", "24") or 24)
    except (TypeError, ValueError):
        lookback_hours = 24
    lookback_hours = int(clamp(lookback_hours, 1, 168))
    analyses = load_recent_article_ai_analyses(lookback_hours=lookback_hours)
    return jsonify({
        "lookback_hours": lookback_hours,
        "count": len(analyses),
        "articles": [analysis.to_dict() for analysis in analyses],
    })


@app.route("/api/tickers")
def api_tickers():
    if external_worker_mode():
        runtime_payload = runtime_snapshot_from_db(include_history=False)
        if runtime_payload and isinstance(runtime_payload.get("ticks"), dict):
            return jsonify(runtime_payload["ticks"])
    with _lock:
        return jsonify(_ticks)


@app.route("/api/snapshot")
def api_snapshot():
    include_history = request.args.get("history", "0") in {"1", "true", "yes"}
    if external_worker_mode():
        runtime_payload = runtime_snapshot_from_db(include_history=include_history)
        if runtime_payload:
            return jsonify(runtime_payload)
    return jsonify(market_data_snapshot(include_history=include_history))


@app.route("/api/symbols/search")
def api_symbol_search():
    query = request.args.get("q", "")
    try:
        limit = min(max(int(request.args.get("limit", 10)), 1), 20)
    except Exception:
        limit = 10
    results, seen = [], set()
    for item in search_symbols(query, limit=limit * 2):
        symbol = item.get("symbol")
        if symbol and symbol not in seen:
            seen.add(symbol)
            results.append(item)
    if len(_clean_market_symbol(query)) >= 2:
        for item in upstox_symbol_search_results(query, limit=limit):
            symbol = item.get("symbol")
            if symbol and symbol not in seen:
                seen.add(symbol)
                results.append(item)
    return jsonify({"results": results[:limit]})


@app.route("/api/app-state", methods=["GET", "POST"])
def api_app_state():
    if request.method == "POST":
        payload = request.get_json(silent=True) or {}
        state = update_app_state(payload)
        with _lock:
            has_stored_state = _has_persisted_state
        return jsonify({"state": state, "hasStoredState": has_stored_state})

    with _lock:
        has_stored_state = _has_persisted_state
    return jsonify({
        "state": get_app_state_copy(),
        "hasStoredState": has_stored_state,
    })


@app.route("/api/quotes")
def api_quotes():
    symbols = sanitize_symbol_list(request.args.get("symbols", ""))
    status = get_market_status()
    stale_after = nse_quote_cache_ttl(status) * 2
    with _lock:
        cached_quotes = {sym: _tracked_symbol_quotes.get(sym) for sym in symbols}
    refresh_symbols = [
        sym for sym, quote in cached_quotes.items()
        if quote is None or (quote_age_seconds(quote) is not None and quote_age_seconds(quote) > stale_after)
    ]
    fresh_quotes = refresh_quote_cache_for_symbols(refresh_symbols)
    if fresh_quotes:
        with _lock:
            _tracked_symbol_quotes.update(fresh_quotes)
        try:
            rebuild_computed_payloads()
            persist_runtime_snapshot_payload()
            broadcast_market_snapshot()
        except Exception:
            pass
    merged = {sym: fresh_quotes.get(sym) or cached_quotes.get(sym) for sym in symbols}
    out = format_quotes_for_client({sym: quote for sym, quote in merged.items() if quote}, status=status)
    return jsonify(out)


@app.route("/api/history")
def api_history():
    if external_worker_mode():
        runtime_payload = runtime_snapshot_from_db(include_history=True)
        if runtime_payload and isinstance(runtime_payload.get("history"), dict):
            return jsonify(runtime_payload["history"])
    with _lock:
        return jsonify(_price_history)


@app.route("/api/analytics")
def api_analytics():
    if external_worker_mode():
        runtime_payload = runtime_snapshot_from_db(include_history=False)
        if runtime_payload and isinstance(runtime_payload.get("analytics"), dict):
            return jsonify(runtime_payload["analytics"])
    with _lock:
        payload = dict(_analytics_payload)
    return jsonify(payload)


@app.route("/api/derivatives/overview")
def api_derivatives_overview():
    if external_worker_mode():
        runtime_payload = runtime_snapshot_from_db(include_history=False)
        if runtime_payload and isinstance(runtime_payload.get("derivatives"), dict):
            return jsonify(runtime_payload["derivatives"])
    with _lock:
        payload = dict(_derivatives_payload)
    return jsonify(payload)


@app.route("/api/derivatives/option-chain")
def api_derivatives_option_chain():
    underlying = request.args.get("underlying", "NIFTY")
    expiry = request.args.get("expiry") or os.environ.get("UPSTOX_OPTION_EXPIRY", "")
    max_rows = int(request.args.get("maxRows", "80") or 80)
    try:
        payload = fetch_upstox_option_chain(
            underlying=underlying,
            expiry_date=expiry,
            max_rows=max(10, min(max_rows, 200)),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc), "provider": "Upstox"}), 400
    except RuntimeError as exc:
        return jsonify({"error": str(exc), "provider": "Upstox"}), 400
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": "Upstox option-chain request failed", "provider": "Upstox"}), status_code
    except Exception as exc:
        return jsonify({"error": str(exc), "provider": "Upstox"}), 502
    return jsonify(payload)


@app.route("/api/ai-chat", methods=["POST"])
def api_ai_chat():
    payload = request.get_json(silent=True) or {}
    question = _trim_text(payload.get("message"), 900)
    history = payload.get("history") if isinstance(payload.get("history"), list) else []
    if not question:
        return jsonify({"error": "Ask a market question first."}), 400
    try:
        answer, provider_name, model_name = generate_ai_chat_response(question, history)
    except AiProviderConfigurationError as exc:
        return jsonify({"error": str(exc), "provider": ai_chat_provider_name()}), 400
    except Exception as exc:
        return jsonify({"error": f"AI chat failed: {exc}", "provider": ai_chat_provider_name()}), 502
    return jsonify({
        "answer": answer,
        "provider": provider_name,
        "model": model_name,
        "generatedAt": ist_now().isoformat(),
    })


@app.route("/api/integrations/upstox/status")
def api_upstox_status():
    return jsonify(upstox_integration_status())


@app.route("/api/settings/refresh", methods=["GET", "POST"])
def api_settings_refresh():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        seconds = int(data.get("seconds", 0) or 0)
        try:
            current = set_news_refresh_seconds(seconds)
        except ValueError:
            return jsonify({"error": "Unsupported refresh interval", "allowed": ALLOWED_REFRESH_WINDOWS}), 400
        return jsonify({"refreshInterval": current, "allowedRefreshWindows": ALLOWED_REFRESH_WINDOWS})

    return jsonify({
        "refreshInterval": get_news_refresh_seconds(),
        "allowedRefreshWindows": ALLOWED_REFRESH_WINDOWS,
    })


@app.route("/api/health")
def api_health():
    market_status = get_market_status()
    with _lock:
        news_count = len(_arts)
        ticker_count = len(_ticks)
        analytics_ready = bool(_analytics_payload.get("generatedAt"))
        derivatives_ready = bool(_derivatives_payload.get("generatedAt"))
    status = "ok" if not market_status["staleData"] else "degraded"
    return jsonify({
        "status": status,
        "dataProvider": market_data_provider_status(),
        "upstox": upstox_integration_status(),
        "marketStatus": market_status,
        "newsCount": news_count,
        "tickerCount": ticker_count,
        "analyticsReady": analytics_ready,
        "derivativesReady": derivatives_ready,
        "refreshInterval": get_news_refresh_seconds(),
    })


@app.route("/api/tickers/stream")
def api_tickers_stream():
    if external_worker_mode():
        def generate_from_runtime():
            last_payload = ""
            while True:
                payload = runtime_snapshot_from_db(include_history=False) or market_data_snapshot(include_history=False)
                encoded = json.dumps(payload, sort_keys=True)
                if encoded != last_payload:
                    yield "data:" + encoded + "\n\n"
                    last_payload = encoded
                else:
                    yield ": keepalive\n\n"
                time.sleep(ticker_refresh_interval())

        return Response(
            stream_with_context(generate_from_runtime()),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    q: queue.Queue[str] = queue.Queue(maxsize=20)
    with _sse_lock:
        _sse_queues.append(q)
    initial = market_data_snapshot(include_history=True)

    def generate():
        try:
            yield "data:" + json.dumps(initial) + "\n\n"
            while True:
                try:
                    msg = q.get(timeout=30)
                    yield msg
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _sse_lock:
                try:
                    _sse_queues.remove(q)
                except ValueError:
                    pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/")
def index():
    return app.send_static_file("index.html")


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
