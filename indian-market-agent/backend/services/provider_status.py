"""Market data provider and runtime freshness status helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import os
import threading
import time
from typing import Any

try:
    from backend.core.settings import IST
except ModuleNotFoundError:
    from core.settings import IST
except Exception:
    IST = timezone(timedelta(hours=5, minutes=30))


PREOPEN_TICK_INTERVAL_SECONDS = 10
INTRADAY_TICK_INTERVAL_SECONDS = 10
AFTER_HOURS_TICK_INTERVAL_SECONDS = 60
LIVE_NSE_QUOTE_CACHE_TTL = 8.0
CLOSED_NSE_QUOTE_CACHE_TTL = 45.0
NSE_PROVIDER_NAME = "nse"
UPSTOX_PROVIDER_NAME = "upstox"
UPSTOX_STREAM_OPEN_STALE_SECONDS = 12.0
UPSTOX_STREAM_CLOSED_STALE_SECONDS = 180.0

_lock = threading.Lock()
_upstox_rest_status = {
    "lastError": None,
    "lastErrorAt": None,
    "lastOkAt": None,
    "failedKeys": [],
}

_DEFAULT_UPSTOX_STREAM_STATUS = {
    "connected": False,
    "lastConnectAt": None,
    "lastDisconnectAt": None,
    "lastMessageAt": None,
    "lastError": None,
    "mode": "full",
    "desiredSubscriptions": 0,
    "activeSubscriptions": 0,
    "segmentStatus": {},
    "dependencyReady": False,
}


def requested_market_data_provider() -> str:
    provider = os.environ.get("MARKET_DATA_PROVIDER", NSE_PROVIDER_NAME).strip().lower()
    return provider if provider in {NSE_PROVIDER_NAME, UPSTOX_PROVIDER_NAME} else NSE_PROVIDER_NAME


def upstox_analytics_token() -> str:
    return os.environ.get("UPSTOX_ANALYTICS_TOKEN", "").strip()


def upstox_token_source() -> str:
    return "analytics_env" if upstox_analytics_token() else "none"


def upstox_configured() -> bool:
    return bool(upstox_analytics_token())


def upstox_fallback_enabled() -> bool:
    raw = os.environ.get("UPSTOX_FALLBACK_TO_NSE", "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def upstox_debug_enabled() -> bool:
    raw = os.environ.get("UPSTOX_DEBUG", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def upstox_http_transport() -> str:
    transport = os.environ.get("UPSTOX_HTTP_TRANSPORT", "auto").strip().lower()
    return transport if transport in {"auto", "requests", "curl"} else "auto"


def active_market_data_provider(context=None) -> str:
    if requested_market_data_provider() == UPSTOX_PROVIDER_NAME and upstox_configured():
        return UPSTOX_PROVIDER_NAME
    return NSE_PROVIDER_NAME


def ticker_refresh_interval(status: dict | None = None, context=None) -> int:
    status = status or _market_status(context)
    if status.get("session") == "preopen":
        return PREOPEN_TICK_INTERVAL_SECONDS
    if status.get("isMarketOpen"):
        return INTRADAY_TICK_INTERVAL_SECONDS
    return AFTER_HOURS_TICK_INTERVAL_SECONDS


def nse_quote_cache_ttl(status: dict | None = None, context=None) -> float:
    status = status or _market_status(context)
    return LIVE_NSE_QUOTE_CACHE_TTL if status.get("session") in {"preopen", "open"} else CLOSED_NSE_QUOTE_CACHE_TTL


def quote_age_seconds(quote: dict | None, now_ts: float | None = None) -> float | None:
    if not quote or quote.get("fetchedAt") is None:
        return None
    now_ts = time.time() if now_ts is None else now_ts
    return round(max(now_ts - float(quote["fetchedAt"]), 0), 1)


def upstox_stream_stale_after(status: dict | None = None, context=None) -> float:
    status = status or _market_status(context)
    return (
        UPSTOX_STREAM_OPEN_STALE_SECONDS
        if status.get("session") in {"preopen", "open"}
        else UPSTOX_STREAM_CLOSED_STALE_SECONDS
    )


def market_data_provider_status(context=None) -> dict:
    requested = requested_market_data_provider()
    configured = upstox_configured()
    active = active_market_data_provider(context=context)
    stream = upstox_stream_runtime_status(context=context)
    rest = upstox_rest_runtime_status(context=context)
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


def upstox_stream_runtime_status(context=None) -> dict:
    status = _stream_status(context)
    last_message_at = status.get("lastMessageAt")
    status["lastMessageAgeSeconds"] = round(max(time.time() - last_message_at, 0), 1) if last_message_at else None
    status["lastConnectAt"] = _format_timestamp(status.get("lastConnectAt"))
    status["lastDisconnectAt"] = _format_timestamp(status.get("lastDisconnectAt"))
    status["lastMessageAt"] = _format_timestamp(last_message_at)
    return status


def upstox_rest_runtime_status(context=None) -> dict:
    with _lock:
        status = dict(_upstox_rest_status)
    status["transport"] = upstox_http_transport()
    status["curlPreferred"] = _curl_preferred(context)
    if status.get("lastErrorAt"):
        status["lastErrorAt"] = _format_timestamp(status["lastErrorAt"])
    if status.get("lastOkAt"):
        status["lastOkAt"] = _format_timestamp(status["lastOkAt"])
    return status


def set_upstox_rest_status(ok: bool, error: str = "") -> None:
    now = time.time()
    if ok:
        patch_upstox_rest_status(lastError=None, lastOkAt=now, failedKeys=[])
    else:
        patch_upstox_rest_status(lastError=str(error or "")[:240], lastErrorAt=now)


def patch_upstox_rest_status(**patch) -> None:
    with _lock:
        _upstox_rest_status.update(patch)


def upstox_rest_last_ok_at() -> float | None:
    with _lock:
        return _upstox_rest_status.get("lastOkAt")


def _market_status(context) -> dict:
    get_market_status = _context_callable(context, "get_market_status")
    if get_market_status is None:
        return {}
    try:
        return get_market_status() or {}
    except Exception:
        return {}


def _stream_status(context) -> dict:
    status = None
    lock = _context_value(context, "_lock")
    stream_status = _context_value(context, "_upstox_stream_status")
    if isinstance(stream_status, dict):
        if lock is not None:
            with lock:
                status = dict(stream_status)
        else:
            status = dict(stream_status)
    return status or dict(_DEFAULT_UPSTOX_STREAM_STATUS)


def _curl_preferred(context) -> bool:
    preferred = _context_callable(context, "_prefer_upstox_curl")
    if preferred is not None:
        try:
            return bool(preferred())
        except Exception:
            return False
    return upstox_http_transport() == "curl"


def _format_timestamp(value) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(value, IST).isoformat()


def _context_callable(context: Any, name: str):
    value = _context_value(context, name)
    return value if callable(value) else None


def _context_value(context: Any, name: str, default=None):
    if context is None:
        return default
    try:
        return getattr(context, name)
    except AttributeError:
        runtime_state = getattr(context, "runtime_state", None)
        if isinstance(runtime_state, Mapping):
            return runtime_state.get(name, default)
        return default
