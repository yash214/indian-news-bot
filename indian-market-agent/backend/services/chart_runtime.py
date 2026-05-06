"""Chart candle, overlay, and trading-workspace summary helpers."""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable

try:
    from backend.agents.agent_output_store import load_latest_agent_report
    from backend.agents.market_regime.schemas import INDEX_MARKET_CONFIG, is_supported_symbol, normalize_market_symbol
    from backend.core.settings import (
        IST,
        UPSTOX_MARKET_DATA_CACHE_TTL_SECONDS,
        UPSTOX_MARKET_DATA_ENABLED,
        UPSTOX_MARKET_DATA_TIMEOUT_SECONDS,
    )
    from backend.providers.upstox.market_data_provider import UpstoxMarketDataProvider
except ModuleNotFoundError:
    from agents.agent_output_store import load_latest_agent_report
    from agents.market_regime.schemas import INDEX_MARKET_CONFIG, is_supported_symbol, normalize_market_symbol
    from core.settings import IST, UPSTOX_MARKET_DATA_CACHE_TTL_SECONDS, UPSTOX_MARKET_DATA_ENABLED, UPSTOX_MARKET_DATA_TIMEOUT_SECONDS
    from providers.upstox.market_data_provider import UpstoxMarketDataProvider


SUPPORTED_INTERVALS = {"1m": 1, "5m": 5, "15m": 15, "1d": 1440}
SUPPORTED_RANGES = {"1d": 1, "5d": 5, "1m": 30, "3m": 90, "6m": 180}
DEFAULT_INTERVAL = "5m"
DEFAULT_RANGE = "1d"


def get_chart_candles(
    symbol: str = "NIFTY",
    interval: str = DEFAULT_INTERVAL,
    range_: str = DEFAULT_RANGE,
    use_mock: bool = False,
    context=None,
) -> dict:
    clean = normalize_market_symbol(symbol)
    interval = _normalize_interval(interval)
    range_ = _normalize_range(range_)
    warnings: list[str] = []
    if not is_supported_symbol(clean):
        return _candle_response(clean, interval, range_, "fallback", [], warnings=["Unsupported chart symbol. Supported symbols: NIFTY, SENSEX."])

    if use_mock:
        return _candle_response(clean, interval, range_, "mock", _mock_candles(clean, interval, range_), warnings=[])

    candles = _candles_from_runtime_history(clean, interval, context=context)
    if candles:
        return _candle_response(clean, interval, range_, "history", candles, warnings=warnings)
    warnings.append("Runtime price history unavailable for chart symbol.")

    candles, provider_warning = _candles_from_upstox(clean, interval, range_)
    if candles:
        return _candle_response(clean, interval, range_, "upstox", candles, warnings=warnings)
    if provider_warning:
        warnings.append(provider_warning)

    return _candle_response(clean, interval, range_, "fallback", [], warnings=warnings or ["No chart candle data available."])


def get_chart_overlays(
    symbol: str = "NIFTY",
    interval: str = DEFAULT_INTERVAL,
    use_mock: bool = False,
    context=None,
) -> dict:
    clean = normalize_market_symbol(symbol)
    interval = _normalize_interval(interval)
    warnings: list[str] = []
    if not is_supported_symbol(clean):
        return _overlay_response(clean, interval, _empty_overlays(), warnings=["Unsupported chart symbol. Supported symbols: NIFTY, SENSEX."])
    if use_mock:
        candles = _mock_candles(clean, interval, "1d")
        last_time = candles[-1]["time"] if candles else int(time.time())
        return _overlay_response(clean, interval, _mock_overlays(clean, last_time), warnings=[])

    overlays = _empty_overlays()
    last_time = _latest_chart_time(clean, interval, context=context)
    regime = _latest_report("market_regime_agent", clean, "MARKET_REGIME_REPORT")
    fo = _latest_report("fo_structure_agent", clean, "FO_STRUCTURE_REPORT")

    if isinstance(regime, dict):
        _apply_market_regime_overlays(overlays, regime, last_time)
    else:
        warnings.append("Market Regime Agent report unavailable for overlays.")
    if isinstance(fo, dict):
        _apply_fo_overlays(overlays, fo)
    else:
        warnings.append("F&O Structure Agent report unavailable for overlays.")

    return _overlay_response(clean, interval, overlays, warnings=warnings)


def get_workspace_summary(symbol: str = "NIFTY", use_mock: bool = False, context=None) -> dict:
    clean = normalize_market_symbol(symbol)
    warnings: list[str] = []
    if not is_supported_symbol(clean):
        warnings.append("Unsupported workspace symbol. Supported symbols: NIFTY, SENSEX.")
    if use_mock:
        return _mock_workspace_summary(clean)

    snapshot = _call_context(context, "market_data_snapshot", include_history=False)
    if not isinstance(snapshot, dict):
        snapshot = _call_context(context, "runtime_snapshot_from_db", include_history=False)
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    ticks = snapshot.get("ticks") if isinstance(snapshot.get("ticks"), dict) else {}
    market_status = snapshot.get("marketStatus") if isinstance(snapshot.get("marketStatus"), dict) else _call_context(context, "get_market_status") or {}
    reports = _workspace_agent_reports(clean)

    health = reports.get("execution_health_agent") if isinstance(reports.get("execution_health_agent"), dict) else {}
    strategy_suggestion = _strategy_placeholder_from_health(health)
    return {
        "symbol": clean,
        "market_bar": _workspace_market_bar(ticks, market_status, reports, clean),
        "agents": reports | {"risk_agent": {"status": "COMING_SOON"}},
        "strategy_suggestions": [strategy_suggestion],
        "warnings": warnings,
    }


def _candle_response(symbol: str, interval: str, range_: str, source: str, candles: list[dict], warnings: list[str]) -> dict:
    cleaned = _clean_candles(candles)
    return {
        "symbol": symbol,
        "interval": interval,
        "range": range_,
        "source": source,
        "last_updated": datetime.now(IST).isoformat(),
        "is_stale": _candles_stale(cleaned, interval),
        "candles": cleaned,
        "warnings": warnings,
    }


def _overlay_response(symbol: str, interval: str, overlays: dict, warnings: list[str]) -> dict:
    return {
        "symbol": symbol,
        "interval": interval,
        "last_updated": datetime.now(IST).isoformat(),
        "overlays": overlays,
        "warnings": warnings,
    }


def _candles_from_runtime_history(symbol: str, interval: str, context=None) -> list[dict]:
    history = _call_context(context, "get_price_history")
    if not isinstance(history, dict):
        snapshot = _call_context(context, "market_data_snapshot", include_history=True)
        if not isinstance(snapshot, dict):
            snapshot = _call_context(context, "runtime_snapshot_from_db", include_history=True)
        history = snapshot.get("history") if isinstance(snapshot, dict) and isinstance(snapshot.get("history"), dict) else None
    if not isinstance(history, dict):
        history = _context_value(context, "_price_history", {})
    if not isinstance(history, dict):
        return []
    values = None
    for label in _history_labels(symbol):
        candidate = history.get(label)
        if isinstance(candidate, list) and candidate:
            values = candidate
            break
    if not values or len(values) < 2:
        return []
    interval_seconds = _interval_seconds(interval)
    end_ts = int(datetime.now(IST).timestamp())
    start_ts = end_ts - interval_seconds * (len(values) - 1)
    candles = []
    previous = _safe_float(values[0])
    for idx, value in enumerate(values):
        close = _safe_float(value)
        if close is None:
            continue
        open_price = previous if previous is not None else close
        spread = max(abs(close - open_price), close * 0.0008)
        candles.append({
            "time": start_ts + idx * interval_seconds,
            "open": round(open_price, 2),
            "high": round(max(open_price, close) + spread * 0.35, 2),
            "low": round(min(open_price, close) - spread * 0.35, 2),
            "close": round(close, 2),
            "volume": 0,
        })
        previous = close
    return candles


def _candles_from_upstox(symbol: str, interval: str, range_: str) -> tuple[list[dict], str | None]:
    provider = UpstoxMarketDataProvider(
        enabled=UPSTOX_MARKET_DATA_ENABLED,
        timeout_seconds=UPSTOX_MARKET_DATA_TIMEOUT_SECONDS,
        cache_ttl_seconds=UPSTOX_MARKET_DATA_CACHE_TTL_SECONDS,
    )
    if not provider.is_configured():
        return [], "Upstox market-data provider is disabled or not configured."
    config = INDEX_MARKET_CONFIG.get(symbol) or {}
    instrument_key = str(config.get("instrument_key") or "")
    if symbol == "SENSEX":
        instrument_key = provider.discover_instrument_key("SENSEX") or instrument_key
    if not instrument_key:
        return [], "No Upstox instrument key available for chart symbol."
    try:
        if interval == "1d":
            today = date.today()
            from_date = (today - timedelta(days=SUPPORTED_RANGES.get(range_, 1))).isoformat()
            raw = provider.get_historical_candles(instrument_key, unit="days", interval=1, to_date=today.isoformat(), from_date=from_date)
            timeframe = 1440
        else:
            raw = provider.get_intraday_candles(instrument_key, unit="minutes", interval=SUPPORTED_INTERVALS[interval])
            timeframe = SUPPORTED_INTERVALS[interval]
        if not raw:
            return [], provider.last_error or "Upstox returned no chart candles."
        normalized = provider.normalize_candles(raw, symbol, instrument_key, timeframe)
        return [_candle_to_payload(candle) for candle in normalized], provider.last_error or None
    except Exception as exc:
        return [], f"Upstox candle fetch failed: {str(exc)[:180]}"


def _apply_market_regime_overlays(overlays: dict, report: dict, last_time: int) -> None:
    key_levels = report.get("key_levels") if isinstance(report.get("key_levels"), dict) else {}
    for key in ("vwap", "ema_9", "ema_21"):
        value = _safe_float(key_levels.get(key) or report.get(key))
        if value is not None:
            overlays[key] = [{"time": last_time, "value": round(value, 2)}]
    opening_high = _safe_float(key_levels.get("opening_range_high"))
    opening_low = _safe_float(key_levels.get("opening_range_low"))
    if opening_high is not None or opening_low is not None:
        overlays["opening_range"] = {"high": opening_high, "low": opening_low}
    for label, key in (
        ("Previous High", "previous_day_high"),
        ("Previous Low", "previous_day_low"),
        ("Day High", "day_high"),
        ("Day Low", "day_low"),
    ):
        value = _safe_float(key_levels.get(key))
        if value is not None:
            overlays["price_lines"].append({"price": round(value, 2), "label": label, "source": "market_regime_agent"})


def _apply_fo_overlays(overlays: dict, report: dict) -> None:
    for zone in report.get("support_zones") or []:
        normalized = _zone_payload(zone, "Support")
        if normalized:
            overlays["support_zones"].append(normalized)
    for zone in report.get("resistance_zones") or []:
        normalized = _zone_payload(zone, "Resistance")
        if normalized:
            overlays["resistance_zones"].append(normalized)
    max_pain = _safe_float(report.get("max_pain"))
    if max_pain is not None:
        overlays["price_lines"].append({"price": round(max_pain, 2), "label": "Max Pain", "source": "fo_structure_agent"})
    atm = _safe_float(report.get("atm_strike"))
    if atm is not None:
        overlays["price_lines"].append({"price": round(atm, 2), "label": "ATM", "source": "fo_structure_agent"})


def _workspace_agent_reports(symbol: str) -> dict:
    return {
        "news_agent": _latest_report("news_agent", symbol, "NEWS_INDEX_REPORT") or _latest_report("news_agent", "INDIA", "NEWS_INDEX_REPORT") or {},
        "macro_agent": _latest_report("macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT") or {},
        "market_regime_agent": _latest_report("market_regime_agent", symbol, "MARKET_REGIME_REPORT") or {},
        "fo_structure_agent": _latest_report("fo_structure_agent", symbol, "FO_STRUCTURE_REPORT") or {},
        "execution_health_agent": _latest_report("execution_health_agent", "SYSTEM", "EXECUTION_HEALTH_REPORT") or {},
    }


def _strategy_placeholder_from_health(health: dict) -> dict:
    blocked = bool(health.get("fresh_trade_blocked")) if isinstance(health, dict) else True
    overall = str(health.get("overall_health") or "UNKNOWN") if isinstance(health, dict) else "UNKNOWN"
    return {
        "title": "Wait / No Trade" if blocked else "Review Market Setup",
        "confidence": round(_safe_float(health.get("confidence"), 0.58) or 0.58, 2) if isinstance(health, dict) else 0.58,
        "rationale": "Strategy Engine not implemented yet. Execution Health is " + overall + ".",
        "manual_approval_required": True,
        "status": "PLACEHOLDER",
    }


def _mock_workspace_summary(symbol: str) -> dict:
    now = datetime.now(IST).isoformat()
    base = 22480.0 if symbol != "SENSEX" else 74280.0
    return {
        "symbol": symbol,
        "market_bar": _mock_market_bar(now),
        "agents": {
            "news_agent": {"agent_name": "news_agent", "overall_sentiment": "NEUTRAL", "confidence": 0.66, "summary": "Mock news tape is balanced."},
            "macro_agent": {"agent_name": "macro_context_agent", "macro_bias": "NEUTRAL", "confidence": 0.7, "summary": "Macro backdrop is steady."},
            "market_regime_agent": {"agent_name": "market_regime_agent", "primary_regime": "RANGE_BOUND", "directional_bias": "NEUTRAL", "confidence": 0.68},
            "fo_structure_agent": {"agent_name": "fo_structure_agent", "bias": "NEUTRAL", "pcr_state": "NEUTRAL", "confidence": 0.64},
            "execution_health_agent": {"agent_name": "execution_health_agent", "overall_health": "HEALTHY", "fresh_trade_blocked": False, "confidence": 0.91},
            "risk_agent": {"status": "COMING_SOON"},
        },
        "strategy_suggestions": [{
            "title": "Wait / No Trade",
            "confidence": 0.58,
            "rationale": f"Static placeholder around {base:.0f}; Strategy Engine is not implemented yet.",
            "manual_approval_required": True,
            "status": "PLACEHOLDER",
        }],
        "warnings": [],
    }


def _workspace_market_bar(ticks: dict, market_status: dict, reports: dict, symbol: str) -> dict:
    now = datetime.now(IST).isoformat()
    bar = {
        "nifty": _quote_from_labels(ticks, "Nifty 50", "NIFTY", "NIFTY 50"),
        "sensex": _quote_from_labels(ticks, "SENSEX", "Sensex"),
        "india_vix": _quote_from_labels(ticks, "India VIX", "VIX"),
        "usd_inr": _quote_from_labels(ticks, "USD/INR", "USDINR"),
        "gold": _quote_from_labels(ticks, "Gold"),
        "crude": _quote_from_labels(ticks, "Crude Oil", "Brent Crude"),
        "market_status": _market_status_label(market_status),
        "last_updated": now,
    }
    fallbacks = _derived_market_bar(reports, symbol, now)
    mock_fallbacks = _mock_market_bar(now)
    for key in ("nifty", "sensex", "india_vix", "usd_inr", "gold", "crude"):
        if not _quote_has_value(bar.get(key)):
            bar[key] = fallbacks.get(key) if _quote_has_value(fallbacks.get(key)) else mock_fallbacks.get(key, {})
    return bar


def _derived_market_bar(reports: dict, symbol: str, now: str) -> dict:
    macro = reports.get("macro_agent") if isinstance(reports.get("macro_agent"), dict) else {}
    factors = macro.get("factors") if isinstance(macro.get("factors"), dict) else {}
    nifty_report = reports.get("market_regime_agent") if symbol == "NIFTY" and isinstance(reports.get("market_regime_agent"), dict) else _latest_report("market_regime_agent", "NIFTY", "MARKET_REGIME_REPORT") or {}
    sensex_report = reports.get("market_regime_agent") if symbol == "SENSEX" and isinstance(reports.get("market_regime_agent"), dict) else _latest_report("market_regime_agent", "SENSEX", "MARKET_REGIME_REPORT") or {}
    india_vix = _factor_quote(factors.get("india_vix")) or _regime_value_quote(nifty_report, "india_vix")
    return {
        "nifty": _regime_index_quote(nifty_report),
        "sensex": _regime_index_quote(sensex_report),
        "india_vix": india_vix,
        "usd_inr": _factor_quote(factors.get("usd_inr")),
        "gold": _factor_quote(factors.get("gold")),
        "crude": _factor_quote(factors.get("crude")),
        "market_status": "UNKNOWN",
        "last_updated": now,
    }


def _mock_market_bar(now: str) -> dict:
    return {
        "nifty": {"price": 22482.25, "change": 86.2, "pct": 0.38},
        "sensex": {"price": 74216.8, "change": 210.4, "pct": 0.28},
        "india_vix": {"price": 13.8, "change": -0.4, "pct": -2.82},
        "usd_inr": {"price": 83.16, "change": 0.04, "pct": 0.05},
        "gold": {"price": 72340.0, "change": 220.0, "pct": 0.31},
        "crude": {"price": 81.2, "change": -0.6, "pct": -0.73},
        "market_status": "OPEN",
        "last_updated": now,
    }


def _regime_index_quote(report: dict) -> dict:
    if not isinstance(report, dict):
        return {}
    key_levels = report.get("key_levels") if isinstance(report.get("key_levels"), dict) else {}
    price = _safe_float(key_levels.get("latest_close") or report.get("latest_close"))
    previous = _safe_float(key_levels.get("previous_day_close"))
    return _quote_payload(price, previous=previous)


def _regime_value_quote(report: dict, key: str) -> dict:
    key_levels = report.get("key_levels") if isinstance(report, dict) and isinstance(report.get("key_levels"), dict) else {}
    return _quote_payload(_safe_float(key_levels.get(key)))


def _factor_quote(factor: dict) -> dict:
    if not isinstance(factor, dict):
        return {}
    return _quote_payload(_safe_float(factor.get("value")), pct=_safe_float(factor.get("change_pct_1d")))


def _quote_payload(price: float | None, previous: float | None = None, pct: float | None = None) -> dict:
    if price is None:
        return {}
    payload = {"price": round(price, 2)}
    if previous is not None and previous > 0:
        change = price - previous
        payload["change"] = round(change, 2)
        payload["pct"] = round((change / previous) * 100, 2)
    elif pct is not None:
        payload["pct"] = round(pct, 2)
        payload["change"] = round(price * pct / 100, 2)
    return payload


def _quote_has_value(quote: dict | None) -> bool:
    if not isinstance(quote, dict):
        return False
    for key in ("price", "close", "value", "last"):
        if _safe_float(quote.get(key)) is not None:
            return True
    return False


def _mock_candles(symbol: str, interval: str, range_: str) -> list[dict]:
    count = 90 if interval != "1d" else min(max(SUPPORTED_RANGES.get(range_, 1), 30), 180)
    interval_seconds = _interval_seconds(interval)
    end_ts = int(datetime.now(IST).timestamp())
    start_ts = end_ts - interval_seconds * (count - 1)
    base = 22480.0 if symbol == "NIFTY" else 74280.0
    previous = base - 85
    candles = []
    for idx in range(count):
        drift = idx * (1.6 if symbol == "NIFTY" else 4.8)
        wave = math.sin(idx / 5.0) * (22 if symbol == "NIFTY" else 72)
        close = base - 70 + drift + wave
        open_price = previous
        wick = 9 + (idx % 5) * 1.3 if symbol == "NIFTY" else 28 + (idx % 5) * 4
        candles.append({
            "time": start_ts + idx * interval_seconds,
            "open": round(open_price, 2),
            "high": round(max(open_price, close) + wick, 2),
            "low": round(min(open_price, close) - wick, 2),
            "close": round(close, 2),
            "volume": 90_000 + idx * 750,
        })
        previous = close
    return candles


def _mock_overlays(symbol: str, last_time: int) -> dict:
    base = 22480.0 if symbol == "NIFTY" else 74280.0
    return {
        "vwap": [{"time": last_time, "value": round(base - 18, 2)}],
        "ema_9": [{"time": last_time, "value": round(base + 12, 2)}],
        "ema_21": [{"time": last_time, "value": round(base - 4, 2)}],
        "opening_range": {"high": round(base + 58, 2), "low": round(base - 76, 2)},
        "support_zones": [{"low": round(base - 140, 2), "high": round(base - 95, 2), "label": "Support", "source": "fo_structure_agent", "strength": 80}],
        "resistance_zones": [{"low": round(base + 120, 2), "high": round(base + 168, 2), "label": "Resistance", "source": "fo_structure_agent", "strength": 76}],
        "price_lines": [
            {"price": round(base, 2), "label": "Max Pain", "source": "fo_structure_agent"},
            {"price": round(base + 50, 2), "label": "ATM", "source": "fo_structure_agent"},
        ],
        "markers": [],
    }


def _empty_overlays() -> dict:
    return {
        "vwap": [],
        "ema_9": [],
        "ema_21": [],
        "opening_range": {"high": None, "low": None},
        "support_zones": [],
        "resistance_zones": [],
        "price_lines": [],
        "markers": [],
    }


def _clean_candles(candles: list[dict]) -> list[dict]:
    by_time: dict[int, dict] = {}
    for candle in candles or []:
        try:
            timestamp = int(float(candle.get("time")))
            open_price = float(candle.get("open"))
            high = float(candle.get("high"))
            low = float(candle.get("low"))
            close = float(candle.get("close"))
            volume = float(candle.get("volume", 0) or 0)
        except (TypeError, ValueError, AttributeError):
            continue
        if timestamp <= 0 or min(open_price, high, low, close) <= 0 or high < low:
            continue
        by_time[timestamp] = {
            "time": timestamp,
            "open": round(open_price, 2),
            "high": round(high, 2),
            "low": round(low, 2),
            "close": round(close, 2),
            "volume": int(volume) if volume >= 0 else 0,
        }
    return [by_time[key] for key in sorted(by_time)]


def _candle_to_payload(candle) -> dict:
    return {
        "time": int(candle.timestamp.timestamp()),
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume or 0,
    }


def _zone_payload(zone: dict, label: str) -> dict | None:
    if not isinstance(zone, dict):
        return None
    strike = _safe_float(zone.get("strike"))
    if strike is None:
        return None
    width = max(strike * 0.0015, 25.0)
    return {
        "low": round(strike - width / 2, 2),
        "high": round(strike + width / 2, 2),
        "label": label,
        "source": "fo_structure_agent",
        "strength": int(_safe_float(zone.get("strength"), 0) or 0),
    }


def _latest_chart_time(symbol: str, interval: str, context=None) -> int:
    history = _candles_from_runtime_history(symbol, interval, context=context)
    if history:
        return int(history[-1]["time"])
    return int(datetime.now(IST).timestamp())


def _latest_report(agent_name: str, symbol: str, report_type: str) -> dict | None:
    try:
        report = load_latest_agent_report(agent_name, symbol, report_type)
    except Exception:
        return None
    return report if isinstance(report, dict) else None


def _quote_from_labels(ticks: dict, *labels: str) -> dict:
    for label in labels:
        quote = ticks.get(label)
        if isinstance(quote, dict):
            return quote
    return {}


def _market_status_label(status: dict) -> str:
    if not isinstance(status, dict):
        return "UNKNOWN"
    if status.get("session"):
        return str(status["session"]).upper()
    if status.get("status"):
        return str(status["status"]).upper()
    return "OPEN" if status.get("isMarketOpen") else "UNKNOWN"


def _history_labels(symbol: str) -> tuple[str, ...]:
    if symbol == "NIFTY":
        return ("Nifty 50", "NIFTY", "NIFTY 50", "NIFTY50")
    return ("SENSEX", "Sensex", "BSE Sensex")


def _normalize_interval(interval: str) -> str:
    value = str(interval or DEFAULT_INTERVAL).strip().lower()
    return value if value in SUPPORTED_INTERVALS else DEFAULT_INTERVAL


def _normalize_range(range_: str) -> str:
    value = str(range_ or DEFAULT_RANGE).strip().lower()
    return value if value in SUPPORTED_RANGES else DEFAULT_RANGE


def _interval_seconds(interval: str) -> int:
    return SUPPORTED_INTERVALS.get(interval, 5) * 60


def _candles_stale(candles: list[dict], interval: str) -> bool:
    if not candles:
        return True
    last_ts = candles[-1].get("time")
    try:
        age = time.time() - float(last_ts)
    except (TypeError, ValueError):
        return True
    return age > max(3600, _interval_seconds(interval) * 4)


def _call_context(context: Any, name: str, *args, **kwargs):
    candidate = _context_callable(context, name)
    if candidate is None:
        return None
    try:
        return candidate(*args, **kwargs)
    except Exception:
        return None


def _context_callable(context: Any, name: str) -> Callable | None:
    value = _context_value(context, name)
    return value if callable(value) else None


def _context_value(context: Any, name: str, default=None):
    if context is None:
        return default
    try:
        value = getattr(context, name)
        return default if value is None else value
    except AttributeError:
        runtime_state = getattr(context, "runtime_state", None)
        if isinstance(runtime_state, dict):
            return runtime_state.get(name, default)
        return default


def _safe_float(value, default=None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default
