"""Market quote, ticker, snapshot, cache, and SSE runtime helpers."""

from __future__ import annotations

from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from datetime import datetime
import json
import queue
import re
import time
from typing import Any

try:
    import certifi
except ImportError:  # pragma: no cover - app.py already requires certifi.
    certifi = None

try:
    from backend.core.persistence import db_get_json, db_set_json
    from backend.core.settings import IST
    from backend.market.catalog import ANALYTICS_INDEX_NAMES, NSE_INDICES_WANTED, NSE_STOCKS, symbol_directory_entry
    from backend.market.math import safe_float
    from backend.services import provider_status
except ModuleNotFoundError:
    from core.persistence import db_get_json, db_set_json
    from core.settings import IST
    from market.catalog import ANALYTICS_INDEX_NAMES, NSE_INDICES_WANTED, NSE_STOCKS, symbol_directory_entry
    from market.math import safe_float
    from services import provider_status


MAX_QUOTE_WORKERS = 8
MAX_HIST = 40
RUNTIME_SNAPSHOT_STATE_KEY = "runtime_market_snapshot"


def format_quote_for_client(symbol: str, quote: dict | None, history=None, context=None, status: dict | None = None) -> dict:
    status = status or _market_status(context)
    entry = symbol_directory_entry(symbol)
    if not quote:
        return {
            "symbol": symbol,
            "label": symbol,
            "name": (entry or {}).get("name") or symbol,
            "price": None,
            "change": None,
            "pct": None,
            "live": False,
            "sym": "Rs",
            "fetchedAt": None,
            "ageSeconds": None,
            "stale": True,
            "source": None,
        }

    age = provider_status.quote_age_seconds(quote)
    stale_after = provider_status.nse_quote_cache_ttl(status, context=context) * 2
    stale = bool(quote.get("stale")) or age is None or age > stale_after
    name = (entry or {}).get("name") or quote.get("name") or symbol
    payload = {
        "symbol": symbol,
        "label": symbol,
        "name": name,
        "price": quote.get("price"),
        "change": quote.get("change"),
        "pct": quote.get("pct"),
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


def format_quotes_for_client(quotes: dict, history_by_symbol=None, context=None, status: dict | None = None) -> dict:
    status = status or _market_status(context)
    return {
        symbol: format_quote_for_client(symbol, quote, context=context, status=status)
        for symbol, quote in (quotes or {}).items()
        if quote
    }


def refresh_quote_cache_for_symbols(symbols: list[str], context=None) -> dict[str, dict]:
    clean_symbols = []
    for symbol in symbols or []:
        normalized = _clean_general_symbol(symbol, context=context)
        if normalized and normalized not in clean_symbols:
            clean_symbols.append(normalized)
    if not clean_symbols:
        return {}

    quotes: dict[str, dict] = {}
    if provider_status.active_market_data_provider(context=context) == provider_status.UPSTOX_PROVIDER_NAME:
        resolve_key = _context_callable(context, "resolve_upstox_instrument_key")
        fetch_upstox_quotes = _context_callable(context, "fetch_upstox_quotes_by_label")
        label_to_key = {
            symbol: key
            for symbol in clean_symbols
            if resolve_key is not None and (key := resolve_key(symbol))
        }
        try:
            quotes = fetch_upstox_quotes(label_to_key) if fetch_upstox_quotes is not None and label_to_key else {}
        except Exception as exc:
            print(f"[!] Upstox quotes failed; falling back to NSE: {exc}")
            quotes = {}
        if not provider_status.upstox_fallback_enabled():
            return quotes
        pending_symbols = [symbol for symbol in clean_symbols if symbol not in quotes]
    else:
        pending_symbols = list(clean_symbols)

    fetch_nse_quote = _context_callable(context, "_fetch_nse_quote")
    if fetch_nse_quote is None or not pending_symbols:
        return quotes

    def nse_worker(symbol: str) -> tuple[str, dict | None]:
        try:
            return symbol, fetch_nse_quote(symbol)
        except Exception:
            return symbol, None

    max_workers = min(int(_context_value(context, "MAX_QUOTE_WORKERS", MAX_QUOTE_WORKERS) or MAX_QUOTE_WORKERS), len(pending_symbols) or 1)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(nse_worker, symbol) for symbol in pending_symbols]
        for future in as_completed(futures):
            symbol, quote = future.result()
            if quote:
                quotes[symbol] = quote
    return quotes


def refresh_tracked_symbol_quotes(context=None, state: dict | None = None) -> dict[str, dict]:
    state = state or _call_or_default(context, "get_app_state_copy", {})
    tracked_symbols_for_state = _context_callable(context, "tracked_symbols_for_state")
    symbols = tracked_symbols_for_state(state) if tracked_symbols_for_state is not None else []
    quotes = refresh_quote_cache_for_symbols(symbols, context=context)
    lock = _context_value(context, "_lock")
    with _maybe_lock(lock):
        previous = dict(_context_value(context, "_tracked_symbol_quotes", {}) or {})
        for symbol in symbols:
            if symbol in quotes:
                previous[symbol] = quotes[symbol]
        tracked_quotes = {symbol: previous[symbol] for symbol in symbols if symbol in previous}
        _set_context_value(context, "_tracked_symbol_quotes", tracked_quotes)
        return dict(tracked_quotes)


def rebuild_computed_payloads(context=None) -> None:
    build_market_analytics_payload = _context_callable(context, "build_market_analytics_payload")
    build_derivatives_analysis_payload = _context_callable(context, "build_derivatives_analysis_payload")
    analytics_symbols_for_state = _context_callable(context, "analytics_symbols_for_state")
    if build_market_analytics_payload is None or build_derivatives_analysis_payload is None or analytics_symbols_for_state is None:
        return

    state = _call_or_default(context, "get_app_state_copy", {})
    lock = _context_value(context, "_lock")
    with _maybe_lock(lock):
        articles = list(_context_value(context, "_arts", []) or [])
        ticks = dict(_context_value(context, "_ticks", {}) or {})
        index_snapshot = dict(_context_value(context, "_index_snapshot", {}) or {})
        tracked_quotes = dict(_context_value(context, "_tracked_symbol_quotes", {}) or {})
        history = dict(_context_value(context, "_price_history", {}) or {})

    analytics_payload = build_market_analytics_payload(
        articles,
        ticks,
        index_snapshot,
        analytics_symbols_for_state(state),
        quote_map=tracked_quotes,
    )
    derivatives_payload = build_derivatives_analysis_payload(
        articles,
        ticks,
        index_snapshot,
        price_history=history,
        market_status=_market_status(context),
    )
    refreshed_at = time.time()
    with _maybe_lock(lock):
        _set_context_value(context, "_analytics_payload", analytics_payload)
        _set_context_value(context, "_derivatives_payload", derivatives_payload)
        _set_context_value(context, "_last_analytics_refresh_ts", refreshed_at)
        _set_context_value(context, "_last_derivatives_refresh_ts", refreshed_at)


def market_data_snapshot(context=None, include_history: bool = False) -> dict:
    lock = _context_value(context, "_lock")
    with _maybe_lock(lock):
        ticks = dict(_context_value(context, "_ticks", {}) or {})
        tracked_quotes = dict(_context_value(context, "_tracked_symbol_quotes", {}) or {})
        analytics_payload = dict(_context_value(context, "_analytics_payload", {}) or {})
        derivatives_payload = dict(_context_value(context, "_derivatives_payload", {}) or {})
        last_tick_ts = _context_value(context, "_last_tick_refresh_ts")
        last_analytics_ts = _context_value(context, "_last_analytics_refresh_ts")
        last_derivatives_ts = _context_value(context, "_last_derivatives_refresh_ts")
        history = (
            {key: list(values) for key, values in (_context_value(context, "_price_history", {}) or {}).items()}
            if include_history
            else None
        )

    status = _market_status(context)
    payload = {
        "serverTime": _ist_now(context).isoformat(),
        "lastTickAt": last_tick_ts,
        "lastAnalyticsAt": last_analytics_ts,
        "lastDerivativesAt": last_derivatives_ts,
        "marketStatus": status,
        "dataProvider": provider_status.market_data_provider_status(context=context),
        "ticks": ticks,
        "trackedQuotes": format_quotes_for_client(tracked_quotes, context=context, status=status),
        "analytics": analytics_payload,
        "derivatives": derivatives_payload,
    }
    if include_history:
        payload["history"] = history or {}
    return payload


def fetch_live_quote(symbol: str, context=None) -> dict | None:
    clean = _clean_general_symbol(symbol, context=context)
    if provider_status.active_market_data_provider(context=context) == provider_status.UPSTOX_PROVIDER_NAME:
        fetch_upstox_quote = _context_callable(context, "_fetch_upstox_quote")
        try:
            quote = fetch_upstox_quote(clean or symbol) if fetch_upstox_quote is not None else None
            if quote:
                return quote
        except Exception as exc:
            print(f"[!] Upstox {symbol}: {exc}")
        if not provider_status.upstox_fallback_enabled():
            return None
    fetch_nse_quote = _context_callable(context, "_fetch_nse_quote")
    return fetch_nse_quote(clean or symbol) if fetch_nse_quote is not None else None


def fetch_tickers(context=None) -> tuple[dict, dict]:
    out: dict = {}
    analytics_indices: dict = {}
    fetched_at = time.time()

    if provider_status.active_market_data_provider(context=context) == provider_status.UPSTOX_PROVIDER_NAME:
        fetch_upstox_index_quotes = _context_callable(context, "fetch_upstox_index_quotes")
        try:
            upstox_quotes = fetch_upstox_index_quotes() if fetch_upstox_index_quotes is not None else {}
            for label, quote in upstox_quotes.items():
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
        except Exception as exc:
            print(f"[!] Upstox indices: {exc}")

    session_factory = _context_callable(context, "_nse_init_session")
    if session_factory is not None:
        try:
            response = session_factory().get(
                "https://www.nseindia.com/api/allIndices",
                headers=_context_value(context, "NSE_HEADERS", {}),
                timeout=6,
                verify=certifi.where() if certifi is not None else True,
            )
            if response.status_code in {401, 403}:
                response = session_factory(force=True).get(
                    "https://www.nseindia.com/api/allIndices",
                    headers=_context_value(context, "NSE_HEADERS", {}),
                    timeout=6,
                    verify=certifi.where() if certifi is not None else True,
                )
            response.raise_for_status()
            for index in response.json().get("data", []):
                name = index.get("indexSymbol", "")
                if name in ANALYTICS_INDEX_NAMES:
                    label = ANALYTICS_INDEX_NAMES[name]
                    last = safe_float(index.get("last"))
                    prev = safe_float(index.get("previousClose"), last)
                    if last:
                        change = round(last - prev, 2)
                        pct = round((change / prev * 100) if prev else 0, 2)
                        if label not in analytics_indices:
                            analytics_indices[label] = {
                                "price": round(last, 2),
                                "change": change,
                                "pct": pct,
                                "live": True,
                                "sym": "",
                                "fetchedAt": fetched_at,
                                "source": "NSE",
                            }
                if name in NSE_INDICES_WANTED:
                    label = NSE_INDICES_WANTED[name]
                    last = safe_float(index.get("last"))
                    prev = safe_float(index.get("previousClose"), last)
                    change = round(last - prev, 2)
                    pct = round((change / prev * 100) if prev else 0, 2)
                    if label not in out:
                        out[label] = {
                            "price": round(last, 2),
                            "change": change,
                            "pct": pct,
                            "live": True,
                            "sym": "",
                            "fetchedAt": fetched_at,
                            "source": "NSE",
                        }
        except Exception as exc:
            print(f"[!] NSE allIndices: {exc}")

    stock_quotes = refresh_quote_cache_for_symbols(list(NSE_STOCKS.values()), context=context)
    for label, symbol in NSE_STOCKS.items():
        quote = stock_quotes.get(symbol)
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


def broadcast_market_snapshot(context=None) -> None:
    payload = "data:" + json.dumps(market_data_snapshot(context=context, include_history=False)) + "\n\n"
    sse_lock = _context_value(context, "_sse_lock")
    sse_queues = _context_value(context, "_sse_queues", [])
    with _maybe_lock(sse_lock):
        dead = []
        for sse_queue in list(sse_queues or []):
            try:
                sse_queue.put_nowait(payload)
            except queue.Full:
                dead.append(sse_queue)
        for sse_queue in dead:
            try:
                sse_queues.remove(sse_queue)
            except ValueError:
                pass


def broadcast_tickers(data: dict | None = None, context=None) -> None:
    broadcast_market_snapshot(context=context)


def update_price_history(symbol: str | dict, price: float | None = None, context=None) -> None:
    if isinstance(symbol, dict) and price is None:
        for label, data in symbol.items():
            if not isinstance(data, dict):
                continue
            tick_price = data.get("price")
            if tick_price is not None:
                update_price_history(label, tick_price, context=context)
        return

    if price is None:
        return
    max_hist = int(_context_value(context, "MAX_HIST", MAX_HIST) or MAX_HIST)
    lock = _context_value(context, "_lock")
    with _maybe_lock(lock):
        history = dict(_context_value(context, "_price_history", {}) or {})
        values = list(history.get(str(symbol), []))
        values.append(price)
        if len(values) > max_hist:
            values = values[-max_hist:]
        history[str(symbol)] = values
        _set_context_value(context, "_price_history", history)


def persist_runtime_snapshot_payload(context=None) -> None:
    try:
        db_set_json(RUNTIME_SNAPSHOT_STATE_KEY, market_data_snapshot(context=context, include_history=True))
    except Exception as exc:
        print(f"[!] runtime snapshot persist error: {exc}")


def runtime_snapshot_from_db(include_history: bool = False) -> dict | None:
    payload = db_get_json(RUNTIME_SNAPSHOT_STATE_KEY, default=None)
    if not isinstance(payload, dict):
        return None
    if not include_history:
        payload = dict(payload)
        payload.pop("history", None)
    return payload


def market_runtime_status(context=None) -> dict:
    lock = _context_value(context, "_lock")
    with _maybe_lock(lock):
        ticks = dict(_context_value(context, "_ticks", {}) or {})
        tracked_quotes = dict(_context_value(context, "_tracked_symbol_quotes", {}) or {})
        history = dict(_context_value(context, "_price_history", {}) or {})
        analytics_payload = dict(_context_value(context, "_analytics_payload", {}) or {})
        derivatives_payload = dict(_context_value(context, "_derivatives_payload", {}) or {})
        last_tick_ts = _context_value(context, "_last_tick_refresh_ts")
        last_analytics_ts = _context_value(context, "_last_analytics_refresh_ts")
        last_derivatives_ts = _context_value(context, "_last_derivatives_refresh_ts")
    return {
        "tickCount": len(ticks),
        "trackedQuoteCount": len(tracked_quotes),
        "historySymbolCount": len(history),
        "lastTickAt": last_tick_ts,
        "lastAnalyticsAt": last_analytics_ts,
        "lastDerivativesAt": last_derivatives_ts,
        "analyticsReady": bool(analytics_payload.get("generatedAt")),
        "derivativesReady": bool(derivatives_payload.get("generatedAt")),
        "dataProvider": provider_status.market_data_provider_status(context=context),
        "readOnly": True,
    }


def _clean_general_symbol(symbol: str, context=None) -> str:
    cleaner = _context_callable(context, "_clean_general_symbol")
    if cleaner is not None:
        return cleaner(symbol)
    return re.sub(r"[^A-Z0-9&.^-]", "", str(symbol or "").upper().strip())


def _market_status(context) -> dict:
    get_market_status = _context_callable(context, "get_market_status")
    if get_market_status is None:
        return {}
    try:
        return get_market_status() or {}
    except Exception:
        return {}


def _ist_now(context) -> datetime:
    ist_now = _context_callable(context, "ist_now")
    if ist_now is not None:
        try:
            return ist_now()
        except Exception:
            pass
    return datetime.now(IST)


def _call_or_default(context, name: str, default):
    candidate = _context_callable(context, name)
    if candidate is None:
        return default
    try:
        return candidate()
    except Exception:
        return default


def _maybe_lock(lock):
    return lock if lock is not None else nullcontext()


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


def _set_context_value(context: Any, name: str, value) -> None:
    if context is None:
        return
    runtime_state = getattr(context, "runtime_state", None)
    if isinstance(runtime_state, Mapping):
        runtime_state[name] = value
        return
    try:
        setattr(context, name, value)
    except Exception:
        pass
