"""Upstox REST, instrument, quote, option-chain, and stream runtime helpers."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
import json
import os
import re
import secrets
import subprocess
import threading
import time
from urllib.parse import quote

import certifi
import requests

try:
    from backend.core.settings import DATA_DIR
    from backend.market.catalog import NSE_STOCKS, UPSTOX_INDEX_INSTRUMENT_KEYS, symbol_directory_entry
    from backend.providers.upstox.live import build_stream_request, stream_authorize_url, stream_quote_from_feed
    from backend.providers.upstox.market_data import (
        option_underlying_key,
        summarize_upstox_option_chain,
        upstox_instrument_key_for_symbol,
        upstox_quote_from_payload,
        upstox_token_preview,
    )
    from backend.providers.upstox.v3_proto import decode_feed_response
    from backend.services import provider_status
except ModuleNotFoundError:
    from core.settings import DATA_DIR
    from market.catalog import NSE_STOCKS, UPSTOX_INDEX_INSTRUMENT_KEYS, symbol_directory_entry
    from providers.upstox.live import build_stream_request, stream_authorize_url, stream_quote_from_feed
    from providers.upstox.market_data import (
        option_underlying_key,
        summarize_upstox_option_chain,
        upstox_instrument_key_for_symbol,
        upstox_quote_from_payload,
        upstox_token_preview,
    )
    from providers.upstox.v3_proto import decode_feed_response
    from services import provider_status


UPSTOX_DEFAULT_API_BASE = "https://api.upstox.com/v2"
UPSTOX_DEFAULT_V3_API_BASE = "https://api.upstox.com/v3"
UPSTOX_QUOTE_BATCH_LIMIT = 500
UPSTOX_INSTRUMENT_SEARCH_TTL = 86400.0
UPSTOX_STREAM_MODE = "full"
UPSTOX_STREAM_RECONNECT_SECONDS = 5
STREAM_UI_BROADCAST_SECONDS = max(0.25, float(os.environ.get("STREAM_UI_BROADCAST_SECONDS", "1.0") or "1.0"))
MAX_HIST = 40

_lock = threading.Lock()
_thread_local = threading.local()
_upstox_quote_cache: dict[str, tuple[dict, float]] = {}
_upstox_stream_quote_cache: dict[str, tuple[dict, float]] = {}
_upstox_instrument_search_cache: dict[str, tuple[list[dict], float]] = {}
_upstox_curl_preferred_until = 0.0
_last_fast_stream_broadcast_ts = 0.0
_upstox_stream_wakeup = threading.Event()


def upstox_api_base() -> str:
    return os.environ.get("UPSTOX_API_BASE", UPSTOX_DEFAULT_API_BASE).strip().rstrip("/")


def upstox_v3_api_base() -> str:
    configured = os.environ.get("UPSTOX_V3_API_BASE", "").strip().rstrip("/")
    if configured:
        return configured
    base = upstox_api_base()
    return re.sub(r"/v2/?$", "/v3", base) if re.search(r"/v2/?$", base) else UPSTOX_DEFAULT_V3_API_BASE


def upstox_debug_enabled() -> bool:
    return provider_status.upstox_debug_enabled()


def upstox_http_transport() -> str:
    return provider_status.upstox_http_transport()


def upstox_user_agent() -> str:
    return os.environ.get("UPSTOX_USER_AGENT", "curl/8.7.1").strip() or "curl/8.7.1"


def upstox_headers() -> dict[str, str]:
    token = provider_status.upstox_analytics_token()
    if not token:
        raise RuntimeError("UPSTOX_ANALYTICS_TOKEN is not configured")
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": upstox_user_agent(),
    }


def _short_error(exc: Exception | str, limit: int = 180) -> str:
    return str(exc).replace("\n", " ").strip()[:limit]


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


def _prefer_upstox_curl(context=None) -> bool:
    transport = upstox_http_transport()
    if transport == "curl":
        return True
    if transport == "requests":
        return False
    return time.time() < float(_context_value(context, "_upstox_curl_preferred_until", _upstox_curl_preferred_until) or 0.0)


def _mark_upstox_curl_preferred(seconds: float = 900.0, context=None) -> None:
    global _upstox_curl_preferred_until
    current = float(_context_value(context, "_upstox_curl_preferred_until", _upstox_curl_preferred_until) or 0.0)
    preferred_until = max(current, time.time() + seconds)
    if _set_context_value(context, "_upstox_curl_preferred_until", preferred_until):
        return
    _upstox_curl_preferred_until = preferred_until


def _upstox_request_json_with_requests(url: str, timeout: int, context=None) -> dict:
    response = _http_session(context).get(
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


def _upstox_request_json_with_curl(url: str, timeout: int, context=None) -> dict:
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


def upstox_request_json(url: str, timeout: int = 8, context=None) -> dict:
    transport = upstox_http_transport()
    if _prefer_upstox_curl(context=context):
        if upstox_debug_enabled():
            print("[*] Upstox transport: curl")
        return _upstox_request_json_with_curl(url, timeout, context=context)
    try:
        if upstox_debug_enabled():
            print("[*] Upstox transport: requests")
        return _upstox_request_json_with_requests(url, timeout, context=context)
    except UpstoxEdgeBlockedError:
        if transport != "auto":
            raise
        _mark_upstox_curl_preferred(context=context)
        if upstox_debug_enabled():
            print("[*] Upstox requests transport got HTML 403; switching to curl transport")
        return _upstox_request_json_with_curl(url, timeout, context=context)


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
    limit: int = 10,
    context=None,
    exchanges: str = "NSE",
    segments: str = "EQ,INDEX",
    records: int | None = None,
) -> list[dict]:
    query = str(query or "").strip()
    if not query or not provider_status.upstox_configured():
        return []
    records = records if records is not None else limit
    cache_key = _upstox_search_cache_key(query, exchanges, segments, records)
    now = time.time()
    cache = _state_dict(context, "_upstox_instrument_search_cache", _upstox_instrument_search_cache)
    cached = cache.get(cache_key)
    if cached and now - cached[1] < UPSTOX_INSTRUMENT_SEARCH_TTL:
        return list(cached[0])
    payload = upstox_request_json(upstox_instrument_search_url(query, exchanges, segments, records), timeout=6, context=context)
    rows = payload.get("data") if isinstance(payload, dict) else []
    rows = rows if isinstance(rows, list) else []
    cache[cache_key] = (rows, now)
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


def upstox_symbol_search_results(query: str, limit: int = 10, context=None) -> list[dict]:
    q = _clean_market_symbol(query)
    if len(q) < 2:
        return []
    try:
        search = _context_callable(context, "upstox_search_instruments")
        rows = (
            search(query, records=min(max(limit * 2, 8), 30))
            if search is not None
            else upstox_search_instruments(query, records=min(max(limit * 2, 8), 30), context=context)
        )
    except Exception as exc:
        if upstox_debug_enabled():
            print(f"[!] Upstox instrument search failed for {q}: {_short_error(exc)}")
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


def resolve_upstox_instrument_key(symbol: str, context=None) -> str | None:
    clean = _clean_market_symbol(symbol)
    if not clean:
        return None
    static_key = upstox_instrument_key_for_symbol(clean)
    if static_key:
        return static_key
    search = _context_callable(context, "upstox_search_instruments")
    rows = search(clean, records=12) if search is not None else upstox_search_instruments(clean, records=12, context=context)
    for row in rows:
        row_symbol = _symbol_from_upstox_instrument(row)
        instrument_key = str(row.get("instrument_key") or "").strip()
        segment = str(row.get("segment") or "")
        instrument_type = str(row.get("instrument_type") or "")
        if instrument_key and row_symbol == clean and (segment == "NSE_EQ" or segment == "NSE_INDEX" or instrument_type == "INDEX"):
            return instrument_key
    return None


def upstox_quotes_url(instrument_keys: list[str]) -> str:
    encoded_keys = quote(",".join(instrument_keys), safe=",")
    return f"{upstox_api_base()}/market-quote/quotes?instrument_key={encoded_keys}"


def fetch_upstox_quote_batch(
    label_to_key: dict[str, str],
    received_at: float | None = None,
    context=None,
) -> dict[str, dict]:
    received_at = time.time() if received_at is None else received_at
    url = upstox_quotes_url(list(label_to_key.values()))
    if upstox_debug_enabled():
        print(f"[*] Upstox quotes URL: {url}")
    payload = upstox_request_json(url, timeout=8, context=context)
    if payload.get("status") not in {None, "success"}:
        raise RuntimeError(f"Upstox quote request failed: {payload.get('status')}")
    return _parse_upstox_quote_payload(label_to_key, payload, received_at, context=context)


def fetch_upstox_stream_quotes_by_label(label_to_key: dict[str, str], context=None) -> dict[str, dict]:
    if not label_to_key:
        return {}
    now = time.time()
    stale_after = provider_status.upstox_stream_stale_after(context=context)
    out = {}
    lock = _context_value(context, "_lock", _lock)
    stream_cache = _state_dict(context, "_upstox_stream_quote_cache", _upstox_stream_quote_cache)
    with _maybe_lock(lock):
        for label, key in label_to_key.items():
            cached = stream_cache.get(key)
            if not cached:
                continue
            quote_payload = cached[0]
            age = provider_status.quote_age_seconds(quote_payload, now)
            if age is None or age > stale_after:
                continue
            if quote_payload.get("symbol") != label:
                quote_payload = dict(quote_payload, symbol=label)
            out[label] = quote_payload
    return out


def fetch_upstox_quotes_by_label(label_to_key: dict[str, str], context=None) -> dict[str, dict]:
    if not label_to_key or not provider_status.upstox_configured():
        return {}

    now = time.time()
    ttl = provider_status.nse_quote_cache_ttl(context=context)
    out: dict[str, dict] = fetch_upstox_stream_quotes_by_label(label_to_key, context=context)
    pending: dict[str, str] = {}
    quote_cache = _state_dict(context, "_upstox_quote_cache", _upstox_quote_cache)
    for label, key in label_to_key.items():
        if not key or label in out:
            continue
        cache_key = f"{label}|{key}"
        cached = quote_cache.get(cache_key)
        if cached and (now - cached[1] < ttl):
            out[label] = cached[0]
        else:
            pending[label] = key

    batch_fetch = _context_callable(context, "fetch_upstox_quote_batch")
    while pending:
        labels = list(pending.keys())[:UPSTOX_QUOTE_BATCH_LIMIT]
        batch = {label: pending[label] for label in labels}
        try:
            fetched = (
                batch_fetch(batch, now)
                if batch_fetch is not None
                else fetch_upstox_quote_batch(batch, received_at=now, context=context)
            )
            out.update(fetched)
            provider_status.patch_upstox_rest_status(lastError=None, lastOkAt=time.time(), failedKeys=[])
        except Exception as exc:
            batch_error = str(exc)
            if len(batch) == 1:
                label, key = next(iter(batch.items()))
                provider_status.patch_upstox_rest_status(lastError=batch_error[:240], lastErrorAt=time.time(), failedKeys=[key])
                print(f"[!] Upstox {label} ({key}): {_short_error(batch_error, 240)}")
            else:
                print(f"[!] Upstox quote batch rejected; retrying individually: {_short_error(batch_error, 240)}")
                failed_keys: list[str] = []
                successful = False
                for label, key in batch.items():
                    try:
                        single = (
                            batch_fetch({label: key}, now)
                            if batch_fetch is not None
                            else fetch_upstox_quote_batch({label: key}, received_at=now, context=context)
                        )
                        if single:
                            out.update(single)
                            successful = True
                    except Exception as single_exc:
                        failed_keys.append(key)
                        print(f"[!] Upstox {label} ({key}): {_short_error(single_exc, 240)}")
                provider_status.patch_upstox_rest_status(
                    lastError=(batch_error[:240] if failed_keys else None),
                    lastErrorAt=time.time() if failed_keys else None,
                    lastOkAt=time.time() if successful else provider_status.upstox_rest_last_ok_at(),
                    failedKeys=failed_keys,
                )
        for label in labels:
            pending.pop(label, None)
    return out


def _fetch_upstox_quote(symbol: str, context=None) -> dict | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    key = resolve_upstox_instrument_key(clean, context=context)
    if not clean or not key:
        return None
    return fetch_upstox_quotes_by_label({clean: key}, context=context).get(clean)


def fetch_upstox_index_quotes(context=None) -> dict[str, dict]:
    return fetch_upstox_quotes_by_label(dict(UPSTOX_INDEX_INSTRUMENT_KEYS), context=context)


def ticker_payload_from_quote(quote_payload: dict, default_sym: str = "", context=None) -> dict:
    age = provider_status.quote_age_seconds(quote_payload)
    stale_after = provider_status.nse_quote_cache_ttl(context=context) * 2
    stale = bool(quote_payload.get("stale")) or age is None or age > stale_after
    payload = {
        "price": quote_payload["price"],
        "change": quote_payload["change"],
        "pct": quote_payload["pct"],
        "live": not stale,
        "sym": quote_payload.get("sym", default_sym),
        "fetchedAt": quote_payload.get("fetchedAt", time.time()),
        "ageSeconds": age,
        "stale": stale,
        "source": quote_payload.get("source", "Market feed"),
    }
    for key in ["previous_close", "open", "day_high", "day_low", "providerTimestamp", "sourceDetail", "providerError"]:
        if quote_payload.get(key) is not None:
            payload[key] = quote_payload.get(key)
    return payload


def apply_quote_update_to_runtime(label: str, quote_payload: dict, *, update_indexes: bool = True, context=None) -> None:
    now = time.time()
    index_label = next(
        (idx_label for idx_label, key in UPSTOX_INDEX_INSTRUMENT_KEYS.items() if key == quote_payload.get("instrumentKey")),
        label if label in UPSTOX_INDEX_INSTRUMENT_KEYS else None,
    )
    ticker_label = "VIX" if index_label == "India VIX" else index_label or label
    ticker_payload = ticker_payload_from_quote(quote_payload, default_sym="" if index_label else "Rs", context=context)
    price_history_updates: list[tuple[str, float]] = []
    lock = _context_value(context, "_lock", _lock)
    with _maybe_lock(lock):
        app_state = _context_value(context, "_app_state", {}) or {}
        tracked_symbols_for_state = _context_callable(context, "tracked_symbols_for_state")
        tracked_symbols = set(tracked_symbols_for_state(app_state) if tracked_symbols_for_state is not None else [])
        ticks = _state_dict(context, "_ticks", {})
        index_snapshot = _state_dict(context, "_index_snapshot", {})
        tracked_quotes = _state_dict(context, "_tracked_symbol_quotes", {})
        if update_indexes and index_label:
            index_snapshot[index_label] = dict(ticker_payload)
            ticks[ticker_label] = dict(ticker_payload)
            price_history_updates.append((ticker_label, ticker_payload["price"]))
        if label in tracked_symbols:
            tracked_quotes[label] = quote_payload
        _set_context_value(context, "_last_tick_refresh_ts", now)
        price_history = _state_dict(context, "_price_history", {})
        max_hist = int(_context_value(context, "MAX_HIST", MAX_HIST) or MAX_HIST)
        for history_label, price in price_history_updates:
            history = price_history.setdefault(history_label, [])
            if not history or history[-1] != price:
                history.append(price)
            if len(history) > max_hist:
                price_history[history_label] = history[-max_hist:]


def maybe_broadcast_fast_market_snapshot(force: bool = False, context=None) -> None:
    global _last_fast_stream_broadcast_ts
    now = time.time()
    last_broadcast = float(_context_value(context, "_last_fast_stream_broadcast_ts", _last_fast_stream_broadcast_ts) or 0.0)
    if not force and now - last_broadcast < STREAM_UI_BROADCAST_SECONDS:
        return
    if not _set_context_value(context, "_last_fast_stream_broadcast_ts", now):
        _last_fast_stream_broadcast_ts = now
    broadcast = _context_callable(context, "broadcast_market_snapshot")
    if broadcast is not None:
        broadcast()


def upstox_stream_subscription_map(state: dict | None = None, context=None) -> dict[str, str]:
    if state is None:
        get_state = _context_callable(context, "get_app_state_copy")
        state = get_state() if get_state is not None else {}
    labels = dict(UPSTOX_INDEX_INSTRUMENT_KEYS)
    resolver = _context_callable(context, "resolve_upstox_instrument_key")
    for symbol in NSE_STOCKS.values():
        key = resolver(symbol) if resolver is not None else resolve_upstox_instrument_key(symbol, context=context)
        if key:
            labels[symbol] = key
    tracked_symbols_for_state = _context_callable(context, "tracked_symbols_for_state")
    tracked_symbols = tracked_symbols_for_state(state) if tracked_symbols_for_state is not None else []
    for symbol in tracked_symbols:
        key = resolver(symbol) if resolver is not None else resolve_upstox_instrument_key(symbol, context=context)
        if key:
            labels[symbol] = key
    return labels


def upstox_stream_dependencies_ready() -> bool:
    try:
        import websocket  # noqa: F401
    except ImportError:
        return False
    return True


def upstox_stream_authorized_redirect_uri(context=None) -> str:
    payload = upstox_request_json(stream_authorize_url(upstox_v3_api_base()), timeout=8, context=context)
    uri = str((payload.get("data") or {}).get("authorized_redirect_uri") or "").strip()
    if not uri:
        raise RuntimeError("Upstox V3 authorize response did not include authorized_redirect_uri")
    return uri


def _set_upstox_stream_status(context=None, **patch) -> None:
    lock = _context_value(context, "_lock", _lock)
    status = _state_dict(context, "_upstox_stream_status", {})
    with _maybe_lock(lock):
        status.update(patch)


def _send_upstox_stream_request(ws, method: str, instrument_keys: list[str], mode: str = UPSTOX_STREAM_MODE) -> None:
    if not instrument_keys:
        return
    ws.send_binary(build_stream_request(method, instrument_keys, guid=secrets.token_hex(12), mode=mode))


def _apply_upstox_stream_payload(payload: dict, label_by_key: dict[str, str], context=None) -> None:
    current_ts = payload.get("currentTs") or int(time.time() * 1000)
    message_type = payload.get("type")
    if message_type == "market_info":
        _set_upstox_stream_status(
            context=context,
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
        quote_payload = stream_quote_from_feed(
            label,
            instrument_key,
            feed or {},
            current_ts,
            name=(directory_entry or {}).get("name") or label,
        )
        if quote_payload:
            updates[instrument_key] = (quote_payload, time.time())
            apply_quote_update_to_runtime(label, quote_payload, context=context)
            labels_updated = True

    if updates:
        lock = _context_value(context, "_lock", _lock)
        stream_cache = _state_dict(context, "_upstox_stream_quote_cache", _upstox_stream_quote_cache)
        stream_status = _state_dict(context, "_upstox_stream_status", {})
        with _maybe_lock(lock):
            stream_cache.update(updates)
            stream_status["lastMessageAt"] = time.time()
            stream_status["lastError"] = None
    if labels_updated:
        maybe_broadcast_fast_market_snapshot(context=context)


def upstox_stream_loop(context=None) -> None:
    while True:
        desired = upstox_stream_subscription_map(context=context)
        dependency_ready = upstox_stream_dependencies_ready()
        _set_upstox_stream_status(
            context=context,
            dependencyReady=dependency_ready,
            desiredSubscriptions=len({key for key in desired.values() if key}),
            mode=UPSTOX_STREAM_MODE,
        )
        wakeup = _stream_wakeup(context)
        if provider_status.requested_market_data_provider() != provider_status.UPSTOX_PROVIDER_NAME or not provider_status.upstox_configured():
            _set_upstox_stream_status(context=context, connected=False, activeSubscriptions=0)
            wakeup.wait(timeout=10)
            wakeup.clear()
            continue
        if not dependency_ready:
            _set_upstox_stream_status(
                context=context,
                connected=False,
                activeSubscriptions=0,
                lastError="Install websocket-client to enable Upstox V3 streaming",
            )
            wakeup.wait(timeout=30)
            wakeup.clear()
            continue

        ws = None
        try:
            import websocket
            from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException

            uri = upstox_stream_authorized_redirect_uri(context=context)
            ws = websocket.create_connection(
                uri,
                timeout=10,
                enable_multithread=True,
                sslopt={"ca_certs": certifi.where()},
            )
            ws.settimeout(1)
            _set_upstox_stream_status(
                context=context,
                connected=True,
                lastConnectAt=time.time(),
                lastError=None,
                activeSubscriptions=0,
            )
            active_keys: set[str] = set()

            while provider_status.requested_market_data_provider() == provider_status.UPSTOX_PROVIDER_NAME and provider_status.upstox_configured():
                desired = upstox_stream_subscription_map(context=context)
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
                    context=context,
                    desiredSubscriptions=len(desired_keys),
                    activeSubscriptions=len(active_keys),
                )

                try:
                    frame = ws.recv()
                except WebSocketTimeoutException:
                    if wakeup.wait(timeout=0.2):
                        wakeup.clear()
                    continue
                except WebSocketConnectionClosedException as exc:
                    raise RuntimeError(f"Upstox V3 socket closed: {exc}") from exc

                if frame is None:
                    continue
                payload = decode_feed_response(frame.encode("utf-8") if isinstance(frame, str) else frame)
                _apply_upstox_stream_payload(payload, label_by_key, context=context)
                if wakeup.is_set():
                    wakeup.clear()
        except Exception as exc:
            _set_upstox_stream_status(
                context=context,
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
            _set_upstox_stream_status(context=context, connected=False, lastDisconnectAt=time.time(), activeSubscriptions=0)


def upstox_option_chain_url(underlying_key: str, expiry_date: str) -> str:
    return (
        f"{upstox_api_base()}/option/chain"
        f"?instrument_key={quote(underlying_key, safe='')}"
        f"&expiry_date={quote(expiry_date, safe='')}"
    )


def fetch_upstox_option_chain(
    underlying: str,
    expiry: str | None = None,
    context=None,
    expiry_date: str | None = None,
    max_rows: int = 80,
) -> dict | None:
    expiry = expiry_date if expiry_date is not None else expiry
    if not provider_status.upstox_configured():
        return None
    underlying_key = option_underlying_key(underlying)
    if not underlying_key:
        raise ValueError(f"Unsupported Upstox option underlying: {underlying}")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", expiry or ""):
        raise ValueError("expiry must be provided in YYYY-MM-DD format")
    payload = upstox_request_json(upstox_option_chain_url(underlying_key, expiry), timeout=10, context=context)
    if payload.get("status") not in {None, "success"}:
        raise RuntimeError(f"Upstox option-chain request failed: {payload.get('status')}")
    return summarize_upstox_option_chain(payload.get("data") or [], underlying, expiry, max_rows=max_rows)


def upstox_integration_status(context=None) -> dict:
    token = provider_status.upstox_analytics_token()
    provider = provider_status.market_data_provider_status(context=context)
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
        "stream": provider_status.upstox_stream_runtime_status(context=context),
        "dataDir": str(DATA_DIR),
    }


def upstox_runtime_status(context=None) -> dict:
    return {
        "provider": provider_status.market_data_provider_status(context=context),
        "configured": provider_status.upstox_configured(),
        "tokenSource": provider_status.upstox_token_source(),
        "stream": provider_status.upstox_stream_runtime_status(context=context),
        "rest": provider_status.upstox_rest_runtime_status(context=context),
        "readOnly": True,
    }


def _parse_upstox_quote_payload(label_to_key: dict[str, str], payload: dict, received_at: float, context=None) -> dict[str, dict]:
    key_to_label = {key: label for label, key in label_to_key.items()}
    out: dict[str, dict] = {}
    quote_cache = _state_dict(context, "_upstox_quote_cache", _upstox_quote_cache)
    for quote_payload in (payload.get("data") or {}).values():
        instrument_key = quote_payload.get("instrument_token") or quote_payload.get("instrument_key")
        label = key_to_label.get(instrument_key)
        if not label:
            symbol = re.sub(r"[^A-Z0-9&.-]", "", str(quote_payload.get("symbol", "")).upper())
            label = symbol if symbol in label_to_key else None
        if not label:
            continue
        quote_data = upstox_quote_from_payload(label, quote_payload, received_at)
        if not quote_data:
            continue
        cache_key = f"{label}|{label_to_key[label]}"
        quote_cache[cache_key] = (quote_data, received_at)
        out[label] = quote_data
    return out


def _http_session(context):
    session_factory = _context_callable(context, "http_session")
    if session_factory is not None:
        return session_factory()
    sess = getattr(_thread_local, "http_session", None)
    if sess is None:
        sess = requests.Session()
        sess.verify = certifi.where()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "en-IN,en;q=0.9",
        })
        _thread_local.http_session = sess
    return sess


def _stream_wakeup(context):
    return _context_value(context, "_upstox_stream_wakeup", _upstox_stream_wakeup)


def _state_dict(context, name: str, fallback: dict) -> dict:
    value = _context_value(context, name, fallback)
    return value if isinstance(value, dict) else fallback


def _maybe_lock(lock):
    return lock if lock is not None else nullcontext()


def _context_callable(context, name: str):
    value = _context_value(context, name)
    return value if callable(value) else None


def _context_value(context, name: str, default=None):
    if context is None:
        return default
    try:
        value = getattr(context, name)
        return default if value is None else value
    except AttributeError:
        runtime_state = getattr(context, "runtime_state", None)
        if isinstance(runtime_state, Mapping):
            return runtime_state.get(name, default)
        return default


def _set_context_value(context, name: str, value) -> bool:
    if context is None:
        return False
    runtime_state = getattr(context, "runtime_state", None)
    if isinstance(runtime_state, Mapping):
        runtime_state[name] = value
        return True
    try:
        setattr(context, name, value)
        return True
    except Exception:
        return False
