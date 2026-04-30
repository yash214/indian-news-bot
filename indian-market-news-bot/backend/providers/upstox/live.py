from __future__ import annotations

import json
import time
from datetime import datetime


def _safe_float(value, default=0.0) -> float:
    try:
        if value in (None, ""):
            return None if default is None else float(default)
        return float(value)
    except Exception:
        return None if default is None else float(default)


def _parse_upstox_timestamp(value, default_ts: float) -> float:
    try:
        if value is None or value == "":
            return default_ts
        if isinstance(value, (int, float)):
            number = float(value)
            return number / 1000 if number > 10_000_000_000 else number
        text = str(value).strip()
        if text.isdigit():
            number = float(text)
            return number / 1000 if number > 10_000_000_000 else number
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return default_ts


def stream_authorize_url(v3_api_base: str) -> str:
    return f"{v3_api_base.rstrip('/')}/feed/market-data-feed/authorize"


def build_stream_request(method: str, instrument_keys: list[str], guid: str, mode: str = "full") -> bytes:
    payload = {
        "guid": guid,
        "method": method,
        "data": {
            "mode": mode,
            "instrumentKeys": instrument_keys,
        },
    }
    return json.dumps(payload).encode("utf-8")


def day_ohlc(rows: list[dict]) -> dict:
    if not rows:
        return {}
    for row in rows:
        if str(row.get("interval", "")).lower() == "1d":
            return row
    return rows[0]


def stream_quote_from_feed(label: str, instrument_key: str, feed: dict, current_ts: int | float | None, name: str | None = None) -> dict | None:
    current_ts = current_ts or int(time.time() * 1000)
    ltpc = {}
    best_bid = best_ask = None
    volume = oi = None
    today = {}

    if feed.get("fullFeed"):
        full_feed = feed["fullFeed"]
        market_ff = full_feed.get("marketFF") or {}
        index_ff = full_feed.get("indexFF") or {}
        body = market_ff or index_ff
        ltpc = body.get("ltpc") or {}
        today = day_ohlc((body.get("marketOHLC") or {}).get("ohlc") or [])
        bid_ask_quotes = ((body.get("marketLevel") or {}).get("bidAskQuote") or [])
        if bid_ask_quotes:
            best_bid = bid_ask_quotes[0].get("bidP")
            best_ask = bid_ask_quotes[0].get("askP")
        volume = market_ff.get("vtt")
        oi = market_ff.get("oi")
    elif feed.get("firstLevelWithGreeks"):
        level = feed["firstLevelWithGreeks"]
        ltpc = level.get("ltpc") or {}
        depth = level.get("firstDepth") or {}
        best_bid = depth.get("bidP")
        best_ask = depth.get("askP")
        volume = level.get("vtt")
        oi = level.get("oi")
    else:
        ltpc = feed.get("ltpc") or {}

    last = _safe_float(ltpc.get("ltp"))
    if not last:
        return None
    previous_close = _safe_float(ltpc.get("cp"), last)
    change = round(last - previous_close, 2)
    pct = round((change / previous_close * 100) if previous_close else 0, 2)
    fetched_at = _parse_upstox_timestamp(ltpc.get("ltt") or current_ts, time.time())
    return {
        "symbol": label,
        "name": name or label,
        "price": round(last, 2),
        "previous_close": round(previous_close, 2),
        "change": change,
        "pct": pct,
        "day_high": round(_safe_float(today.get("high"), last), 2),
        "day_low": round(_safe_float(today.get("low"), last), 2),
        "open": round(_safe_float(today.get("open"), last), 2),
        "volume": _safe_float(volume),
        "oi": _safe_float(oi),
        "bid": _safe_float(best_bid, None),
        "ask": _safe_float(best_ask, None),
        "fetchedAt": fetched_at,
        "receivedAt": _parse_upstox_timestamp(current_ts, time.time()),
        "source": "Upstox V3",
        "instrumentKey": instrument_key,
    }
