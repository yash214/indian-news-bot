"""Upstox market-data parsing and instrument helpers."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime

try:
    from backend.market.catalog import (
        UPSTOX_DEFAULT_INSTRUMENT_KEYS,
        UPSTOX_OPTION_UNDERLYINGS,
        symbol_directory_entry,
    )
    from backend.market.math import round_or_none, safe_float
    from backend.core.settings import IST
except ModuleNotFoundError:
    from market.catalog import UPSTOX_DEFAULT_INSTRUMENT_KEYS, UPSTOX_OPTION_UNDERLYINGS, symbol_directory_entry
    from market.math import round_or_none, safe_float
    from core.settings import IST


def upstox_token_preview(token: str) -> str:
    token = (token or "").strip()
    if len(token) <= 8:
        return token
    return f"{token[:4]}...{token[-4:]}"


def parse_upstox_instrument_overrides(raw: str | None = None) -> dict[str, str]:
    raw = os.environ.get("UPSTOX_INSTRUMENT_KEYS", "") if raw is None else raw
    raw = (raw or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {
                re.sub(r"[^A-Z0-9&. -]", "", str(symbol).upper()).strip(): str(key).strip()
                for symbol, key in parsed.items()
                if str(symbol).strip() and str(key).strip()
            }
    except Exception:
        pass

    out = {}
    for piece in re.split(r"[;,]", raw):
        if "=" not in piece:
            continue
        symbol, key = piece.split("=", 1)
        symbol = re.sub(r"[^A-Z0-9&. -]", "", symbol.upper()).strip()
        key = key.strip()
        if symbol and key:
            out[symbol] = key
    return out


def upstox_instrument_key_for_symbol(symbol: str) -> str | None:
    clean = re.sub(r"[^A-Z0-9&.-]", "", (symbol or "").upper())
    overrides = parse_upstox_instrument_overrides()
    return overrides.get(clean) or UPSTOX_DEFAULT_INSTRUMENT_KEYS.get(clean)


def option_underlying_key(underlying: str) -> str | None:
    clean = re.sub(r"[^A-Z0-9 ]", "", (underlying or "").upper()).strip()
    overrides = parse_upstox_instrument_overrides()
    return overrides.get(clean) or UPSTOX_OPTION_UNDERLYINGS.get(clean)


def parse_upstox_timestamp(value, default_ts: float) -> float:
    if value is None or value == "":
        return default_ts
    try:
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


def upstox_quote_from_payload(label: str, payload: dict, received_at: float) -> dict | None:
    ohlc = payload.get("ohlc") or {}
    last = safe_float(payload.get("last_price", payload.get("ltp")))
    if not last:
        return None
    previous_close = safe_float(ohlc.get("close", payload.get("close_price")), last)
    net_change = payload.get("net_change")
    change = safe_float(net_change, last - previous_close)
    pct = round((change / previous_close * 100) if previous_close else 0, 2)
    fetched_at = parse_upstox_timestamp(
        payload.get("timestamp") or payload.get("last_trade_time"),
        received_at,
    )
    directory_entry = symbol_directory_entry(label)
    name = (
        payload.get("symbol")
        or payload.get("trading_symbol")
        or (directory_entry or {}).get("name")
        or label
    )
    depth = payload.get("depth") or {}
    best_bid = (depth.get("buy") or [{}])[0] if isinstance(depth.get("buy"), list) else {}
    best_ask = (depth.get("sell") or [{}])[0] if isinstance(depth.get("sell"), list) else {}
    return {
        "symbol": label,
        "name": name,
        "price": round(last, 2),
        "previous_close": round(previous_close, 2),
        "change": round(change, 2),
        "pct": pct,
        "day_high": round(safe_float(ohlc.get("high"), last), 2),
        "day_low": round(safe_float(ohlc.get("low"), last), 2),
        "open": round(safe_float(ohlc.get("open"), last), 2),
        "volume": safe_float(payload.get("volume")),
        "oi": safe_float(payload.get("oi")),
        "bid": safe_float(best_bid.get("price")),
        "ask": safe_float(best_ask.get("price")),
        "fetchedAt": fetched_at,
        "receivedAt": received_at,
        "source": "Upstox",
        "instrumentKey": payload.get("instrument_token") or payload.get("instrument_key"),
    }


def summarize_upstox_option_chain(rows: list[dict], underlying: str, expiry_date: str, max_rows: int = 80) -> dict:
    spot = None
    compact_rows = []
    total_call_oi = total_put_oi = 0.0
    total_call_prev_oi = total_put_prev_oi = 0.0
    max_call = {"strike": None, "oi": -1.0}
    max_put = {"strike": None, "oi": -1.0}

    for row in rows or []:
        strike = safe_float(row.get("strike_price"))
        spot = safe_float(row.get("underlying_spot_price"), spot or 0.0) or spot
        call_md = (row.get("call_options") or {}).get("market_data") or {}
        put_md = (row.get("put_options") or {}).get("market_data") or {}
        call_greeks = (row.get("call_options") or {}).get("option_greeks") or {}
        put_greeks = (row.get("put_options") or {}).get("option_greeks") or {}
        call_oi = safe_float(call_md.get("oi"))
        put_oi = safe_float(put_md.get("oi"))
        call_prev_oi = safe_float(call_md.get("prev_oi"))
        put_prev_oi = safe_float(put_md.get("prev_oi"))
        total_call_oi += call_oi
        total_put_oi += put_oi
        total_call_prev_oi += call_prev_oi
        total_put_prev_oi += put_prev_oi
        if call_oi > max_call["oi"]:
            max_call = {"strike": strike, "oi": call_oi}
        if put_oi > max_put["oi"]:
            max_put = {"strike": strike, "oi": put_oi}
        compact_rows.append({
            "strike": strike,
            "call": {
                "ltp": safe_float(call_md.get("ltp")),
                "oi": call_oi,
                "changeInOi": call_oi - call_prev_oi,
                "volume": safe_float(call_md.get("volume")),
                "bid": safe_float(call_md.get("bid_price")),
                "ask": safe_float(call_md.get("ask_price")),
                "iv": safe_float(call_greeks.get("iv")),
                "delta": safe_float(call_greeks.get("delta")),
            },
            "put": {
                "ltp": safe_float(put_md.get("ltp")),
                "oi": put_oi,
                "changeInOi": put_oi - put_prev_oi,
                "volume": safe_float(put_md.get("volume")),
                "bid": safe_float(put_md.get("bid_price")),
                "ask": safe_float(put_md.get("ask_price")),
                "iv": safe_float(put_greeks.get("iv")),
                "delta": safe_float(put_greeks.get("delta")),
            },
        })

    if spot:
        compact_rows.sort(key=lambda row: abs(row["strike"] - spot))
    else:
        compact_rows.sort(key=lambda row: row["strike"])
    limited_rows = sorted(compact_rows[:max_rows], key=lambda row: row["strike"])
    call_change = total_call_oi - total_call_prev_oi
    put_change = total_put_oi - total_put_prev_oi
    pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi else None
    return {
        "provider": "Upstox",
        "underlying": underlying,
        "expiry": expiry_date,
        "generatedAt": datetime.now(IST).isoformat(),
        "summary": {
            "spot": round_or_none(spot),
            "pcr": pcr,
            "totalCallOi": round(total_call_oi),
            "totalPutOi": round(total_put_oi),
            "callOiChange": round(call_change),
            "putOiChange": round(put_change),
            "maxCallOiStrike": max_call["strike"],
            "maxPutOiStrike": max_put["strike"],
            "flowBias": "Put writing support" if put_change > call_change else "Call writing pressure" if call_change > put_change else "Balanced",
        },
        "rows": limited_rows,
    }
