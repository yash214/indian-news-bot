"""Read-only Upstox options provider for the F&O Structure Agent."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

try:
    from backend.agents.fo_structure.schemas import OptionChainSnapshot, OptionStrike, normalize_fo_symbol
except ModuleNotFoundError:
    from agents.fo_structure.schemas import OptionChainSnapshot, OptionStrike, normalize_fo_symbol


UPSTOX_API_BASE = "https://api.upstox.com"
OPTION_CONTRACTS_ENDPOINT = "/v2/option/contract"
OPTION_CHAIN_ENDPOINT = "/v2/option/chain"
OPTION_GREEKS_ENDPOINT = "/v3/market-quote/option-greek"
INSTRUMENT_SEARCH_ENDPOINT = "/v2/instruments/search"


class UpstoxOptionsProvider:
    """Safe read-only wrapper around Upstox market-data options APIs."""

    def __init__(
        self,
        token: str | None = None,
        enabled: bool = False,
        timeout_seconds: int = 8,
        cache_ttl_seconds: int = 300,
        session: Any | None = None,
    ):
        self.token = (token if token is not None else os.environ.get("UPSTOX_ANALYTICS_TOKEN", "")).strip()
        self.enabled = bool(enabled)
        self.timeout_seconds = int(timeout_seconds or 8)
        self.cache_ttl_seconds = int(cache_ttl_seconds or 300)
        self.session = session or requests.Session()
        self.last_success_at: datetime | None = None
        self.last_error = ""
        self._cache: dict[str, tuple[float, dict]] = {}

    def is_configured(self) -> bool:
        return bool(self.enabled and self.token)

    def get_option_contracts(self, underlying_key: str, expiry_date: str | None = None) -> dict | None:
        params = {"instrument_key": underlying_key}
        if expiry_date:
            params["expiry_date"] = expiry_date
        return self._get_json(OPTION_CONTRACTS_ENDPOINT, params=params, cache_key=f"contracts:{underlying_key}:{expiry_date or ''}")

    def get_put_call_option_chain(self, underlying_key: str, expiry_date: str) -> dict | None:
        return self._get_json(
            OPTION_CHAIN_ENDPOINT,
            params={"instrument_key": underlying_key, "expiry_date": expiry_date},
            cache_key=f"chain:{underlying_key}:{expiry_date}",
        )

    def get_option_greeks(self, instrument_keys: list[str]) -> dict | None:
        keys = [str(key).strip() for key in instrument_keys if str(key).strip()]
        if not keys:
            return None
        return self._get_json(
            OPTION_GREEKS_ENDPOINT,
            params={"instrument_key": ",".join(keys[:50])},
            cache_key=f"greeks:{','.join(keys[:50])}",
        )

    def discover_underlying_key(self, symbol: str) -> str | None:
        clean = normalize_fo_symbol(symbol)
        if clean == "NIFTY":
            return "NSE_INDEX|Nifty 50"
        if clean == "SENSEX":
            rows = self._get_json(
                INSTRUMENT_SEARCH_ENDPOINT,
                params={"query": "SENSEX", "exchanges": "BSE", "segments": "INDEX", "page_number": "1", "records": "10"},
                cache_key="search:SENSEX:BSE:INDEX",
            )
            data = rows.get("data") if isinstance(rows, dict) else []
            for row in data if isinstance(data, list) else []:
                key = str(row.get("instrument_key") or row.get("instrument_token") or "").strip()
                symbol_text = str(row.get("trading_symbol") or row.get("name") or row.get("symbol") or "").upper()
                if key and "SENSEX" in symbol_text:
                    return key
            return "BSE_INDEX|SENSEX"
        return None

    def normalize_option_chain(self, raw: dict, symbol: str, expiry: str) -> OptionChainSnapshot | None:
        try:
            rows = raw.get("data") if isinstance(raw, dict) else raw
            if isinstance(rows, dict):
                rows = rows.get("option_chain") or rows.get("records") or rows.get("data")
            if not isinstance(rows, list) or not rows:
                self.last_error = "Upstox option chain response had no rows."
                return None
            strikes: list[OptionStrike] = []
            spot = None
            lot_size = None
            underlying_key = ""
            for row in rows:
                if not isinstance(row, dict):
                    continue
                strike_price = _safe_float(row.get("strike_price"))
                if strike_price is None:
                    continue
                spot = _safe_float(row.get("underlying_spot_price"), spot)
                underlying_key = str(row.get("underlying_key") or row.get("instrument_key") or underlying_key or "")
                lot_size = _safe_int(row.get("lot_size") or row.get("minimum_lot"), lot_size)
                call_options = row.get("call_options") or {}
                put_options = row.get("put_options") or {}
                call_md = call_options.get("market_data") or {}
                put_md = put_options.get("market_data") or {}
                call_greeks = call_options.get("option_greeks") or {}
                put_greeks = put_options.get("option_greeks") or {}
                call_oi = _safe_int(call_md.get("oi"))
                call_prev_oi = _safe_int(call_md.get("prev_oi"))
                put_oi = _safe_int(put_md.get("oi"))
                put_prev_oi = _safe_int(put_md.get("prev_oi"))
                strikes.append(OptionStrike(
                    strike=float(strike_price),
                    call_ltp=_safe_float(call_md.get("ltp")),
                    call_oi=call_oi,
                    call_prev_oi=call_prev_oi,
                    call_change_oi=_change_oi(call_oi, call_prev_oi),
                    call_volume=_safe_int(call_md.get("volume")),
                    call_iv=_safe_float(call_greeks.get("iv")),
                    call_delta=_safe_float(call_greeks.get("delta")),
                    call_gamma=_safe_float(call_greeks.get("gamma")),
                    call_theta=_safe_float(call_greeks.get("theta")),
                    call_vega=_safe_float(call_greeks.get("vega")),
                    call_bid_price=_safe_float(call_md.get("bid_price")),
                    call_ask_price=_safe_float(call_md.get("ask_price")),
                    call_bid_qty=_safe_int(call_md.get("bid_qty")),
                    call_ask_qty=_safe_int(call_md.get("ask_qty")),
                    put_ltp=_safe_float(put_md.get("ltp")),
                    put_oi=put_oi,
                    put_prev_oi=put_prev_oi,
                    put_change_oi=_change_oi(put_oi, put_prev_oi),
                    put_volume=_safe_int(put_md.get("volume")),
                    put_iv=_safe_float(put_greeks.get("iv")),
                    put_delta=_safe_float(put_greeks.get("delta")),
                    put_gamma=_safe_float(put_greeks.get("gamma")),
                    put_theta=_safe_float(put_greeks.get("theta")),
                    put_vega=_safe_float(put_greeks.get("vega")),
                    put_bid_price=_safe_float(put_md.get("bid_price")),
                    put_ask_price=_safe_float(put_md.get("ask_price")),
                    put_bid_qty=_safe_int(put_md.get("bid_qty")),
                    put_ask_qty=_safe_int(put_md.get("ask_qty")),
                ))
            if not strikes:
                self.last_error = "Upstox option chain response had no usable strikes."
                return None
            strikes.sort(key=lambda item: item.strike)
            return OptionChainSnapshot(
                symbol=normalize_fo_symbol(symbol),
                underlying_key=underlying_key,
                spot=float(spot or 0.0),
                expiry=expiry,
                timestamp=datetime.now(timezone.utc),
                lot_size=lot_size,
                strikes=strikes,
                source="upstox",
                source_status=self.source_status(),
            )
        except Exception as exc:
            self.last_error = f"Upstox option-chain normalize error: {str(exc)[:240]}"
            return None

    def source_status(self) -> dict:
        return {
            "provider": "upstox_options",
            "enabled": self.enabled,
            "configured": self.is_configured(),
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_error": self.last_error,
            "using_fallback": False,
            "stale": False,
        }

    def _get_json(self, endpoint: str, *, params: dict, cache_key: str | None = None) -> dict | None:
        if not self.enabled:
            self.last_error = "Upstox options provider is disabled."
            return None
        if not self.token:
            self.last_error = "UPSTOX_ANALYTICS_TOKEN is not configured."
            return None
        if cache_key:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached
        url = f"{UPSTOX_API_BASE}{endpoint}"
        try:
            response = self.session.get(url, params=params, headers=self._headers(), timeout=self.timeout_seconds)
            status_code = int(getattr(response, "status_code", 200) or 200)
            if status_code >= 400:
                self.last_error = f"Upstox options HTTP {status_code} for {endpoint}."
                return None
            payload = response.json()
            if isinstance(payload, dict) and payload.get("status") not in (None, "success"):
                self.last_error = f"Upstox options API returned status {payload.get('status')}."
                return None
            self.last_success_at = datetime.now(timezone.utc)
            self.last_error = ""
            return self._cache_set(cache_key, payload) if cache_key else payload
        except requests.RequestException as exc:
            self.last_error = f"Upstox options request error: {str(exc)[:240]}"
            return None
        except ValueError as exc:
            self.last_error = f"Upstox options JSON parse error: {str(exc)[:240]}"
            return None
        except Exception as exc:
            self.last_error = f"Upstox options error: {str(exc)[:240]}"
            return None

    def _headers(self) -> dict:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }

    def _cache_get(self, key: str) -> dict | None:
        cached = self._cache.get(key)
        if not cached:
            return None
        stored_at, value = cached
        if (time.time() - stored_at) > self.cache_ttl_seconds:
            return None
        return value

    def _cache_set(self, key: str | None, value: dict) -> dict:
        if key:
            self._cache[key] = (time.time(), value)
        return value


def _safe_float(value, default=None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _safe_int(value, default=None) -> int | None:
    number = _safe_float(value)
    if number is None:
        return default
    return int(round(number))


def _change_oi(oi: int | None, prev_oi: int | None) -> int | None:
    if oi is None or prev_oi is None:
        return None
    return int(oi - prev_oi)
