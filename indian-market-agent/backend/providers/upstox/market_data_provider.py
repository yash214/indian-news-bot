"""Safe read-only Upstox candle/quote provider for the Market Regime Agent."""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote

import requests

try:
    from backend.agents.market_regime.schemas import INDEX_MARKET_CONFIG, MarketCandle, normalize_market_symbol
    from backend.core.settings import IST
except ModuleNotFoundError:
    from agents.market_regime.schemas import INDEX_MARKET_CONFIG, MarketCandle, normalize_market_symbol
    from core.settings import IST


UPSTOX_API_BASE = "https://api.upstox.com"
INTRADAY_CANDLE_V3_ENDPOINT_TEMPLATE = "/v3/historical-candle/intraday/{instrument_key}/{unit}/{interval}"
HISTORICAL_CANDLE_V3_ENDPOINT_TEMPLATE = "/v3/historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}"
OHLC_QUOTE_V3_ENDPOINT = "/v3/market-quote/ohlc"
LTP_QUOTE_V3_ENDPOINT = "/v3/market-quote/ltp"
MARKET_STATUS_ENDPOINT = "/v2/market/status"
INSTRUMENT_SEARCH_ENDPOINT = "/v2/instruments/search"


class UpstoxMarketDataProvider:
    """Safe read-only wrapper around Upstox market-data APIs.

    This provider intentionally exposes only market-data endpoints. It has no
    order, account, portfolio, or OAuth behavior.
    """

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

    def get_intraday_candles(self, instrument_key: str, unit: str = "minutes", interval: int = 5) -> dict | None:
        endpoint = INTRADAY_CANDLE_V3_ENDPOINT_TEMPLATE.format(
            instrument_key=_path_key(instrument_key),
            unit=unit,
            interval=int(interval or 5),
        )
        return self._get_json(endpoint, params={}, cache_key=f"intraday:{instrument_key}:{unit}:{interval}")

    def get_historical_candles(
        self,
        instrument_key: str,
        unit: str = "days",
        interval: int = 1,
        to_date: str | None = None,
        from_date: str | None = None,
    ) -> dict | None:
        to_date = to_date or date.today().isoformat()
        from_date = from_date or (date.fromisoformat(to_date) - timedelta(days=10)).isoformat()
        endpoint = HISTORICAL_CANDLE_V3_ENDPOINT_TEMPLATE.format(
            instrument_key=_path_key(instrument_key),
            unit=unit,
            interval=int(interval or 1),
            to_date=to_date,
            from_date=from_date,
        )
        return self._get_json(endpoint, params={}, cache_key=f"historical:{instrument_key}:{unit}:{interval}:{to_date}:{from_date}")

    def get_ohlc_quote(self, instrument_keys: list[str], interval: str = "1d") -> dict | None:
        keys = _clean_keys(instrument_keys)
        if not keys:
            return None
        return self._get_json(
            OHLC_QUOTE_V3_ENDPOINT,
            params={"instrument_key": ",".join(keys), "interval": interval},
            cache_key=f"ohlc:{','.join(keys)}:{interval}",
        )

    def get_ltp_quote(self, instrument_keys: list[str]) -> dict | None:
        keys = _clean_keys(instrument_keys)
        if not keys:
            return None
        return self._get_json(
            LTP_QUOTE_V3_ENDPOINT,
            params={"instrument_key": ",".join(keys)},
            cache_key=f"ltp:{','.join(keys)}",
        )

    def get_market_status(self, exchange: str | None = None) -> dict | None:
        params = {"exchange": exchange} if exchange else {}
        return self._get_json(MARKET_STATUS_ENDPOINT, params=params, cache_key=f"status:{exchange or 'all'}")

    def discover_instrument_key(self, symbol: str) -> str | None:
        clean = normalize_market_symbol(symbol)
        if clean == "NIFTY":
            return str(INDEX_MARKET_CONFIG["NIFTY"]["instrument_key"])
        if clean != "SENSEX":
            return None
        rows = self._get_json(
            INSTRUMENT_SEARCH_ENDPOINT,
            params={"query": "SENSEX", "exchanges": "BSE", "segments": "INDEX", "page_number": "1", "records": "10"},
            cache_key="search:SENSEX:BSE:INDEX",
        )
        data = rows.get("data") if isinstance(rows, dict) else []
        for row in data if isinstance(data, list) else []:
            if not isinstance(row, dict):
                continue
            key = str(row.get("instrument_key") or row.get("instrument_token") or "").strip()
            symbol_text = str(row.get("trading_symbol") or row.get("name") or row.get("symbol") or "").upper()
            if key and "SENSEX" in symbol_text:
                return key
        self.last_error = self.last_error or "SENSEX instrument key was not found in Upstox search."
        return None

    def normalize_candles(self, raw: dict, symbol: str, instrument_key: str, timeframe_minutes: int) -> list[MarketCandle]:
        try:
            rows = _extract_candle_rows(raw)
            candles: list[MarketCandle] = []
            for row in rows:
                if not isinstance(row, (list, tuple)) or len(row) < 5:
                    continue
                timestamp = _parse_timestamp(row[0])
                open_price = _safe_float(row[1])
                high = _safe_float(row[2])
                low = _safe_float(row[3])
                close = _safe_float(row[4])
                if timestamp is None or open_price is None or high is None or low is None or close is None:
                    continue
                volume = _safe_float(row[5]) if len(row) > 5 else None
                open_interest = _safe_float(row[6]) if len(row) > 6 else None
                candles.append(MarketCandle(
                    timestamp=timestamp,
                    open=float(open_price),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    volume=volume,
                    open_interest=open_interest,
                ))
            candles.sort(key=lambda candle: candle.timestamp)
            if not candles:
                self.last_error = f"Upstox candle response had no usable candles for {normalize_market_symbol(symbol)}."
            return candles
        except Exception as exc:
            self.last_error = f"Upstox candle normalize error: {str(exc)[:240]}"
            return []

    def source_status(self) -> dict:
        return {
            "provider": "upstox_market_data",
            "enabled": self.enabled,
            "configured": self.is_configured(),
            "read_only": True,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_error": self.last_error,
            "using_fallback": False,
            "stale": False,
            "cache_ttl_seconds": self.cache_ttl_seconds,
        }

    def _get_json(self, endpoint: str, *, params: dict, cache_key: str | None = None) -> dict | None:
        if not self.enabled:
            self.last_error = "Upstox market-data provider is disabled."
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
                self.last_error = f"Upstox market-data HTTP {status_code} for {endpoint}."
                return None
            payload = response.json()
            if isinstance(payload, dict) and payload.get("status") not in (None, "success"):
                self.last_error = f"Upstox market-data API returned status {payload.get('status')}."
                return None
            self.last_success_at = datetime.now(timezone.utc)
            self.last_error = ""
            return self._cache_set(cache_key, payload) if cache_key else payload
        except requests.RequestException as exc:
            self.last_error = f"Upstox market-data request error: {str(exc)[:240]}"
            return None
        except ValueError as exc:
            self.last_error = f"Upstox market-data JSON parse error: {str(exc)[:240]}"
            return None
        except Exception as exc:
            self.last_error = f"Upstox market-data error: {str(exc)[:240]}"
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


def _extract_candle_rows(raw: dict | list) -> list:
    rows = raw
    if isinstance(raw, dict):
        rows = raw.get("data", raw)
    if isinstance(rows, dict):
        rows = rows.get("candles") or rows.get("data") or rows.get("records") or []
    return rows if isinstance(rows, list) else []


def _parse_timestamp(value) -> datetime | None:
    try:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=IST)
        if isinstance(value, (int, float)):
            number = float(value)
            return datetime.fromtimestamp(number / 1000 if number > 10_000_000_000 else number, tz=timezone.utc).astimezone(IST)
        text = str(value or "").strip()
        if not text:
            return None
        if text.isdigit():
            number = float(text)
            return datetime.fromtimestamp(number / 1000 if number > 10_000_000_000 else number, tz=timezone.utc).astimezone(IST)
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=IST)
    except Exception:
        return None


def _path_key(instrument_key: str) -> str:
    return quote(str(instrument_key or "").strip(), safe="")


def _clean_keys(instrument_keys: list[str]) -> list[str]:
    return [str(key).strip() for key in instrument_keys if str(key).strip()]


def _safe_float(value, default=None) -> float | None:
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default
