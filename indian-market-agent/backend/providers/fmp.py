"""Safe optional Financial Modeling Prep provider for macro context."""

from __future__ import annotations

import os
import time
from datetime import datetime

import requests

try:
    from backend.core.settings import FMP_CACHE_TTL_SECONDS, FMP_ENABLED, FMP_TIMEOUT_SECONDS
except ModuleNotFoundError:
    from core.settings import FMP_CACHE_TTL_SECONDS, FMP_ENABLED, FMP_TIMEOUT_SECONDS


class FMPProvider:
    BASE_URL = "https://financialmodelingprep.com"
    STABLE_QUOTE_PATH = "/stable/quote"
    ECONOMIC_CALENDAR_PATH = "/stable/economic-calendar"
    ECONOMIC_INDICATORS_PATH = "/stable/economic-indicators"

    FOREX_SYMBOLS = {
        "usd_inr": ["USDINR", "USDINR=X"],
    }
    COMMODITY_SYMBOLS = {
        # FMP symbol coverage can vary by plan, so these fallbacks are intentionally broad.
        "gold": ["GCUSD", "XAUUSD", "GC=F"],
        "wti": ["CLUSD", "CL=F", "USOIL"],
        "brent": ["BZUSD", "BZ=F", "BRENT"],
    }
    INDEX_SYMBOLS = {
        "sp500": ["^GSPC", "SPY"],
        "nasdaq": ["^IXIC", "QQQ"],
        "dow": ["^DJI", "DIA"],
    }
    INDICATOR_NAMES = ["GDP", "inflationRate", "unemploymentRate", "federalFunds"]

    def __init__(self, api_key: str | None = None, enabled: bool = False, timeout_seconds: int = 8):
        self.api_key = (api_key if api_key is not None else os.environ.get("FMP_API_KEY", "")).strip() or None
        self.enabled = bool(enabled or _env_bool("FMP_ENABLED", FMP_ENABLED))
        self.timeout_seconds = int(timeout_seconds or os.environ.get("FMP_TIMEOUT_SECONDS") or FMP_TIMEOUT_SECONDS)
        self.cache_ttl_seconds = int(os.environ.get("FMP_CACHE_TTL_SECONDS") or FMP_CACHE_TTL_SECONDS)
        self.session = requests.Session()
        self._cache: dict[str, tuple[float, object]] = {}

    def is_configured(self) -> bool:
        return self.enabled and bool(self.api_key)

    def get_usd_inr(self) -> dict | None:
        if not self.is_configured():
            return None
        cached = self._cache_get("usd_inr", ttl_seconds=min(self.cache_ttl_seconds, 3600))
        if cached is not None:
            return cached
        quote = self._quote_from_symbols(self.FOREX_SYMBOLS["usd_inr"], name="usd_inr")
        return self._cache_set("usd_inr", quote) if quote else None

    def get_gold(self) -> dict | None:
        if not self.is_configured():
            return None
        cached = self._cache_get("gold", ttl_seconds=min(self.cache_ttl_seconds, 3600))
        if cached is not None:
            return cached
        quote = self._quote_from_symbols(self.COMMODITY_SYMBOLS["gold"], name="gold")
        return self._cache_set("gold", quote) if quote else None

    def get_crude(self) -> dict | None:
        if not self.is_configured():
            return None
        cached = self._cache_get("crude", ttl_seconds=min(self.cache_ttl_seconds, 3600))
        if cached is not None:
            return cached
        wti = self._quote_from_symbols(self.COMMODITY_SYMBOLS["wti"], name="WTI crude")
        brent = self._quote_from_symbols(self.COMMODITY_SYMBOLS["brent"], name="Brent crude")
        primary = wti or brent
        if not primary:
            return None
        payload = {
            "name": "crude",
            "symbol": primary.get("symbol"),
            "value": primary.get("value"),
            "change_pct_1d": primary.get("change_pct_1d"),
            "change_pct_5d": primary.get("change_pct_5d"),
            "components": {"wti": wti, "brent": brent},
            "source": "fmp",
            "raw": {"wti": wti, "brent": brent},
        }
        return self._cache_set("crude", payload)

    def get_us_indices(self) -> dict:
        if not self.is_configured():
            return {}
        cached = self._cache_get("us_indices", ttl_seconds=min(self.cache_ttl_seconds, 3600))
        if cached is not None:
            return cached
        components = {}
        changes = []
        for label, symbols in self.INDEX_SYMBOLS.items():
            quote = self._quote_from_symbols(symbols, name=label)
            if quote:
                components[label] = quote
                if quote.get("change_pct_1d") is not None:
                    changes.append(float(quote["change_pct_1d"]))
        if not components:
            return {}
        average = round(sum(changes) / len(changes), 2) if changes else None
        payload = {
            "name": "global_cues",
            "symbol": "US_INDEX_BASKET",
            "value": average,
            "average_change_pct_1d": average,
            "change_pct_1d": average,
            "components": components,
            "source": "fmp",
            "raw": components,
        }
        return self._cache_set("us_indices", payload)

    def get_economic_calendar(self, from_date: str, to_date: str) -> list[dict]:
        if not self.is_configured():
            return []
        cache_key = f"economic_calendar:{from_date}:{to_date}"
        cached = self._cache_get(cache_key, ttl_seconds=max(self.cache_ttl_seconds, 10800))
        if cached is not None:
            return cached
        payload = self._request_json(
            self.ECONOMIC_CALENDAR_PATH,
            params={"from": from_date, "to": to_date},
        )
        rows = payload if isinstance(payload, list) else []
        normalized = []
        for row in rows:
            event = self._normalize_economic_event(row)
            if event:
                normalized.append(event)
        return self._cache_set(cache_key, normalized)

    def get_basic_macro_snapshot(self) -> dict:
        if not self.is_configured():
            return {}
        cached = self._cache_get("basic_macro_snapshot", ttl_seconds=86400)
        if cached is not None:
            return cached
        indicators = {}
        for name in self.INDICATOR_NAMES:
            payload = self._request_json(self.ECONOMIC_INDICATORS_PATH, params={"name": name})
            rows = payload if isinstance(payload, list) else []
            if not rows:
                continue
            latest = rows[0] if isinstance(rows[0], dict) else None
            if not latest:
                continue
            indicators[name] = {
                "name": name,
                "date": latest.get("date"),
                "value": _safe_float(latest.get("value", latest.get("actual"))),
                "source": "fmp",
                "raw": latest,
            }
        result = {"source": "fmp", "indicators": indicators}
        return self._cache_set("basic_macro_snapshot", result)

    def _quote_from_symbols(self, symbols: list[str], *, name: str) -> dict | None:
        for symbol in symbols:
            payload = self._request_json(self.STABLE_QUOTE_PATH, params={"symbol": symbol})
            quote = self._normalize_quote_payload(payload, fallback_name=name, fallback_symbol=symbol)
            if quote is not None:
                return quote
        return None

    def _request_json(self, path: str, params: dict | None = None):
        if not self.is_configured():
            return None
        params = dict(params or {})
        params["apikey"] = self.api_key
        url = f"{self.BASE_URL}{path}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            if response.status_code in {401, 403, 404, 429}:
                return None
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError):
            return None
        return payload

    def _normalize_quote_payload(self, payload, *, fallback_name: str, fallback_symbol: str) -> dict | None:
        row = None
        if isinstance(payload, list) and payload:
            row = payload[0] if isinstance(payload[0], dict) else None
        elif isinstance(payload, dict):
            row = payload
        if not isinstance(row, dict):
            return None
        value = _safe_float(row.get("price", row.get("lastPrice", row.get("value"))))
        if value is None:
            return None
        change_pct = _safe_float(row.get("changesPercentage", row.get("changePercent", row.get("percentageChange"))))
        if change_pct is None:
            previous_close = _safe_float(row.get("previousClose", row.get("previous_close")))
            change = _safe_float(row.get("change"))
            if previous_close not in (None, 0.0) and change is not None:
                change_pct = round((change / previous_close) * 100, 2)
        return {
            "name": str(row.get("name") or fallback_name),
            "symbol": str(row.get("symbol") or fallback_symbol),
            "value": value,
            "change_pct_1d": change_pct,
            "change_pct_5d": None,
            "source": "fmp",
            "raw": row,
        }

    def _normalize_economic_event(self, row: dict) -> dict | None:
        if not isinstance(row, dict):
            return None
        event_name = str(row.get("event") or row.get("name") or "").strip()
        if not event_name:
            return None
        event_time = row.get("date") or row.get("eventTime") or row.get("time")
        parsed_time = None
        if isinstance(event_time, str) and event_time.strip():
            try:
                parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
            except ValueError:
                parsed_time = None
        return {
            "country": str(row.get("country") or row.get("countryCode") or ""),
            "event": event_name,
            "importance": str(row.get("importance") or row.get("impact") or row.get("impactLevel") or "low").lower(),
            "event_time": parsed_time.isoformat() if parsed_time else None,
            "actual": _safe_float(row.get("actual")),
            "forecast": _safe_float(row.get("forecast", row.get("consensus"))),
            "previous": _safe_float(row.get("previous")),
            "source": "fmp",
            "raw": row,
        }

    def _cache_get(self, key: str, *, ttl_seconds: int):
        cached = self._cache.get(key)
        if not cached:
            return None
        stored_at, value = cached
        if (time.time() - stored_at) > ttl_seconds:
            return None
        return value

    def _cache_set(self, key: str, value):
        self._cache[key] = (time.time(), value)
        return value


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return None


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in {"1", "true", "yes", "on"}
