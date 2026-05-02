"""Safe India VIX wrapper for macro context."""

from __future__ import annotations

try:
    from backend.market.catalog import INDEX_SYMBOL_ALIASES
except ModuleNotFoundError:
    from market.catalog import INDEX_SYMBOL_ALIASES


class IndiaVixProvider:
    """Small wrapper around whichever VIX source the app already has available."""

    def __init__(self, fetcher=None):
        self.fetcher = fetcher
        self.instrument_key = INDEX_SYMBOL_ALIASES.get("INDIAVIX", {}).get("instrumentKey")

    def get_india_vix(self) -> dict | None:
        if not callable(self.fetcher):
            # TODO: add a direct Upstox/NSE fetch path once the shared quote provider is extracted.
            return None
        try:
            payload = self.fetcher()
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        value = _safe_float(payload.get("price", payload.get("last_price", payload.get("value"))))
        if value is None:
            return None
        return {
            "name": "india_vix",
            "symbol": "INDIAVIX",
            "value": value,
            "change_pct_1d": _safe_float(payload.get("pct", payload.get("change_pct_1d", payload.get("changePercent")))),
            "change_pct_5d": _safe_float(payload.get("change_pct_5d")),
            "source": payload.get("source") or "upstox",
            "stale": bool(payload.get("staleData") or payload.get("stale", False)),
            "raw": payload,
        }


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
