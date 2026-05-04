"""Snapshot builder for Market Regime candle features."""

from __future__ import annotations

import math
from datetime import datetime, timedelta

try:
    from backend.agents.market_regime.feature_builder import build_market_feature_snapshot
    from backend.agents.market_regime.schemas import (
        INDEX_MARKET_CONFIG,
        MarketCandle,
        MarketFeatureSnapshot,
        is_supported_symbol,
        normalize_market_symbol,
    )
    from backend.core.settings import (
        IST,
        MARKET_REGIME_TIMEFRAME_MINUTES,
        MARKET_REGIME_TIMEZONE,
        UPSTOX_MARKET_DATA_CACHE_TTL_SECONDS,
        UPSTOX_MARKET_DATA_ENABLED,
        UPSTOX_MARKET_DATA_TIMEOUT_SECONDS,
    )
    from backend.providers.upstox.market_data_provider import UpstoxMarketDataProvider
except ModuleNotFoundError:
    from agents.market_regime.feature_builder import build_market_feature_snapshot
    from agents.market_regime.schemas import INDEX_MARKET_CONFIG, MarketCandle, MarketFeatureSnapshot, is_supported_symbol, normalize_market_symbol
    from core.settings import (
        IST,
        MARKET_REGIME_TIMEFRAME_MINUTES,
        MARKET_REGIME_TIMEZONE,
        UPSTOX_MARKET_DATA_CACHE_TTL_SECONDS,
        UPSTOX_MARKET_DATA_ENABLED,
        UPSTOX_MARKET_DATA_TIMEOUT_SECONDS,
    )
    from providers.upstox.market_data_provider import UpstoxMarketDataProvider


INDIA_VIX_INSTRUMENT_KEY = "NSE_INDEX|India VIX"


class MarketRegimeSnapshotBuilder:
    def __init__(self, provider: UpstoxMarketDataProvider | None = None):
        self.provider = provider or UpstoxMarketDataProvider(
            enabled=UPSTOX_MARKET_DATA_ENABLED,
            timeout_seconds=UPSTOX_MARKET_DATA_TIMEOUT_SECONDS,
            cache_ttl_seconds=UPSTOX_MARKET_DATA_CACHE_TTL_SECONDS,
        )

    def build(
        self,
        symbol: str = "NIFTY",
        timeframe_minutes: int = 5,
        use_mock: bool = False,
    ) -> MarketFeatureSnapshot | None:
        if use_mock:
            return build_mock_market_feature_snapshot(symbol=symbol)
        clean = normalize_market_symbol(symbol)
        if not is_supported_symbol(clean) or not self.provider.is_configured():
            return None

        config = INDEX_MARKET_CONFIG[clean]
        instrument_key = str(config["instrument_key"])
        timeframe = int(timeframe_minutes or config.get("default_timeframe_minutes") or MARKET_REGIME_TIMEFRAME_MINUTES or 5)
        raw_intraday = self.provider.get_intraday_candles(instrument_key, unit="minutes", interval=timeframe)
        if not raw_intraday and clean == "SENSEX":
            discovered = self.provider.discover_instrument_key("SENSEX")
            if discovered:
                instrument_key = discovered
                raw_intraday = self.provider.get_intraday_candles(instrument_key, unit="minutes", interval=timeframe)
        if not raw_intraday:
            return None

        candles = self.provider.normalize_candles(raw_intraday, clean, instrument_key, timeframe)
        if not candles:
            return None

        previous_day_candle = self._previous_day_candle(clean, instrument_key, timeframe)
        india_vix, india_vix_change_pct = self._india_vix()
        status = self.provider.source_status()
        status["timeframe_minutes"] = timeframe
        return build_market_feature_snapshot(
            clean,
            instrument_key,
            candles,
            previous_day_candle=previous_day_candle,
            india_vix=india_vix,
            india_vix_change_pct=india_vix_change_pct,
            source_status=status,
        )

    def _previous_day_candle(self, symbol: str, instrument_key: str, timeframe_minutes: int) -> MarketCandle | None:
        raw = self.provider.get_historical_candles(instrument_key, unit="days", interval=1)
        if not raw and symbol == "SENSEX":
            discovered = self.provider.discover_instrument_key("SENSEX")
            if discovered and discovered != instrument_key:
                raw = self.provider.get_historical_candles(discovered, unit="days", interval=1)
                instrument_key = discovered
        if not raw:
            return None
        candles = self.provider.normalize_candles(raw, symbol, instrument_key, timeframe_minutes)
        if not candles:
            return None
        today = datetime.now(IST).date()
        previous = [candle for candle in candles if candle.timestamp.date() < today]
        return (previous or candles)[-1]

    def _india_vix(self) -> tuple[float | None, float | None]:
        raw = self.provider.get_ohlc_quote([INDIA_VIX_INSTRUMENT_KEY], interval="1d")
        if not raw:
            return None, None
        return _extract_quote_value(raw, INDIA_VIX_INSTRUMENT_KEY)


def build_mock_market_feature_snapshot(symbol: str = "NIFTY", regime_hint: str | None = None) -> MarketFeatureSnapshot:
    clean = normalize_market_symbol(symbol)
    if not is_supported_symbol(clean):
        clean = "NIFTY"
    config = INDEX_MARKET_CONFIG[clean]
    hint = str(regime_hint or "bullish").strip().lower()
    base = 22450.0 if clean == "NIFTY" else 74250.0
    candles = _mock_candles(base=base, hint=hint)
    previous_day = MarketCandle(
        timestamp=candles[0].timestamp - timedelta(days=1),
        open=base - 70,
        high=base + 55,
        low=base - 145,
        close=base - 25,
        volume=1_000_000,
    )
    india_vix = 29.0 if hint == "high_vol" else 15.5
    india_vix_change_pct = 12.0 if hint == "high_vol" else 1.2
    return build_market_feature_snapshot(
        clean,
        str(config["instrument_key"]),
        candles,
        previous_day_candle=previous_day,
        india_vix=india_vix,
        india_vix_change_pct=india_vix_change_pct,
        source_status={
            "provider": "mock",
            "enabled": True,
            "configured": True,
            "last_error": "",
            "timezone": MARKET_REGIME_TIMEZONE,
            "timeframe_minutes": 5,
        },
    )


def _mock_candles(base: float, hint: str, count: int = 40) -> list[MarketCandle]:
    timeframe = 5
    start = datetime.now(IST) - timedelta(minutes=(count - 1) * timeframe)
    candles: list[MarketCandle] = []
    previous_close = base
    for idx in range(count):
        timestamp = start + timedelta(minutes=idx * timeframe)
        if hint == "bearish":
            close = base + 45 - idx * 5.8
            wiggle = 9 + (idx % 3)
        elif hint == "range":
            close = base + math.sin(idx / 2.4) * 8
            if idx < 3:
                close = base + (idx - 1) * 24
            wiggle = 9
        elif hint == "choppy":
            close = base + (18 if idx % 2 == 0 else -18) + math.sin(idx) * 5
            if idx < 3:
                close = base + (idx - 1) * 35
            wiggle = 34
        elif hint == "high_vol":
            close = base + (idx * 3.5) + (42 if idx % 2 == 0 else -38)
            wiggle = 58
        else:
            close = base - 45 + idx * 5.8
            wiggle = 9 + (idx % 3)
        open_price = previous_close
        high = max(open_price, close) + wiggle
        low = min(open_price, close) - wiggle
        volume = 80_000 + (idx * 1_200)
        candles.append(MarketCandle(
            timestamp=timestamp,
            open=round(open_price, 2),
            high=round(high, 2),
            low=round(low, 2),
            close=round(close, 2),
            volume=volume,
        ))
        previous_close = close
    return candles


def _extract_quote_value(raw: dict, instrument_key: str) -> tuple[float | None, float | None]:
    data = raw.get("data") if isinstance(raw, dict) else {}
    rows = data if isinstance(data, dict) else {}
    candidates = []
    for key in (instrument_key, instrument_key.replace("|", ":"), "NSE_INDEX:India VIX", "India VIX"):
        payload = rows.get(key) if isinstance(rows, dict) else None
        if isinstance(payload, dict):
            candidates.append(payload)
    if isinstance(rows, dict):
        candidates.extend(payload for payload in rows.values() if isinstance(payload, dict))
    for payload in candidates:
        ohlc = payload.get("ohlc") or {}
        value = _safe_float(payload.get("last_price") or payload.get("ltp") or ohlc.get("close") or payload.get("close"))
        previous_close = _safe_float(ohlc.get("close") or payload.get("prev_close") or payload.get("close_price"))
        if value is not None:
            change_pct = None
            if previous_close:
                change_pct = (value - previous_close) / previous_close * 100
            return value, change_pct
    return None, None


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
