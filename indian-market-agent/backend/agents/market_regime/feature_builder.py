"""Build indicator-rich snapshots for the Market Regime Agent."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

try:
    from backend.agents.market_regime.indicators import (
        calculate_atr,
        calculate_day_high_low,
        calculate_ema,
        calculate_opening_range,
        calculate_rsi,
        calculate_vwap,
    )
    from backend.agents.market_regime.schemas import MarketCandle, MarketFeatureSnapshot, normalize_market_symbol
    from backend.core.settings import IST, MARKET_REGIME_TIMEFRAME_MINUTES
except ModuleNotFoundError:
    from agents.market_regime.indicators import calculate_atr, calculate_day_high_low, calculate_ema, calculate_opening_range, calculate_rsi, calculate_vwap
    from agents.market_regime.schemas import MarketCandle, MarketFeatureSnapshot, normalize_market_symbol
    from core.settings import IST, MARKET_REGIME_TIMEFRAME_MINUTES


def build_market_feature_snapshot(
    symbol: str,
    instrument_key: str,
    candles: list[MarketCandle],
    previous_day_candle: MarketCandle | None = None,
    india_vix: float | None = None,
    india_vix_change_pct: float | None = None,
    source_status: dict | None = None,
) -> MarketFeatureSnapshot:
    clean = normalize_market_symbol(symbol)
    ordered = sorted([candle for candle in candles if candle is not None], key=lambda candle: candle.timestamp)
    timeframe_minutes = int((source_status or {}).get("timeframe_minutes") or MARKET_REGIME_TIMEFRAME_MINUTES or 5)
    latest = ordered[-1] if ordered else None
    closes = [candle.close for candle in ordered]

    vwap = calculate_vwap(ordered)
    ema_9 = calculate_ema(closes, 9)
    ema_21 = calculate_ema(closes, 21)
    rsi_14 = calculate_rsi(closes, 14)
    atr_14 = calculate_atr(ordered, 14)
    latest_close = latest.close if latest else None
    atr_pct = (atr_14 / latest_close * 100) if atr_14 is not None and latest_close else None
    opening_range_high, opening_range_low = calculate_opening_range(ordered, minutes=15)
    day_high, day_low = calculate_day_high_low(ordered)
    source = str((source_status or {}).get("provider") or "upstox")
    timestamp = latest.timestamp if latest else datetime.now(IST)

    data_quality = _build_data_quality(
        ordered,
        timeframe_minutes,
        vwap=vwap,
        ema_9=ema_9,
        ema_21=ema_21,
        rsi_14=rsi_14,
        atr_14=atr_14,
        opening_range_high=opening_range_high,
        opening_range_low=opening_range_low,
        latest_close=latest_close,
    )

    return MarketFeatureSnapshot(
        symbol=clean,
        instrument_key=instrument_key,
        timestamp=timestamp,
        timeframe_minutes=timeframe_minutes,
        candles=ordered,
        latest_close=latest_close,
        vwap=vwap,
        ema_9=ema_9,
        ema_21=ema_21,
        rsi_14=rsi_14,
        atr_14=atr_14,
        atr_pct=atr_pct,
        opening_range_high=opening_range_high,
        opening_range_low=opening_range_low,
        previous_day_high=previous_day_candle.high if previous_day_candle else None,
        previous_day_low=previous_day_candle.low if previous_day_candle else None,
        previous_day_close=previous_day_candle.close if previous_day_candle else None,
        day_high=day_high,
        day_low=day_low,
        india_vix=india_vix,
        india_vix_change_pct=india_vix_change_pct,
        data_quality=data_quality,
        source=source,
        source_status=source_status or {},
    )


def _build_data_quality(
    candles: list[MarketCandle],
    timeframe_minutes: int,
    *,
    vwap: float | None,
    ema_9: float | None,
    ema_21: float | None,
    rsi_14: float | None,
    atr_14: float | None,
    opening_range_high: float | None,
    opening_range_low: float | None,
    latest_close: float | None,
) -> dict:
    candle_count = len(candles)
    volume_available = any((candle.volume or 0) > 0 for candle in candles)
    missing_fields = []
    if latest_close is None:
        missing_fields.append("latest_close")
    if vwap is None:
        missing_fields.append("vwap")
    if ema_9 is None:
        missing_fields.append("ema_9")
    if ema_21 is None:
        missing_fields.append("ema_21")
    if rsi_14 is None:
        missing_fields.append("rsi_14")
    if atr_14 is None:
        missing_fields.append("atr_14")
    if opening_range_high is None or opening_range_low is None:
        missing_fields.append("opening_range")

    last_timestamp = candles[-1].timestamp if candles else None
    stale_cutoff_seconds = max(360, (timeframe_minutes or 5) * 60 + 90)
    is_stale = _is_stale(last_timestamp, stale_cutoff_seconds)

    return {
        "is_stale": is_stale,
        "candle_count": candle_count,
        "missing_fields": missing_fields,
        "enough_data_for_rsi": candle_count >= 15,
        "enough_data_for_atr": candle_count >= 15,
        "volume_available": volume_available,
        "stale_cutoff_seconds": stale_cutoff_seconds,
        "first_candle_at": candles[0].timestamp.isoformat() if candles else None,
        "last_candle_at": last_timestamp.isoformat() if last_timestamp else None,
    }


def _is_stale(timestamp: datetime | None, stale_cutoff_seconds: int) -> bool:
    if timestamp is None:
        return True
    ts = timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    now = datetime.now(ts.tzinfo or timezone.utc)
    if ts.date() != now.date():
        return True
    return (now - ts) > timedelta(seconds=stale_cutoff_seconds)
