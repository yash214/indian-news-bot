"""Small dependency-free indicator helpers for market-regime features."""

from __future__ import annotations

from datetime import timedelta

try:
    from backend.agents.market_regime.schemas import MarketCandle
except ModuleNotFoundError:
    from agents.market_regime.schemas import MarketCandle


def calculate_ema(values: list[float], period: int) -> float | None:
    clean = [_safe_float(value) for value in values]
    clean = [value for value in clean if value is not None]
    if period <= 0 or len(clean) < period:
        return None
    ema = sum(clean[:period]) / period
    multiplier = 2 / (period + 1)
    for value in clean[period:]:
        ema = (value - ema) * multiplier + ema
    return float(ema)


def calculate_rsi(values: list[float], period: int = 14) -> float | None:
    clean = [_safe_float(value) for value in values]
    clean = [value for value in clean if value is not None]
    if period <= 0 or len(clean) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, period + 1):
        change = clean[idx] - clean[idx - 1]
        gains.append(max(change, 0.0))
        losses.append(abs(min(change, 0.0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    for idx in range(period + 1, len(clean)):
        change = clean[idx] - clean[idx - 1]
        gain = max(change, 0.0)
        loss = abs(min(change, 0.0))
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def calculate_atr(candles: list[MarketCandle], period: int = 14) -> float | None:
    if period <= 0 or len(candles) <= period:
        return None
    true_ranges: list[float] = []
    ordered = _ordered_candles(candles)
    for idx in range(1, len(ordered)):
        current = ordered[idx]
        previous = ordered[idx - 1]
        true_ranges.append(max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        ))
    if len(true_ranges) < period:
        return None
    atr = sum(true_ranges[:period]) / period
    for true_range in true_ranges[period:]:
        atr = ((atr * (period - 1)) + true_range) / period
    return float(atr)


def calculate_vwap(candles: list[MarketCandle]) -> float | None:
    total_value = 0.0
    total_volume = 0.0
    for candle in candles:
        volume = _safe_float(candle.volume)
        if volume is None or volume <= 0:
            continue
        typical_price = (candle.high + candle.low + candle.close) / 3
        total_value += typical_price * volume
        total_volume += volume
    if total_volume <= 0:
        return None
    return float(total_value / total_volume)


def calculate_opening_range(candles: list[MarketCandle], minutes: int = 15) -> tuple[float | None, float | None]:
    ordered = _ordered_candles(candles)
    if not ordered or minutes <= 0:
        return None, None
    start = ordered[0].timestamp
    cutoff = start + timedelta(minutes=minutes)
    opening = [candle for candle in ordered if candle.timestamp < cutoff]
    if not opening:
        opening = ordered[:1]
    return max(candle.high for candle in opening), min(candle.low for candle in opening)


def calculate_day_high_low(candles: list[MarketCandle]) -> tuple[float | None, float | None]:
    if not candles:
        return None, None
    return max(candle.high for candle in candles), min(candle.low for candle in candles)


def detect_higher_highs_lows(candles: list[MarketCandle], lookback: int = 5) -> bool:
    recent = _ordered_candles(candles)[-lookback:]
    if lookback <= 1 or len(recent) < lookback:
        return False
    high_steps = sum(1 for prev, curr in zip(recent, recent[1:]) if curr.high > prev.high)
    low_steps = sum(1 for prev, curr in zip(recent, recent[1:]) if curr.low > prev.low)
    required = max(2, lookback - 2)
    return high_steps >= required and low_steps >= required and recent[-1].high > recent[0].high and recent[-1].low > recent[0].low


def detect_lower_highs_lows(candles: list[MarketCandle], lookback: int = 5) -> bool:
    recent = _ordered_candles(candles)[-lookback:]
    if lookback <= 1 or len(recent) < lookback:
        return False
    high_steps = sum(1 for prev, curr in zip(recent, recent[1:]) if curr.high < prev.high)
    low_steps = sum(1 for prev, curr in zip(recent, recent[1:]) if curr.low < prev.low)
    required = max(2, lookback - 2)
    return high_steps >= required and low_steps >= required and recent[-1].high < recent[0].high and recent[-1].low < recent[0].low


def count_vwap_crosses(candles: list[MarketCandle], vwap: float | None, lookback: int = 10) -> int:
    if vwap is None or lookback <= 1:
        return 0
    recent = _ordered_candles(candles)[-lookback:]
    if len(recent) <= 1:
        return 0
    signs: list[int] = []
    for candle in recent:
        if candle.close > vwap:
            signs.append(1)
        elif candle.close < vwap:
            signs.append(-1)
        else:
            signs.append(0)
    crosses = 0
    previous = 0
    for sign in signs:
        if sign == 0:
            continue
        if previous and sign != previous:
            crosses += 1
        previous = sign
    return crosses


def _ordered_candles(candles: list[MarketCandle]) -> list[MarketCandle]:
    return sorted([candle for candle in candles if candle is not None], key=lambda candle: candle.timestamp)


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
