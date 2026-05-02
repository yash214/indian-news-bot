"""Reusable numeric helpers and technical-indicator calculations."""

from __future__ import annotations

import math
from statistics import mean, pstdev


def safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
        return float(value)
    except Exception:
        return default


def round_or_none(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    return mean(values[-length:])


def pct_return(values: list[float], periods: int) -> float | None:
    if len(values) <= periods:
        return None
    prev = values[-periods - 1]
    if not prev:
        return None
    return (values[-1] - prev) / prev * 100


def realized_vol(values: list[float], periods: int = 20) -> float | None:
    if len(values) <= periods:
        return None
    rets = []
    window = values[-(periods + 1):]
    for prev, cur in zip(window, window[1:]):
        if prev:
            rets.append((cur - prev) / prev)
    if len(rets) < 2:
        return None
    return pstdev(rets) * math.sqrt(252) * 100


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains, losses = [], []
    for prev, cur in zip(values[-(period + 1):], values[-period:]):
        delta = cur - prev
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_gain = mean(gains)
    avg_loss = mean(losses)
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def trend_label(price: float, sma20_val: float | None, sma50_val: float | None, rsi_val: float | None) -> str:
    if not sma20_val or not sma50_val or rsi_val is None:
        return "Developing"
    if price > sma20_val > sma50_val and rsi_val >= 55:
        return "Uptrend"
    if price < sma20_val < sma50_val and rsi_val <= 45:
        return "Downtrend"
    if price > sma20_val and rsi_val >= 50:
        return "Accumulation"
    if price < sma20_val and rsi_val <= 50:
        return "Distribution"
    return "Range"


def setup_label(
    price: float,
    high20: float | None,
    low20: float | None,
    sma20_val: float | None,
    rsi_val: float | None,
    ret5: float | None,
) -> str:
    if high20 and price >= high20 * 0.995 and (rsi_val or 0) >= 58:
        return "Breakout watch"
    if low20 and price <= low20 * 1.01 and (rsi_val or 100) <= 42:
        return "Breakdown risk"
    if sma20_val and price > sma20_val and (ret5 or 0) > 1:
        return "Momentum long"
    if sma20_val and price < sma20_val and (ret5 or 0) < -1:
        return "Trend weak"
    return "Wait for setup"


def relative_gap(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 2)


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def score_band(value: float | None, positive_strong: float, positive_mild: float, negative_mild: float, negative_strong: float) -> int:
    if value is None:
        return 0
    if value >= positive_strong:
        return 2
    if value >= positive_mild:
        return 1
    if value <= negative_strong:
        return -2
    if value <= negative_mild:
        return -1
    return 0


def intraday_return(values: list[float], periods: int = 3) -> float | None:
    if len(values) <= periods:
        return None
    prev = values[-periods - 1]
    if not prev:
        return None
    return round((values[-1] - prev) / prev * 100, 2)


def intraday_range_pct(values: list[float], periods: int = 12) -> float | None:
    if len(values) < 2:
        return None
    window = values[-periods:] if len(values) >= periods else values
    low = min(window)
    high = max(window)
    if not low:
        return None
    return round((high - low) / low * 100, 2)


def implied_move_points(price: float | None, vix_price: float | None) -> tuple[float | None, float | None]:
    if price is None or vix_price is None:
        return None, None
    move_pct = (vix_price / 100) / math.sqrt(252) * 100
    move_points = price * move_pct / 100
    return round(move_points, 2), round(move_pct, 2)


def bias_from_score(score: int) -> tuple[str, str]:
    if score >= 5:
        return "Strong Long Bias", "bull"
    if score >= 2:
        return "Long Bias", "bull"
    if score <= -5:
        return "Strong Short Bias", "bear"
    if score <= -2:
        return "Short Bias", "bear"
    return "Two-Way / Mean Reversion", "neutral"


def day_type_from_context(score: int, vix_price: float | None, short_momentum: float | None, intraday_range: float | None) -> tuple[str, str]:
    if abs(score) >= 5 and (vix_price or 0) < 16:
        return "Trend Day", "Directional conditions are aligned and volatility is still controlled."
    if abs(score) >= 4 and (vix_price or 0) >= 16:
        return "Volatile Trend", "Directional edge exists, but option premium and reversals can be sharper."
    if (vix_price or 0) >= 17 and (intraday_range or 0) >= 0.8:
        return "High Gamma Two-Way", "Expect wider intraday swings and faster invalidation if momentum stalls."
    if abs(short_momentum or 0) < 0.2 and (intraday_range or 0) < 0.5:
        return "Range / Fade Day", "Momentum is not broad enough yet, so breakout follow-through is less reliable."
    return "Rotation Day", "Leadership is shifting, so relative strength and confirmation matter more than raw direction."


def conviction_from_score(score: int, data_points: int) -> int:
    base = 42 + abs(score) * 8 + min(data_points, 8) * 2
    return int(clamp(base, 35, 88))


def format_level(value: float | None, prefix: str = "") -> str:
    if value is None:
        return "Unavailable"
    return f"{prefix}{value:,.2f}"
