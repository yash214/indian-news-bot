"""Rule-based Market Regime scoring."""

from __future__ import annotations

try:
    from backend.agents.market_regime.indicators import (
        count_vwap_crosses,
        detect_higher_highs_lows,
        detect_lower_highs_lows,
    )
    from backend.agents.market_regime.regime_rules import (
        BIAS_NEUTRAL,
        FILTER_WAIT,
        REGIME_BREAKDOWN,
        REGIME_BREAKOUT_UP,
        REGIME_CHOPPY,
        REGIME_HIGH_VOLATILITY,
        REGIME_LOW_VOLATILITY,
        REGIME_RANGE_BOUND,
        REGIME_TRENDING_DOWN,
        REGIME_TRENDING_UP,
        REGIME_UNCLEAR,
        RULESET_VERSION,
        clamp_confidence,
        clamp_score,
        directional_bias_for_regime,
        trade_filter_for_regime,
    )
    from backend.agents.market_regime.schemas import MarketFeatureSnapshot
    from backend.agents.market_regime.volatility import score_volatility
except ModuleNotFoundError:
    from agents.market_regime.indicators import count_vwap_crosses, detect_higher_highs_lows, detect_lower_highs_lows
    from agents.market_regime.regime_rules import (
        BIAS_NEUTRAL,
        FILTER_WAIT,
        REGIME_BREAKDOWN,
        REGIME_BREAKOUT_UP,
        REGIME_CHOPPY,
        REGIME_HIGH_VOLATILITY,
        REGIME_LOW_VOLATILITY,
        REGIME_RANGE_BOUND,
        REGIME_TRENDING_DOWN,
        REGIME_TRENDING_UP,
        REGIME_UNCLEAR,
        RULESET_VERSION,
        clamp_confidence,
        clamp_score,
        directional_bias_for_regime,
        trade_filter_for_regime,
    )
    from agents.market_regime.schemas import MarketFeatureSnapshot
    from agents.market_regime.volatility import score_volatility


def score_market_regime(snapshot: MarketFeatureSnapshot) -> dict:
    reasons: list[str] = []
    warnings: list[str] = []
    data_quality = snapshot.data_quality or {}
    candle_count = int(data_quality.get("candle_count") or len(snapshot.candles or []))

    if not snapshot.latest_close or candle_count < 5:
        warnings.append("Insufficient candle data for market-regime classification.")
        return _result(
            primary_regime=REGIME_UNCLEAR,
            secondary_regime=None,
            confidence=0.35,
            bull_score=0,
            bear_score=0,
            range_score=0,
            chop_score=0,
            volatility_score=0,
            reasons=reasons,
            warnings=warnings,
            snapshot=snapshot,
        )

    bull_score = 0
    bear_score = 0
    range_score = 0
    chop_score = 0
    latest_close = float(snapshot.latest_close)

    if snapshot.vwap is not None:
        if latest_close > snapshot.vwap:
            bull_score += 20
            reasons.append("Latest close is above VWAP.")
        elif latest_close < snapshot.vwap:
            bear_score += 20
            reasons.append("Latest close is below VWAP.")
        if _near_pct(latest_close, snapshot.vwap, 0.10):
            range_score += 20
            reasons.append("Price is close to VWAP.")
    else:
        warnings.append("VWAP unavailable because volume is missing or zero.")

    if snapshot.ema_9 is not None:
        if latest_close > snapshot.ema_9:
            bull_score += 15
            reasons.append("Latest close is above EMA 9.")
        elif latest_close < snapshot.ema_9:
            bear_score += 15
            reasons.append("Latest close is below EMA 9.")

    if snapshot.ema_9 is not None and snapshot.ema_21 is not None:
        if snapshot.ema_9 > snapshot.ema_21:
            bull_score += 15
            reasons.append("EMA 9 is above EMA 21.")
        elif snapshot.ema_9 < snapshot.ema_21:
            bear_score += 15
            reasons.append("EMA 9 is below EMA 21.")
        if _near_pct(snapshot.ema_9, snapshot.ema_21, 0.10):
            range_score += 15
            chop_score += 10
            reasons.append("EMA 9 and EMA 21 are close together.")

    if snapshot.rsi_14 is not None:
        if snapshot.rsi_14 > 55:
            bull_score += 10
            reasons.append(f"RSI 14 is bullish at {round(snapshot.rsi_14, 2)}.")
        elif snapshot.rsi_14 < 45:
            bear_score += 10
            reasons.append(f"RSI 14 is bearish at {round(snapshot.rsi_14, 2)}.")
        if 45 <= snapshot.rsi_14 <= 55:
            range_score += 15
            chop_score += 15
            reasons.append("RSI 14 is neutral.")

    if snapshot.opening_range_high is not None and snapshot.opening_range_low is not None:
        if latest_close > snapshot.opening_range_high:
            bull_score += 15
            reasons.append("Price broke above the opening range.")
        elif latest_close < snapshot.opening_range_low:
            bear_score += 15
            reasons.append("Price broke below the opening range.")
        else:
            range_score += 15
            reasons.append("Latest close is inside the opening range.")

    if detect_higher_highs_lows(snapshot.candles, lookback=5):
        bull_score += 10
        reasons.append("Recent candles show higher highs and higher lows.")
    if detect_lower_highs_lows(snapshot.candles, lookback=5):
        bear_score += 10
        reasons.append("Recent candles show lower highs and lower lows.")

    if snapshot.atr_pct is not None and snapshot.atr_pct <= 0.08:
        range_score += 10
        reasons.append("ATR percent is low.")

    vwap_crosses = count_vwap_crosses(snapshot.candles, snapshot.vwap, lookback=10)
    if vwap_crosses >= 3:
        chop_score += 20
        reasons.append(f"Price crossed VWAP {vwap_crosses} times in the last 10 candles.")
    if _small_net_large_ranges(snapshot):
        chop_score += 15
        reasons.append("Recent movement is net-flat despite large candle ranges.")
    if _failed_opening_range_break(snapshot):
        chop_score += 10
        reasons.append("Opening-range break attempt failed back into range.")

    volatility_score, volatility_reasons, volatility_warnings = score_volatility(snapshot)
    reasons.extend(volatility_reasons)
    warnings.extend(volatility_warnings)

    bull_score = clamp_score(bull_score)
    bear_score = clamp_score(bear_score)
    range_score = clamp_score(range_score)
    chop_score = clamp_score(chop_score)
    primary_regime = _classify(snapshot, bull_score, bear_score, range_score, chop_score, volatility_score)
    secondary_regime = _secondary_regime(snapshot, primary_regime, volatility_score)
    confidence = _confidence(primary_regime, bull_score, bear_score, range_score, chop_score, volatility_score, snapshot)
    if data_quality.get("is_stale"):
        warnings.append("Market-regime candle data is stale.")

    return _result(
        primary_regime=primary_regime,
        secondary_regime=secondary_regime,
        confidence=confidence,
        bull_score=bull_score,
        bear_score=bear_score,
        range_score=range_score,
        chop_score=chop_score,
        volatility_score=volatility_score,
        reasons=reasons,
        warnings=warnings,
        snapshot=snapshot,
    )


def _classify(snapshot: MarketFeatureSnapshot, bull_score: int, bear_score: int, range_score: int, chop_score: int, volatility_score: int) -> str:
    data_quality = snapshot.data_quality or {}
    if (data_quality.get("candle_count") or 0) < 5 or not snapshot.latest_close:
        return REGIME_UNCLEAR
    if volatility_score >= 85:
        return REGIME_HIGH_VOLATILITY
    if chop_score >= 55:
        return REGIME_CHOPPY
    if bull_score >= 65 and _above_opening_range(snapshot):
        return REGIME_BREAKOUT_UP
    if bear_score >= 65 and _below_opening_range(snapshot):
        return REGIME_BREAKDOWN
    if bull_score >= 60 and bull_score > bear_score + 15 and chop_score < 50:
        return REGIME_TRENDING_UP
    if bear_score >= 60 and bear_score > bull_score + 15 and chop_score < 50:
        return REGIME_TRENDING_DOWN
    if range_score >= 55 and bull_score < 60 and bear_score < 60:
        return REGIME_RANGE_BOUND
    if volatility_score <= 25 and range_score >= 45:
        return REGIME_LOW_VOLATILITY
    return REGIME_UNCLEAR


def _secondary_regime(snapshot: MarketFeatureSnapshot, primary_regime: str, volatility_score: int) -> str | None:
    if primary_regime == REGIME_TRENDING_UP and _above_opening_range(snapshot):
        return REGIME_BREAKOUT_UP
    if primary_regime == REGIME_TRENDING_DOWN and _below_opening_range(snapshot):
        return REGIME_BREAKDOWN
    if volatility_score >= 70 and primary_regime != REGIME_HIGH_VOLATILITY:
        return REGIME_HIGH_VOLATILITY
    return None


def _confidence(
    primary_regime: str,
    bull_score: int,
    bear_score: int,
    range_score: int,
    chop_score: int,
    volatility_score: int,
    snapshot: MarketFeatureSnapshot,
) -> float:
    if primary_regime in {REGIME_TRENDING_UP, REGIME_BREAKOUT_UP, REGIME_TRENDING_DOWN, REGIME_BREAKDOWN}:
        confidence = max(bull_score, bear_score) / 100
    elif primary_regime == REGIME_HIGH_VOLATILITY:
        confidence = volatility_score / 100
    elif primary_regime in {REGIME_RANGE_BOUND, REGIME_CHOPPY, REGIME_LOW_VOLATILITY}:
        confidence = max(range_score, chop_score) / 100
    else:
        confidence = 0.35

    missing = set((snapshot.data_quality or {}).get("missing_fields") or [])
    if "vwap" in missing:
        confidence -= 0.05
    if "rsi_14" in missing:
        confidence -= 0.05
    if "atr_14" in missing:
        confidence -= 0.05
    candle_count = int((snapshot.data_quality or {}).get("candle_count") or 0)
    if candle_count < 8:
        confidence -= 0.15
    elif candle_count < 20:
        confidence -= 0.08
    if (snapshot.data_quality or {}).get("is_stale"):
        confidence -= 0.10
    return clamp_confidence(confidence)


def _result(
    *,
    primary_regime: str,
    secondary_regime: str | None,
    confidence: float,
    bull_score: int,
    bear_score: int,
    range_score: int,
    chop_score: int,
    volatility_score: int,
    reasons: list[str],
    warnings: list[str],
    snapshot: MarketFeatureSnapshot,
) -> dict:
    data_stale = bool((snapshot.data_quality or {}).get("is_stale"))
    directional_bias = directional_bias_for_regime(primary_regime) if primary_regime != REGIME_UNCLEAR else BIAS_NEUTRAL
    trade_filter = trade_filter_for_regime(
        primary_regime,
        volatility_score,
        india_vix=snapshot.india_vix,
        data_stale=data_stale,
    ) if primary_regime != REGIME_UNCLEAR else (trade_filter_for_regime(primary_regime, volatility_score, data_stale=data_stale) if data_stale else FILTER_WAIT)
    return {
        "ruleset_version": RULESET_VERSION,
        "primary_regime": primary_regime,
        "secondary_regime": secondary_regime,
        "confidence": clamp_confidence(confidence),
        "bull_score": clamp_score(bull_score),
        "bear_score": clamp_score(bear_score),
        "trend_score": clamp_score(max(bull_score, bear_score)),
        "range_score": clamp_score(range_score),
        "chop_score": clamp_score(chop_score),
        "volatility_score": clamp_score(volatility_score),
        "directional_bias": directional_bias,
        "trade_filter": trade_filter,
        "reasons": _dedupe(reasons)[:10],
        "warnings": _dedupe(warnings)[:10],
    }


def _near_pct(value: float | None, target: float | None, threshold_pct: float) -> bool:
    if value is None or target in (None, 0):
        return False
    return abs(value - target) / abs(target) * 100 <= threshold_pct


def _above_opening_range(snapshot: MarketFeatureSnapshot) -> bool:
    return bool(snapshot.latest_close is not None and snapshot.opening_range_high is not None and snapshot.latest_close > snapshot.opening_range_high)


def _below_opening_range(snapshot: MarketFeatureSnapshot) -> bool:
    return bool(snapshot.latest_close is not None and snapshot.opening_range_low is not None and snapshot.latest_close < snapshot.opening_range_low)


def _small_net_large_ranges(snapshot: MarketFeatureSnapshot) -> bool:
    recent = (snapshot.candles or [])[-10:]
    if len(recent) < 5 or not recent[0].close:
        return False
    net_pct = abs(recent[-1].close - recent[0].close) / abs(recent[0].close) * 100
    ranges = [((candle.high - candle.low) / candle.close * 100) for candle in recent if candle.close]
    avg_range_pct = sum(ranges) / len(ranges) if ranges else 0
    return net_pct <= 0.15 and avg_range_pct >= 0.20


def _failed_opening_range_break(snapshot: MarketFeatureSnapshot) -> bool:
    if snapshot.opening_range_high is None or snapshot.opening_range_low is None or snapshot.latest_close is None:
        return False
    recent = (snapshot.candles or [])[-10:]
    if len(recent) < 5:
        return False
    inside = snapshot.opening_range_low <= snapshot.latest_close <= snapshot.opening_range_high
    broke_high = any(candle.high > snapshot.opening_range_high for candle in recent)
    broke_low = any(candle.low < snapshot.opening_range_low for candle in recent)
    return inside and (broke_high or broke_low)


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        clean = str(item)
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out
