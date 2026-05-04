"""Reusable labels and thresholds for deterministic regime scoring."""

from __future__ import annotations


RULESET_VERSION = "market-regime-v1"

REGIME_TRENDING_UP = "TRENDING_UP"
REGIME_TRENDING_DOWN = "TRENDING_DOWN"
REGIME_RANGE_BOUND = "RANGE_BOUND"
REGIME_BREAKOUT_UP = "BREAKOUT_UP"
REGIME_BREAKDOWN = "BREAKDOWN"
REGIME_HIGH_VOLATILITY = "HIGH_VOLATILITY"
REGIME_CHOPPY = "CHOPPY"
REGIME_LOW_VOLATILITY = "LOW_VOLATILITY"
REGIME_UNCLEAR = "UNCLEAR"

BIAS_BULLISH = "BULLISH"
BIAS_BEARISH = "BEARISH"
BIAS_NEUTRAL = "NEUTRAL"

FILTER_NO_FILTER = "NO_FILTER"
FILTER_WAIT = "WAIT"
FILTER_REDUCE_SIZE = "REDUCE_POSITION_SIZE"
FILTER_AVOID_DIRECTIONAL = "AVOID_DIRECTIONAL"
FILTER_BLOCK_FRESH_TRADES = "BLOCK_FRESH_TRADES"


def clamp_score(value: float | int) -> int:
    return int(max(0, min(100, round(float(value or 0)))))


def clamp_confidence(value: float) -> float:
    return round(max(0.35, min(0.95, float(value or 0.35))), 2)


def directional_bias_for_regime(primary_regime: str) -> str:
    if primary_regime in {REGIME_TRENDING_UP, REGIME_BREAKOUT_UP}:
        return BIAS_BULLISH
    if primary_regime in {REGIME_TRENDING_DOWN, REGIME_BREAKDOWN}:
        return BIAS_BEARISH
    return BIAS_NEUTRAL


def trade_filter_for_regime(primary_regime: str, volatility_score: int, *, india_vix: float | None = None, data_stale: bool = False) -> str:
    if data_stale or (india_vix is not None and india_vix >= 28):
        return FILTER_BLOCK_FRESH_TRADES
    if primary_regime == REGIME_HIGH_VOLATILITY:
        return FILTER_AVOID_DIRECTIONAL if volatility_score >= 90 else FILTER_REDUCE_SIZE
    if primary_regime == REGIME_CHOPPY:
        return FILTER_AVOID_DIRECTIONAL
    if primary_regime in {REGIME_RANGE_BOUND, REGIME_UNCLEAR, REGIME_LOW_VOLATILITY}:
        return FILTER_WAIT
    return FILTER_NO_FILTER
