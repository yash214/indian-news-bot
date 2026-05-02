"""Aggregation and strategy guidance for macro factor scores."""

from __future__ import annotations

from math import fabs

try:
    from backend.agents.macro_context.schemas import MacroFactorScore, StrategyEngineGuidance
    from backend.shared.enums import (
        TRADE_FILTER_BLOCK_FRESH_TRADES,
        TRADE_FILTER_EVENT_RISK_WAIT,
        TRADE_FILTER_NO_FILTER,
        TRADE_FILTER_REDUCE_LONG_CONFIDENCE,
        TRADE_FILTER_REDUCE_SHORT_CONFIDENCE,
    )
except ModuleNotFoundError:
    from agents.macro_context.schemas import MacroFactorScore, StrategyEngineGuidance
    from shared.enums import (
        TRADE_FILTER_BLOCK_FRESH_TRADES,
        TRADE_FILTER_EVENT_RISK_WAIT,
        TRADE_FILTER_NO_FILTER,
        TRADE_FILTER_REDUCE_LONG_CONFIDENCE,
        TRADE_FILTER_REDUCE_SHORT_CONFIDENCE,
    )


BIAS_VALUES = {
    "BULLISH": 1.0,
    "NEUTRAL": 0.0,
    "MIXED": 0.0,
    "BEARISH": -1.0,
    "RISK_OFF": -0.7,
    "CAUTION": -0.4,
    "EVENT_RISK": -0.6,
}


def weighted_macro_score(scores: dict[str, MacroFactorScore]) -> float:
    total = 0.0
    for score in scores.values():
        total += BIAS_VALUES.get(score.bias, 0.0) * score.impact * score.confidence
    return round(total, 2)


def classify_macro_bias(
    scores: dict[str, MacroFactorScore],
    *,
    extreme_risk: bool = False,
    severe_event_risk: bool = False,
) -> str:
    if extreme_risk:
        return "EVENT_RISK"
    total = weighted_macro_score(scores)
    if severe_event_risk and total <= -2.0:
        return "EVENT_RISK"
    if total >= 8:
        return "BULLISH"
    if total >= 3:
        return "MIXED_BULLISH"
    if total > -3:
        return "NEUTRAL"
    if total > -8:
        return "MIXED_BEARISH"
    return "BEARISH"


def calculate_report_confidence(
    scores: dict[str, MacroFactorScore],
    *,
    required_factor_count: int,
    warning_count: int = 0,
) -> float:
    if not scores:
        return 0.2
    avg_confidence = sum(score.confidence for score in scores.values()) / max(1, len(scores))
    coverage = min(1.0, len(scores) / max(1, required_factor_count))
    penalty = min(0.25, warning_count * 0.03)
    return round(max(0.2, min(0.95, avg_confidence * (0.5 + 0.5 * coverage) - penalty)), 2)


def determine_trade_filter(
    macro_bias: str,
    scores: dict[str, MacroFactorScore],
    *,
    extreme_risk: bool = False,
    has_high_event_risk: bool = False,
) -> str:
    if extreme_risk:
        return TRADE_FILTER_BLOCK_FRESH_TRADES
    if has_high_event_risk:
        return TRADE_FILTER_EVENT_RISK_WAIT
    if macro_bias in {"BEARISH", "MIXED_BEARISH"}:
        return TRADE_FILTER_REDUCE_LONG_CONFIDENCE
    if macro_bias in {"BULLISH", "MIXED_BULLISH"}:
        return TRADE_FILTER_REDUCE_SHORT_CONFIDENCE
    return TRADE_FILTER_NO_FILTER


def build_strategy_guidance(
    macro_bias: str,
    trade_filter: str,
    scores: dict[str, MacroFactorScore],
) -> StrategyEngineGuidance:
    india_vix_score = scores.get("india_vix")
    vix_elevated = bool(india_vix_score and india_vix_score.bias in {"CAUTION", "EVENT_RISK"})
    guidance = StrategyEngineGuidance(notes=_guidance_note(macro_bias, trade_filter))

    if macro_bias == "BULLISH":
        guidance.long_confidence_adjustment = 5
        guidance.short_confidence_adjustment = -10
    elif macro_bias == "MIXED_BULLISH":
        guidance.long_confidence_adjustment = 3
        guidance.short_confidence_adjustment = -5
    elif macro_bias == "MIXED_BEARISH":
        guidance.long_confidence_adjustment = -10
        guidance.short_confidence_adjustment = 5
        guidance.reduce_position_size = vix_elevated
    elif macro_bias == "BEARISH":
        guidance.long_confidence_adjustment = -15
        guidance.short_confidence_adjustment = 8
        guidance.reduce_position_size = True
    elif macro_bias == "EVENT_RISK":
        guidance.long_confidence_adjustment = -10
        guidance.short_confidence_adjustment = -10
        guidance.reduce_position_size = True

    if trade_filter == TRADE_FILTER_BLOCK_FRESH_TRADES:
        guidance.block_fresh_trades = True
        guidance.reduce_position_size = True
    elif trade_filter == TRADE_FILTER_EVENT_RISK_WAIT:
        guidance.reduce_position_size = True

    return guidance


def build_major_drivers(scores: dict[str, MacroFactorScore], limit: int = 4) -> list[str]:
    ranked = sorted(
        scores.values(),
        key=lambda item: fabs(BIAS_VALUES.get(item.bias, 0.0) * item.impact * item.confidence),
        reverse=True,
    )
    drivers = []
    for score in ranked:
        if not score.reason:
            continue
        drivers.append(f"{score.name}: {score.reason}")
        if len(drivers) >= limit:
            break
    return drivers


def summarize_report(macro_bias: str, trade_filter: str, scores: dict[str, MacroFactorScore]) -> str:
    drivers = build_major_drivers(scores, limit=2)
    if not drivers:
        return f"Macro bias is {macro_bias} with trade filter {trade_filter}."
    return f"Macro bias is {macro_bias} with trade filter {trade_filter}. Key drivers: {'; '.join(drivers)}"


def _guidance_note(macro_bias: str, trade_filter: str) -> str:
    if trade_filter == TRADE_FILTER_BLOCK_FRESH_TRADES:
        return "Fresh trades should stay blocked until macro shock conditions cool."
    if trade_filter == TRADE_FILTER_EVENT_RISK_WAIT:
        return "Wait for event risk to pass before trusting fresh entries."
    if macro_bias in {"BEARISH", "MIXED_BEARISH"}:
        return "Macro context argues for lower long conviction."
    if macro_bias in {"BULLISH", "MIXED_BULLISH"}:
        return "Macro context favors reducing short-side conviction."
    return "Macro context is balanced and does not require a directional filter."
