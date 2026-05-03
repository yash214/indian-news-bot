"""Rule-based F&O structure scoring."""

from __future__ import annotations

try:
    from backend.agents.fo_structure.schemas import FOStrategyEngineGuidance
except ModuleNotFoundError:
    from agents.fo_structure.schemas import FOStrategyEngineGuidance


def score_fo_structure(
    snapshot,
    pcr,
    pcr_state,
    support_zones,
    resistance_zones,
    major_put_writing,
    major_call_writing,
    call_unwinding,
    put_unwinding,
    expiry_risk,
    warnings,
) -> dict:
    bull_score = 0
    bear_score = 0
    reasons: list[str] = []

    if pcr_state in {"BULLISH", "EXTREME_BULLISH"}:
        bull_score += 20
        reasons.append(f"PCR state is {pcr_state}.")
    if pcr_state in {"BEARISH", "EXTREME_BEARISH"}:
        bear_score += 20
        reasons.append(f"PCR state is {pcr_state}.")

    if _has_near_cluster(major_put_writing, snapshot.spot, below_or_near=True):
        bull_score += 15
        reasons.append("Strong put writing is visible below or near spot.")
    if _has_near_cluster(major_call_writing, snapshot.spot, below_or_near=False):
        bear_score += 15
        reasons.append("Strong call writing is visible above or near spot.")
    if support_zones and support_zones[0].strike <= snapshot.spot * 1.01:
        bull_score += 15
        reasons.append("A strong support zone is near or below spot.")
    if resistance_zones and resistance_zones[0].strike >= snapshot.spot * 0.99:
        bear_score += 15
        reasons.append("A strong resistance zone is near or above spot.")
    if _has_near_cluster(call_unwinding, snapshot.spot):
        bull_score += 10
        reasons.append("Call unwinding near spot reduces overhead pressure.")
    if _has_near_cluster(put_unwinding, snapshot.spot):
        bear_score += 10
        reasons.append("Put unwinding near spot weakens downside support.")
    if support_zones and snapshot.spot >= support_zones[0].strike:
        bull_score += 10
    if resistance_zones and snapshot.spot <= resistance_zones[0].strike:
        bear_score += 10
    if support_zones and resistance_zones:
        if support_zones[0].strength > resistance_zones[0].strength:
            bull_score += 10
            reasons.append("Strongest support is stronger than strongest resistance.")
        elif resistance_zones[0].strength > support_zones[0].strength:
            bear_score += 10
            reasons.append("Strongest resistance is stronger than strongest support.")

    if bull_score >= 60 and bull_score > bear_score:
        bias = "BULLISH"
    elif bear_score >= 60 and bear_score > bull_score:
        bias = "BEARISH"
    elif abs(bull_score - bear_score) <= 10:
        bias = "MIXED"
    else:
        bias = "NEUTRAL"

    confidence = max(bull_score, bear_score) / 100
    confidence = max(0.35, min(0.95, confidence))
    if _has_severe_warning(warnings):
        confidence = max(0.35, confidence - 0.15)
    if expiry_risk == "EXTREME":
        confidence = min(confidence, 0.55)

    guidance = _guidance_for_bias(bias, expiry_risk)
    return {
        "bias": bias,
        "confidence": round(confidence, 2),
        "bull_score": bull_score,
        "bear_score": bear_score,
        "reasons": reasons,
        "strategy_engine_guidance": guidance,
    }


def _guidance_for_bias(bias: str, expiry_risk: str) -> FOStrategyEngineGuidance:
    prefer_defined_risk = True
    reduce_position_size = expiry_risk in {"HIGH", "EXTREME"}
    avoid_directional = expiry_risk == "EXTREME"
    if bias == "BULLISH":
        bullish_adj, bearish_adj = (8, -6)
        notes = "F&O structure supports bullish bias, but keep it read-only and defined-risk."
    elif bias == "BEARISH":
        bullish_adj, bearish_adj = (-6, 8)
        notes = "F&O structure supports bearish bias, but keep it read-only and defined-risk."
    elif bias == "MIXED":
        bullish_adj = bearish_adj = 0
        avoid_directional = True
        notes = "F&O structure is mixed; future strategy engine should avoid directional conviction."
    else:
        bullish_adj = bearish_adj = 0
        avoid_directional = True
        notes = "F&O structure is neutral; future strategy engine should avoid directional conviction."
    return FOStrategyEngineGuidance(
        bullish_confidence_adjustment=bullish_adj,
        bearish_confidence_adjustment=bearish_adj,
        prefer_defined_risk=prefer_defined_risk,
        reduce_position_size=reduce_position_size,
        avoid_directional_trade=avoid_directional,
        notes=notes,
    )


def _has_near_cluster(clusters, spot: float, below_or_near: bool | None = None) -> bool:
    for cluster in clusters:
        if not spot:
            continue
        near = abs(cluster.strike - spot) / spot <= 0.02
        side_ok = True
        if below_or_near is True:
            side_ok = cluster.strike <= spot * 1.01
        elif below_or_near is False:
            side_ok = cluster.strike >= spot * 0.99
        if near and side_ok and cluster.strength >= 40:
            return True
    return False


def _has_severe_warning(warnings: list[str]) -> bool:
    severe_terms = ("empty", "fewer than 5", "zero", "invalid", "missing", "unavailable")
    return any(any(term in warning.lower() for term in severe_terms) for warning in warnings)
