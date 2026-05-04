"""Volatility scoring for Market Regime snapshots."""

from __future__ import annotations

try:
    from backend.agents.market_regime.schemas import MarketFeatureSnapshot
    from backend.agents.market_regime.regime_rules import clamp_score
except ModuleNotFoundError:
    from agents.market_regime.schemas import MarketFeatureSnapshot
    from agents.market_regime.regime_rules import clamp_score


def score_volatility(snapshot: MarketFeatureSnapshot) -> tuple[int, list[str], list[str]]:
    score = 0
    reasons: list[str] = []
    warnings: list[str] = []

    if snapshot.india_vix is not None:
        if snapshot.india_vix >= 28:
            score += 90
            reasons.append(f"India VIX is extreme at {round(snapshot.india_vix, 2)}.")
            warnings.append("Extreme volatility: India VIX is at or above 28.")
        elif snapshot.india_vix >= 22:
            score += 75
            reasons.append(f"India VIX is high at {round(snapshot.india_vix, 2)}.")
        elif snapshot.india_vix >= 18:
            score += 55
            reasons.append(f"India VIX is elevated at {round(snapshot.india_vix, 2)}.")

    if snapshot.india_vix_change_pct is not None and snapshot.india_vix_change_pct >= 10:
        score += 20
        warnings.append(f"India VIX is up {round(snapshot.india_vix_change_pct, 2)}% today.")
        reasons.append("VIX expansion increases regime risk.")

    if snapshot.atr_pct is not None:
        if snapshot.atr_pct >= 0.35:
            score += 20
            reasons.append(f"ATR is high at {round(snapshot.atr_pct, 3)}% of price.")
        elif snapshot.atr_pct <= 0.08:
            reasons.append(f"ATR is low at {round(snapshot.atr_pct, 3)}% of price.")

    return clamp_score(score), reasons, warnings
