"""PCR calculation helpers for the F&O Structure Agent."""

from __future__ import annotations

try:
    from backend.agents.fo_structure.schemas import OptionChainSnapshot
except ModuleNotFoundError:
    from agents.fo_structure.schemas import OptionChainSnapshot


def calculate_pcr(snapshot: OptionChainSnapshot) -> float | None:
    total_call_oi = sum(int(strike.call_oi or 0) for strike in snapshot.strikes)
    total_put_oi = sum(int(strike.put_oi or 0) for strike in snapshot.strikes)
    if total_call_oi <= 0:
        return None
    return round(total_put_oi / total_call_oi, 2)


def classify_pcr(pcr: float | None) -> str:
    if pcr is None:
        return "UNKNOWN"
    if pcr > 1.50:
        return "EXTREME_BULLISH"
    if pcr > 1.10:
        return "BULLISH"
    if 0.90 <= pcr <= 1.10:
        return "NEUTRAL"
    if pcr < 0.70:
        return "EXTREME_BEARISH"
    if pcr < 0.90:
        return "BEARISH"
    return "UNKNOWN"
