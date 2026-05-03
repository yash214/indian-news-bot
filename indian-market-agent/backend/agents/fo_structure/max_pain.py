"""Max-pain calculation for option-chain snapshots."""

from __future__ import annotations

try:
    from backend.agents.fo_structure.schemas import OptionChainSnapshot
except ModuleNotFoundError:
    from agents.fo_structure.schemas import OptionChainSnapshot


def calculate_max_pain(snapshot: OptionChainSnapshot) -> float | None:
    strikes_with_oi = [
        strike for strike in snapshot.strikes
        if (strike.call_oi or 0) > 0 or (strike.put_oi or 0) > 0
    ]
    if len(strikes_with_oi) < 3:
        return None
    best_strike = None
    best_payout = None
    for settlement in [strike.strike for strike in strikes_with_oi]:
        call_payout = sum((strike.call_oi or 0) * max(0.0, settlement - strike.strike) for strike in strikes_with_oi)
        put_payout = sum((strike.put_oi or 0) * max(0.0, strike.strike - settlement) for strike in strikes_with_oi)
        total_payout = call_payout + put_payout
        if best_payout is None or total_payout < best_payout:
            best_payout = total_payout
            best_strike = settlement
    return best_strike
