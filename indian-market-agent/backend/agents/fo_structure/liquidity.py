"""Liquidity checks for option-chain snapshots."""

from __future__ import annotations

try:
    from backend.agents.fo_structure.schemas import OptionChainSnapshot
except ModuleNotFoundError:
    from agents.fo_structure.schemas import OptionChainSnapshot


def check_liquidity(snapshot: OptionChainSnapshot) -> list[str]:
    warnings: list[str] = []
    if not snapshot.strikes:
        return ["Option-chain strikes list is empty."]
    if len(snapshot.strikes) < 5:
        warnings.append("Option-chain has fewer than 5 strikes.")

    total_oi = sum((strike.call_oi or 0) + (strike.put_oi or 0) for strike in snapshot.strikes)
    if total_oi <= 0:
        warnings.append("Total call+put OI is zero.")

    option_count = len(snapshot.strikes) * 2
    missing_ltp = sum(1 for strike in snapshot.strikes for value in (strike.call_ltp, strike.put_ltp) if value is None)
    missing_bid_ask = 0
    for strike in snapshot.strikes:
        for bid, ask in ((strike.call_bid_price, strike.call_ask_price), (strike.put_bid_price, strike.put_ask_price)):
            if bid is None or ask is None:
                missing_bid_ask += 1
    if option_count and missing_ltp / option_count > 0.40:
        warnings.append("More than 40% of option LTP values are missing.")
    if option_count and missing_bid_ask / option_count > 0.40:
        warnings.append("More than 40% of option bid/ask prices are missing.")

    atm = min(snapshot.strikes, key=lambda strike: abs(strike.strike - snapshot.spot))
    for label, bid, ask, ltp in (
        ("ATM call", atm.call_bid_price, atm.call_ask_price, atm.call_ltp),
        ("ATM put", atm.put_bid_price, atm.put_ask_price, atm.put_ltp),
    ):
        if bid is None or ask is None or not ltp:
            continue
        spread_pct = (ask - bid) / ltp * 100
        if spread_pct > 10:
            warnings.append(f"{label} bid/ask spread is wider than 10%.")
    return warnings
