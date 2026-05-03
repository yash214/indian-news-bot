"""Support/resistance zone detection from option-chain OI."""

from __future__ import annotations

try:
    from backend.agents.fo_structure.schemas import OptionChainSnapshot, StrikeZone
except ModuleNotFoundError:
    from agents.fo_structure.schemas import OptionChainSnapshot, StrikeZone


def find_atm_strike(snapshot: OptionChainSnapshot) -> float | None:
    if not snapshot or not snapshot.strikes:
        return None
    return min(snapshot.strikes, key=lambda strike: abs(strike.strike - snapshot.spot)).strike


def find_support_zones(snapshot: OptionChainSnapshot, top_n: int = 3) -> list[StrikeZone]:
    max_put_oi = max((strike.put_oi or 0 for strike in snapshot.strikes), default=0)
    max_put_change = max((max(strike.put_change_oi or 0, 0) for strike in snapshot.strikes), default=0)
    zones = []
    for strike in snapshot.strikes:
        distance = _distance_pct(strike.strike, snapshot.spot)
        if strike.strike > snapshot.spot and (distance is None or distance > 0.5):
            continue
        if (strike.put_oi or 0) <= 0 and (strike.put_change_oi or 0) <= 0:
            continue
        strength = _zone_strength(strike.put_oi, strike.put_change_oi, max_put_oi, max_put_change, distance)
        if strength <= 0:
            continue
        zones.append(StrikeZone(
            strike=strike.strike,
            strength=strength,
            reason="High put OI and put writing can act as option-chain support.",
            oi=strike.put_oi,
            change_oi=strike.put_change_oi,
            distance_from_spot_pct=distance,
        ))
    return sorted(zones, key=lambda zone: (zone.strength, zone.oi or 0), reverse=True)[:top_n]


def find_resistance_zones(snapshot: OptionChainSnapshot, top_n: int = 3) -> list[StrikeZone]:
    max_call_oi = max((strike.call_oi or 0 for strike in snapshot.strikes), default=0)
    max_call_change = max((max(strike.call_change_oi or 0, 0) for strike in snapshot.strikes), default=0)
    zones = []
    for strike in snapshot.strikes:
        distance = _distance_pct(strike.strike, snapshot.spot)
        if strike.strike < snapshot.spot and (distance is None or distance > 0.5):
            continue
        if (strike.call_oi or 0) <= 0 and (strike.call_change_oi or 0) <= 0:
            continue
        strength = _zone_strength(strike.call_oi, strike.call_change_oi, max_call_oi, max_call_change, distance)
        if strength <= 0:
            continue
        zones.append(StrikeZone(
            strike=strike.strike,
            strength=strength,
            reason="High call OI and call writing can act as option-chain resistance.",
            oi=strike.call_oi,
            change_oi=strike.call_change_oi,
            distance_from_spot_pct=distance,
        ))
    return sorted(zones, key=lambda zone: (zone.strength, zone.oi or 0), reverse=True)[:top_n]


def _zone_strength(oi: int | None, change_oi: int | None, max_oi: int, max_change: int, distance_pct: float | None) -> int:
    oi_score = ((oi or 0) / max_oi) if max_oi > 0 else 0.0
    change_score = (max(change_oi or 0, 0) / max_change) if max_change > 0 else 0.0
    proximity_score = max(0.0, 1.0 - (distance_pct or 0.0) / 3.0)
    return int(round(min(100.0, (oi_score * 50) + (change_score * 35) + (proximity_score * 15))))


def _distance_pct(strike: float, spot: float) -> float | None:
    if not spot:
        return None
    return round(abs(strike - spot) / spot * 100, 2)
