"""Open-interest writing and unwinding cluster detection."""

from __future__ import annotations

try:
    from backend.agents.fo_structure.schemas import OICluster, OptionChainSnapshot
except ModuleNotFoundError:
    from agents.fo_structure.schemas import OICluster, OptionChainSnapshot


def find_major_put_writing(snapshot: OptionChainSnapshot, top_n: int = 3) -> list[OICluster]:
    return _clusters(snapshot, side="put", positive=True, top_n=top_n, reason="Put writing detected from positive put change in OI.")


def find_major_call_writing(snapshot: OptionChainSnapshot, top_n: int = 3) -> list[OICluster]:
    return _clusters(snapshot, side="call", positive=True, top_n=top_n, reason="Call writing detected from positive call change in OI.")


def find_call_unwinding(snapshot: OptionChainSnapshot, top_n: int = 3) -> list[OICluster]:
    return _clusters(snapshot, side="call", positive=False, top_n=top_n, reason="Call unwinding detected from negative call change in OI.")


def find_put_unwinding(snapshot: OptionChainSnapshot, top_n: int = 3) -> list[OICluster]:
    return _clusters(snapshot, side="put", positive=False, top_n=top_n, reason="Put unwinding detected from negative put change in OI.")


def _clusters(snapshot: OptionChainSnapshot, *, side: str, positive: bool, top_n: int, reason: str) -> list[OICluster]:
    candidates = []
    max_abs_change = max((
        abs(_change_oi(strike, side) or 0)
        for strike in snapshot.strikes
        if _matches(_change_oi(strike, side), positive)
    ), default=0)
    max_oi = max((
        _current_oi(strike, side) or 0
        for strike in snapshot.strikes
        if _matches(_change_oi(strike, side), positive)
    ), default=0)
    for strike in snapshot.strikes:
        change_oi = _change_oi(strike, side)
        if not _matches(change_oi, positive):
            continue
        oi = _current_oi(strike, side)
        proximity = _proximity_score(strike.strike, snapshot.spot)
        change_score = abs(change_oi or 0) / max_abs_change if max_abs_change else 0.0
        oi_score = (oi or 0) / max_oi if max_oi else 0.0
        strength = int(round(min(100.0, change_score * 60 + proximity * 25 + oi_score * 15)))
        candidates.append(OICluster(strike=strike.strike, change_oi=int(change_oi or 0), oi=oi, strength=strength, reason=reason))
    return sorted(candidates, key=lambda item: (item.strength, abs(item.change_oi), item.oi or 0), reverse=True)[:top_n]


def _matches(change_oi: int | None, positive: bool) -> bool:
    if change_oi is None:
        return False
    return change_oi > 0 if positive else change_oi < 0


def _change_oi(strike, side: str) -> int | None:
    return strike.put_change_oi if side == "put" else strike.call_change_oi


def _current_oi(strike, side: str) -> int | None:
    return strike.put_oi if side == "put" else strike.call_oi


def _proximity_score(strike: float, spot: float) -> float:
    if not spot:
        return 0.0
    distance_pct = abs(strike - spot) / spot * 100
    return max(0.0, 1.0 - distance_pct / 4.0)
