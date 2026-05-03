"""Expiry-risk classification for F&O structure reports."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from backend.agents.fo_structure.max_pain import calculate_max_pain
    from backend.agents.fo_structure.schemas import OptionChainSnapshot
    from backend.core.settings import IST
except ModuleNotFoundError:
    from agents.fo_structure.max_pain import calculate_max_pain
    from agents.fo_structure.schemas import OptionChainSnapshot
    from core.settings import IST


def calculate_expiry_risk(snapshot: OptionChainSnapshot, now: datetime | None = None) -> tuple[str, list[str]]:
    warnings: list[str] = []
    try:
        expiry_date = datetime.strptime(str(snapshot.expiry or ""), "%Y-%m-%d").date()
    except ValueError:
        return ("UNKNOWN", ["Invalid or missing expiry date."])

    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=IST)
    now = now.astimezone(IST)
    today = now.date()

    if expiry_date == today:
        minutes = now.hour * 60 + now.minute
        if minutes >= (14 * 60 + 30):
            risk = "EXTREME"
        elif minutes >= (13 * 60):
            risk = "HIGH"
        else:
            risk = "MEDIUM"
    elif expiry_date == today + timedelta(days=1):
        risk = "MEDIUM"
    else:
        risk = "LOW"

    max_pain = calculate_max_pain(snapshot)
    if max_pain and snapshot.spot:
        distance_pct = abs(snapshot.spot - max_pain) / snapshot.spot * 100
        if distance_pct <= 0.25 and risk == "LOW":
            risk = "MEDIUM"
            warnings.append("Spot is very close to max pain; expiry pinning risk is elevated.")

    return (risk, warnings)
