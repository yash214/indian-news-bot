"""Deterministic factor scoring rules for the Macro Context Agent."""

from __future__ import annotations

from datetime import datetime

try:
    from backend.agents.macro_context.schemas import MacroEvent, MacroFactorInput, MacroFactorScore
    from backend.core.settings import IST
except ModuleNotFoundError:
    from agents.macro_context.schemas import MacroEvent, MacroFactorInput, MacroFactorScore
    from core.settings import IST


HIGH_IMPACT_EVENT_KEYWORDS = {
    "rbi policy",
    "mpc",
    "india cpi",
    "india gdp",
    "india wpi",
    "india iip",
    "us cpi",
    "federal reserve",
    "fed decision",
    "fomc",
    "non farm payrolls",
    "nfp",
    "us gdp",
    "crude inventory",
    "budget",
    "election result",
}
MEDIUM_IMPACT_EVENT_KEYWORDS = {"inflation", "payroll", "pmi", "wpi", "iip", "treasury", "yield"}


def score_factor(name: str, factor: MacroFactorInput | None) -> MacroFactorScore | None:
    if factor is None:
        return None
    clean = str(name or factor.name or "").strip().lower()
    if clean == "usd_inr":
        return _score_usd_inr(factor)
    if clean == "crude":
        return _score_crude(factor)
    if clean == "gold":
        return _score_gold(factor)
    if clean == "india_vix":
        return _score_india_vix(factor)
    if clean == "global_cues":
        return _score_global_cues(factor)
    return MacroFactorScore(
        name=factor.name,
        symbol=factor.symbol,
        value=factor.value,
        change_pct_1d=factor.change_pct_1d,
        change_pct_5d=factor.change_pct_5d,
        bias="NEUTRAL",
        impact=1,
        confidence=0.3,
        source=factor.source,
        reason="No explicit macro rule exists for this factor yet.",
    )


def score_economic_calendar(events: list[MacroEvent], now: datetime | None = None) -> tuple[MacroFactorScore | None, list[str], bool]:
    now = now or datetime.now(IST)
    if not events:
        return None, [], False

    severe = False
    drivers: list[str] = []
    highest: MacroFactorScore | None = None
    today = now.astimezone(IST).date()
    for event in events:
        if not _is_supported_country(event.country):
            continue
        if event.event_time and event.event_time.astimezone(IST).date() != today:
            continue
        event_name = str(event.event or "").strip()
        importance = str(event.importance or "").strip().lower()
        normalized = event_name.lower()
        if _matches_keyword(normalized, HIGH_IMPACT_EVENT_KEYWORDS) or importance in {"high", "very high", "severe"}:
            highest = MacroFactorScore(
                name="economic_calendar",
                symbol=None,
                value=None,
                change_pct_1d=None,
                change_pct_5d=None,
                bias="EVENT_RISK",
                impact=8,
                confidence=0.85,
                source=event.source,
                reason=f"High-impact macro event on deck: {event_name}.",
            )
            drivers.append(f"Event risk: {event_name}")
            severe = severe or importance in {"very high", "severe"}
            break
        if _matches_keyword(normalized, MEDIUM_IMPACT_EVENT_KEYWORDS) or importance == "medium":
            if highest is None:
                highest = MacroFactorScore(
                    name="economic_calendar",
                    symbol=None,
                    value=None,
                    change_pct_1d=None,
                    change_pct_5d=None,
                    bias="CAUTION",
                    impact=5,
                    confidence=0.7,
                    source=event.source,
                    reason=f"Medium-impact macro event approaching: {event_name}.",
                )
            drivers.append(f"Calendar watch: {event_name}")

    return highest, drivers[:3], severe


def is_extreme_macro_shock(name: str, factor: MacroFactorInput | None, score: MacroFactorScore | None) -> bool:
    if factor is None or score is None:
        return False
    clean = str(name or "").lower()
    change = factor.change_pct_1d
    value = factor.value
    if clean == "india_vix":
        return (value is not None and value >= 28) or (change is not None and change >= 15)
    if clean == "crude":
        return change is not None and change >= 5
    if clean == "usd_inr":
        return change is not None and change >= 1
    return False


def _score_usd_inr(factor: MacroFactorInput) -> MacroFactorScore:
    change = factor.change_pct_1d
    if change is None:
        return _missing_score(factor, "USD/INR data is unavailable, so FX pressure is uncertain.")
    if change >= 1.0:
        return _build_score(factor, "BEARISH", 8, 0.85, "Sharp rupee weakness can signal external pressure, imported inflation risk and possible FII sentiment pressure.")
    if change >= 0.5:
        return _build_score(factor, "BEARISH", 7, 0.8, "Rupee weakness is adding pressure to broad-market risk appetite.")
    if change >= 0.2:
        return _build_score(factor, "BEARISH", 4, 0.72, "Moderate rupee weakness slightly raises macro pressure.")
    if change <= -0.3:
        return _build_score(factor, "BULLISH", 5, 0.78, "Rupee strength can reduce external pressure and support risk sentiment.")
    return _build_score(factor, "NEUTRAL", 2, 0.65, "USD/INR is not flashing a major macro signal.")


def _score_crude(factor: MacroFactorInput) -> MacroFactorScore:
    change = factor.change_pct_1d
    if change is None:
        return _missing_score(factor, "Crude data is unavailable, so import-bill pressure is uncertain.")
    if change >= 5.0:
        return _build_score(factor, "BEARISH", 9, 0.9, "Crude shock can pressure India's import bill, inflation expectations and rupee.")
    if change >= 2.5:
        return _build_score(factor, "BEARISH", 8, 0.84, "Sharp crude strength is a meaningful headwind for India.")
    if change >= 1.0:
        return _build_score(factor, "BEARISH", 5, 0.76, "Crude is rising enough to lean against broad risk appetite.")
    if change <= -2.0:
        return _build_score(factor, "BULLISH", 6, 0.8, "Falling crude can ease import-bill and inflation pressure.")
    return _build_score(factor, "NEUTRAL", 2, 0.65, "Crude is broadly stable.")


def _score_gold(factor: MacroFactorInput) -> MacroFactorScore:
    change = factor.change_pct_1d
    if change is None:
        return _missing_score(factor, "Gold data is unavailable, so the risk-off proxy is incomplete.")
    if change >= 2.0:
        return _build_score(factor, "RISK_OFF", 6, 0.78, "Gold strength suggests a stronger risk-off or inflation hedge bid.")
    if change >= 1.0:
        return _build_score(factor, "RISK_OFF", 4, 0.72, "Gold is firm enough to hint at a mild risk-off backdrop.")
    if change <= -1.5:
        return _build_score(factor, "BULLISH", 3, 0.62, "Gold softness can align with steadier risk appetite.")
    return _build_score(factor, "NEUTRAL", 1, 0.55, "Gold is not adding much macro signal today.")


def _score_india_vix(factor: MacroFactorInput) -> MacroFactorScore:
    change = factor.change_pct_1d
    value = factor.value
    level_score = _build_score(factor, "NEUTRAL", 2, 0.7, "India VIX is in a calmer range.")
    if value is None:
        level_score = _missing_score(factor, "India VIX is unavailable, so volatility risk is less certain.")
    elif value >= 28:
        level_score = _build_score(factor, "EVENT_RISK", 9, 0.95, "India VIX is elevated enough to flag event-risk conditions.")
    elif value >= 22:
        level_score = _build_score(factor, "CAUTION", 7, 0.86, "India VIX is elevated and argues for more conservative sizing.")
    elif value >= 18:
        level_score = _build_score(factor, "CAUTION", 5, 0.8, "India VIX is above the calm zone and deserves caution.")

    change_score = None
    if change is not None:
        if change >= 15:
            change_score = _build_score(factor, "EVENT_RISK", 10, 0.95, "India VIX is repricing sharply higher, which is a strong macro shock warning.")
        elif change >= 10:
            change_score = _build_score(factor, "EVENT_RISK", 9, 0.9, "India VIX is surging and points to rising event risk.")
        elif change >= 5:
            change_score = _build_score(factor, "CAUTION", 7, 0.84, "India VIX is rising fast enough to reduce comfort on fresh entries.")
        elif change <= -5:
            change_score = _build_score(factor, "BULLISH", 4, 0.72, "India VIX is cooling, which helps directional confidence.")

    if change_score and _severity_rank(change_score) > _severity_rank(level_score):
        return change_score
    return level_score


def _score_global_cues(factor: MacroFactorInput) -> MacroFactorScore:
    change = factor.change_pct_1d
    reason = "Global cues are mixed to neutral."
    if isinstance(factor.raw, dict):
        nasdaq = _safe_float((factor.raw.get("components") or {}).get("nasdaq", {}).get("change_pct_1d"))
        if nasdaq is not None and nasdaq <= -1.0:
            reason = "Nasdaq weakness may pressure Indian IT sentiment."
    if change is None:
        return _missing_score(factor, "US/global cues are unavailable, so overnight risk appetite is uncertain.")
    if change >= 0.5:
        return _build_score(factor, "BULLISH", 5, 0.76, "US/global cues are supportive for Indian risk sentiment.")
    if change <= -0.5:
        return _build_score(factor, "BEARISH", 5, 0.76, reason)
    return _build_score(factor, "NEUTRAL", 2, 0.62, reason)


def _build_score(factor: MacroFactorInput, bias: str, impact: int, confidence: float, reason: str) -> MacroFactorScore:
    return MacroFactorScore(
        name=factor.name,
        symbol=factor.symbol,
        value=factor.value,
        change_pct_1d=factor.change_pct_1d,
        change_pct_5d=factor.change_pct_5d,
        bias=bias,
        impact=impact,
        confidence=confidence,
        source=factor.source,
        reason=reason,
    )


def _missing_score(factor: MacroFactorInput, reason: str) -> MacroFactorScore:
    return _build_score(factor, "NEUTRAL", 1, 0.25, reason)


def _matches_keyword(value: str, keywords: set[str]) -> bool:
    return any(keyword in value for keyword in keywords)


def _is_supported_country(country: str) -> bool:
    clean = str(country or "").strip().lower()
    return clean in {"india", "ind", "united states", "united states of america", "usa", "us"}


def _severity_rank(score: MacroFactorScore) -> tuple[int, int]:
    priority = {
        "EVENT_RISK": 5,
        "RISK_OFF": 4,
        "BEARISH": 3,
        "CAUTION": 2,
        "BULLISH": 1,
        "NEUTRAL": 0,
        "MIXED": 0,
    }
    return (priority.get(score.bias, 0), score.impact)


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
