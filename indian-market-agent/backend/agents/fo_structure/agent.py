"""Read-only F&O Structure Agent."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from backend.agents.agent_output_store import save_agent_output
    from backend.agents.fo_structure.expiry_risk import calculate_expiry_risk
    from backend.agents.fo_structure.liquidity import check_liquidity
    from backend.agents.fo_structure.max_pain import calculate_max_pain
    from backend.agents.fo_structure.oi_analyzer import (
        find_call_unwinding,
        find_major_call_writing,
        find_major_put_writing,
        find_put_unwinding,
    )
    from backend.agents.fo_structure.pcr import calculate_pcr, classify_pcr
    from backend.agents.fo_structure.schemas import (
        FOStructureReport,
        FOStrategyEngineGuidance,
        OptionChainSnapshot,
        is_supported_symbol,
        normalize_fo_symbol,
        unsupported_symbol_warning,
    )
    from backend.agents.fo_structure.scoring import score_fo_structure
    from backend.agents.fo_structure.strike_zones import find_atm_strike, find_resistance_zones, find_support_zones
    from backend.core.settings import FO_AGENT_REFRESH_SECONDS, IST
except ModuleNotFoundError:
    from agents.agent_output_store import save_agent_output
    from agents.fo_structure.expiry_risk import calculate_expiry_risk
    from agents.fo_structure.liquidity import check_liquidity
    from agents.fo_structure.max_pain import calculate_max_pain
    from agents.fo_structure.oi_analyzer import find_call_unwinding, find_major_call_writing, find_major_put_writing, find_put_unwinding
    from agents.fo_structure.pcr import calculate_pcr, classify_pcr
    from agents.fo_structure.schemas import FOStructureReport, FOStrategyEngineGuidance, OptionChainSnapshot, is_supported_symbol, normalize_fo_symbol, unsupported_symbol_warning
    from agents.fo_structure.scoring import score_fo_structure
    from agents.fo_structure.strike_zones import find_atm_strike, find_resistance_zones, find_support_zones
    from core.settings import FO_AGENT_REFRESH_SECONDS, IST


class FOStructureAgent:
    AGENT_NAME = "fo_structure_agent"

    def analyze(self, snapshot: OptionChainSnapshot | None, symbol: str | None = None) -> FOStructureReport:
        clean = normalize_fo_symbol(symbol or (snapshot.symbol if snapshot else "NIFTY"))
        if not is_supported_symbol(clean):
            return self._safe_report(clean, warnings=[unsupported_symbol_warning(clean)])
        if snapshot is None:
            return self._safe_report(clean, warnings=[f"Option-chain snapshot unavailable for {clean}."])
        if not snapshot.strikes or snapshot.spot <= 0:
            return self._safe_report(
                clean,
                expiry=snapshot.expiry,
                warnings=["Invalid option-chain snapshot: missing strikes or spot."],
                source_status=snapshot.source_status,
            )

        warnings = check_liquidity(snapshot)
        pcr = calculate_pcr(snapshot)
        pcr_state = classify_pcr(pcr)
        atm_strike = find_atm_strike(snapshot)
        support_zones = find_support_zones(snapshot)
        resistance_zones = find_resistance_zones(snapshot)
        major_put_writing = find_major_put_writing(snapshot)
        major_call_writing = find_major_call_writing(snapshot)
        call_unwinding = find_call_unwinding(snapshot)
        put_unwinding = find_put_unwinding(snapshot)
        max_pain = calculate_max_pain(snapshot)
        expiry_risk, expiry_warnings = calculate_expiry_risk(snapshot)
        warnings.extend(expiry_warnings)

        score = score_fo_structure(
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
        )
        preferred_option_zone = _preferred_option_zone(snapshot.spot, support_zones, resistance_zones)
        reasons = _build_reasons(score["reasons"], pcr, pcr_state, max_pain, expiry_risk)

        # TODO: Future learning layer should compare predicted bias vs spot move
        # after 15/30/60 minutes, zone holds/breaks, PCR false signals, and expiry
        # risk usefulness.
        report = FOStructureReport(
            agent_name=self.AGENT_NAME,
            symbol=clean,
            expiry=snapshot.expiry,
            generated_at=datetime.now(IST),
            valid_until=datetime.now(IST) + timedelta(seconds=FO_AGENT_REFRESH_SECONDS),
            stale_after_seconds=FO_AGENT_REFRESH_SECONDS,
            bias=score["bias"],
            confidence=score["confidence"],
            pcr=pcr,
            pcr_state=pcr_state,
            support_zones=support_zones,
            resistance_zones=resistance_zones,
            major_put_writing=major_put_writing,
            major_call_writing=major_call_writing,
            call_unwinding=call_unwinding,
            put_unwinding=put_unwinding,
            max_pain=max_pain,
            atm_strike=atm_strike,
            expiry_risk=expiry_risk,
            preferred_option_zone=preferred_option_zone,
            strategy_engine_guidance=score["strategy_engine_guidance"],
            reasons=reasons,
            warnings=warnings,
            source_status=snapshot.source_status,
        )
        self._persist_report(report)
        return report

    def _safe_report(self, symbol: str, *, expiry: str | None = None, warnings: list[str] | None = None, source_status: dict | None = None) -> FOStructureReport:
        generated_at = datetime.now(IST)
        guidance = FOStrategyEngineGuidance(
            bullish_confidence_adjustment=0,
            bearish_confidence_adjustment=0,
            prefer_defined_risk=True,
            reduce_position_size=True,
            avoid_directional_trade=True,
            notes="F&O structure unavailable or unsupported; future strategy engine should avoid directional conviction.",
        )
        report = FOStructureReport(
            agent_name=self.AGENT_NAME,
            symbol=normalize_fo_symbol(symbol),
            expiry=expiry,
            generated_at=generated_at,
            valid_until=generated_at + timedelta(seconds=FO_AGENT_REFRESH_SECONDS),
            stale_after_seconds=FO_AGENT_REFRESH_SECONDS,
            bias="NEUTRAL",
            confidence=0.35,
            pcr=None,
            pcr_state="UNKNOWN",
            expiry_risk="UNKNOWN",
            preferred_option_zone={},
            strategy_engine_guidance=guidance,
            reasons=[],
            warnings=warnings or [],
            source_status=source_status or {},
        )
        self._persist_report(report)
        return report

    def _persist_report(self, report: FOStructureReport) -> None:
        try:
            save_agent_output(f"{self.AGENT_NAME}:{report.symbol}:FO_STRUCTURE_REPORT", report.to_dict())
        except Exception:
            return


def _preferred_option_zone(spot: float, support_zones, resistance_zones) -> dict:
    strongest_support = support_zones[0] if support_zones else None
    strongest_resistance = resistance_zones[0] if resistance_zones else None
    resistance_below = [zone for zone in resistance_zones if zone.strike < spot and zone.strength >= 60]
    support_above = [zone for zone in support_zones if zone.strike > spot and zone.strength >= 60]
    return {
        "bullish_short_leg_near": strongest_resistance.strike if strongest_resistance else None,
        "bearish_short_leg_near": strongest_support.strike if strongest_support else None,
        "avoid_short_call_below": max((zone.strike for zone in resistance_below), default=None),
        "avoid_short_put_above": min((zone.strike for zone in support_above), default=None),
    }


def _build_reasons(base_reasons: list[str], pcr: float | None, pcr_state: str, max_pain: float | None, expiry_risk: str) -> list[str]:
    reasons = list(base_reasons)
    reasons.append(f"PCR is {pcr} ({pcr_state})." if pcr is not None else "PCR is unavailable.")
    if max_pain is not None:
        reasons.append(f"Max pain is near {max_pain}.")
    reasons.append(f"Expiry risk is {expiry_risk}.")
    return reasons[:8]
