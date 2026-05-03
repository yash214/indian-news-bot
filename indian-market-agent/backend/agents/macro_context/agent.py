"""Deterministic Macro Context Agent."""

from __future__ import annotations

from datetime import timedelta

try:
    from backend.agents.agent_output_store import save_agent_output
    from backend.agents.macro_context.factor_rules import is_extreme_macro_shock, score_economic_calendar, score_factor
    from backend.agents.macro_context.macro_scoring import (
        build_major_drivers,
        build_strategy_guidance,
        calculate_report_confidence,
        classify_macro_bias,
        determine_trade_filter,
        summarize_report,
        weighted_macro_score,
    )
    from backend.agents.macro_context.schemas import MacroContextReport, MacroSnapshot
    from backend.core.settings import MACRO_AGENT_SNAPSHOT_TTL_SECONDS
except ModuleNotFoundError:
    from agents.agent_output_store import save_agent_output
    from agents.macro_context.factor_rules import is_extreme_macro_shock, score_economic_calendar, score_factor
    from agents.macro_context.macro_scoring import (
        build_major_drivers,
        build_strategy_guidance,
        calculate_report_confidence,
        classify_macro_bias,
        determine_trade_filter,
        summarize_report,
        weighted_macro_score,
    )
    from agents.macro_context.schemas import MacroContextReport, MacroSnapshot
    from core.settings import MACRO_AGENT_SNAPSHOT_TTL_SECONDS


class MacroContextAgent:
    AGENT_NAME = "macro_context_agent"
    OUTPUT_KEY = f"{AGENT_NAME}:INDIA:MACRO_CONTEXT_REPORT"
    REQUIRED_FACTORS = ("usd_inr", "crude", "india_vix", "global_cues")

    def analyze(self, snapshot: MacroSnapshot) -> MacroContextReport:
        factor_scores = {}
        warnings = []

        for key, factor in snapshot.factors.items():
            score = score_factor(key, factor)
            if score is not None:
                factor_scores[key] = score

        calendar_score, calendar_drivers, severe_event_risk = score_economic_calendar(snapshot.events, now=snapshot.timestamp)
        if calendar_score is not None:
            factor_scores["economic_calendar"] = calendar_score

        for required in self.REQUIRED_FACTORS:
            if required not in snapshot.factors:
                warnings.append(f"Missing macro factor: {required}.")

        source_status = dict(snapshot.source_status or {})
        for provider, status in source_status.items():
            if isinstance(status, dict) and status.get("last_error"):
                warnings.append(f"{provider} warning: {status['last_error']}")

        extreme_risk = False
        for key, factor in snapshot.factors.items():
            if is_extreme_macro_shock(key, factor, factor_scores.get(key)):
                extreme_risk = True
                break
        if severe_event_risk:
            extreme_risk = extreme_risk or False

        macro_bias = classify_macro_bias(factor_scores, extreme_risk=extreme_risk, severe_event_risk=severe_event_risk)
        trade_filter = determine_trade_filter(
            macro_bias,
            factor_scores,
            extreme_risk=extreme_risk,
            has_high_event_risk=bool(calendar_score and calendar_score.bias == "EVENT_RISK"),
        )
        guidance = build_strategy_guidance(macro_bias, trade_filter, factor_scores)
        confidence = calculate_report_confidence(
            factor_scores,
            required_factor_count=len(self.REQUIRED_FACTORS) + 1,
            warning_count=len(warnings),
        )
        if len(snapshot.factors) <= 1:
            confidence = min(confidence, 0.45)
        major_drivers = build_major_drivers(factor_scores)
        for driver in calendar_drivers:
            if driver not in major_drivers:
                major_drivers.append(driver)
        impact_score = min(10, int(round(abs(weighted_macro_score(factor_scores)))))
        report = MacroContextReport(
            agent_name=self.AGENT_NAME,
            generated_at=snapshot.timestamp,
            valid_until=snapshot.timestamp + timedelta(seconds=MACRO_AGENT_SNAPSHOT_TTL_SECONDS),
            stale_after_seconds=MACRO_AGENT_SNAPSHOT_TTL_SECONDS,
            market=snapshot.market,
            macro_bias=macro_bias,
            impact_score=impact_score,
            confidence=confidence,
            trade_filter=trade_filter,
            factors=factor_scores,
            major_drivers=major_drivers[:5],
            strategy_engine_guidance=guidance,
            warnings=warnings,
            source_status=source_status,
            summary=summarize_report(macro_bias, trade_filter, factor_scores),
        )
        self._persist_report(report)
        return report

    def _persist_report(self, report: MacroContextReport) -> None:
        payload = report.to_dict()
        try:
            save_agent_output(self.OUTPUT_KEY, payload)
        except Exception:
            return
