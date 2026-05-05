"""Execution health scoring and strategy-gate guidance."""

from __future__ import annotations

try:
    from backend.agents.execution_health.schemas import (
        CRITICAL,
        DEGRADED,
        FAIL,
        HEALTHY,
        PASS,
        UNHEALTHY,
        UNKNOWN,
        UNKNOWN_CHECK,
        WARN,
        HealthCheck,
        StrategyEngineHealthGuidance,
        check_to_dict,
    )
except ModuleNotFoundError:
    from agents.execution_health.schemas import CRITICAL, DEGRADED, FAIL, HEALTHY, PASS, UNHEALTHY, UNKNOWN, UNKNOWN_CHECK, WARN, HealthCheck, StrategyEngineHealthGuidance, check_to_dict


def score_execution_health(
    checks: list[HealthCheck],
    market_session: dict | None = None,
) -> dict:
    check_dicts = [check_to_dict(check) for check in checks or []]
    if not check_dicts:
        return _result(UNKNOWN, 0, blockers=[], warnings=["No execution-health checks were available."])

    useful_checks = [check for check in check_dicts if check.get("status") == PASS or check.get("details", {}).get("reason") not in {"missing_report", "missing_runtime"}]
    if not useful_checks:
        return _result(UNKNOWN, 0, blockers=[], warnings=["Not enough runtime or agent-report data to determine execution health."])

    health_score = 100
    blockers: list[str] = []
    warnings: list[str] = []
    has_warning = False
    critical_runtime_fail = False
    for check in check_dicts:
        status = check.get("status")
        severity = check.get("severity")
        if status == PASS:
            continue
        has_warning = True
        health_score -= _penalty_for_check(check)
        message = str(check.get("message") or "Execution health check did not pass.")
        if status == FAIL and severity == CRITICAL:
            blockers.append(message)
        else:
            warnings.append(message)
        if status == FAIL and severity == CRITICAL and _is_market_runtime_critical(check):
            critical_runtime_fail = True

    health_score = max(0, min(100, int(round(health_score))))
    market_live = _market_live_or_unknown(market_session)
    if critical_runtime_fail and market_live:
        overall = UNHEALTHY
    elif health_score >= 85 and not has_warning:
        overall = HEALTHY
    elif health_score >= 60:
        overall = DEGRADED
    elif health_score > 0:
        overall = UNHEALTHY
    else:
        overall = UNHEALTHY
    return _result(overall, health_score, blockers=blockers, warnings=warnings)


def _penalty_for_check(check: dict) -> int:
    status = check.get("status")
    details = check.get("details") or {}
    name = str(check.get("name") or "").lower()
    report_type = str(details.get("report_type") or "").upper()
    category = str(details.get("category") or "").lower()
    provider = str(details.get("provider") or "").lower()
    critical = bool(details.get("critical"))

    if status == UNKNOWN_CHECK and critical:
        return 20
    if "market_data" in name or category == "market_data":
        return 40
    if report_type == "FO_STRUCTURE_REPORT" or "fo_structure" in name:
        return 35
    if report_type == "MARKET_REGIME_REPORT" or "market_regime" in name:
        return 30
    if "upstox" in provider and critical:
        return 25
    if "option" in name or "option_chain" in name:
        return 20
    if report_type == "MACRO_CONTEXT_REPORT" or "macro_context" in name:
        return 15
    if report_type == "NEWS_INDEX_REPORT" or "news_agent" in name:
        return 10
    if category == "provider":
        return 10 + (10 if details.get("using_fallback") else 0)
    if status == UNKNOWN_CHECK:
        return 10
    return 10


def _result(overall: str, health_score: int, *, blockers: list[str], warnings: list[str]) -> dict:
    if overall == UNKNOWN:
        confidence = 0.35
    else:
        confidence = max(0.35, min(0.99, health_score / 100))
    guidance = _guidance(overall, health_score, blockers)
    return {
        "overall_health": overall,
        "trade_allowed": _trade_allowed(overall, blockers),
        "fresh_trade_blocked": _fresh_trade_blocked(overall, blockers),
        "confidence": confidence,
        "health_score": health_score,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "strategy_engine_guidance": guidance,
    }


def _guidance(overall: str, health_score: int, blockers: list[str]) -> StrategyEngineHealthGuidance:
    if overall == HEALTHY:
        return StrategyEngineHealthGuidance(
            allow_strategy_evaluation=True,
            allow_trade_proposal=True,
            allow_live_execution=False,
            reduce_confidence=False,
            confidence_penalty=0,
            notes="System health is sufficient for strategy evaluation and read-only trade proposal generation.",
        )
    if overall == DEGRADED:
        penalty = max(10, min(25, 85 - max(60, health_score)))
        allow_trade_proposal = not blockers
        return StrategyEngineHealthGuidance(
            allow_strategy_evaluation=True,
            allow_trade_proposal=allow_trade_proposal,
            allow_live_execution=False,
            reduce_confidence=True,
            confidence_penalty=penalty,
            notes="System health is degraded; strategy evaluation can continue with lower confidence.",
        )
    if overall == UNHEALTHY:
        return StrategyEngineHealthGuidance(
            allow_strategy_evaluation=True,
            allow_trade_proposal=False,
            allow_live_execution=False,
            reduce_confidence=True,
            confidence_penalty=40,
            notes="Critical health checks failed; fresh trade proposals must be blocked.",
        )
    return StrategyEngineHealthGuidance(
        allow_strategy_evaluation=True,
        allow_trade_proposal=False,
        allow_live_execution=False,
        reduce_confidence=True,
        confidence_penalty=25,
        notes="Execution health is unknown; keep dashboard evaluation read-only and block fresh proposals.",
    )


def _trade_allowed(overall: str, blockers: list[str]) -> bool:
    if overall == HEALTHY:
        return True
    if overall == DEGRADED:
        return not blockers
    return False


def _fresh_trade_blocked(overall: str, blockers: list[str]) -> bool:
    if overall in {UNHEALTHY, UNKNOWN}:
        return True
    return bool(blockers)


def _is_market_runtime_critical(check: dict) -> bool:
    details = check.get("details") or {}
    name = str(check.get("name") or "").lower()
    report_type = str(details.get("report_type") or "").upper()
    category = str(details.get("category") or "").lower()
    return (
        category == "market_data"
        or "market_data" in name
        or report_type in {"MARKET_REGIME_REPORT", "FO_STRUCTURE_REPORT"}
    )


def _market_live_or_unknown(market_session: dict | None) -> bool:
    if not isinstance(market_session, dict) or not market_session:
        return True
    if market_session.get("isMarketOpen") is not None:
        return bool(market_session.get("isMarketOpen"))
    if market_session.get("is_open") is not None:
        return bool(market_session.get("is_open"))
    session = str(market_session.get("session") or market_session.get("status") or "").lower()
    if session in {"closed", "postclose", "weekend", "holiday"}:
        return False
    return True
