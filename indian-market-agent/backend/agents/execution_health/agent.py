"""Deterministic Execution Health Agent."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from backend.agents.agent_output_store import save_agent_report
    from backend.agents.execution_health.freshness import age_seconds, check_report_freshness
    from backend.agents.execution_health.provider_checks import check_provider_statuses
    from backend.agents.execution_health.scoring import score_execution_health
    from backend.agents.execution_health.schemas import (
        CRITICAL,
        EXECUTION_HEALTH_STALE_AFTER_SECONDS,
        FAIL,
        FO_STRUCTURE_MAX_AGE_SECONDS,
        HEALTHY,
        INFO,
        MACRO_CONTEXT_MAX_AGE_SECONDS,
        MARKET_DATA_MAX_AGE_SECONDS,
        MARKET_REGIME_MAX_AGE_SECONDS,
        NEWS_AGENT_MAX_AGE_SECONDS,
        PASS,
        UNKNOWN,
        UNKNOWN_CHECK,
        WARNING,
        AgentFreshness,
        ExecutionHealthReport,
        ExecutionHealthSnapshot,
        HealthCheck,
        StrategyEngineHealthGuidance,
    )
    from backend.core.settings import IST
except ModuleNotFoundError:
    from agents.agent_output_store import save_agent_report
    from agents.execution_health.freshness import age_seconds, check_report_freshness
    from agents.execution_health.provider_checks import check_provider_statuses
    from agents.execution_health.scoring import score_execution_health
    from agents.execution_health.schemas import CRITICAL, EXECUTION_HEALTH_STALE_AFTER_SECONDS, FAIL, FO_STRUCTURE_MAX_AGE_SECONDS, HEALTHY, INFO, MACRO_CONTEXT_MAX_AGE_SECONDS, MARKET_DATA_MAX_AGE_SECONDS, MARKET_REGIME_MAX_AGE_SECONDS, NEWS_AGENT_MAX_AGE_SECONDS, PASS, UNKNOWN, UNKNOWN_CHECK, WARNING, AgentFreshness, ExecutionHealthReport, ExecutionHealthSnapshot, HealthCheck, StrategyEngineHealthGuidance
    from core.settings import IST


class ExecutionHealthAgent:
    AGENT_NAME = "execution_health_agent"
    SYMBOL = "SYSTEM"
    REPORT_TYPE = "EXECUTION_HEALTH_REPORT"

    def analyze(self, snapshot: ExecutionHealthSnapshot | None) -> ExecutionHealthReport:
        now = datetime.now(IST)
        if snapshot is None:
            report = self._unknown_report(now, "Execution health snapshot unavailable")
            self._persist_report(report)
            return report

        checks: list[HealthCheck] = []
        freshness: dict[str, dict] = {}
        latest_reports = snapshot.latest_reports if isinstance(snapshot.latest_reports, dict) else {}
        for key, agent_name, symbol, report_type, threshold, critical, required in self._report_specs(latest_reports):
            report = latest_reports.get(key)
            if report is None and not required:
                continue
            check, item = check_report_freshness(
                agent_name=agent_name,
                symbol=symbol,
                report_type=report_type,
                report=report,
                max_age_seconds=threshold,
                critical=critical,
                now=now,
            )
            checks.append(check)
            freshness[key] = item.to_dict()

        checks.append(self._market_data_check(snapshot, now))
        checks.extend(self._runtime_checks(snapshot))
        provider_checks, normalized_provider_status, provider_blockers, provider_warnings = check_provider_statuses(snapshot.provider_status)
        checks.extend(provider_checks)

        score = score_execution_health(checks, market_session=snapshot.market_session)
        blockers = _unique(score["blockers"] + provider_blockers)
        warnings = _unique(score["warnings"] + provider_warnings)
        generated_at = now
        report = ExecutionHealthReport(
            agent_name=self.AGENT_NAME,
            symbol=self.SYMBOL,
            generated_at=generated_at,
            valid_until=generated_at + timedelta(seconds=EXECUTION_HEALTH_STALE_AFTER_SECONDS),
            stale_after_seconds=EXECUTION_HEALTH_STALE_AFTER_SECONDS,
            overall_health=score["overall_health"],
            trade_allowed=score["trade_allowed"],
            fresh_trade_blocked=score["fresh_trade_blocked"],
            confidence=round(float(score["confidence"]), 3),
            health_score=int(score["health_score"]),
            checks=checks,
            provider_status=normalized_provider_status,
            agent_freshness=freshness,
            runtime_status=snapshot.runtime_status if isinstance(snapshot.runtime_status, dict) else {},
            market_session=snapshot.market_session if isinstance(snapshot.market_session, dict) else {},
            blockers=blockers,
            warnings=warnings,
            strategy_engine_guidance=score["strategy_engine_guidance"],
            source_status=snapshot.source_status if isinstance(snapshot.source_status, dict) else {},
        )
        self._persist_report(report)
        return report

    def _report_specs(self, latest_reports: dict) -> list[tuple[str, str, str, str, int, bool, bool]]:
        specs = [
            ("market_regime:NIFTY", "market_regime_agent", "NIFTY", "MARKET_REGIME_REPORT", MARKET_REGIME_MAX_AGE_SECONDS, True, True),
            ("fo_structure:NIFTY", "fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT", FO_STRUCTURE_MAX_AGE_SECONDS, True, True),
            ("macro_context:INDIA", "macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT", MACRO_CONTEXT_MAX_AGE_SECONDS, False, True),
        ]
        if "market_regime:SENSEX" in latest_reports:
            specs.append(("market_regime:SENSEX", "market_regime_agent", "SENSEX", "MARKET_REGIME_REPORT", MARKET_REGIME_MAX_AGE_SECONDS, False, False))
        if "fo_structure:SENSEX" in latest_reports:
            specs.append(("fo_structure:SENSEX", "fo_structure_agent", "SENSEX", "FO_STRUCTURE_REPORT", FO_STRUCTURE_MAX_AGE_SECONDS, False, False))
        if "news:NIFTY" in latest_reports:
            specs.append(("news:NIFTY", "news_agent", "NIFTY", "NEWS_INDEX_REPORT", NEWS_AGENT_MAX_AGE_SECONDS, False, True))
        elif "news:INDIA" in latest_reports:
            specs.append(("news:INDIA", "news_agent", "INDIA", "NEWS_INDEX_REPORT", NEWS_AGENT_MAX_AGE_SECONDS, False, True))
        else:
            specs.append(("news:NIFTY", "news_agent", "NIFTY", "NEWS_INDEX_REPORT", NEWS_AGENT_MAX_AGE_SECONDS, False, True))
        return specs

    def _market_data_check(self, snapshot: ExecutionHealthSnapshot, now: datetime) -> HealthCheck:
        runtime = snapshot.runtime_status if isinstance(snapshot.runtime_status, dict) else {}
        market_runtime = runtime.get("market_runtime_status") if isinstance(runtime.get("market_runtime_status"), dict) else {}
        market_live = _market_is_live(snapshot.market_session)
        market_unknown = _market_session_unknown(snapshot.market_session)
        details = {
            "category": "market_data",
            "critical": bool(market_live),
            "max_age_seconds": MARKET_DATA_MAX_AGE_SECONDS,
        }
        if not market_runtime:
            details["reason"] = "missing_runtime"
            return HealthCheck(
                name="market_data:freshness",
                status=UNKNOWN_CHECK,
                severity=WARNING,
                age_seconds=None,
                message="Market runtime status unavailable.",
                details=details,
            )
        tick_count = int(_safe_number(market_runtime.get("tickCount"), 0) or 0)
        last_tick = market_runtime.get("lastTickAt") or market_runtime.get("last_tick_at")
        tick_age = age_seconds(last_tick, now=now)
        details["tick_count"] = tick_count
        if tick_age is None:
            details["reason"] = "missing_market_tick_timestamp"
            status = FAIL if market_live and not market_unknown else UNKNOWN_CHECK
            severity = CRITICAL if status == FAIL else WARNING
            message = "Market quote data has no latest tick timestamp."
        elif tick_age > MARKET_DATA_MAX_AGE_SECONDS:
            details["reason"] = "stale_market_data"
            status = FAIL if market_live else UNKNOWN_CHECK
            severity = CRITICAL if status == FAIL else WARNING
            message = f"Market quote data stale: age {tick_age:.1f}s exceeds {MARKET_DATA_MAX_AGE_SECONDS}s."
        elif tick_count <= 0:
            details["reason"] = "missing_market_ticks"
            status = FAIL if market_live else UNKNOWN_CHECK
            severity = CRITICAL if status == FAIL else WARNING
            message = "Market runtime has no ticker data."
        else:
            details["reason"] = "fresh_market_data"
            status = PASS
            severity = INFO
            message = f"Market quote data fresh: age {tick_age:.1f}s."
        return HealthCheck(
            name="market_data:freshness",
            status=status,
            severity=severity,
            age_seconds=tick_age,
            message=message,
            details=details,
        )

    def _runtime_checks(self, snapshot: ExecutionHealthSnapshot) -> list[HealthCheck]:
        runtime = snapshot.runtime_status if isinstance(snapshot.runtime_status, dict) else {}
        checks = []
        for name in ("market_runtime_status", "upstox_runtime_status"):
            status = runtime.get(name)
            if not isinstance(status, dict):
                checks.append(HealthCheck(
                    name=f"runtime:{name}",
                    status=UNKNOWN_CHECK,
                    severity=WARNING,
                    age_seconds=None,
                    message=f"{name} unavailable.",
                    details={"category": "runtime", "runtime": name, "reason": "missing_runtime", "critical": False},
                ))
                continue
            last_error = _nested_error(status)
            if last_error:
                checks.append(HealthCheck(
                    name=f"runtime:{name}",
                    status=UNKNOWN_CHECK,
                    severity=WARNING,
                    age_seconds=None,
                    message=f"{name} reports error: {last_error}",
                    details={"category": "runtime", "runtime": name, "reason": "runtime_error", "critical": False},
                ))
            else:
                checks.append(HealthCheck(
                    name=f"runtime:{name}",
                    status=PASS,
                    severity=INFO,
                    age_seconds=None,
                    message=f"{name} available.",
                    details={"category": "runtime", "runtime": name, "reason": "runtime_available", "critical": False},
                ))
        return checks

    def _unknown_report(self, now: datetime, message: str) -> ExecutionHealthReport:
        check = HealthCheck(
            name="execution_health:snapshot",
            status=UNKNOWN_CHECK,
            severity=CRITICAL,
            age_seconds=None,
            message=message,
            details={"category": "execution_health", "critical": True, "reason": "missing_snapshot"},
        )
        guidance = StrategyEngineHealthGuidance(
            allow_strategy_evaluation=True,
            allow_trade_proposal=False,
            allow_live_execution=False,
            reduce_confidence=True,
            confidence_penalty=25,
            notes="Execution health snapshot is unavailable; keep evaluation read-only and block fresh proposals.",
        )
        return ExecutionHealthReport(
            agent_name=self.AGENT_NAME,
            symbol=self.SYMBOL,
            generated_at=now,
            valid_until=now + timedelta(seconds=EXECUTION_HEALTH_STALE_AFTER_SECONDS),
            stale_after_seconds=EXECUTION_HEALTH_STALE_AFTER_SECONDS,
            overall_health=UNKNOWN,
            trade_allowed=False,
            fresh_trade_blocked=True,
            confidence=0.35,
            health_score=0,
            checks=[check],
            blockers=[message],
            strategy_engine_guidance=guidance,
            source_status={"error": message},
        )

    def _persist_report(self, report: ExecutionHealthReport) -> None:
        payload = report.to_dict()
        try:
            save_agent_report(
                agent_name=self.AGENT_NAME,
                symbol=self.SYMBOL,
                report_type=self.REPORT_TYPE,
                payload=payload,
                generated_at=report.generated_at.isoformat(),
                valid_until=report.valid_until.isoformat(),
                stale_after_seconds=report.stale_after_seconds,
                bias=report.overall_health,
                confidence=report.confidence,
                ruleset_version="execution_health_rules_v1",
                agent_version="1.0.0",
            )
        except Exception:
            return


def _market_is_live(market_session: dict | None) -> bool:
    if not isinstance(market_session, dict):
        return False
    if market_session.get("isMarketOpen") is not None:
        return bool(market_session.get("isMarketOpen"))
    if market_session.get("is_open") is not None:
        return bool(market_session.get("is_open"))
    session = str(market_session.get("session") or market_session.get("status") or "").lower()
    return session in {"open", "preopen"}


def _market_session_unknown(market_session: dict | None) -> bool:
    if not isinstance(market_session, dict) or not market_session:
        return True
    if market_session.get("isMarketOpen") is not None or market_session.get("is_open") is not None:
        return False
    session = str(market_session.get("session") or market_session.get("status") or "").lower()
    return session in {"", "unknown"}


def _nested_error(status: dict) -> str | None:
    for key in ("lastError", "last_error", "error"):
        if status.get(key):
            return str(status[key])[:240]
    for key in ("rest", "stream"):
        nested = status.get(key)
        if isinstance(nested, dict):
            error = _nested_error(nested)
            if error:
                return error
    return None


def _safe_number(value, default=None):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _unique(values: list[str]) -> list[str]:
    out = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out
