"""Freshness helpers for Execution Health Agent inputs."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

try:
    from backend.agents.execution_health.schemas import (
        CRITICAL,
        FAIL,
        INFO,
        PASS,
        UNKNOWN_CHECK,
        WARN,
        WARNING,
        AgentFreshness,
        HealthCheck,
    )
    from backend.core.settings import IST
except ModuleNotFoundError:
    from agents.execution_health.schemas import CRITICAL, FAIL, INFO, PASS, UNKNOWN_CHECK, WARN, WARNING, AgentFreshness, HealthCheck
    from core.settings import IST


def parse_timestamp(value) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=IST)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=IST)
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=IST)
        except (OverflowError, OSError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None
    if text.replace(".", "", 1).isdigit():
        try:
            return datetime.fromtimestamp(float(text), tz=IST)
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=IST)
    except (TypeError, ValueError):
        return None


def age_seconds(value, now: datetime | None = None) -> float | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    current = _now(now).astimezone(parsed.tzinfo)
    return round(max((current - parsed).total_seconds(), 0.0), 1)


def is_timestamp_in_future(value, now=None, tolerance_seconds=60) -> bool:
    parsed = parse_timestamp(value)
    if parsed is None:
        return False
    current = _now(now).astimezone(parsed.tzinfo)
    return (parsed - current).total_seconds() > tolerance_seconds


def is_report_stale(report: dict, max_age_seconds: int, now=None) -> tuple[bool, float | None, str]:
    generated_at = _report_value(report, "generated_at", "generatedAt")
    valid_until = _report_value(report, "valid_until", "validUntil")
    current = _now(now)

    valid_dt = parse_timestamp(valid_until)
    report_age = age_seconds(generated_at, now=current)
    if valid_dt is not None:
        if current.astimezone(valid_dt.tzinfo) > valid_dt:
            return True, report_age, "Report valid_until has expired."
        return False, report_age, "Report is within valid_until."

    generated_dt = parse_timestamp(generated_at)
    if generated_dt is None:
        return True, None, "Report is missing generated_at and valid_until."

    if report_age is None:
        return True, None, "Report age could not be calculated."
    if report_age > max_age_seconds:
        return True, report_age, f"Report is stale: age {report_age:.1f}s exceeds {max_age_seconds}s."
    return False, report_age, f"Report is fresh: age {report_age:.1f}s is within {max_age_seconds}s."


def check_report_freshness(
    agent_name: str,
    symbol: str,
    report_type: str,
    report: dict | None,
    max_age_seconds: int,
    critical: bool,
    now=None,
) -> tuple[HealthCheck, AgentFreshness]:
    stale_after = _safe_int(_report_value(report, "stale_after_seconds", "staleAfterSeconds")) if isinstance(report, dict) else None
    generated_at = _iso_or_none(_report_value(report, "generated_at", "generatedAt")) if isinstance(report, dict) else None
    valid_until = _iso_or_none(_report_value(report, "valid_until", "validUntil")) if isinstance(report, dict) else None
    name = f"agent_freshness:{agent_name}:{symbol}:{report_type}"
    details = {
        "category": "agent_freshness",
        "agent_name": agent_name,
        "symbol": symbol,
        "report_type": report_type,
        "critical": critical,
        "max_age_seconds": max_age_seconds,
    }

    if not isinstance(report, dict) or not report:
        status = FAIL if critical else WARN
        severity = CRITICAL if critical else WARNING
        message = f"Missing {'critical' if critical else 'optional'} report: {agent_name}:{symbol}:{report_type}."
        details["reason"] = "missing_report"
        check = HealthCheck(name=name, status=status, severity=severity, age_seconds=None, message=message, details=details)
        freshness = AgentFreshness(agent_name, symbol, report_type, status, None, None, None, stale_after, message)
        return check, freshness

    if not generated_at and not valid_until:
        status = UNKNOWN_CHECK
        severity = CRITICAL if critical else WARNING
        message = f"Report freshness is unknown for {agent_name}:{symbol}:{report_type}; timestamps are missing."
        details["reason"] = "missing_timestamps"
        check = HealthCheck(name=name, status=status, severity=severity, age_seconds=None, message=message, details=details)
        freshness = AgentFreshness(agent_name, symbol, report_type, status, None, None, None, stale_after, message)
        return check, freshness

    if is_timestamp_in_future(generated_at, now=now):
        report_age = age_seconds(generated_at, now=now)
        status = FAIL
        severity = CRITICAL if critical else WARNING
        message = f"Report timestamp is in the future for {agent_name}:{symbol}:{report_type}."
        details["reason"] = "future_timestamp"
        check = HealthCheck(name=name, status=status, severity=severity, age_seconds=report_age, message=message, details=details)
        freshness = AgentFreshness(agent_name, symbol, report_type, status, report_age, generated_at, valid_until, stale_after, message)
        return check, freshness

    stale, report_age, stale_message = is_report_stale(report, max_age_seconds, now=now)
    if stale:
        status = FAIL if critical else WARN
        severity = CRITICAL if critical else WARNING
        details["reason"] = "stale_report"
        message = f"{agent_name}:{symbol}:{report_type} stale. {stale_message}"
    else:
        status = PASS
        severity = INFO
        details["reason"] = "fresh_report"
        message = f"{agent_name}:{symbol}:{report_type} fresh. {stale_message}"

    check = HealthCheck(name=name, status=status, severity=severity, age_seconds=report_age, message=message, details=details)
    freshness = AgentFreshness(agent_name, symbol, report_type, status, report_age, generated_at, valid_until, stale_after, message)
    return check, freshness


def _now(now=None) -> datetime:
    if isinstance(now, datetime):
        return now if now.tzinfo else now.replace(tzinfo=IST)
    return datetime.now(IST)


def _report_value(report: dict | None, *keys: str) -> Any:
    if not isinstance(report, dict):
        return None
    for key in keys:
        if report.get(key) not in (None, ""):
            return report.get(key)
    return None


def _iso_or_none(value) -> str | None:
    parsed = parse_timestamp(value)
    return parsed.isoformat() if parsed is not None else None


def _safe_int(value) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None
