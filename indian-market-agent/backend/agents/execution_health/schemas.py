"""Typed payloads for the Execution Health Agent."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any


MARKET_DATA_MAX_AGE_SECONDS = 90
MARKET_REGIME_MAX_AGE_SECONDS = 360
FO_STRUCTURE_MAX_AGE_SECONDS = 360
OPTION_CHAIN_MAX_AGE_SECONDS = 300
MACRO_CONTEXT_MAX_AGE_SECONDS = 3600
NEWS_AGENT_MAX_AGE_SECONDS = 3600
PROVIDER_STATUS_MAX_AGE_SECONDS = 300
EXECUTION_HEALTH_STALE_AFTER_SECONDS = 60

HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
UNHEALTHY = "UNHEALTHY"
UNKNOWN = "UNKNOWN"
OVERALL_HEALTH_STATES = {HEALTHY, DEGRADED, UNHEALTHY, UNKNOWN}

PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"
UNKNOWN_CHECK = "UNKNOWN"
CHECK_STATUSES = {PASS, WARN, FAIL, UNKNOWN_CHECK}

INFO = "INFO"
WARNING = "WARNING"
CRITICAL = "CRITICAL"
SEVERITIES = {INFO, WARNING, CRITICAL}

PROVIDER_OK = "OK"
PROVIDER_DEGRADED = "DEGRADED"
PROVIDER_FAIL = "FAIL"
PROVIDER_UNKNOWN = "UNKNOWN"
PROVIDER_STATUSES = {PROVIDER_OK, PROVIDER_DEGRADED, PROVIDER_FAIL, PROVIDER_UNKNOWN}


def serialize(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {str(key): serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [serialize(item) for item in value]
    return value


@dataclass
class HealthCheck:
    name: str
    status: str
    severity: str
    age_seconds: float | None
    message: str
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return serialize(asdict(self))


@dataclass
class AgentFreshness:
    agent_name: str
    symbol: str
    report_type: str
    status: str
    age_seconds: float | None
    generated_at: str | None
    valid_until: str | None
    stale_after_seconds: int | None
    message: str

    def to_dict(self) -> dict:
        return serialize(asdict(self))


@dataclass
class ProviderHealth:
    provider: str
    status: str
    enabled: bool | None
    configured: bool | None
    last_success_at: str | None
    last_error: str | None
    using_fallback: bool
    stale: bool
    message: str

    def to_dict(self) -> dict:
        return serialize(asdict(self))


@dataclass
class StrategyEngineHealthGuidance:
    allow_strategy_evaluation: bool
    allow_trade_proposal: bool
    allow_live_execution: bool
    reduce_confidence: bool
    confidence_penalty: int
    notes: str

    def to_dict(self) -> dict:
        return serialize(asdict(self))


@dataclass
class ExecutionHealthSnapshot:
    generated_at: datetime
    latest_reports: dict = field(default_factory=dict)
    provider_status: dict = field(default_factory=dict)
    runtime_status: dict = field(default_factory=dict)
    market_session: dict = field(default_factory=dict)
    source_status: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return serialize(asdict(self))


@dataclass
class ExecutionHealthReport:
    agent_name: str
    symbol: str
    generated_at: datetime
    valid_until: datetime
    stale_after_seconds: int
    overall_health: str
    trade_allowed: bool
    fresh_trade_blocked: bool
    confidence: float
    health_score: int
    checks: list[HealthCheck] = field(default_factory=list)
    provider_status: dict = field(default_factory=dict)
    agent_freshness: dict = field(default_factory=dict)
    runtime_status: dict = field(default_factory=dict)
    market_session: dict = field(default_factory=dict)
    blockers: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    strategy_engine_guidance: StrategyEngineHealthGuidance = field(
        default_factory=lambda: StrategyEngineHealthGuidance(
            allow_strategy_evaluation=True,
            allow_trade_proposal=False,
            allow_live_execution=False,
            reduce_confidence=True,
            confidence_penalty=25,
            notes="Execution health has not been established.",
        )
    )
    source_status: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return serialize(asdict(self))


def check_to_dict(check: HealthCheck | dict) -> dict:
    return check.to_dict() if hasattr(check, "to_dict") else serialize(check)
