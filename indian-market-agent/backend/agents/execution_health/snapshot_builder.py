"""Build Execution Health Agent snapshots from existing runtime state."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Callable

try:
    from backend.agents.agent_output_store import load_latest_agent_report
    from backend.agents.execution_health.schemas import (
        EXECUTION_HEALTH_STALE_AFTER_SECONDS,
        FO_STRUCTURE_MAX_AGE_SECONDS,
        MACRO_CONTEXT_MAX_AGE_SECONDS,
        MARKET_DATA_MAX_AGE_SECONDS,
        MARKET_REGIME_MAX_AGE_SECONDS,
        NEWS_AGENT_MAX_AGE_SECONDS,
        ExecutionHealthSnapshot,
    )
    from backend.core.settings import IST
except ModuleNotFoundError:
    from agents.agent_output_store import load_latest_agent_report
    from agents.execution_health.schemas import EXECUTION_HEALTH_STALE_AFTER_SECONDS, FO_STRUCTURE_MAX_AGE_SECONDS, MACRO_CONTEXT_MAX_AGE_SECONDS, MARKET_DATA_MAX_AGE_SECONDS, MARKET_REGIME_MAX_AGE_SECONDS, NEWS_AGENT_MAX_AGE_SECONDS, ExecutionHealthSnapshot
    from core.settings import IST


LATEST_REPORT_SPECS = (
    ("macro_context:INDIA", "macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT"),
    ("fo_structure:NIFTY", "fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT"),
    ("fo_structure:SENSEX", "fo_structure_agent", "SENSEX", "FO_STRUCTURE_REPORT"),
    ("market_regime:NIFTY", "market_regime_agent", "NIFTY", "MARKET_REGIME_REPORT"),
    ("market_regime:SENSEX", "market_regime_agent", "SENSEX", "MARKET_REGIME_REPORT"),
    ("news:NIFTY", "news_agent", "NIFTY", "NEWS_INDEX_REPORT"),
    ("news:INDIA", "news_agent", "INDIA", "NEWS_INDEX_REPORT"),
)


class ExecutionHealthSnapshotBuilder:
    def __init__(self, context=None, mock_scenario: str | None = None):
        self.context = context
        self.mock_scenario = (mock_scenario or "healthy").strip().lower()

    def build(self, use_mock: bool = False) -> ExecutionHealthSnapshot:
        if use_mock:
            return self.build_mock_snapshot(self.mock_scenario)
        source_status = {"builder": "execution_health_snapshot_builder", "errors": []}
        latest_reports = self._load_latest_reports(source_status)
        return ExecutionHealthSnapshot(
            generated_at=datetime.now(IST),
            latest_reports=latest_reports,
            provider_status=self._collect_provider_status(source_status),
            runtime_status=self._collect_runtime_status(source_status),
            market_session=self._market_session(source_status),
            source_status=source_status,
        )

    def build_mock_snapshot(self, scenario: str = "healthy") -> ExecutionHealthSnapshot:
        scenario = (scenario or "healthy").strip().lower()
        now = datetime.now(IST)
        source_status = {"builder": "execution_health_snapshot_builder", "mock": True, "scenario": scenario}
        if scenario == "startup":
            return ExecutionHealthSnapshot(
                generated_at=now,
                latest_reports={},
                provider_status={},
                runtime_status={},
                market_session={"status": "UNKNOWN", "is_open": None},
                source_status=source_status,
            )

        market_age = MARKET_DATA_MAX_AGE_SECONDS + 180 if scenario == "unhealthy" else 10
        latest_reports = {
            "macro_context:INDIA": _mock_report("macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT", now, MACRO_CONTEXT_MAX_AGE_SECONDS),
            "fo_structure:NIFTY": _mock_report(
                "fo_structure_agent",
                "NIFTY",
                "FO_STRUCTURE_REPORT",
                now - timedelta(seconds=FO_STRUCTURE_MAX_AGE_SECONDS + 180) if scenario == "unhealthy" else now,
                FO_STRUCTURE_MAX_AGE_SECONDS,
            ),
            "market_regime:NIFTY": _mock_report(
                "market_regime_agent",
                "NIFTY",
                "MARKET_REGIME_REPORT",
                now - timedelta(seconds=MARKET_REGIME_MAX_AGE_SECONDS + 180) if scenario == "unhealthy" else now,
                MARKET_REGIME_MAX_AGE_SECONDS,
            ),
            "news:NIFTY": _mock_report(
                "news_agent",
                "NIFTY",
                "NEWS_INDEX_REPORT",
                now - timedelta(seconds=NEWS_AGENT_MAX_AGE_SECONDS + 120) if scenario == "degraded" else now,
                NEWS_AGENT_MAX_AGE_SECONDS,
            ),
        }
        provider_status = {
            "market_data_provider_status": {
                "requested": "upstox" if scenario == "degraded" else "nse",
                "active": "nse",
                "upstoxConfigured": scenario != "degraded",
                "fallbackToNse": scenario == "degraded",
                "degraded": scenario == "degraded",
                "reason": "Mock fallback provider used" if scenario == "degraded" else "Mock NSE public endpoints enabled",
            },
            "upstox_runtime_status": {
                "configured": scenario != "degraded",
                "tokenSource": "none",
                "stream": {"connected": False, "lastError": None},
                "rest": {"lastError": "Mock optional Upstox unavailable"} if scenario == "degraded" else {"lastError": None},
                "readOnly": True,
            },
        }
        runtime_status = {
            "market_runtime_status": {
                "tickCount": 3,
                "trackedQuoteCount": 2,
                "lastTickAt": (now - timedelta(seconds=market_age)).timestamp(),
                "readOnly": True,
            },
            "fo_runtime_status": {"enabled": True, "read_only": True},
            "market_regime_runtime_status": {"enabled": True, "read_only": True},
            "macro_runtime_status": {"enabled": True, "read_only": True},
            "news_runtime_status": {"enabled": True, "latest_count": 4, "read_only": True},
            "upstox_runtime_status": provider_status["upstox_runtime_status"],
        }
        return ExecutionHealthSnapshot(
            generated_at=now,
            latest_reports=latest_reports,
            provider_status=provider_status,
            runtime_status=runtime_status,
            market_session={"status": "OPEN", "session": "open", "is_open": True, "isMarketOpen": True},
            source_status=source_status,
        )

    def _load_latest_reports(self, source_status: dict) -> dict:
        latest: dict[str, dict] = {}
        loaded = []
        for key, agent_name, symbol, report_type in LATEST_REPORT_SPECS:
            try:
                report = load_latest_agent_report(agent_name, symbol, report_type)
            except Exception as exc:
                source_status["errors"].append(f"{key}: {str(exc)[:160]}")
                report = None
            if report:
                latest[key] = _jsonable(report)
                loaded.append(key)
        source_status["latest_reports_loaded"] = loaded
        return latest

    def _collect_provider_status(self, source_status: dict) -> dict:
        return _collect_context_statuses(
            self.context,
            (
                "provider_status",
                "market_data_provider_status",
                "upstox_runtime_status",
                "upstox_stream_runtime_status",
                "upstox_rest_runtime_status",
            ),
            source_status,
        )

    def _collect_runtime_status(self, source_status: dict) -> dict:
        return _collect_context_statuses(
            self.context,
            (
                "market_runtime_status",
                "news_runtime_status",
                "macro_runtime_status",
                "fo_runtime_status",
                "market_regime_runtime_status",
                "upstox_runtime_status",
                "analytics_runtime_status",
                "background_runtime_status",
            ),
            source_status,
        )

    def _market_session(self, source_status: dict) -> dict:
        for name in ("get_market_status", "active_market_status", "market_status"):
            candidate = _context_callable(self.context, name)
            if candidate is None:
                continue
            try:
                value = candidate()
                return _jsonable(value) if isinstance(value, dict) else {"status": str(value), "is_open": None}
            except Exception as exc:
                source_status.setdefault("errors", []).append(f"{name}: {str(exc)[:160]}")
        value = _context_value(self.context, "market_status")
        if isinstance(value, dict):
            return _jsonable(value)
        return {"status": "UNKNOWN", "is_open": None}


def _mock_report(agent_name: str, symbol: str, report_type: str, generated_at: datetime, max_age_seconds: int) -> dict:
    return {
        "agent_name": agent_name,
        "symbol": symbol,
        "report_type": report_type,
        "generated_at": generated_at.isoformat(),
        "valid_until": (generated_at + timedelta(seconds=max_age_seconds)).isoformat(),
        "stale_after_seconds": max_age_seconds,
        "confidence": 0.82,
    }


def _collect_context_statuses(context, names: tuple[str, ...], source_status: dict) -> dict:
    statuses: dict[str, dict] = {}
    for name in names:
        fn = _context_callable(context, name)
        if fn is None:
            continue
        try:
            value = fn()
        except Exception as exc:
            source_status.setdefault("errors", []).append(f"{name}: {str(exc)[:160]}")
            continue
        statuses[name] = _jsonable(value if isinstance(value, dict) else {"value": value})
    return statuses


def _context_callable(context: Any, name: str) -> Callable | None:
    value = _context_value(context, name)
    return value if callable(value) else None


def _context_value(context: Any, name: str, default=None):
    if context is None:
        return default
    try:
        value = getattr(context, name)
        return default if value is None else value
    except AttributeError:
        runtime_state = getattr(context, "runtime_state", None)
        if isinstance(runtime_state, dict):
            return runtime_state.get(name, default)
        return default


def _jsonable(value):
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return _jsonable(value.to_dict())
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value
