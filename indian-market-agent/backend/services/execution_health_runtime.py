"""Runtime helpers for the Execution Health Agent."""

from __future__ import annotations

try:
    from backend.agents.agent_output_store import load_latest_agent_report
    from backend.agents.execution_health import ExecutionHealthAgent
    from backend.agents.execution_health.schemas import EXECUTION_HEALTH_STALE_AFTER_SECONDS
    from backend.agents.execution_health.snapshot_builder import ExecutionHealthSnapshotBuilder
except ModuleNotFoundError:
    from agents.agent_output_store import load_latest_agent_report
    from agents.execution_health import ExecutionHealthAgent
    from agents.execution_health.schemas import EXECUTION_HEALTH_STALE_AFTER_SECONDS
    from agents.execution_health.snapshot_builder import ExecutionHealthSnapshotBuilder


_last_error: str | None = None


def build_execution_health_snapshot(use_mock: bool = False, context=None, mock_scenario: str | None = None, scenario: str | None = None):
    builder = ExecutionHealthSnapshotBuilder(context=context, mock_scenario=mock_scenario or scenario)
    return builder.build(use_mock=use_mock)


def run_execution_health_cycle(
    force_refresh: bool = False,
    use_mock: bool = False,
    context=None,
    mock_scenario: str | None = None,
    scenario: str | None = None,
):
    global _last_error
    try:
        snapshot = build_execution_health_snapshot(use_mock=use_mock, context=context, mock_scenario=mock_scenario, scenario=scenario)
        report = ExecutionHealthAgent().analyze(snapshot)
        _last_error = None
        if force_refresh:
            print("[*] Execution health refresh forced via API or worker call")
        return report
    except Exception as exc:
        _last_error = str(exc)[:240]
        return ExecutionHealthAgent().analyze(None)


def get_latest_execution_health_report():
    return load_latest_agent_report(
        ExecutionHealthAgent.AGENT_NAME,
        ExecutionHealthAgent.SYMBOL,
        ExecutionHealthAgent.REPORT_TYPE,
    )


def execution_health_runtime_status() -> dict:
    latest = get_latest_execution_health_report()
    return {
        "agent": ExecutionHealthAgent.AGENT_NAME,
        "symbol": ExecutionHealthAgent.SYMBOL,
        "report_type": ExecutionHealthAgent.REPORT_TYPE,
        "enabled": True,
        "stale_after_seconds": EXECUTION_HEALTH_STALE_AFTER_SECONDS,
        "latest_report_available": bool(latest),
        "latest_overall_health": latest.get("overall_health") if isinstance(latest, dict) else None,
        "last_error": _last_error,
        "read_only": True,
    }
