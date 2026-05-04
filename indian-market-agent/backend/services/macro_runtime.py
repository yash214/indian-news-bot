"""Runtime helpers for the Macro Context Agent."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

try:
    from backend.agents.agent_output_store import load_latest_agent_report
    from backend.agents.macro_context import MacroContextAgent
    from backend.agents.macro_context.schedule import get_next_macro_refresh_time, is_macro_refresh_due
    from backend.agents.macro_context.snapshot_builder import MacroSnapshotBuilder
    from backend.core.settings import (
        FMP_ENABLED,
        MACRO_AGENT_ENABLED,
        MACRO_AGENT_REFRESH_MODE,
        MACRO_AGENT_SNAPSHOT_TTL_SECONDS,
        MACRO_AGENT_TIMEZONE,
    )
    from backend.providers.fmp import FMPProvider
    from backend.providers.india_vix_provider import IndiaVixProvider
except ModuleNotFoundError:
    from agents.agent_output_store import load_latest_agent_report
    from agents.macro_context import MacroContextAgent
    from agents.macro_context.schedule import get_next_macro_refresh_time, is_macro_refresh_due
    from agents.macro_context.snapshot_builder import MacroSnapshotBuilder
    from core.settings import (
        FMP_ENABLED,
        MACRO_AGENT_ENABLED,
        MACRO_AGENT_REFRESH_MODE,
        MACRO_AGENT_SNAPSHOT_TTL_SECONDS,
        MACRO_AGENT_TIMEZONE,
    )
    from providers.fmp import FMPProvider
    from providers.india_vix_provider import IndiaVixProvider


def build_macro_snapshot(use_mock: bool = False, context=None):
    builder = MacroSnapshotBuilder(
        fmp_provider=FMPProvider(),
        india_vix_provider=IndiaVixProvider(fetcher=_context_callable(context, "current_india_vix_quote")),
    )
    return builder.build_mock_snapshot() if use_mock else builder.build()


def run_macro_context_cycle(
    force_refresh: bool = False,
    use_mock: bool = False,
    context=None,
):
    snapshot = build_macro_snapshot(use_mock=use_mock, context=context)
    report = MacroContextAgent().analyze(snapshot)
    if force_refresh:
        print("[*] Macro context refresh forced via API or worker call")
    return report


def get_latest_macro_context_report():
    return load_latest_agent_report(
        MacroContextAgent.AGENT_NAME,
        "INDIA",
        "MACRO_CONTEXT_REPORT",
    )


def macro_runtime_status() -> dict:
    fmp_provider = FMPProvider()
    return {
        "agent": MacroContextAgent.AGENT_NAME,
        "enabled": MACRO_AGENT_ENABLED,
        "refresh_mode": MACRO_AGENT_REFRESH_MODE,
        "snapshot_ttl_seconds": MACRO_AGENT_SNAPSHOT_TTL_SECONDS,
        "timezone": MACRO_AGENT_TIMEZONE,
        "fmp_enabled": FMP_ENABLED,
        "fmp_configured": fmp_provider.is_configured(),
        "india_vix_source": "runtime_context",
        "read_only": True,
    }


def macro_refresh_due(now: datetime, last_run_at: datetime | None) -> bool:
    return (
        MACRO_AGENT_ENABLED
        and MACRO_AGENT_REFRESH_MODE == "scheduled"
        and is_macro_refresh_due(now, last_run_at)
    )


def next_macro_refresh_time(now: datetime):
    return get_next_macro_refresh_time(now)


def _context_callable(context: Any, name: str) -> Callable | None:
    if context is None:
        return None
    try:
        candidate = getattr(context, name)
    except AttributeError:
        return None
    return candidate if callable(candidate) else None
