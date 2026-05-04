"""Runtime helpers for the F&O Structure Agent."""

from __future__ import annotations

try:
    from backend.agents.agent_output_store import load_latest_agent_report
    from backend.agents.fo_structure import FOStructureAgent
    from backend.agents.fo_structure.schemas import is_supported_symbol, normalize_fo_symbol
    from backend.agents.fo_structure.snapshot_builder import FOSnapshotBuilder, build_mock_option_chain_snapshot
    from backend.core.settings import (
        FO_AGENT_ENABLED,
        FO_AGENT_REFRESH_SECONDS,
        FO_AGENT_TIMEZONE,
        UPSTOX_OPTIONS_CACHE_TTL_SECONDS,
        UPSTOX_OPTIONS_ENABLED,
        UPSTOX_OPTIONS_TIMEOUT_SECONDS,
    )
    from backend.providers.upstox.options_provider import UpstoxOptionsProvider
except ModuleNotFoundError:
    from agents.agent_output_store import load_latest_agent_report
    from agents.fo_structure import FOStructureAgent
    from agents.fo_structure.schemas import is_supported_symbol, normalize_fo_symbol
    from agents.fo_structure.snapshot_builder import FOSnapshotBuilder, build_mock_option_chain_snapshot
    from core.settings import (
        FO_AGENT_ENABLED,
        FO_AGENT_REFRESH_SECONDS,
        FO_AGENT_TIMEZONE,
        UPSTOX_OPTIONS_CACHE_TTL_SECONDS,
        UPSTOX_OPTIONS_ENABLED,
        UPSTOX_OPTIONS_TIMEOUT_SECONDS,
    )
    from providers.upstox.options_provider import UpstoxOptionsProvider


def build_fo_snapshot(
    symbol: str = "NIFTY",
    expiry: str | None = None,
    use_mock: bool = False,
    context=None,
):
    clean = normalize_fo_symbol(symbol)
    if use_mock:
        return build_mock_option_chain_snapshot(symbol=clean) if is_supported_symbol(clean) else None
    if not FO_AGENT_ENABLED:
        return None
    return FOSnapshotBuilder(provider=_options_provider()).build(symbol=clean, expiry=expiry)


def run_fo_structure_cycle(
    symbol: str = "NIFTY",
    expiry: str | None = None,
    force_refresh: bool = False,
    use_mock: bool = False,
    context=None,
):
    clean = normalize_fo_symbol(symbol)
    snapshot = build_fo_snapshot(symbol=clean, expiry=expiry, use_mock=use_mock, context=context)
    report = FOStructureAgent().analyze(snapshot, symbol=clean)
    if force_refresh:
        print(f"[*] F&O structure refresh forced for {clean}")
    return report


def get_latest_fo_structure_report(symbol: str = "NIFTY"):
    clean = normalize_fo_symbol(symbol)
    return load_latest_agent_report(
        FOStructureAgent.AGENT_NAME,
        clean,
        "FO_STRUCTURE_REPORT",
    )


def fo_runtime_status() -> dict:
    provider = _options_provider()
    return {
        "agent": FOStructureAgent.AGENT_NAME,
        "enabled": FO_AGENT_ENABLED,
        "refresh_seconds": FO_AGENT_REFRESH_SECONDS,
        "timezone": FO_AGENT_TIMEZONE,
        "upstox_options_enabled": UPSTOX_OPTIONS_ENABLED,
        "upstox_options_configured": provider.is_configured(),
        "read_only": True,
        "supported_symbols": ["NIFTY", "SENSEX"],
    }


def _options_provider() -> UpstoxOptionsProvider:
    return UpstoxOptionsProvider(
        enabled=UPSTOX_OPTIONS_ENABLED,
        timeout_seconds=UPSTOX_OPTIONS_TIMEOUT_SECONDS,
        cache_ttl_seconds=UPSTOX_OPTIONS_CACHE_TTL_SECONDS,
    )
