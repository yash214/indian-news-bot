"""Thin runtime helpers for the Market Regime Agent."""

from __future__ import annotations

try:
    from backend.agents.agent_output_store import load_agent_output
    from backend.agents.market_regime.agent import MarketRegimeAgent
    from backend.agents.market_regime.schemas import is_supported_symbol, normalize_market_symbol
    from backend.agents.market_regime.snapshot_builder import (
        MarketRegimeSnapshotBuilder,
        build_mock_market_feature_snapshot,
    )
    from backend.core.settings import (
        MARKET_REGIME_AGENT_ENABLED,
        MARKET_REGIME_REFRESH_SECONDS,
        MARKET_REGIME_TIMEFRAME_MINUTES,
        UPSTOX_MARKET_DATA_ENABLED,
    )
except ModuleNotFoundError:
    from agents.agent_output_store import load_agent_output
    from agents.market_regime.agent import MarketRegimeAgent
    from agents.market_regime.schemas import is_supported_symbol, normalize_market_symbol
    from agents.market_regime.snapshot_builder import MarketRegimeSnapshotBuilder, build_mock_market_feature_snapshot
    from core.settings import MARKET_REGIME_AGENT_ENABLED, MARKET_REGIME_REFRESH_SECONDS, MARKET_REGIME_TIMEFRAME_MINUTES, UPSTOX_MARKET_DATA_ENABLED


def build_market_regime_snapshot(symbol: str = "NIFTY", timeframe_minutes: int = 5, use_mock: bool = False, regime_hint: str | None = None):
    clean = normalize_market_symbol(symbol)
    if use_mock:
        if not is_supported_symbol(clean):
            return None
        return build_mock_market_feature_snapshot(symbol=clean, regime_hint=regime_hint)
    return MarketRegimeSnapshotBuilder().build(symbol=clean, timeframe_minutes=timeframe_minutes, use_mock=False)


def run_market_regime_cycle(
    symbol: str = "NIFTY",
    timeframe_minutes: int = 5,
    force_refresh: bool = False,
    use_mock: bool = False,
    regime_hint: str | None = None,
):
    clean = normalize_market_symbol(symbol)
    if not MARKET_REGIME_AGENT_ENABLED and not use_mock:
        return MarketRegimeAgent().analyze(None, symbol=clean)
    snapshot = build_market_regime_snapshot(
        symbol=clean,
        timeframe_minutes=timeframe_minutes or MARKET_REGIME_TIMEFRAME_MINUTES,
        use_mock=use_mock,
        regime_hint=regime_hint,
    )
    report = MarketRegimeAgent().analyze(snapshot, symbol=clean)
    if force_refresh:
        return report
    return report


def get_latest_market_regime_report(symbol: str = "NIFTY"):
    clean = normalize_market_symbol(symbol)
    return load_agent_output(f"{MarketRegimeAgent.AGENT_NAME}:{clean}:MARKET_REGIME_REPORT")


def market_regime_runtime_status() -> dict:
    return {
        "agent": MarketRegimeAgent.AGENT_NAME,
        "enabled": MARKET_REGIME_AGENT_ENABLED,
        "refresh_seconds": MARKET_REGIME_REFRESH_SECONDS,
        "timeframe_minutes": MARKET_REGIME_TIMEFRAME_MINUTES,
        "upstox_market_data_enabled": UPSTOX_MARKET_DATA_ENABLED,
        "read_only": True,
        "supported_symbols": ["NIFTY", "SENSEX"],
    }
