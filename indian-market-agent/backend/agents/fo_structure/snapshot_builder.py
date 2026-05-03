"""Snapshot builder for F&O option-chain structure inputs."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from backend.agents.fo_structure.schemas import (
        INDEX_OPTION_CONFIG,
        OptionChainSnapshot,
        OptionStrike,
        is_supported_symbol,
        normalize_fo_symbol,
    )
    from backend.core.settings import (
        FO_AGENT_TIMEZONE,
        IST,
        UPSTOX_OPTIONS_CACHE_TTL_SECONDS,
        UPSTOX_OPTIONS_ENABLED,
        UPSTOX_OPTIONS_TIMEOUT_SECONDS,
    )
    from backend.providers.upstox.options_provider import UpstoxOptionsProvider
except ModuleNotFoundError:
    from agents.fo_structure.schemas import INDEX_OPTION_CONFIG, OptionChainSnapshot, OptionStrike, is_supported_symbol, normalize_fo_symbol
    from core.settings import FO_AGENT_TIMEZONE, IST, UPSTOX_OPTIONS_CACHE_TTL_SECONDS, UPSTOX_OPTIONS_ENABLED, UPSTOX_OPTIONS_TIMEOUT_SECONDS
    from providers.upstox.options_provider import UpstoxOptionsProvider


class FOSnapshotBuilder:
    def __init__(self, provider: UpstoxOptionsProvider | None = None):
        self.provider = provider or UpstoxOptionsProvider(
            enabled=UPSTOX_OPTIONS_ENABLED,
            timeout_seconds=UPSTOX_OPTIONS_TIMEOUT_SECONDS,
            cache_ttl_seconds=UPSTOX_OPTIONS_CACHE_TTL_SECONDS,
        )

    def build(self, symbol: str, expiry: str | None = None) -> OptionChainSnapshot | None:
        clean = normalize_fo_symbol(symbol)
        if not is_supported_symbol(clean) or not self.provider.is_configured():
            return None
        config = INDEX_OPTION_CONFIG[clean]
        underlying_key = str(config["underlying_key"])
        selected_expiry = expiry

        contracts = None
        if not selected_expiry:
            contracts = self.provider.get_option_contracts(underlying_key)
            if not contracts and clean == "SENSEX":
                discovered = self.provider.discover_underlying_key("SENSEX")
                if discovered:
                    underlying_key = discovered
                    contracts = self.provider.get_option_contracts(underlying_key)
            selected_expiry = _nearest_expiry(contracts)
            if not selected_expiry:
                return None

        raw_chain = self.provider.get_put_call_option_chain(underlying_key, selected_expiry)
        if not raw_chain and clean == "SENSEX":
            discovered = self.provider.discover_underlying_key("SENSEX")
            if discovered and discovered != underlying_key:
                underlying_key = discovered
                raw_chain = self.provider.get_put_call_option_chain(underlying_key, selected_expiry)
        if not raw_chain:
            return None
        snapshot = self.provider.normalize_option_chain(raw_chain, clean, selected_expiry)
        if snapshot:
            snapshot.underlying_key = underlying_key
            snapshot.source_status = self.provider.source_status()
        return snapshot


def build_mock_option_chain_snapshot(symbol: str = "NIFTY") -> OptionChainSnapshot:
    clean = normalize_fo_symbol(symbol)
    if not is_supported_symbol(clean):
        clean = "NIFTY"
    config = INDEX_OPTION_CONFIG[clean]
    spot = 22475.0 if clean == "NIFTY" else 74250.0
    step = int(config["default_strike_step"])
    atm = round(spot / step) * step
    strikes = []
    raw_rows = [
        (-3, 70000, 68000, 125000, 98000, 80, 118),
        (-2, 90000, 88000, 180000, 140000, 95, 102),
        (-1, 120000, 121000, 240000, 182000, 110, 86),
        (0, 150000, 147000, 150000, 145000, 130, 124),
        (1, 235000, 184000, 120000, 123000, 92, 110),
        (2, 260000, 208000, 85000, 90000, 72, 128),
        (3, 185000, 174000, 65000, 70000, 58, 140),
    ]
    for offset, call_oi, call_prev, put_oi, put_prev, call_ltp, put_ltp in raw_rows:
        strike = float(atm + offset * step)
        strikes.append(OptionStrike(
            strike=strike,
            call_ltp=float(call_ltp),
            call_oi=call_oi,
            call_prev_oi=call_prev,
            call_change_oi=call_oi - call_prev,
            call_volume=1000 + abs(offset) * 150,
            call_iv=14.5,
            call_delta=0.45,
            call_gamma=0.01,
            call_theta=-12.0,
            call_vega=8.0,
            call_bid_price=float(call_ltp - 1),
            call_ask_price=float(call_ltp + 1),
            call_bid_qty=100,
            call_ask_qty=125,
            put_ltp=float(put_ltp),
            put_oi=put_oi,
            put_prev_oi=put_prev,
            put_change_oi=put_oi - put_prev,
            put_volume=1100 + abs(offset) * 125,
            put_iv=15.0,
            put_delta=-0.45,
            put_gamma=0.01,
            put_theta=-12.0,
            put_vega=8.0,
            put_bid_price=float(put_ltp - 1),
            put_ask_price=float(put_ltp + 1),
            put_bid_qty=120,
            put_ask_qty=140,
        ))
    return OptionChainSnapshot(
        symbol=clean,
        underlying_key=str(config["underlying_key"]),
        spot=spot,
        expiry=(datetime.now(IST).date() + timedelta(days=3)).isoformat(),
        timestamp=datetime.now(IST),
        lot_size=50 if clean == "NIFTY" else 10,
        strikes=strikes,
        source="mock",
        source_status={"provider": "mock", "enabled": True, "configured": True, "last_error": "", "timezone": FO_AGENT_TIMEZONE},
    )


def _nearest_expiry(contracts: dict | None) -> str | None:
    rows = contracts.get("data") if isinstance(contracts, dict) else []
    if not isinstance(rows, list):
        return None
    expiries = sorted({
        str(row.get("expiry") or row.get("expiry_date") or "").strip()
        for row in rows if isinstance(row, dict) and str(row.get("expiry") or row.get("expiry_date") or "").strip()
    })
    if not expiries:
        return None
    today = datetime.now(IST).date()
    future = []
    for expiry in expiries:
        try:
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
        except ValueError:
            continue
        if expiry_date >= today:
            future.append(expiry)
    return future[0] if future else expiries[0]
