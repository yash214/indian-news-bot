"""Typed payloads for the read-only F&O Structure Agent."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import re


VALID_FO_BIASES = {"BULLISH", "BEARISH", "NEUTRAL", "MIXED"}
VALID_PCR_STATES = {"BULLISH", "BEARISH", "NEUTRAL", "EXTREME_BULLISH", "EXTREME_BEARISH", "UNKNOWN"}
VALID_EXPIRY_RISKS = {"LOW", "MEDIUM", "HIGH", "EXTREME", "UNKNOWN"}

INDEX_OPTION_CONFIG = {
    "NIFTY": {
        "display_name": "NIFTY 50",
        "exchange": "NSE",
        "underlying_key": "NSE_INDEX|Nifty 50",
        "default_strike_step": 50,
        "supported": True,
    },
    "SENSEX": {
        "display_name": "SENSEX",
        "exchange": "BSE",
        "underlying_key": "BSE_INDEX|SENSEX",
        "default_strike_step": 100,
        "supported": True,
    },
}
SUPPORTED_SYMBOLS = tuple(INDEX_OPTION_CONFIG.keys())


def normalize_fo_symbol(symbol: str | None) -> str:
    clean = re.sub(r"[^A-Z0-9]", "", str(symbol or "NIFTY").upper())
    if clean in {"NIFTY", "NIFTY50"}:
        return "NIFTY"
    if clean == "SENSEX":
        return "SENSEX"
    if clean in {"BANKNIFTY", "NIFTYBANK"}:
        return "BANKNIFTY"
    return clean or "NIFTY"


def is_supported_symbol(symbol: str | None) -> bool:
    return normalize_fo_symbol(symbol) in INDEX_OPTION_CONFIG


def unsupported_symbol_warning(symbol: str | None) -> str:
    clean = normalize_fo_symbol(symbol)
    return f"Unsupported symbol: {clean}. Supported symbols: NIFTY, SENSEX."


def _serialize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    return value


@dataclass
class OptionStrike:
    strike: float
    call_ltp: float | None = None
    call_oi: int | None = None
    call_prev_oi: int | None = None
    call_change_oi: int | None = None
    call_volume: int | None = None
    call_iv: float | None = None
    call_delta: float | None = None
    call_gamma: float | None = None
    call_theta: float | None = None
    call_vega: float | None = None
    call_bid_price: float | None = None
    call_ask_price: float | None = None
    call_bid_qty: int | None = None
    call_ask_qty: int | None = None
    put_ltp: float | None = None
    put_oi: int | None = None
    put_prev_oi: int | None = None
    put_change_oi: int | None = None
    put_volume: int | None = None
    put_iv: float | None = None
    put_delta: float | None = None
    put_gamma: float | None = None
    put_theta: float | None = None
    put_vega: float | None = None
    put_bid_price: float | None = None
    put_ask_price: float | None = None
    put_bid_qty: int | None = None
    put_ask_qty: int | None = None

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class OptionChainSnapshot:
    symbol: str
    underlying_key: str
    spot: float
    expiry: str
    timestamp: datetime
    lot_size: int | None = None
    strikes: list[OptionStrike] = field(default_factory=list)
    source: str = "upstox"
    source_status: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class StrikeZone:
    strike: float
    strength: int
    reason: str
    oi: int | None = None
    change_oi: int | None = None
    distance_from_spot_pct: float | None = None

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class OICluster:
    strike: float
    change_oi: int
    oi: int | None = None
    strength: int = 0
    reason: str = ""

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class FOStrategyEngineGuidance:
    bullish_confidence_adjustment: int = 0
    bearish_confidence_adjustment: int = 0
    prefer_defined_risk: bool = True
    reduce_position_size: bool = False
    avoid_directional_trade: bool = True
    notes: str = ""

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class FOStructureReport:
    agent_name: str
    symbol: str
    expiry: str | None
    generated_at: datetime
    valid_until: datetime
    stale_after_seconds: int
    bias: str
    confidence: float
    pcr: float | None
    pcr_state: str
    support_zones: list[StrikeZone] = field(default_factory=list)
    resistance_zones: list[StrikeZone] = field(default_factory=list)
    major_put_writing: list[OICluster] = field(default_factory=list)
    major_call_writing: list[OICluster] = field(default_factory=list)
    call_unwinding: list[OICluster] = field(default_factory=list)
    put_unwinding: list[OICluster] = field(default_factory=list)
    max_pain: float | None = None
    atm_strike: float | None = None
    expiry_risk: str = "UNKNOWN"
    preferred_option_zone: dict = field(default_factory=dict)
    strategy_engine_guidance: FOStrategyEngineGuidance = field(default_factory=FOStrategyEngineGuidance)
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    source_status: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return _serialize(asdict(self))
