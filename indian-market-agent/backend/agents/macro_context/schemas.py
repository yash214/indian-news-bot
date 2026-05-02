"""Typed payloads for deterministic macro-context reports."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime


VALID_MACRO_BIASES = {
    "BULLISH",
    "MIXED_BULLISH",
    "NEUTRAL",
    "MIXED_BEARISH",
    "BEARISH",
    "RISK_OFF",
    "EVENT_RISK",
}
VALID_FACTOR_BIASES = {"BULLISH", "BEARISH", "NEUTRAL", "MIXED", "CAUTION", "RISK_OFF", "EVENT_RISK"}
VALID_TRADE_FILTERS = {
    "NO_FILTER",
    "REDUCE_LONG_CONFIDENCE",
    "REDUCE_SHORT_CONFIDENCE",
    "EVENT_RISK_WAIT",
    "BLOCK_FRESH_TRADES",
}


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
class MacroFactorInput:
    name: str
    symbol: str | None = None
    value: float | None = None
    change_pct_1d: float | None = None
    change_pct_5d: float | None = None
    source: str | None = None
    stale: bool = False
    raw: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class MacroEvent:
    country: str
    event: str
    importance: str
    event_time: datetime | None = None
    actual: float | None = None
    forecast: float | None = None
    previous: float | None = None
    source: str | None = None

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class MacroSnapshot:
    market: str
    timestamp: datetime
    factors: dict[str, MacroFactorInput] = field(default_factory=dict)
    events: list[MacroEvent] = field(default_factory=list)
    source_status: dict[str, dict] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class MacroFactorScore:
    name: str
    symbol: str | None = None
    value: float | None = None
    change_pct_1d: float | None = None
    change_pct_5d: float | None = None
    bias: str = "NEUTRAL"
    impact: int = 0
    confidence: float = 0.0
    source: str | None = None
    reason: str = ""

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class StrategyEngineGuidance:
    long_confidence_adjustment: int = 0
    short_confidence_adjustment: int = 0
    block_fresh_trades: bool = False
    reduce_position_size: bool = False
    notes: str = ""

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class MacroContextReport:
    agent_name: str
    generated_at: datetime
    valid_until: datetime
    stale_after_seconds: int
    market: str
    macro_bias: str
    impact_score: int
    confidence: float
    trade_filter: str
    factors: dict[str, MacroFactorScore] = field(default_factory=dict)
    major_drivers: list[str] = field(default_factory=list)
    strategy_engine_guidance: StrategyEngineGuidance = field(default_factory=StrategyEngineGuidance)
    warnings: list[str] = field(default_factory=list)
    source_status: dict[str, dict] = field(default_factory=dict)
    summary: str = ""

    def to_dict(self) -> dict:
        return _serialize(asdict(self))
