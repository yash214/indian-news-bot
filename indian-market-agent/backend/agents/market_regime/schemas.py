"""Typed payloads for the read-only Market Regime Agent."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import re


VALID_PRIMARY_REGIMES = {
    "TRENDING_UP",
    "TRENDING_DOWN",
    "RANGE_BOUND",
    "BREAKOUT_UP",
    "BREAKDOWN",
    "HIGH_VOLATILITY",
    "CHOPPY",
    "LOW_VOLATILITY",
    "UNCLEAR",
}
VALID_DIRECTIONAL_BIASES = {"BULLISH", "BEARISH", "NEUTRAL"}
VALID_TRADE_FILTERS = {
    "NO_FILTER",
    "WAIT",
    "REDUCE_POSITION_SIZE",
    "AVOID_DIRECTIONAL",
    "BLOCK_FRESH_TRADES",
}

INDEX_MARKET_CONFIG = {
    "NIFTY": {
        "display_name": "NIFTY 50",
        "exchange": "NSE",
        "instrument_key": "NSE_INDEX|Nifty 50",
        "default_timeframe_minutes": 5,
        "supported": True,
    },
    "SENSEX": {
        "display_name": "SENSEX",
        "exchange": "BSE",
        "instrument_key": "BSE_INDEX|SENSEX",
        "default_timeframe_minutes": 5,
        "supported": True,
    },
}
SUPPORTED_SYMBOLS = tuple(INDEX_MARKET_CONFIG.keys())


def normalize_market_symbol(symbol: str | None) -> str:
    clean = re.sub(r"[^A-Z0-9]", "", str(symbol or "NIFTY").upper())
    if clean in {"NIFTY", "NIFTY50"}:
        return "NIFTY"
    if clean == "SENSEX":
        return "SENSEX"
    if clean in {"BANKNIFTY", "NIFTYBANK"}:
        return "BANKNIFTY"
    return clean or "NIFTY"


def is_supported_symbol(symbol: str | None) -> bool:
    return normalize_market_symbol(symbol) in INDEX_MARKET_CONFIG


def unsupported_symbol_warning(symbol: str | None) -> str:
    clean = normalize_market_symbol(symbol)
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
class MarketCandle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None = None
    open_interest: float | None = None

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class MarketFeatureSnapshot:
    symbol: str
    instrument_key: str
    timestamp: datetime
    timeframe_minutes: int
    candles: list[MarketCandle] = field(default_factory=list)
    latest_close: float | None = None
    vwap: float | None = None
    ema_9: float | None = None
    ema_21: float | None = None
    rsi_14: float | None = None
    atr_14: float | None = None
    atr_pct: float | None = None
    opening_range_high: float | None = None
    opening_range_low: float | None = None
    previous_day_high: float | None = None
    previous_day_low: float | None = None
    previous_day_close: float | None = None
    day_high: float | None = None
    day_low: float | None = None
    india_vix: float | None = None
    india_vix_change_pct: float | None = None
    data_quality: dict = field(default_factory=dict)
    source: str = "upstox"
    source_status: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class MarketRegimeGuidance:
    bullish_confidence_adjustment: int = 0
    bearish_confidence_adjustment: int = 0
    avoid_directional_trade: bool = True
    prefer_breakout_strategy: bool = False
    prefer_mean_reversion: bool = False
    reduce_position_size: bool = False
    notes: str = ""

    def to_dict(self) -> dict:
        return _serialize(asdict(self))


@dataclass
class MarketRegimeReport:
    agent_name: str
    symbol: str
    generated_at: datetime
    valid_until: datetime
    stale_after_seconds: int
    primary_regime: str
    secondary_regime: str | None
    confidence: float
    trend_score: int
    range_score: int
    volatility_score: int
    chop_score: int
    directional_bias: str
    trade_filter: str
    key_levels: dict = field(default_factory=dict)
    strategy_engine_guidance: MarketRegimeGuidance = field(default_factory=MarketRegimeGuidance)
    reasons: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    source_status: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return _serialize(asdict(self))
