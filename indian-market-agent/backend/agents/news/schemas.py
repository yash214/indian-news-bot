"""Typed payloads for article-level news intelligence and index reports."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field


VALID_SENTIMENTS = {"bullish", "bearish", "neutral"}
VALID_CATEGORIES = {"macro", "market_flow", "sector", "company", "global", "policy", "general"}
VALID_INDICES = {"NIFTY", "BANKNIFTY"}
VALID_MACRO_TAGS = {
    "CRUDE",
    "USDINR",
    "RBI_POLICY",
    "FED",
    "FII_FLOWS",
    "GEOPOLITICAL_RISK",
    "INDIA_VIX",
    "GLOBAL_CUES",
}
VALID_RISK_LEVELS = {"low", "medium", "high"}
VALID_TRADE_FILTERS = {
    "NO_FILTER",
    "REDUCE_LONG_CONFIDENCE",
    "REDUCE_SHORT_CONFIDENCE",
    "EVENT_RISK_WAIT",
    "BLOCK_FRESH_TRADES",
}
VALID_INDEX_SENTIMENTS = {"BULLISH", "MIXED_BULLISH", "NEUTRAL", "MIXED_BEARISH", "BEARISH"}
VALID_MARKET_REGIME_HINTS = {"NEWS_SUPPORTIVE", "NEWS_NEGATIVE", "NEWS_MIXED", "EVENT_RISK", "NO_MAJOR_NEWS"}


@dataclass
class EventRisk:
    is_event_risk: bool = False
    risk_level: str = "low"
    reason: str = ""

    @classmethod
    def from_dict(cls, payload: dict | None) -> "EventRisk":
        payload = payload if isinstance(payload, dict) else {}
        risk_level = str(payload.get("risk_level") or payload.get("riskLevel") or "low").strip().lower()
        if risk_level not in VALID_RISK_LEVELS:
            risk_level = "low"
        return cls(
            is_event_risk=bool(payload.get("is_event_risk", payload.get("isEventRisk", False))),
            risk_level=risk_level,
            reason=str(payload.get("reason") or "")[:240],
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class StrategyEngineGuidance:
    long_confidence_adjustment: int = 0
    short_confidence_adjustment: int = 0
    block_fresh_trades: bool = False
    notes: str = ""
    avoid_until: str | None = None

    @classmethod
    def from_dict(cls, payload: dict | None) -> "StrategyEngineGuidance":
        payload = payload if isinstance(payload, dict) else {}
        return cls(
            long_confidence_adjustment=_safe_int(
                payload.get("long_confidence_adjustment", payload.get("longConfidenceAdjustment", 0)),
                default=0,
            ),
            short_confidence_adjustment=_safe_int(
                payload.get("short_confidence_adjustment", payload.get("shortConfidenceAdjustment", 0)),
                default=0,
            ),
            block_fresh_trades=bool(payload.get("block_fresh_trades", payload.get("blockFreshTrades", False))),
            notes=str(payload.get("notes") or "")[:500],
            avoid_until=payload.get("avoid_until") or payload.get("avoidUntil"),
        )

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ArticleAIAnalysis:
    article_id: str
    title: str
    source: str
    url: str
    published_at: str
    analyzed_at: str
    summary: str
    published_ts: float = 0.0
    sentiment: str = "neutral"
    impact_score: int = 0
    confidence: float = 0.0
    category: str = "general"
    affected_indices: list[str] = field(default_factory=list)
    affected_sectors: list[str] = field(default_factory=list)
    macro_tags: list[str] = field(default_factory=list)
    event_risk: EventRisk = field(default_factory=EventRisk)
    trade_filter: str = "NO_FILTER"
    strategy_engine_guidance: StrategyEngineGuidance = field(default_factory=StrategyEngineGuidance)
    reasons: list[str] = field(default_factory=list)
    raw_llm_json: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: dict) -> "ArticleAIAnalysis":
        payload = payload if isinstance(payload, dict) else {}
        return cls(
            article_id=str(payload.get("article_id") or payload.get("articleId") or ""),
            title=str(payload.get("title") or ""),
            source=str(payload.get("source") or ""),
            url=str(payload.get("url") or payload.get("link") or ""),
            published_at=str(payload.get("published_at") or payload.get("publishedAt") or payload.get("published") or ""),
            published_ts=_safe_float(payload.get("published_ts", payload.get("publishedTs", payload.get("ts", 0.0))), default=0.0),
            analyzed_at=str(payload.get("analyzed_at") or payload.get("analyzedAt") or ""),
            summary=str(payload.get("summary") or ""),
            sentiment=str(payload.get("sentiment") or "neutral"),
            impact_score=_safe_int(payload.get("impact_score", payload.get("impactScore", 0)), default=0),
            confidence=_safe_float(payload.get("confidence"), default=0.0),
            category=str(payload.get("category") or "general"),
            affected_indices=_string_list(payload.get("affected_indices") or payload.get("affectedIndices")),
            affected_sectors=_string_list(payload.get("affected_sectors") or payload.get("affectedSectors")),
            macro_tags=_string_list(payload.get("macro_tags") or payload.get("macroTags")),
            event_risk=EventRisk.from_dict(payload.get("event_risk") or payload.get("eventRisk")),
            trade_filter=str(payload.get("trade_filter") or payload.get("tradeFilter") or "NO_FILTER"),
            strategy_engine_guidance=StrategyEngineGuidance.from_dict(
                payload.get("strategy_engine_guidance") or payload.get("strategyEngineGuidance")
            ),
            reasons=_string_list(payload.get("reasons")),
            raw_llm_json=payload.get("raw_llm_json") if isinstance(payload.get("raw_llm_json"), dict) else {},
        )

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["event_risk"] = self.event_risk.to_dict()
        payload["strategy_engine_guidance"] = self.strategy_engine_guidance.to_dict()
        return payload


@dataclass
class IndexNewsReport:
    report_type: str
    index: str
    generated_at: str
    lookback_hours: int
    overall_sentiment: str
    impact_score: int
    confidence: float
    trade_filter: str
    market_regime_hint: str
    major_drivers: list[dict] = field(default_factory=list)
    bullish_factors: list[str] = field(default_factory=list)
    bearish_factors: list[str] = field(default_factory=list)
    affected_indices: list[str] = field(default_factory=list)
    affected_sectors: list[str] = field(default_factory=list)
    risk_events: list[dict] = field(default_factory=list)
    strategy_engine_guidance: StrategyEngineGuidance = field(default_factory=StrategyEngineGuidance)
    top_articles: list[dict] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["strategy_engine_guidance"] = self.strategy_engine_guidance.to_dict()
        return payload


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _string_list(value) -> list[str]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out
