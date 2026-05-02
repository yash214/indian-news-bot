"""Deterministic aggregation of article AI analyses into index news reports."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone

try:
    from backend.market.math import clamp
    from backend.news.schemas import ArticleAIAnalysis, IndexNewsReport, StrategyEngineGuidance
except ModuleNotFoundError:
    from market.math import clamp
    from news.schemas import ArticleAIAnalysis, IndexNewsReport, StrategyEngineGuidance


SENTIMENT_VALUE = {"bullish": 1.0, "neutral": 0.0, "bearish": -1.0}
TRADE_FILTER_PRIORITY = {
    "NO_FILTER": 0,
    "REDUCE_LONG_CONFIDENCE": 1,
    "REDUCE_SHORT_CONFIDENCE": 1,
    "EVENT_RISK_WAIT": 2,
    "BLOCK_FRESH_TRADES": 3,
}


class NewsReportAggregator:
    """Builds a rolling index-level report without making an LLM call."""

    def __init__(self, analyses: list[ArticleAIAnalysis | dict], *, generated_at: str | None = None) -> None:
        self.analyses = [item if isinstance(item, ArticleAIAnalysis) else ArticleAIAnalysis.from_dict(item) for item in analyses]
        self.generated_at = generated_at or datetime.now(timezone.utc).isoformat()

    def build_report(self, index: str = "NIFTY", lookback_hours: int = 24) -> IndexNewsReport:
        index = _normalize_index(index)
        relevant = _dedupe_analyses([analysis for analysis in self.analyses if index in analysis.affected_indices])
        relevant.sort(key=lambda item: (item.impact_score * item.confidence, item.impact_score), reverse=True)
        if not relevant:
            return self.empty_report(index=index, lookback_hours=lookback_hours)

        weighted_scores = [
            SENTIMENT_VALUE.get(analysis.sentiment, 0.0) * analysis.impact_score * analysis.confidence
            for analysis in relevant
        ]
        total_weight = sum(max(analysis.impact_score * analysis.confidence, 0.01) for analysis in relevant)
        net_score = sum(weighted_scores)
        normalized_bias = net_score / total_weight if total_weight else 0.0
        overall_sentiment = _overall_sentiment(normalized_bias)
        impact_score = int(round(clamp(sum(analysis.impact_score * max(analysis.confidence, 0.05) for analysis in relevant) / sum(max(analysis.confidence, 0.05) for analysis in relevant), 0, 10)))
        confidence = round(clamp(sum(analysis.confidence * max(analysis.impact_score, 1) for analysis in relevant) / sum(max(analysis.impact_score, 1) for analysis in relevant), 0.0, 1.0), 2)

        trade_filter = _highest_trade_filter(relevant)
        guidance = _aggregate_guidance(relevant, trade_filter)
        major_drivers = _major_drivers(relevant)
        bullish_factors, bearish_factors = _factor_lists(relevant)
        affected_sectors = _frequency_sorted(flatten(analysis.affected_sectors for analysis in relevant))[:10]
        risk_events = _risk_events(relevant)
        top_articles = [_top_article_payload(analysis) for analysis in relevant[:8]]
        market_regime_hint = _market_regime_hint(overall_sentiment, risk_events)
        summary = _summary_text(index, overall_sentiment, impact_score, confidence, trade_filter, major_drivers, risk_events, len(relevant))

        return IndexNewsReport(
            report_type="ROLLING_24H_INDEX_NEWS_REPORT",
            index=index,
            generated_at=self.generated_at,
            lookback_hours=lookback_hours,
            overall_sentiment=overall_sentiment,
            impact_score=impact_score,
            confidence=confidence,
            trade_filter=trade_filter,
            market_regime_hint=market_regime_hint,
            major_drivers=major_drivers,
            bullish_factors=bullish_factors,
            bearish_factors=bearish_factors,
            affected_indices=[index],
            affected_sectors=affected_sectors,
            risk_events=risk_events,
            strategy_engine_guidance=guidance,
            top_articles=top_articles,
            summary=summary,
        )

    def empty_report(self, index: str = "NIFTY", lookback_hours: int = 24) -> IndexNewsReport:
        index = _normalize_index(index)
        return IndexNewsReport(
            report_type="ROLLING_24H_INDEX_NEWS_REPORT",
            index=index,
            generated_at=self.generated_at,
            lookback_hours=lookback_hours,
            overall_sentiment="NEUTRAL",
            impact_score=0,
            confidence=0.0,
            trade_filter="NO_FILTER",
            market_regime_hint="NO_MAJOR_NEWS",
            affected_indices=[index],
            strategy_engine_guidance=StrategyEngineGuidance(notes="No analyzed news available for this lookback window."),
            summary="No analyzed news available for the selected lookback window.",
        )


def flatten(groups):
    for group in groups:
        for item in group:
            yield item


def _normalize_index(index: str) -> str:
    clean = str(index or "NIFTY").upper().replace(" ", "").replace("-", "_")
    if clean in {"BANK_NIFTY", "BANKNIFTY", "NIFTYBANK"}:
        return "BANKNIFTY"
    return "NIFTY"


def _dedupe_analyses(analyses: list[ArticleAIAnalysis]) -> list[ArticleAIAnalysis]:
    seen = set()
    out = []
    for analysis in analyses:
        key = (analysis.article_id or analysis.url or analysis.title).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(analysis)
    return out


def _overall_sentiment(normalized_bias: float) -> str:
    if normalized_bias >= 0.35:
        return "BULLISH"
    if normalized_bias >= 0.12:
        return "MIXED_BULLISH"
    if normalized_bias <= -0.35:
        return "BEARISH"
    if normalized_bias <= -0.12:
        return "MIXED_BEARISH"
    return "NEUTRAL"


def _highest_trade_filter(analyses: list[ArticleAIAnalysis]) -> str:
    current = "NO_FILTER"
    for analysis in analyses:
        candidate = analysis.trade_filter or "NO_FILTER"
        if analysis.event_risk.is_event_risk and analysis.event_risk.risk_level == "high":
            candidate = "BLOCK_FRESH_TRADES"
        elif analysis.event_risk.is_event_risk and analysis.event_risk.risk_level == "medium":
            candidate = "EVENT_RISK_WAIT"
        if TRADE_FILTER_PRIORITY.get(candidate, 0) > TRADE_FILTER_PRIORITY.get(current, 0):
            current = candidate
    return current


def _aggregate_guidance(analyses: list[ArticleAIAnalysis], trade_filter: str) -> StrategyEngineGuidance:
    long_adjustment = 0
    short_adjustment = 0
    notes = []
    block = trade_filter == "BLOCK_FRESH_TRADES"
    for analysis in analyses:
        sign = SENTIMENT_VALUE.get(analysis.sentiment, 0.0)
        adjustment = int(round(analysis.impact_score * max(analysis.confidence, 0.0)))
        if sign < 0:
            long_adjustment -= adjustment
        elif sign > 0:
            short_adjustment -= adjustment
        guidance = analysis.strategy_engine_guidance
        long_adjustment += int(guidance.long_confidence_adjustment or 0)
        short_adjustment += int(guidance.short_confidence_adjustment or 0)
        block = block or bool(guidance.block_fresh_trades)
        if guidance.notes and guidance.notes not in notes:
            notes.append(guidance.notes)
    return StrategyEngineGuidance(
        long_confidence_adjustment=int(clamp(long_adjustment, -50, 25)),
        short_confidence_adjustment=int(clamp(short_adjustment, -50, 25)),
        block_fresh_trades=block,
        notes=" ".join(notes[:2]) or "Aggregated from article-level news intelligence.",
    )


def _major_drivers(analyses: list[ArticleAIAnalysis]) -> list[dict]:
    driver_scores: dict[str, float] = defaultdict(float)
    driver_impacts: dict[str, float] = defaultdict(float)
    driver_counts: Counter = Counter()
    driver_examples: dict[str, str] = {}
    for analysis in analyses:
        drivers = list(analysis.macro_tags) or [analysis.category.upper()]
        for driver in drivers:
            weight = analysis.impact_score * max(analysis.confidence, 0.05)
            driver_scores[driver] += SENTIMENT_VALUE.get(analysis.sentiment, 0.0) * weight
            driver_impacts[driver] += weight
            driver_counts[driver] += 1
            driver_examples.setdefault(driver, _first_reason(analysis))
    ranked = sorted(driver_counts, key=lambda key: (driver_impacts[key], driver_counts[key]), reverse=True)
    out = []
    for driver in ranked[:6]:
        impact = int(round(clamp(driver_impacts[driver] / max(driver_counts[driver], 1), 0, 10)))
        bias = "BULLISH" if driver_scores[driver] > 0.75 else "BEARISH" if driver_scores[driver] < -0.75 else "NEUTRAL"
        out.append(
            {
                "driver": driver,
                "bias": bias,
                "impact": impact,
                "why_it_matters": driver_examples.get(driver) or "Repeated in recent analyzed news.",
            }
        )
    return out


def _factor_lists(analyses: list[ArticleAIAnalysis]) -> tuple[list[str], list[str]]:
    bullish, bearish = [], []
    for analysis in analyses:
        text = _first_reason(analysis) or analysis.summary
        if analysis.sentiment == "bullish" and text and text not in bullish:
            bullish.append(text)
        elif analysis.sentiment == "bearish" and text and text not in bearish:
            bearish.append(text)
        if len(bullish) >= 5 and len(bearish) >= 5:
            break
    return bullish[:5], bearish[:5]


def _risk_events(analyses: list[ArticleAIAnalysis]) -> list[dict]:
    out = []
    for analysis in analyses:
        if not analysis.event_risk.is_event_risk:
            continue
        event = analysis.event_risk.reason or analysis.title
        action = "Block fresh trades" if analysis.event_risk.risk_level == "high" else "Wait for event clarity"
        item = {"event": event, "risk_level": analysis.event_risk.risk_level, "action": action}
        if item not in out:
            out.append(item)
        if len(out) >= 5:
            break
    return out


def _top_article_payload(analysis: ArticleAIAnalysis) -> dict:
    return {
        "title": analysis.title,
        "source": analysis.source,
        "url": analysis.url,
        "sentiment": analysis.sentiment,
        "impact_score": analysis.impact_score,
        "confidence": analysis.confidence,
        "macro_tags": list(analysis.macro_tags),
    }


def _frequency_sorted(items) -> list[str]:
    counts = Counter(item for item in items if item)
    return [item for item, _count in counts.most_common()]


def _first_reason(analysis: ArticleAIAnalysis) -> str:
    if analysis.reasons:
        return analysis.reasons[0]
    words = analysis.summary.split()
    return " ".join(words[:28]).strip()


def _market_regime_hint(overall_sentiment: str, risk_events: list[dict]) -> str:
    if any(item.get("risk_level") == "high" for item in risk_events):
        return "EVENT_RISK"
    if overall_sentiment in {"BULLISH", "MIXED_BULLISH"}:
        return "NEWS_SUPPORTIVE"
    if overall_sentiment in {"BEARISH", "MIXED_BEARISH"}:
        return "NEWS_NEGATIVE"
    return "NEWS_MIXED"


def _summary_text(
    index: str,
    overall_sentiment: str,
    impact_score: int,
    confidence: float,
    trade_filter: str,
    major_drivers: list[dict],
    risk_events: list[dict],
    count: int,
) -> str:
    driver_text = ", ".join(driver["driver"] for driver in major_drivers[:3]) or "no dominant driver"
    risk_text = " Event risk is elevated." if risk_events else ""
    return (
        f"{index} news tone is {overall_sentiment} across {count} analyzed articles, "
        f"with impact {impact_score}/10 and confidence {confidence:.2f}. "
        f"Main drivers: {driver_text}. Trade filter: {trade_filter}.{risk_text}"
    )
