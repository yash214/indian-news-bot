"""AI article-analysis prompts and sanitizers."""

from __future__ import annotations

import json
import re

try:
    from backend.news.summaries import normalize_ai_summary
except ModuleNotFoundError:
    from news.summaries import normalize_ai_summary


VALID_SENTIMENTS = {"bullish", "bearish", "neutral"}
VALID_SECTORS = {"IT", "Banking", "Pharma", "Auto", "Energy", "FMCG", "Metals", "Infra", "General"}
VALID_INDEX_TONES = {"bullish", "bearish", "neutral", "limited"}


def _clean_text(value, max_len: int = 220) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:max_len].strip()


def _clamp_float(value, low: float, high: float, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return min(max(parsed, low), high)


def _clamp_int(value, low: int, high: int, default: int) -> int:
    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        return default
    return min(max(parsed, low), high)


def build_article_analysis_prompt(article: dict) -> str:
    impact_meta = article.get("impactMeta") or {}
    sentiment = article.get("sentiment") or {}
    article_data = {
        "headline": article.get("title") or "",
        "source": article.get("source") or "Unknown",
        "feed": article.get("feed") or "",
        "scope": article.get("scope") or "",
        "published": article.get("published") or "",
        "url": article.get("resolvedLink") or article.get("link") or "",
        "currentRuleSector": article.get("sector") or "General",
        "currentRuleSentiment": sentiment.get("label") or "neutral",
        "currentRuleImpactScore": article.get("impact", 0),
        "currentRuleImpactReasons": impact_meta.get("reasons") or [],
        "currentRuleMatchedKeywords": impact_meta.get("matchedKeywords") or [],
        "textSource": article.get("articleTextSource") or "rss-feed",
        "availableText": article.get("articleText") or article.get("sourceSummary") or article.get("summary") or "",
    }
    return (
        "You are a senior Indian equity-market analyst helping an intraday trading dashboard.\n"
        "Analyze the article for likely impact on major Indian indices. Return valid JSON only, with no markdown.\n"
        "Do not give buy/sell trading advice. Do not invent facts, levels, figures, catalysts, or timelines.\n"
        "Be conservative about index impact: if evidence is thin, use neutral impact or empty affected_indices.\n"
        "If an article is company-specific and not index-relevant, affected_indices can be empty; include NIFTY only if truly relevant.\n"
        "Do not invent numbers, support/resistance levels, earnings figures, or catalysts not present in the article data.\n"
        "The summary must be useful to a trader and preserve material facts from the article body.\n"
        "Rank facts by market importance: core development, exact companies/entities, material numbers, market reaction, sector/index read-through, risks, and what to watch.\n"
        "Include every important figure present in the article, such as profit, revenue, margins, order value, stake change, price move, target price, support/resistance, index level, valuation, date, timeline, guidance, or management commentary.\n"
        "Explain the mechanism of impact: earnings, margins, asset quality, order book, regulation, liquidity, rates, crude, currency, flows, sector rotation, or risk appetite.\n"
        "Avoid vague filler like 'may impact sentiment' unless you clearly say how and where the impact can show up.\n"
        "Impact score means expected relevance to Nifty, Bank Nifty, major sector indices, or market-wide risk appetite, not how exciting the headline sounds.\n"
        "If textSource is article-page, base the summary on availableText first and use the headline only as context.\n"
        "Use allowed enums only. Use confidence from 0.0 to 1.0.\n"
        "Use this exact JSON shape:\n"
        "{"
        "\"summary\":\"5-6 sentence, 140-220 word plain-English market brief with all material numbers\","
        "\"sentiment\":\"bullish|bearish|neutral\","
        "\"impact_score\":0,"
        "\"confidence\":0.0,"
        "\"category\":\"macro|market_flow|sector|company|global|policy|general\","
        "\"affected_indices\":[\"NIFTY\",\"BANKNIFTY\"],"
        "\"affected_sectors\":[\"Banking\",\"Oil & Gas\",\"IT\"],"
        "\"macro_tags\":[\"CRUDE\",\"USDINR\",\"RBI_POLICY\",\"FED\",\"FII_FLOWS\",\"GEOPOLITICAL_RISK\",\"INDIA_VIX\",\"GLOBAL_CUES\"],"
        "\"event_risk\":{\"is_event_risk\":false,\"risk_level\":\"low|medium|high\",\"reason\":\"short reason\"},"
        "\"trade_filter\":\"NO_FILTER|REDUCE_LONG_CONFIDENCE|REDUCE_SHORT_CONFIDENCE|EVENT_RISK_WAIT|BLOCK_FRESH_TRADES\","
        "\"strategy_engine_guidance\":{\"long_confidence_adjustment\":0,\"short_confidence_adjustment\":0,\"block_fresh_trades\":false,\"notes\":\"short note\"},"
        "\"reasons\":[\"short reason 1\",\"short reason 2\"]"
        "}\n\n"
        "Article data:\n"
        f"{json.dumps(article_data, ensure_ascii=False, sort_keys=True)}"
    )


def extract_json_object(text: str) -> dict:
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def normalize_article_analysis(payload: dict, fallback_article: dict | None = None) -> dict:
    if not isinstance(payload, dict):
        payload = {}
    fallback_article = fallback_article or {}
    try:
        from backend.news.agent import NewsIntelligenceAgent, article_analysis_to_legacy_dict
    except ModuleNotFoundError:
        from news.agent import NewsIntelligenceAgent, article_analysis_to_legacy_dict

    analysis = NewsIntelligenceAgent().normalize_llm_analysis(fallback_article, payload, None)
    return article_analysis_to_legacy_dict(analysis)
