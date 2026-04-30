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
        "Analyze the article for likely impact on major Indian indices. Return JSON only, with no markdown.\n"
        "Be conservative: if evidence is thin, use neutral/limited and say details are limited.\n"
        "Do not invent numbers, support/resistance levels, earnings figures, or catalysts not present in the article data.\n"
        "The summary must be useful to a trader and preserve material facts from the article body.\n"
        "Rank facts by market importance: core development, exact companies/entities, material numbers, market reaction, sector/index read-through, risks, and what to watch.\n"
        "Include every important figure present in the article, such as profit, revenue, margins, order value, stake change, price move, target price, support/resistance, index level, valuation, date, timeline, guidance, or management commentary.\n"
        "Explain the mechanism of impact: earnings, margins, asset quality, order book, regulation, liquidity, rates, crude, currency, flows, sector rotation, or risk appetite.\n"
        "Avoid vague filler like 'may impact sentiment' unless you clearly say how and where the impact can show up.\n"
        "Impact score means expected relevance to Nifty, Bank Nifty, major sector indices, or market-wide risk appetite, not how exciting the headline sounds.\n"
        "If textSource is article-page, base the summary on availableText first and use the headline only as context.\n"
        "Use this exact JSON shape:\n"
        "{"
        "\"summary\":\"5-6 sentence, 140-220 word plain-English market brief with all material numbers\","
        "\"sentiment\":\"bullish|bearish|neutral\","
        "\"impactScore\":0,"
        "\"confidence\":0.0,"
        "\"sector\":\"IT|Banking|Pharma|Auto|Energy|FMCG|Metals|Infra|General\","
        "\"indexImpact\":{\"nifty\":\"bullish|bearish|neutral|limited\","
        "\"bankNifty\":\"bullish|bearish|neutral|limited\","
        "\"sectorIndex\":\"bullish|bearish|neutral|limited\","
        "\"timeframe\":\"intraday|1-3 days|unclear\"},"
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
        return {}
    fallback_article = fallback_article or {}
    fallback_sentiment = (fallback_article.get("sentiment") or {}).get("label") or "neutral"
    fallback_impact = fallback_article.get("impact", 0)
    fallback_sector = fallback_article.get("sector") or "General"

    sentiment = str(payload.get("sentiment") or fallback_sentiment).strip().lower()
    if sentiment not in VALID_SENTIMENTS:
        sentiment = "neutral"

    sector = _clean_text(payload.get("sector") or fallback_sector, max_len=32)
    if sector not in VALID_SECTORS:
        sector = fallback_sector if fallback_sector in VALID_SECTORS else "General"

    index_impact_payload = payload.get("indexImpact") if isinstance(payload.get("indexImpact"), dict) else {}
    index_impact = {}
    for key in ("nifty", "bankNifty", "sectorIndex"):
        tone = str(index_impact_payload.get(key) or "limited").strip().lower()
        index_impact[key] = tone if tone in VALID_INDEX_TONES else "limited"
    timeframe = _clean_text(index_impact_payload.get("timeframe") or "unclear", max_len=24).lower()
    if timeframe not in {"intraday", "1-3 days", "unclear"}:
        timeframe = "unclear"
    index_impact["timeframe"] = timeframe

    reasons = []
    raw_reasons = payload.get("reasons") if isinstance(payload.get("reasons"), list) else []
    for reason in raw_reasons:
        text = _clean_text(reason, max_len=120)
        if text:
            reasons.append(text)
        if len(reasons) >= 5:
            break

    summary = normalize_ai_summary(payload.get("summary") or "")
    if not summary:
        return {}

    return {
        "summary": summary,
        "sentiment": sentiment,
        "impactScore": _clamp_int(payload.get("impactScore"), 0, 10, _clamp_int(fallback_impact, 0, 10, 0)),
        "confidence": round(_clamp_float(payload.get("confidence"), 0.0, 1.0, 0.5), 2),
        "sector": sector,
        "indexImpact": index_impact,
        "reasons": reasons,
    }
