"""Article-level News Intelligence Agent normalization and fallbacks."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

try:
    from backend.core.settings import IST
    from backend.news.scoring import classify, impact_details, sentiment
    from backend.news.summaries import normalize_ai_summary
    from backend.news.text import clean_summary, url_hash
    from backend.news.schemas import (
        ArticleAIAnalysis,
        EventRisk,
        StrategyEngineGuidance,
        VALID_CATEGORIES,
        VALID_INDICES,
        VALID_MACRO_TAGS,
        VALID_SENTIMENTS,
        VALID_TRADE_FILTERS,
    )
except ModuleNotFoundError:
    from core.settings import IST
    from news.scoring import classify, impact_details, sentiment
    from news.summaries import normalize_ai_summary
    from news.text import clean_summary, url_hash
    from news.schemas import (
        ArticleAIAnalysis,
        EventRisk,
        StrategyEngineGuidance,
        VALID_CATEGORIES,
        VALID_INDICES,
        VALID_MACRO_TAGS,
        VALID_SENTIMENTS,
        VALID_TRADE_FILTERS,
    )


SECTOR_ALIASES = {
    "bank": "Banking",
    "banks": "Banking",
    "banking": "Banking",
    "financials": "Banking",
    "oil": "Oil & Gas",
    "oil & gas": "Oil & Gas",
    "energy": "Oil & Gas",
    "information technology": "IT",
    "technology": "IT",
    "it": "IT",
    "pharma": "Pharma",
    "pharmaceuticals": "Pharma",
    "auto": "Auto",
    "automobiles": "Auto",
    "fmcg": "FMCG",
    "metals": "Metals",
    "infra": "Infra",
    "infrastructure": "Infra",
    "general": "General",
}

MACRO_KEYWORDS = {
    "CRUDE": ("crude", "brent", "wti", "oil", "opec"),
    "USDINR": ("usd/inr", "usd-inr", "rupee", "dollar", "currency"),
    "RBI_POLICY": ("rbi", "repo rate", "monetary policy", "rate cut", "rate hike"),
    "FED": ("fed", "federal reserve", "jerome powell", "us rates"),
    "FII_FLOWS": ("fii", "foreign institutional", "foreign investors", "flows"),
    "GEOPOLITICAL_RISK": ("war", "geopolitical", "middle east", "iran", "israel", "tariff", "sanction"),
    "INDIA_VIX": ("india vix", "vix", "volatility"),
    "GLOBAL_CUES": ("global cues", "wall street", "nasdaq", "dow", "s&p", "asia markets"),
}


class NewsIntelligenceAgent:
    """Converts LLM JSON or rule scoring into a stable article intelligence schema."""

    def analyze_article(self, article_dict: dict, llm_json: dict | None = None) -> ArticleAIAnalysis:
        if isinstance(llm_json, dict) and llm_json:
            return self.normalize_llm_analysis(article_dict, llm_json, None)
        return self.fallback_from_rules(article_dict)

    def normalize_llm_analysis(
        self,
        article_dict: dict,
        llm_json: dict,
        rule_score_fallback: dict | None = None,
    ) -> ArticleAIAnalysis:
        if not isinstance(llm_json, dict) or not llm_json:
            return self.fallback_from_rules(article_dict, rule_score_fallback)

        summary = normalize_ai_summary(llm_json.get("summary") or "")
        if not summary:
            return self.fallback_from_rules(article_dict, rule_score_fallback)

        sentiment_label = _normalize_sentiment(llm_json.get("sentiment"), article_dict)
        impact_score = _clamp_int(
            _first_present(llm_json, "impact_score", "impactScore"),
            0,
            10,
            _rule_impact(article_dict, rule_score_fallback),
        )
        confidence = _normalize_confidence(llm_json.get("confidence"), default=0.55)
        category = _normalize_category(llm_json.get("category"), article_dict)
        sectors = _normalize_sectors(_first_present(llm_json, "affected_sectors", "affectedSectors"))
        legacy_sector = str(llm_json.get("sector") or "").strip()
        if legacy_sector and not sectors:
            sectors = _normalize_sectors([legacy_sector])
        if not sectors:
            fallback_sector = str(article_dict.get("sector") or "").strip()
            sectors = _normalize_sectors([fallback_sector]) if fallback_sector else []

        macro_tags = _normalize_macro_tags(_first_present(llm_json, "macro_tags", "macroTags"))
        inferred_tags = _infer_macro_tags(article_dict, llm_json)
        for tag in inferred_tags:
            if tag not in macro_tags:
                macro_tags.append(tag)

        indices = _normalize_indices(_first_present(llm_json, "affected_indices", "affectedIndices"))
        if not indices:
            indices = _indices_from_legacy_index_impact(llm_json, sentiment_label, impact_score, sectors, macro_tags)

        event_risk = EventRisk.from_dict(_first_present(llm_json, "event_risk", "eventRisk"))
        if not event_risk.is_event_risk:
            event_risk = _infer_event_risk(article_dict, macro_tags, impact_score)

        trade_filter = _normalize_trade_filter(_first_present(llm_json, "trade_filter", "tradeFilter"))
        if trade_filter == "NO_FILTER":
            trade_filter = _trade_filter_from_context(sentiment_label, impact_score, event_risk)

        guidance = StrategyEngineGuidance.from_dict(
            _first_present(llm_json, "strategy_engine_guidance", "strategyEngineGuidance")
        )
        guidance = _merge_guidance_with_deterministic_defaults(guidance, sentiment_label, impact_score, confidence, trade_filter, event_risk)

        reasons = _clean_list(llm_json.get("reasons"), max_items=6, max_len=180)
        if not reasons:
            reasons = _rule_reasons(article_dict, rule_score_fallback)

        return ArticleAIAnalysis(
            article_id=_article_id(article_dict),
            title=str(article_dict.get("title") or llm_json.get("title") or ""),
            source=str(article_dict.get("source") or llm_json.get("source") or ""),
            url=str(article_dict.get("resolvedLink") or article_dict.get("link") or llm_json.get("url") or ""),
            published_at=_article_published_at(article_dict),
            analyzed_at=datetime.now(timezone.utc).isoformat(),
            published_ts=_article_published_ts(article_dict),
            summary=summary,
            sentiment=sentiment_label,
            impact_score=impact_score,
            confidence=confidence,
            category=category,
            affected_indices=indices,
            affected_sectors=sectors,
            macro_tags=macro_tags,
            event_risk=event_risk,
            trade_filter=trade_filter,
            strategy_engine_guidance=guidance,
            reasons=reasons,
            raw_llm_json=dict(llm_json),
        )

    def fallback_from_rules(self, article_dict: dict, rule_score_fallback: dict | None = None) -> ArticleAIAnalysis:
        title = str(article_dict.get("title") or "")
        body = str(article_dict.get("sourceSummary") or article_dict.get("summary") or "")
        source = str(article_dict.get("source") or "")
        existing_sentiment = article_dict.get("sentiment") if isinstance(article_dict.get("sentiment"), dict) else None
        sent = existing_sentiment or sentiment(title, body)
        impact_score = _rule_impact(article_dict, rule_score_fallback)
        sector = str(article_dict.get("sector") or classify(f"{title} {body}") or "General")
        sectors = _normalize_sectors([sector])
        macro_tags = _infer_macro_tags(article_dict, {})
        category = _normalize_category(None, article_dict)
        sentiment_label = _normalize_sentiment(sent.get("label"), article_dict)
        event_risk = _infer_event_risk(article_dict, macro_tags, impact_score)
        trade_filter = _trade_filter_from_context(sentiment_label, impact_score, event_risk)
        confidence = 0.45 if impact_score >= 5 else 0.35
        guidance = _merge_guidance_with_deterministic_defaults(
            StrategyEngineGuidance(),
            sentiment_label,
            impact_score,
            confidence,
            trade_filter,
            event_risk,
        )
        summary = normalize_ai_summary(body) or clean_summary(body) or title
        return ArticleAIAnalysis(
            article_id=_article_id(article_dict),
            title=title,
            source=source,
            url=str(article_dict.get("resolvedLink") or article_dict.get("link") or ""),
            published_at=_article_published_at(article_dict),
            analyzed_at=datetime.now(timezone.utc).isoformat(),
            published_ts=_article_published_ts(article_dict),
            summary=summary,
            sentiment=sentiment_label,
            impact_score=impact_score,
            confidence=confidence,
            category=category,
            affected_indices=_rule_indices(article_dict, impact_score, sectors, macro_tags),
            affected_sectors=sectors,
            macro_tags=macro_tags,
            event_risk=event_risk,
            trade_filter=trade_filter,
            strategy_engine_guidance=guidance,
            reasons=_rule_reasons(article_dict, rule_score_fallback),
            raw_llm_json={},
        )


def article_analysis_to_legacy_dict(analysis: ArticleAIAnalysis, *, analysis_source: str | None = None) -> dict:
    """Return the richer schema while preserving existing dashboard keys."""
    payload = analysis.to_dict()
    source = analysis_source or ("ai" if analysis.raw_llm_json else "rules")
    sector = analysis.affected_sectors[0] if analysis.affected_sectors else "General"
    payload.update(
        {
            "impactScore": analysis.impact_score,
            "sector": sector,
            "indexImpact": _legacy_index_impact(analysis),
            "analysisSource": source,
        }
    )
    return payload


def _legacy_index_impact(analysis: ArticleAIAnalysis) -> dict:
    return {
        "nifty": analysis.sentiment if "NIFTY" in analysis.affected_indices else "limited",
        "bankNifty": analysis.sentiment if "BANKNIFTY" in analysis.affected_indices else "limited",
        "sectorIndex": analysis.sentiment if analysis.affected_sectors else "limited",
        "timeframe": "intraday" if analysis.impact_score >= 6 else "1-3 days" if analysis.impact_score >= 3 else "unclear",
    }


def _first_present(payload: dict, *keys):
    for key in keys:
        if key in payload:
            return payload.get(key)
    return None


def _article_id(article: dict) -> str:
    existing = str(article.get("id") or "").strip()
    if existing:
        return existing
    seed = str(article.get("link") or article.get("resolvedLink") or article.get("title") or "")
    return url_hash(seed) if seed else ""


def _article_published_at(article: dict) -> str:
    if article.get("published_at"):
        return str(article.get("published_at"))
    if article.get("published"):
        return str(article.get("published"))
    try:
        ts = float(article.get("ts") or 0.0)
    except (TypeError, ValueError):
        ts = 0.0
    if ts > 0:
        return datetime.fromtimestamp(ts, tz=IST).isoformat()
    return ""


def _article_published_ts(article: dict) -> float:
    try:
        return float(article.get("ts") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalize_sentiment(value, article: dict) -> str:
    text = str(value or "").strip().lower()
    if text in VALID_SENTIMENTS:
        return text
    fallback = article.get("sentiment") if isinstance(article.get("sentiment"), dict) else {}
    text = str(fallback.get("label") or "neutral").strip().lower()
    return text if text in VALID_SENTIMENTS else "neutral"


def _normalize_category(value, article: dict) -> str:
    text = str(value or "").strip().lower()
    if text in VALID_CATEGORIES:
        return text
    blob = f"{article.get('title') or ''} {article.get('summary') or ''} {article.get('sourceSummary') or ''}".lower()
    tags = _infer_macro_tags(article, {})
    if "RBI_POLICY" in tags or "policy" in blob or "sebi" in blob:
        return "policy"
    if tags:
        return "macro"
    if str(article.get("scope") or "").lower() == "global":
        return "global"
    sector = str(article.get("sector") or "General")
    return "general" if sector == "General" else "sector"


def _normalize_confidence(value, default: float = 0.5) -> float:
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        confidence = default
    if confidence > 1.0:
        confidence = confidence / 100.0
    return round(max(0.0, min(confidence, 1.0)), 2)


def _clamp_int(value, low: int, high: int, default: int) -> int:
    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(parsed, high))


def _clean_list(value, *, max_items: int = 8, max_len: int = 120) -> list[str]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        text = re.sub(r"\s+", " ", str(item or "")).strip()
        if text and text not in out:
            out.append(text[:max_len].strip())
        if len(out) >= max_items:
            break
    return out


def _normalize_sectors(value) -> list[str]:
    raw = value if isinstance(value, list) else []
    out = []
    for item in raw:
        key = re.sub(r"\s+", " ", str(item or "").strip()).lower()
        sector = SECTOR_ALIASES.get(key) or str(item or "").strip()
        if not sector:
            continue
        if sector not in out:
            out.append(sector[:40])
    return out[:8]


def _normalize_indices(value) -> list[str]:
    raw = value if isinstance(value, list) else []
    out = []
    for item in raw:
        text = str(item or "").upper().replace(" ", "").replace("-", "")
        if text in {"BANKNIFTY", "NIFTYBANK"}:
            text = "BANKNIFTY"
        elif text in {"NIFTY", "NIFTY50"}:
            text = "NIFTY"
        if text in VALID_INDICES and text not in out:
            out.append(text)
    return out


def _normalize_macro_tags(value) -> list[str]:
    raw = value if isinstance(value, list) else []
    out = []
    for item in raw:
        tag = str(item or "").upper().replace(" ", "_").replace("-", "_")
        if tag in VALID_MACRO_TAGS and tag not in out:
            out.append(tag)
    return out


def _infer_macro_tags(article: dict, llm_json: dict) -> list[str]:
    blob = " ".join(
        [
            str(article.get("title") or ""),
            str(article.get("summary") or ""),
            str(article.get("sourceSummary") or ""),
            json.dumps(llm_json, ensure_ascii=False) if llm_json else "",
        ]
    ).lower()
    tags = []
    for tag, keywords in MACRO_KEYWORDS.items():
        if any(keyword in blob for keyword in keywords):
            tags.append(tag)
    return tags


def _indices_from_legacy_index_impact(
    llm_json: dict,
    sentiment_label: str,
    impact_score: int,
    sectors: list[str],
    macro_tags: list[str],
) -> list[str]:
    index_impact = llm_json.get("indexImpact") if isinstance(llm_json.get("indexImpact"), dict) else {}
    out = []
    nifty_tone = str(index_impact.get("nifty") or "").lower()
    bank_tone = str(index_impact.get("bankNifty") or index_impact.get("bank_nifty") or "").lower()
    if nifty_tone in {"bullish", "bearish", "neutral"}:
        out.append("NIFTY")
    if bank_tone in {"bullish", "bearish", "neutral"}:
        out.append("BANKNIFTY")
    if not out:
        if any(tag in macro_tags for tag in {"CRUDE", "USDINR", "RBI_POLICY", "FED", "FII_FLOWS", "GLOBAL_CUES", "INDIA_VIX"}):
            out.append("NIFTY")
        if any(sector == "Banking" for sector in sectors) and impact_score >= 4:
            out.append("BANKNIFTY")
        if sentiment_label != "neutral" and impact_score >= 7 and "NIFTY" not in out:
            out.append("NIFTY")
    return out[:2]


def _infer_event_risk(article: dict, macro_tags: list[str], impact_score: int) -> EventRisk:
    blob = f"{article.get('title') or ''} {article.get('summary') or ''} {article.get('sourceSummary') or ''}".lower()
    is_event = bool({"RBI_POLICY", "FED", "GEOPOLITICAL_RISK", "INDIA_VIX"} & set(macro_tags))
    is_event = is_event or any(word in blob for word in ("election", "budget", "war", "policy meet", "rate decision"))
    if not is_event:
        return EventRisk()
    risk_level = "high" if impact_score >= 8 or "war" in blob else "medium" if impact_score >= 5 else "low"
    reason = "Potential macro/event-risk headline that can change index risk appetite."
    return EventRisk(is_event_risk=True, risk_level=risk_level, reason=reason)


def _normalize_trade_filter(value) -> str:
    text = str(value or "NO_FILTER").strip().upper()
    return text if text in VALID_TRADE_FILTERS else "NO_FILTER"


def _trade_filter_from_context(sentiment_label: str, impact_score: int, event_risk: EventRisk) -> str:
    if event_risk.is_event_risk and event_risk.risk_level == "high":
        return "BLOCK_FRESH_TRADES"
    if event_risk.is_event_risk and event_risk.risk_level == "medium":
        return "EVENT_RISK_WAIT"
    if sentiment_label == "bearish" and impact_score >= 7:
        return "REDUCE_LONG_CONFIDENCE"
    if sentiment_label == "bullish" and impact_score >= 7:
        return "REDUCE_SHORT_CONFIDENCE"
    return "NO_FILTER"


def _merge_guidance_with_deterministic_defaults(
    guidance: StrategyEngineGuidance,
    sentiment_label: str,
    impact_score: int,
    confidence: float,
    trade_filter: str,
    event_risk: EventRisk,
) -> StrategyEngineGuidance:
    deterministic = max(0, min(20, int(round(impact_score * confidence * 1.5))))
    if sentiment_label == "bearish" and guidance.long_confidence_adjustment == 0:
        guidance.long_confidence_adjustment = -deterministic
    if sentiment_label == "bullish" and guidance.short_confidence_adjustment == 0:
        guidance.short_confidence_adjustment = -deterministic
    if trade_filter == "BLOCK_FRESH_TRADES" or (event_risk.is_event_risk and event_risk.risk_level == "high"):
        guidance.block_fresh_trades = True
    if not guidance.notes:
        guidance.notes = "Conservative news adjustment generated from article-level market impact."
    return guidance


def _rule_impact(article: dict, rule_score_fallback: dict | None = None) -> int:
    if rule_score_fallback and "impact_score" in rule_score_fallback:
        return _clamp_int(rule_score_fallback.get("impact_score"), 0, 10, 0)
    try:
        return _clamp_int(article.get("impact"), 0, 10, 0)
    except Exception:
        title = str(article.get("title") or "")
        body = str(article.get("sourceSummary") or article.get("summary") or "")
        sent = sentiment(title, body)
        score, _meta = impact_details(title, body, sent, source=str(article.get("source") or ""))
        return score


def _rule_reasons(article: dict, rule_score_fallback: dict | None = None) -> list[str]:
    if rule_score_fallback and isinstance(rule_score_fallback.get("reasons"), list):
        return _clean_list(rule_score_fallback["reasons"], max_items=6, max_len=160)
    impact_meta = article.get("impactMeta") if isinstance(article.get("impactMeta"), dict) else {}
    reasons = _clean_list(impact_meta.get("reasons"), max_items=6, max_len=160)
    return reasons or ["Rule-based fallback analysis used because model output was unavailable or invalid."]


def _rule_indices(article: dict, impact_score: int, sectors: list[str], macro_tags: list[str]) -> list[str]:
    out = []
    if any(tag in macro_tags for tag in {"CRUDE", "USDINR", "RBI_POLICY", "FED", "FII_FLOWS", "GLOBAL_CUES", "INDIA_VIX"}):
        out.append("NIFTY")
    if "Banking" in sectors and impact_score >= 4:
        out.append("BANKNIFTY")
    if impact_score >= 7 and "NIFTY" not in out:
        out.append("NIFTY")
    return out
