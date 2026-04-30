"""News classification, sentiment, and impact scoring."""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime

try:
    from backend.market.math import clamp
    from backend.news.sources import LOCAL_SCOPE
    from backend.news.text import keyword_found
    from backend.core.settings import IST
except ModuleNotFoundError:
    from market.math import clamp
    from news.sources import LOCAL_SCOPE
    from news.text import keyword_found
    from core.settings import IST


SECTOR_KW = {
    "IT": ["infosys", "tcs", "wipro", "hcl", "tech mahindra", "coforge", "mphasis",
           "ltimindtree", "hexaware", "software", "it sector", "nifty it", "nasdaq", "accenture"],
    "Banking": ["hdfc bank", "icici bank", "sbi", "kotak", "axis bank", "rbi", "banking",
                "npa", "credit", "loan", "nifty bank", "bandhan", "indusind", "yes bank"],
    "Pharma": ["sun pharma", "cipla", "dr reddy", "aurobindo", "divi", "lupin", "alkem",
               "pharmaceutical", "drug", "fda", "usfda", "biocon", "glenmark"],
    "Auto": ["maruti", "tata motors", "m&m", "bajaj auto", "hero motocorp", "eicher",
             "automobile", "ev", "electric vehicle", "auto sector", "tvs", "ola electric"],
    "Energy": ["reliance", "ongc", "ntpc", "power grid", "adani energy", "torrent power",
               "oil", "gas", "crude", "crude oil", "brent", "wti", "opec", "solar",
               "renewable", "bpcl", "ioc", "coal"],
    "FMCG": ["hindustan unilever", "hul", "itc", "nestle", "dabur", "emami",
             "britannia", "fmcg", "consumer goods", "marico", "colgate", "godrej"],
    "Metals": ["tata steel", "jsw", "hindalco", "vedanta", "coal india", "jindal",
               "steel", "aluminium", "copper", "metal", "nmdc", "sail", "moil"],
    "Infra": ["l&t", "larsen", "adani ports", "delhivery", "ircon", "rvnl",
              "infrastructure", "construction", "cement", "road", "nhpc", "abb"],
}

BULLISH = {
    "surge": 3, "soar": 3, "rally": 2, "record high": 3, "all-time high": 3, "52-week high": 2,
    "outperform": 2, "upgrade": 2, "beat estimates": 3, "breakout": 2, "top gainer": 2,
    "gain": 1, "rise": 1, "growth": 1, "positive": 1, "strong": 1, "robust": 2,
    "bullish": 3, "buy": 2, "recovery": 2, "rebound": 2, "jump": 2,
    "boost": 2, "higher": 1, "profit": 1, "earnings beat": 3, "outperformed": 2,
    "above expectations": 2, "overweight": 2, "accumulate": 2, "target raised": 3,
    "dividend": 1, "buyback": 2, "record profit": 3, "margin expansion": 2,
}

BEARISH = {
    "crash": 3, "plunge": 3, "collapse": 3, "sell-off": 3, "selloff": 3,
    "bearish": 3, "downgrade": 2, "miss": 2, "fall": 2, "drop": 2,
    "decline": 2, "weak": 1, "loss": 2, "risk": 1, "concern": 1,
    "pressure": 1, "correction": 2, "slump": 2, "negative": 1, "warning": 2,
    "below expectations": 2, "cut": 1, "layoffs": 2, "slowdown": 2,
    "recession": 3, "underweight": 2, "underperform": 2, "target cut": 3,
    "margin squeeze": 2, "write-off": 3, "fraud": 3, "default": 3, "penalty": 2,
    "52-week low": 2, "circuit breaker": 2, "suspension": 2, "ban": 2,
}

IMPACT_EVENTS = {
    "rbi": 2.0, "sebi": 1.8, "budget": 1.8, "gdp": 1.4, "inflation": 1.5,
    "fed": 1.4, "tariff": 1.4, "sanction": 1.5, "crash": 2.4,
    "surge": 1.2, "record": 1.1, "ban": 2.0, "merger": 1.6,
    "acquisition": 1.6, "takeover": 1.8, "quarterly results": 1.4,
    "earnings": 1.2, "dividend": 0.7, "rate cut": 2.0, "rate hike": 2.0,
    "repo rate": 1.8, "policy": 1.2, "war": 1.8, "election": 1.2,
    "ipo": 0.8, "block deal": 1.2, "bulk deal": 1.2, "fii": 1.0,
    "dii": 0.9, "circuit": 1.8, "halt": 1.8, "usfda": 1.5,
    "crude oil": 1.5, "brent": 1.2, "wti": 1.1, "opec": 1.2,
    "q1": 0.6, "q2": 0.6, "q3": 0.6, "q4": 0.6,
}
IMPACT_KW = list(IMPACT_EVENTS)

MARKET_CONTEXT = {
    "nifty": 1.0, "sensex": 0.9, "bank nifty": 1.0, "rupee": 0.8,
    "usd/inr": 0.8, "india vix": 0.8, "f&o": 0.8, "derivatives": 0.7,
    "options": 0.6, "futures": 0.6, "nse": 0.6, "bse": 0.5,
    "infosys": 0.5, "tcs": 0.5, "reliance": 0.5, "hdfc bank": 0.5,
    "icici bank": 0.5, "sbi": 0.5,
}

TITLE_SHOCK = {
    "crash": 1.5, "plunge": 1.4, "collapse": 1.4, "surge": 1.1,
    "soar": 1.1, "record": 0.9, "ban": 1.2, "fraud": 1.5,
    "default": 1.5, "penalty": 1.0, "rate cut": 1.1, "rate hike": 1.1,
}

LOW_SIGNAL_HEADLINES = {
    "stocks to watch": 1.2, "market live": 1.0, "live updates": 1.0,
    "top news": 0.8, "opening bell": 0.7, "closing bell": 0.7,
    "trade setup": 0.8, "day trading guide": 0.8,
}

SOURCE_IMPACT = {
    "reuters": 1.0, "bloomberg": 1.0, "cnbc tv18": 0.9, "cnbc-tv18": 0.9,
    "moneycontrol": 0.9, "ndtv profit": 0.85, "business standard": 0.8,
    "economic times": 0.8, "et markets": 0.8, "businessline": 0.75,
    "livemint": 0.75, "financial express": 0.7, "financial times": 0.8,
    "wall street journal": 0.75, "wsj": 0.75, "barrons": 0.65,
    "marketwatch": 0.55, "yahoo finance": 0.5, "google news": 0.3,
}


def weighted_keyword_hits(
    title: str,
    body: str,
    weights: dict[str, float],
    title_multiplier: float = 1.35,
    body_multiplier: float = 1.0,
) -> tuple[float, list[str]]:
    score = 0.0
    hits: list[str] = []
    title = title or ""
    body = body or ""
    for keyword, weight in weights.items():
        if keyword_found(title, keyword):
            score += weight * title_multiplier
            hits.append(keyword)
        elif body and keyword_found(body, keyword):
            score += weight * body_multiplier
            hits.append(keyword)
    return score, hits


def classify(text: str) -> str:
    t = (text or "").lower()
    scores = {sec: sum(1 for k in kws if keyword_found(t, k)) for sec, kws in SECTOR_KW.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "General"


def sentiment(title: str, body: str) -> dict:
    bull, _ = weighted_keyword_hits(title, body, BULLISH, title_multiplier=1.6)
    bear, _ = weighted_keyword_hits(title, body, BEARISH, title_multiplier=1.6)
    total = bull + bear
    edge = abs(bull - bear)
    if total <= 0 or edge < 0.75 or edge / total < 0.15:
        return {"label": "neutral", "score": 0.0}
    score = round(min(max(bull, bear) / (total + 2.0), 1.0), 2)
    if bull > bear:
        return {"label": "bullish", "score": score}
    if bear > bull:
        return {"label": "bearish", "score": score}
    return {"label": "neutral", "score": 0.0}


def source_impact_score(source: str) -> float:
    source_l = (source or "").lower()
    for key, score in SOURCE_IMPACT.items():
        if key in source_l:
            return score
    return 0.35 if source_l else 0.0


def recency_impact_score(published_dt: datetime | None, now: datetime | None = None) -> float:
    if published_dt is None:
        return 0.0
    now = now or datetime.now(IST)
    if published_dt.tzinfo is None:
        published_dt = published_dt.replace(tzinfo=IST)
    age_hours = max((now - published_dt.astimezone(IST)).total_seconds() / 3600, 0)
    if age_hours <= 0.5:
        return 1.0
    if age_hours <= 2:
        return 0.8
    if age_hours <= 6:
        return 0.5
    if age_hours <= 24:
        return 0.2
    if age_hours <= 72:
        return -0.3
    return -0.8


def low_signal_penalty(title: str, body: str, core_score: float) -> tuple[float, list[str]]:
    score, hits = weighted_keyword_hits(title, body, LOW_SIGNAL_HEADLINES, title_multiplier=1.0)
    if not hits:
        return 0.0, []
    penalty = min(score, 1.4)
    if core_score >= 4:
        penalty *= 0.45
    return round(penalty, 2), hits


def impact_details(
    title: str,
    body: str,
    sent: dict,
    source: str = "",
    published_dt: datetime | None = None,
    scope: str = LOCAL_SCOPE,
    now: datetime | None = None,
) -> tuple[int, dict]:
    event_raw, event_hits = weighted_keyword_hits(title, body, IMPACT_EVENTS, title_multiplier=1.35)
    context_raw, context_hits = weighted_keyword_hits(title, body, MARKET_CONTEXT, title_multiplier=1.2)
    shock_raw, shock_hits = weighted_keyword_hits(title, "", TITLE_SHOCK, title_multiplier=1.0)

    sentiment_component = min(float(sent.get("score", 0.0)) * 4.2, 3.6)
    event_component = min(event_raw, 4.2)
    context_component = min(context_raw, 1.2)
    title_component = min(shock_raw, 1.4)
    core_score = sentiment_component + event_component + context_component + title_component

    source_component = source_impact_score(source)
    freshness_component = recency_impact_score(published_dt, now)
    if core_score < 2:
        source_component *= 0.35
        freshness_component *= 0.35
    if scope == LOCAL_SCOPE and context_hits:
        context_component = min(context_component + 0.25, 1.3)

    penalty, low_signal_hits = low_signal_penalty(title, body, core_score)
    raw_score = core_score + source_component + freshness_component - penalty
    if core_score < 1:
        raw_score = min(raw_score, 2.0)

    score = int(clamp(round(raw_score), 0, 10))
    matched = sorted(set(event_hits + context_hits + shock_hits))
    reasons = []
    if sent.get("label") != "neutral":
        reasons.append(f"{sent.get('label')} sentiment")
    if event_hits:
        reasons.append("event: " + ", ".join(event_hits[:4]))
    if context_hits:
        reasons.append("market context: " + ", ".join(context_hits[:3]))
    if source_component >= 0.6:
        reasons.append(f"source: {source}")
    if freshness_component >= 0.6:
        reasons.append("fresh headline")
    if penalty:
        reasons.append("generic headline penalty")

    return score, {
        "raw": round(raw_score, 2),
        "components": {
            "sentiment": round(sentiment_component, 2),
            "event": round(event_component, 2),
            "context": round(context_component, 2),
            "title": round(title_component, 2),
            "source": round(source_component, 2),
            "freshness": round(freshness_component, 2),
            "penalty": round(penalty, 2),
        },
        "matchedKeywords": matched[:12],
        "lowSignalKeywords": low_signal_hits,
        "reasons": reasons[:6],
    }


def impact(
    title: str,
    body: str,
    sent: dict,
    source: str = "",
    published_dt: datetime | None = None,
    scope: str = LOCAL_SCOPE,
) -> int:
    score, _ = impact_details(title, body, sent, source=source, published_dt=published_dt, scope=scope)
    return score


def build_sector_news_scores(articles: list[dict]) -> dict[str, dict]:
    scores: dict[str, dict] = defaultdict(lambda: {"score": 0.0, "count": 0, "bull": 0, "bear": 0})
    now_ts = time.time()
    for art in articles:
        sector = art.get("sector") or "General"
        sent = art.get("sentiment", {}).get("label", "neutral")
        sign = 1 if sent == "bullish" else -1 if sent == "bearish" else 0
        age_hours = max((now_ts - art.get("ts", now_ts)) / 3600, 0)
        recency = max(0.35, 1.35 - (age_hours / 24))
        weight = art.get("impact", 0) * recency
        if art.get("analysisSource") == "ai":
            try:
                confidence = float((art.get("aiAnalysis") or {}).get("confidence", 0.5))
            except (TypeError, ValueError):
                confidence = 0.5
            weight *= max(0.65, min(1.2, 0.75 + (confidence * 0.45)))
        scores[sector]["score"] += sign * weight
        scores[sector]["count"] += 1
        if sent == "bullish":
            scores[sector]["bull"] += 1
        elif sent == "bearish":
            scores[sector]["bear"] += 1
    return {k: {"score": round(v["score"], 2), **v} for k, v in scores.items()}


def sector_bias_label(score: float) -> tuple[str, str]:
    if score >= 6:
        return "Positive", "bull"
    if score <= -6:
        return "Negative", "bear"
    return "Mixed", "neutral"
