"""Runtime helpers for AI chat and AI news-summary orchestration."""

from __future__ import annotations

from pathlib import Path
import json
import os
import re
import threading
import time
from datetime import datetime, timezone

import certifi
import feedparser
import requests

try:
    from backend.agents.news.ai import NewsAiSummaryService
    from backend.agents.news.report_store import save_article_ai_analysis
    from backend.agents.news.sources import GLOBAL_SCOPE, LOCAL_SCOPE, google_news_search_rss
    from backend.agents.news.text import clean_summary, feed_publisher_label, strip_html
    from backend.core.persistence import (
        STATE_DB_PATH,
        load_persisted_ai_news_analysis,
        load_persisted_ai_news_summary,
        persist_ai_news_analysis,
        persist_ai_news_summary,
    )
    from backend.core.settings import IST
    from backend.market.math import round_or_none
    from backend.providers.ai import (
        AiProviderConfigurationError,
        ai_model_name_from_env,
        ai_provider_name_from_env,
        create_ai_text_provider,
    )
    from backend.services.news_runtime import _get_feed as get_feed
except ModuleNotFoundError:
    from agents.news.ai import NewsAiSummaryService
    from agents.news.report_store import save_article_ai_analysis
    from agents.news.sources import GLOBAL_SCOPE, LOCAL_SCOPE, google_news_search_rss
    from agents.news.text import clean_summary, feed_publisher_label, strip_html
    from core.persistence import (
        STATE_DB_PATH,
        load_persisted_ai_news_analysis,
        load_persisted_ai_news_summary,
        persist_ai_news_analysis,
        persist_ai_news_summary,
    )
    from core.settings import IST
    from market.math import round_or_none
    from providers.ai import AiProviderConfigurationError, ai_model_name_from_env, ai_provider_name_from_env, create_ai_text_provider
    from services.news_runtime import _get_feed as get_feed


AI_CHAT_MAX_HISTORY = max(0, int(os.environ.get("AI_CHAT_MAX_HISTORY", "8") or "8"))
AI_CHAT_MAX_CONTEXT_ARTICLES = max(1, int(os.environ.get("AI_CHAT_MAX_CONTEXT_ARTICLES", "10") or "10"))
AI_CHAT_MAX_WEB_RESULTS = max(0, int(os.environ.get("AI_CHAT_MAX_WEB_RESULTS", "6") or "6"))
AI_CHAT_WEB_CACHE_TTL = max(30.0, float(os.environ.get("AI_CHAT_WEB_CACHE_TTL", "180") or "180"))
AI_CHAT_CONTEXT_CHAR_LIMIT = max(4000, int(os.environ.get("AI_CHAT_CONTEXT_CHAR_LIMIT", "14000") or "14000"))
AI_CHAT_MAX_TOKENS = max(200, int(os.environ.get("AI_CHAT_MAX_TOKENS", "750") or "750"))

_ai_summary_service: NewsAiSummaryService | None = None
_ai_summary_service_context_id: int | None = None
_ai_chat_web_cache: dict[str, tuple[list[dict], float]] = {}
_fallback_articles: list[dict] = []
_fallback_lock = threading.Lock()
_thread_local = threading.local()


def ai_chat_provider_name() -> str:
    configured = os.environ.get("AI_CHAT_PROVIDER", "").strip().lower().replace("-", "_")
    aliases = {
        "ollama": "ollama",
        "local": "ollama",
        "bedrock": "bedrock",
        "bedrock_api_key": "bedrock-api-key",
        "bedrock_responses": "bedrock-api-key",
        "bedrock_openai": "bedrock-api-key",
    }
    if configured:
        return aliases.get(configured, configured)
    if os.environ.get("BEDROCK_API_KEY", "").strip():
        return "bedrock-api-key"
    if os.environ.get("AWS_ACCESS_KEY_ID", "").strip() or os.environ.get("AWS_PROFILE", "").strip():
        return "bedrock"
    return ai_provider_name_from_env()


def _trim_text(value, limit: int = 420) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _compact_chat_history(history) -> list[dict]:
    if not isinstance(history, list):
        return []
    out = []
    for item in history[-AI_CHAT_MAX_HISTORY:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").lower()
        if role not in {"user", "assistant"}:
            continue
        content = _trim_text(item.get("content"), 900)
        if content:
            out.append({"role": role, "content": content})
    return out


def _price_momentum_for_chat(history: list | tuple | None) -> dict | None:
    if not history:
        return None
    values = []
    for item in list(history)[-10:]:
        try:
            values.append(round(float(item), 4))
        except Exception:
            continue
    if not values:
        return None
    start = values[0]
    latest = values[-1]
    change = round(latest - start, 4)
    pct = round((change / start * 100) if start else 0.0, 3)
    return {
        "samples": values,
        "sampleCount": len(values),
        "trailChange": change,
        "trailPct": pct,
    }


def _compact_quote_for_chat(label: str, quote: dict | None, history: list | tuple | None = None, context=None) -> dict | None:
    if not quote:
        return None
    compact = {
        "label": label,
        "price": round_or_none(quote.get("price")),
        "previousClose": round_or_none(quote.get("previous_close")),
        "open": round_or_none(quote.get("open")),
        "dayHigh": round_or_none(quote.get("day_high")),
        "dayLow": round_or_none(quote.get("day_low")),
        "change": round_or_none(quote.get("change")),
        "pct": round_or_none(quote.get("pct")),
        "source": quote.get("source") or "market feed",
        "providerTimestamp": quote.get("providerTimestamp"),
        "ageSeconds": _quote_age_seconds(quote, context=context),
        "currencySymbol": quote.get("sym", ""),
    }
    momentum = _price_momentum_for_chat(history)
    if momentum:
        compact["recentMomentum"] = momentum
    return compact


def _chat_query_terms(question: str) -> set[str]:
    aliases = {
        "bent": "brent",
        "xau": "gold",
        "rupee": "inr",
        "dollar": "usd",
        "oil": "crude",
    }
    terms = set()
    for term in re.findall(r"[a-zA-Z0-9/.-]{2,}", str(question or "").lower()):
        clean = term.strip("./-")
        if clean in {"what", "why", "does", "with", "from", "this", "that", "today", "market", "move", "moving"}:
            continue
        terms.add(aliases.get(clean, clean))
    if "usd/inr" in str(question or "").lower():
        terms.update({"usd", "inr"})
    return terms


def _article_relevance_score(article: dict, query_terms: set[str]) -> tuple[int, float]:
    ai_analysis = article.get("aiAnalysis") if isinstance(article.get("aiAnalysis"), dict) else {}
    ai_reasons = " ".join(ai_analysis.get("reasons") or [])
    index_impact = json.dumps(ai_analysis.get("indexImpact") or {}, ensure_ascii=False)
    text = f"{article.get('title', '')} {article.get('summary', '')} {article.get('sector', '')} {ai_reasons} {index_impact}".lower()
    term_hits = sum(1 for term in query_terms if term and term in text)
    if "brent" in query_terms and ("brent" in text or "crude" in text or "oil" in text):
        term_hits += 2
    impact_score = int(article.get("impact") or 0)
    return (term_hits * 10 + impact_score, float(article.get("ts") or 0))


def _articles_for_ai_chat(context=None) -> list[dict]:
    if _call_context_bool(context, "external_worker_mode"):
        runtime_payload = _call_context(context, "runtime_news_payload_from_db") or {}
        raw_articles = runtime_payload.get("articles") if isinstance(runtime_payload.get("articles"), list) else []
    else:
        lock = getattr(context, "_lock", None)
        if lock is not None and hasattr(context, "_arts"):
            with lock:
                raw_articles = list(getattr(context, "_arts", []))
        else:
            raw_articles = list(getattr(context, "_arts", _fallback_articles))
    return raw_articles


def _article_ai_context(article: dict, *, summary_limit: int = 700) -> dict:
    sentiment_payload = article.get("sentiment") if isinstance(article.get("sentiment"), dict) else {}
    impact_meta = article.get("impactMeta") if isinstance(article.get("impactMeta"), dict) else {}
    ai_meta = article.get("aiAnalysis") if isinstance(article.get("aiAnalysis"), dict) else {}
    impact_ai = impact_meta.get("ai") if isinstance(impact_meta.get("ai"), dict) else {}
    reasons = ai_meta.get("reasons") or impact_meta.get("reasons") or []
    return {
        "title": _trim_text(article.get("title"), 180),
        "source": article.get("source"),
        "published": article.get("published"),
        "sector": article.get("sector"),
        "scope": article.get("scope"),
        "impact": article.get("impact"),
        "sentiment": sentiment_payload.get("label") or article.get("sentiment"),
        "summary": _trim_text(article.get("summary"), summary_limit),
        "summarySource": article.get("summarySource") or "feed",
        "analysisSource": article.get("analysisSource") or "",
        "indexImpact": ai_meta.get("indexImpact") or impact_ai.get("indexImpact") or {},
        "reasons": reasons[:5] if isinstance(reasons, list) else [],
        "textSource": ai_meta.get("textSource") or impact_ai.get("textSource") or "",
        "inputChars": ai_meta.get("inputChars") or impact_ai.get("inputChars") or 0,
    }


def _topic_ai_summaries_for_ai_chat(question: str, limit: int = 8, context=None) -> list[dict]:
    query_terms = _chat_query_terms(question)
    candidates = []
    for article in _articles_for_ai_chat(context=context):
        if article.get("summarySource") != "ai" or not str(article.get("summary") or "").strip():
            continue
        score = _article_relevance_score(article, query_terms)
        if score[0] <= 0 and query_terms:
            continue
        candidates.append((score, article))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return [_article_ai_context(article, summary_limit=760) for _score, article in candidates[:limit]]


def _recent_articles_for_ai_chat(question: str, limit: int = AI_CHAT_MAX_CONTEXT_ARTICLES, context=None) -> list[dict]:
    raw_articles = _articles_for_ai_chat(context=context)
    query_terms = _chat_query_terms(question)
    scored = sorted(
        raw_articles,
        key=lambda article: _article_relevance_score(article, query_terms),
        reverse=True,
    )
    return [_article_ai_context(article, summary_limit=520) for article in scored[:limit]]


def _ai_chat_web_query(question: str) -> str:
    text = str(question or "").strip()
    lower = text.lower().replace("bent", "brent")
    if "brent" in lower or "crude" in lower or "oil" in lower:
        return "Brent crude oil prices why up today OPEC inventory demand geopolitics"
    if "usd" in lower or "inr" in lower or "rupee" in lower:
        return "USD INR rupee why moving today RBI dollar oil yields"
    if "nifty bank" in lower or "bank nifty" in lower:
        return "Bank Nifty why moving today Indian banks RBI stocks"
    if "nifty" in lower or "sensex" in lower:
        return "Nifty Sensex why moving today Indian stock market"
    return f"{text} market news finance today"


def _internet_results_for_ai_chat(question: str, context=None, feed_fetcher=None, web_cache: dict | None = None) -> list[dict]:
    if AI_CHAT_MAX_WEB_RESULTS <= 0:
        return []
    query = _ai_chat_web_query(question)
    cache = _ai_chat_web_cache if web_cache is None else web_cache
    cache_key = query.lower()
    now = time.time()
    cached = cache.get(cache_key)
    if cached and now - cached[1] < AI_CHAT_WEB_CACHE_TTL:
        return list(cached[0])

    fetcher = feed_fetcher or get_feed
    results: list[dict] = []
    seen_links: set[str] = set()
    for scope in [GLOBAL_SCOPE, LOCAL_SCOPE]:
        if len(results) >= AI_CHAT_MAX_WEB_RESULTS:
            break
        try:
            data = fetcher(google_news_search_rss(query, scope))
            feed = feedparser.parse(data)
        except Exception as exc:
            if _call_context_bool(context, "upstox_debug_enabled"):
                print(f"[!] AI chat web context ({scope}): {exc}")
            continue
        for entry in feed.entries[:AI_CHAT_MAX_WEB_RESULTS * 2]:
            link = str(entry.get("link") or "").strip()
            if link and link in seen_links:
                continue
            seen_links.add(link)
            source_meta = entry.get("source") or {}
            publisher = strip_html(source_meta.get("title", "")) if hasattr(source_meta, "get") else ""
            title = strip_html(entry.get("title", "")).strip()
            summary = clean_summary(strip_html(entry.get("summary", entry.get("description", ""))))
            if not title:
                continue
            try:
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                published_dt = datetime(*published[:6], tzinfo=timezone.utc).astimezone(IST) if published else None
            except Exception:
                published_dt = None
            results.append({
                "title": _trim_text(title, 220),
                "source": publisher or feed_publisher_label("Google News"),
                "scope": scope,
                "published": published_dt.isoformat() if published_dt else None,
                "summary": _trim_text(summary, 480),
                "link": link,
            })
            if len(results) >= AI_CHAT_MAX_WEB_RESULTS:
                break
    cache[cache_key] = (list(results), now)
    return results


def build_ai_chat_context(question: str, history=None, context=None, feed_fetcher=None, web_cache: dict | None = None) -> dict:
    snapshot = None
    if _call_context_bool(context, "external_worker_mode"):
        snapshot = _call_context(context, "runtime_snapshot_from_db", include_history=True)
    snapshot = snapshot if isinstance(snapshot, dict) else _call_context(context, "market_data_snapshot", include_history=True)
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    analytics = snapshot.get("analytics") if isinstance(snapshot.get("analytics"), dict) else {}
    derivatives = snapshot.get("derivatives") if isinstance(snapshot.get("derivatives"), dict) else {}
    ticks = snapshot.get("ticks") if isinstance(snapshot.get("ticks"), dict) else {}
    tracked_quotes = snapshot.get("trackedQuotes") if isinstance(snapshot.get("trackedQuotes"), dict) else {}
    history_map = snapshot.get("history") if isinstance(snapshot.get("history"), dict) else {}
    status = snapshot.get("marketStatus") if isinstance(snapshot.get("marketStatus"), dict) else (_call_context(context, "get_market_status") or {})
    provider = snapshot.get("dataProvider") if isinstance(snapshot.get("dataProvider"), dict) else (_call_context(context, "market_data_provider_status") or {})

    return {
        "asOf": (_call_context(context, "ist_now") or datetime.now(IST)).isoformat(),
        "marketStatus": {
            "session": status.get("session"),
            "label": status.get("sessionLabel"),
            "reason": status.get("reason"),
            "tickerAgeSeconds": status.get("tickerAgeSeconds"),
            "newsAgeSeconds": status.get("newsAgeSeconds"),
            "staleData": status.get("staleData"),
        },
        "dataProvider": {
            "active": provider.get("active"),
            "requested": provider.get("requested"),
            "reason": provider.get("reason"),
            "streamConnected": provider.get("streamConnected"),
            "degraded": provider.get("degraded"),
        },
        "tickerTape": {
            label: compact
            for label, quote in ticks.items()
            if (compact := _compact_quote_for_chat(label, quote, history_map.get(label), context=context))
        },
        "trackedQuotes": {
            label: compact
            for label, quote in tracked_quotes.items()
            if (compact := _compact_quote_for_chat(label, quote, history_map.get(label), context=context))
        },
        "analytics": {
            "overviewCards": analytics.get("overviewCards", [])[:7],
            "alerts": analytics.get("alerts", [])[:5],
            "sectorBoard": analytics.get("sectorBoard", [])[:8],
            "keyLevels": analytics.get("keyLevels", [])[:6],
        },
        "derivatives": {
            "predictionCards": derivatives.get("predictionCards", [])[:6],
            "riskFlags": derivatives.get("riskFlags", [])[:6],
            "contextNotes": derivatives.get("contextNotes", [])[:6],
            "crossAssetRows": derivatives.get("crossAssetRows", [])[:6],
            "relativeValueRows": derivatives.get("relativeValueRows", [])[:6],
        },
        "topicAiSummaries": _topic_ai_summaries_for_ai_chat(question, context=context),
        "recentNews": _recent_articles_for_ai_chat(question, context=context),
        "internetNews": _internet_results_for_ai_chat(question, context=context, feed_fetcher=feed_fetcher, web_cache=web_cache),
    }


def build_ai_chat_prompt(question: str, context_payload: dict, history=None) -> str:
    compact_history = _compact_chat_history(history)
    context_json = json.dumps(context_payload, ensure_ascii=False, sort_keys=True)
    if len(context_json) > AI_CHAT_CONTEXT_CHAR_LIMIT:
        context_json = context_json[:AI_CHAT_CONTEXT_CHAR_LIMIT].rstrip() + "...<truncated>"
    history_text = "\n".join(f"{item['role']}: {item['content']}" for item in compact_history) or "No previous chat."
    return f"""You are StockTerminal's AI Market Chat for an intraday Indian-market dashboard.

Rules:
- Use only the supplied dashboard context and recent news. If the context is not enough, say exactly what is missing.
- Treat internetNews as fresh web/news evidence fetched by the app at request time. Mention source names when those items drive the answer.
- Treat topicAiSummaries as pre-analyzed article evidence selected for the user's topic. Use them before generic recentNews, but decide whether they actually explain the move.
- If the user writes "bent crude", interpret it as "Brent crude".
- Explain market moves by combining live price change, recentMomentum, source/age of quote, topicAiSummaries, fresh internetNews, macro/sector context, and uncertainty.
- If the evidence is weak or unrelated, say that clearly and give the most likely drivers to verify next.
- Keep the answer concise but useful: start with a one-sentence direct answer, then 3-5 evidence bullets, then one "Watch next" line.
- Do not use markdown headings like ###. Use plain labels if a section label helps.
- Do not invent facts, prices, sources, or breaking news.
- Do not place trades or promise outcomes. You can describe bias, risk, and what to watch next.

Recent chat:
{history_text}

User question:
{question}

Live dashboard context JSON:
{context_json}
"""


def generate_ai_chat_response(
    question: str,
    history=None,
    context=None,
    provider_factory=None,
    context_builder=None,
    http_session_factory=None,
) -> tuple[str, str, str]:
    provider_name = ai_chat_provider_name()
    provider_factory = provider_factory or create_ai_text_provider
    provider = provider_factory(http_session_factory=http_session_factory or _http_session, provider_name=provider_name)
    if not provider.is_configured():
        raise AiProviderConfigurationError(
            "AI chat is not configured. Set BEDROCK_API_KEY for Bedrock API-key chat, or configure AWS credentials for Bedrock Runtime."
        )
    context_payload = context_builder(question) if callable(context_builder) else build_ai_chat_context(question, history=history, context=context)
    prompt = build_ai_chat_prompt(question, context_payload, history=history)
    answer = provider.generate_text(
        prompt=prompt,
        temperature=0.2,
        max_tokens=AI_CHAT_MAX_TOKENS,
        json_mode=False,
    )
    return answer.strip(), provider_name, ai_model_name_from_env(provider_name)


def handle_ai_article_analysis_applied(article: dict, context=None) -> None:
    if context is None:
        return
    article_id = str(article.get("id") or "")
    lock = getattr(context, "_lock", None)
    if lock is None:
        return
    with lock:
        article_is_live = bool(article_id) and any(str(current.get("id") or "") == article_id for current in getattr(context, "_arts", []))
        articles = list(getattr(context, "_arts", []))
        feed_status = dict(getattr(context, "_feed_status", {}))
        updated = getattr(context, "_updated", "")
        refreshed_at = getattr(context, "_last_news_refresh_ts", None) or time.time()
    if not article_is_live:
        return
    try:
        _call_context(context, "rebuild_computed_payloads")
        _call_context(context, "persist_runtime_news_payload", articles, feed_status, updated, refreshed_at)
        _call_context(context, "persist_runtime_snapshot_payload")
        _call_context(context, "broadcast_market_snapshot")
    except Exception as exc:
        print(f"[!] AI analytics refresh error: {exc}")


def persist_news_article_analysis(cache_key: str, article: dict, analysis: dict, path: Path = STATE_DB_PATH) -> None:
    persist_ai_news_analysis(cache_key, article, analysis, path)
    try:
        save_article_ai_analysis(analysis, path)
    except Exception as exc:
        print(f"[!] news agent article analysis persist error: {exc}")


def ai_summary_service(context=None) -> NewsAiSummaryService:
    global _ai_summary_service, _ai_summary_service_context_id
    context_id = id(context) if context is not None else None
    if _ai_summary_service is None or (_ai_summary_service_context_id is None and context is not None):
        _ai_summary_service = NewsAiSummaryService(
            http_session_factory=_context_callable(context, "http_session") or _http_session,
            load_persisted_summary=load_persisted_ai_news_summary,
            load_persisted_analysis=load_persisted_ai_news_analysis,
            persist_summary=persist_ai_news_summary,
            persist_analysis=persist_news_article_analysis,
            articles_factory=_articles_factory(context),
            articles_lock=getattr(context, "_lock", None) or _fallback_lock,
            on_analysis_applied=lambda article: handle_ai_article_analysis_applied(article, context=context),
        )
        _ai_summary_service_context_id = context_id
    return _ai_summary_service


def ollama_api_base(context=None) -> str:
    return ai_summary_service(context=context).ollama_api_base()


def ai_news_summary_model(context=None) -> str:
    return ai_summary_service(context=context).ai_news_summary_model()


def ai_news_summaries_enabled(context=None) -> bool:
    return ai_summary_service(context=context).ai_news_summaries_enabled()


def article_extraction_enabled(context=None) -> bool:
    return ai_summary_service(context=context).article_extraction_enabled()


def article_link_supports_direct_extraction(link: str, context=None) -> bool:
    return ai_summary_service(context=context).article_link_supports_direct_extraction(link)


def fetch_accessible_article_text(article: dict | str, title: str = "", max_chars: int = 4000, context=None) -> str:
    payload = {"link": article, "title": title} if isinstance(article, str) else article
    text = ai_summary_service(context=context).fetch_accessible_article_text(payload)
    return text[:max_chars] if max_chars else text


def prepare_article_for_ai_summary(article: dict, context=None) -> dict:
    return ai_summary_service(context=context).prepare_article_for_ai_summary(article)


def truncate_ai_summary_input(text: str, max_chars: int = 4500, context=None) -> str:
    if max_chars == 4500:
        return ai_summary_service(context=context).truncate_ai_summary_input(text)
    clean = str(text or "")
    return clean if len(clean) <= max_chars else clean[:max_chars].rstrip() + "...<truncated>"


def ai_summary_cache_key(article: dict, context=None) -> str:
    return ai_summary_service(context=context).ai_summary_cache_key(article)


def ai_analysis_cache_key(article: dict, context=None) -> str:
    return ai_summary_service(context=context).ai_analysis_cache_key(article)


def get_cached_ai_news_summary(cache_key: str, context=None) -> str:
    return ai_summary_service(context=context).get_cached_ai_news_summary(cache_key)


def article_is_in_ai_summary_window(article: dict, now: float | None = None, context=None) -> bool:
    return ai_summary_service(context=context).article_is_in_ai_summary_window(article, now)


def article_has_ai_summary(article: dict, context=None) -> bool:
    return ai_summary_service(context=context).article_has_ai_summary(article)


def ai_summary_update_payload(article: dict, context=None) -> dict:
    return ai_summary_service(context=context).ai_summary_update_payload(article)


def hydrate_article_from_ai_cache(article: dict, context=None) -> bool:
    return ai_summary_service(context=context).hydrate_article_from_ai_cache(article)


def ai_summary_progress_for_articles(articles: list[dict], now: float | None = None, context=None) -> dict:
    return ai_summary_service(context=context).ai_summary_progress_for_articles(articles, now)


def ai_summary_executor(context=None):
    return ai_summary_service(context=context).ai_summary_executor_instance()


def apply_ai_summary_to_article(article: dict, summary: str, context=None) -> None:
    ai_summary_service(context=context).apply_ai_summary_to_article(article, summary)


def generate_ai_news_summary(article: dict, context=None) -> str:
    return ai_summary_service(context=context).generate_ai_news_summary(article)


def queue_ai_news_summary(article: dict, context=None) -> None:
    ai_summary_service(context=context).queue_ai_news_summary(article)


def enrich_articles_with_ai_summaries(articles: list[dict], context=None) -> None:
    ai_summary_service(context=context).enrich_articles_with_ai_summaries(articles)


def ai_runtime_status() -> dict:
    provider_name = ai_chat_provider_name()
    service = ai_summary_service()
    return {
        "chat_provider": provider_name,
        "chat_model": ai_model_name_from_env(provider_name),
        "chat_web_results": AI_CHAT_MAX_WEB_RESULTS,
        "news_summaries_enabled": service.ai_news_summaries_enabled(),
        "article_extraction_enabled": service.article_extraction_enabled(),
        "read_only": True,
    }


def _quote_age_seconds(quote: dict | None, context=None) -> float | None:
    value = _call_context(context, "quote_age_seconds", quote)
    if value is not None:
        return value
    if not quote or quote.get("fetchedAt") is None:
        return None
    return round(max(time.time() - float(quote["fetchedAt"]), 0), 1)


def _articles_factory(context=None):
    if context is None:
        return lambda: _fallback_articles
    return lambda: getattr(context, "_arts", _fallback_articles)


def _call_context(context, name: str, *args, **kwargs):
    candidate = _context_callable(context, name)
    if candidate is None:
        return None
    return candidate(*args, **kwargs)


def _call_context_bool(context, name: str) -> bool:
    try:
        candidate = getattr(context, name)
    except AttributeError:
        return False
    if callable(candidate):
        return bool(candidate())
    return bool(candidate)


def _context_callable(context, name: str):
    if context is None:
        return None
    try:
        candidate = getattr(context, name)
    except AttributeError:
        return None
    return candidate if callable(candidate) else None


def _http_session() -> requests.Session:
    session = getattr(_thread_local, "http_session", None)
    if session is None:
        session = requests.Session()
        session.verify = certifi.where()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept": "application/json,text/html,*/*",
            "Accept-Language": "en-IN,en;q=0.9",
        })
        _thread_local.http_session = session
    return session
