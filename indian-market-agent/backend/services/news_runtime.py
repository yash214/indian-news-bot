"""Runtime helpers for dashboard news refresh and persisted news payloads."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import threading
import time

import certifi
import feedparser
import requests

try:
    from backend.agents.news.scoring import classify, impact_details, sentiment
    from backend.agents.news.sources import RSS_FEEDS
    from backend.agents.news.text import (
        build_article_preview,
        clean_headline,
        feed_publisher_label,
        normalized_headline,
        strip_html,
        url_hash,
    )
    from backend.core.persistence import db_get_json, db_set_json, persist_refresh_settings
    from backend.core.settings import ALLOWED_REFRESH_WINDOWS, IST
except ModuleNotFoundError:
    from agents.news.scoring import classify, impact_details, sentiment
    from agents.news.sources import RSS_FEEDS
    from agents.news.text import build_article_preview, clean_headline, feed_publisher_label, normalized_headline, strip_html, url_hash
    from core.persistence import db_get_json, db_set_json, persist_refresh_settings
    from core.settings import ALLOWED_REFRESH_WINDOWS, IST


MAX_NEWS_WORKERS = 8
RUNTIME_NEWS_STATE_KEY = "runtime_news_payload"

_lock = threading.Lock()
_thread_local = threading.local()
_news_refresh_seconds = 300
_refresh_wakeup = None
_ai_summary_progress_for_articles = None
_enrich_articles_with_ai_summaries = None
_latest_articles: list[dict] = []
_latest_feed_status: dict = {}
_latest_updated: str = ""
_last_news_refresh_ts: float | None = None


def configure_news_runtime(
    *,
    refresh_seconds: int | None = None,
    refresh_wakeup=None,
    ai_summary_progress_for_articles=None,
    enrich_articles_with_ai_summaries=None,
) -> None:
    global _news_refresh_seconds, _refresh_wakeup, _ai_summary_progress_for_articles, _enrich_articles_with_ai_summaries
    with _lock:
        if refresh_seconds is not None:
            _news_refresh_seconds = int(refresh_seconds)
        _refresh_wakeup = refresh_wakeup
        if ai_summary_progress_for_articles is not None:
            _ai_summary_progress_for_articles = ai_summary_progress_for_articles
        if enrich_articles_with_ai_summaries is not None:
            _enrich_articles_with_ai_summaries = enrich_articles_with_ai_summaries


def get_news_refresh_seconds() -> int:
    with _lock:
        return _news_refresh_seconds


def set_news_refresh_seconds(seconds: int) -> int:
    global _news_refresh_seconds
    if seconds not in ALLOWED_REFRESH_WINDOWS:
        raise ValueError("Unsupported refresh interval")
    with _lock:
        _news_refresh_seconds = seconds
        wakeup = _refresh_wakeup
    persist_refresh_settings(seconds)
    if wakeup is not None:
        wakeup.set()
    return seconds


def _get_feed(url: str) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.google.com/",
    }
    response = _http_session().get(url, headers=headers, timeout=8)
    response.raise_for_status()
    return response.content


def fetch_feed_articles(feed_cfg: dict[str, str]) -> tuple[str, dict, list[dict]]:
    src = feed_cfg["name"]
    url = feed_cfg["url"]
    scope = feed_cfg["scope"]
    articles = []
    try:
        data = _get_feed(url)
        feed = feedparser.parse(data)
        for entry in feed.entries[:18]:
            source_meta = entry.get("source") or {}
            publisher = strip_html(source_meta.get("title", "")) if hasattr(source_meta, "get") else ""
            publisher = publisher or feed_publisher_label(src)
            title = clean_headline(entry.get("title", ""), publisher)
            summary = build_article_preview(title, entry.get("summary", entry.get("description", "")), publisher)
            link = entry.get("link", "#")
            article_id = url_hash(link) if link != "#" else url_hash(title[:60])
            if not title:
                continue

            try:
                published = entry.get("published_parsed") or entry.get("updated_parsed")
                published_dt = datetime(*published[:6], tzinfo=timezone.utc).astimezone(IST) if published else ist_now()
            except Exception:
                published_dt = ist_now()

            sent = sentiment(title, summary)
            impact_score, impact_meta = impact_details(
                title,
                summary,
                sent,
                source=publisher,
                published_dt=published_dt,
                scope=scope,
            )
            articles.append({
                "id": article_id,
                "title": title,
                "titleKey": normalized_headline(title),
                "summary": summary,
                "sourceSummary": summary,
                "link": link,
                "source": publisher,
                "feed": src,
                "scope": scope,
                "sector": classify(title + " " + summary),
                "sentiment": sent,
                "impact": impact_score,
                "impactMeta": impact_meta,
                "published": published_dt.strftime("%d %b %H:%M"),
                "ts": published_dt.timestamp(),
            })
        return src, {"ok": True, "count": len(articles), "scope": scope}, articles
    except Exception as exc:
        print(f"[!] Feed {src}: {exc}")
        return src, {"ok": False, "error": str(exc)[:120], "scope": scope}, []


def fetch_news(context=None) -> tuple[list[dict], dict]:
    seen_hashes, seen_titles, out = set(), set(), []
    status = {}

    with ThreadPoolExecutor(max_workers=min(MAX_NEWS_WORKERS, len(RSS_FEEDS))) as executor:
        futures = [executor.submit(fetch_feed_articles, feed_cfg) for feed_cfg in RSS_FEEDS]
        for future in as_completed(futures):
            src, feed_status, articles = future.result()
            status[src] = feed_status
            for article in articles:
                title_key = article.pop("titleKey", "")
                if article["id"] in seen_hashes or title_key in seen_titles:
                    continue
                seen_hashes.add(article["id"])
                seen_titles.add(title_key)
                out.append(article)

    out.sort(key=lambda item: -item["ts"])
    _enrich_articles(out, context=context)
    _remember_latest(out, status, ist_now().strftime("%H:%M:%S"), time.time())
    return out, status


def persist_runtime_news_payload(
    articles: list[dict] | None = None,
    feed_status: dict | None = None,
    updated: str | None = None,
    refreshed_at: float | None = None,
    context=None,
) -> None:
    articles = list(_latest_articles if articles is None else articles)
    feed_status = dict(_latest_feed_status if feed_status is None else feed_status)
    updated = _latest_updated if updated is None else updated
    refreshed_at = _last_news_refresh_ts if refreshed_at is None else refreshed_at
    try:
        db_set_json(
            RUNTIME_NEWS_STATE_KEY,
            {
                "articles": articles,
                "feedStatus": feed_status,
                "updated": updated,
                "lastNewsRefreshTs": refreshed_at,
                "aiSummaryProgress": _ai_summary_progress(articles, context=context),
            },
        )
    except Exception as exc:
        print(f"[!] runtime news persist error: {exc}")


def runtime_news_payload_from_db() -> dict | None:
    payload = db_get_json(RUNTIME_NEWS_STATE_KEY, default=None)
    return payload if isinstance(payload, dict) else None


def refresh_news_now(context=None) -> list[dict]:
    articles, feed_status = fetch_news(context=context)
    updated = ist_now().strftime("%H:%M:%S")
    refreshed_at = time.time()
    _remember_latest(articles, feed_status, updated, refreshed_at)
    persist_runtime_news_payload(articles, feed_status, updated, refreshed_at, context=context)
    return articles


def get_latest_news_items(context=None) -> list[dict]:
    if context is not None:
        lock = getattr(context, "_lock", None)
        if lock is not None and hasattr(context, "_arts"):
            with lock:
                return list(getattr(context, "_arts", []))
        if hasattr(context, "_arts"):
            return list(getattr(context, "_arts", []))
    with _lock:
        return list(_latest_articles)


def news_runtime_status() -> dict:
    with _lock:
        article_count = len(_latest_articles)
        last_refresh = _last_news_refresh_ts
        refresh_seconds = _news_refresh_seconds
    return {
        "enabled": True,
        "feed_count": len(RSS_FEEDS),
        "latest_count": article_count,
        "last_refresh_ts": last_refresh,
        "refresh_seconds": refresh_seconds,
        "read_only": True,
    }


def ist_now() -> datetime:
    return datetime.now(IST)


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


def _enrich_articles(articles: list[dict], *, context=None) -> None:
    enrich = _callable_from_context(context, "enrich_articles_with_ai_summaries") or _enrich_articles_with_ai_summaries
    if callable(enrich):
        enrich(articles)


def _ai_summary_progress(articles: list[dict], *, context=None) -> dict:
    progress = _callable_from_context(context, "ai_summary_progress_for_articles") or _ai_summary_progress_for_articles
    if callable(progress):
        return progress(articles)
    total = len(articles)
    complete = sum(1 for article in articles if article.get("summarySource") == "ai")
    return {"total": total, "complete": complete, "pending": max(total - complete, 0)}


def _remember_latest(articles: list[dict], feed_status: dict, updated: str, refreshed_at: float) -> None:
    global _latest_articles, _latest_feed_status, _latest_updated, _last_news_refresh_ts
    with _lock:
        _latest_articles = list(articles)
        _latest_feed_status = dict(feed_status)
        _latest_updated = updated
        _last_news_refresh_ts = refreshed_at


def _callable_from_context(context, name: str):
    if context is None:
        return None
    try:
        value = getattr(context, name)
    except AttributeError:
        return None
    return value if callable(value) else None
