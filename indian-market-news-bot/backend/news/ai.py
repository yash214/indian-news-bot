"""AI summary enrichment for news articles."""

from __future__ import annotations

import json
import os
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

try:
    from backend.news.analysis import build_article_analysis_prompt, extract_json_object, normalize_article_analysis
    from backend.news.summaries import build_news_summary_prompt, normalize_ai_summary
    from backend.news.article_extract import article_text_is_useful, extract_article_text
    from backend.news.text import url_hash
    from backend.news.url_resolver import is_google_news_url, resolve_google_news_url
    from backend.core.settings import AI_ARTICLE_ANALYSIS_PROMPT_VERSION, AI_SUMMARY_PROMPT_VERSION
    from backend.providers.ai import ai_model_name_from_env, ai_provider_name_from_env, create_ai_text_provider
except ModuleNotFoundError:
    from news.analysis import build_article_analysis_prompt, extract_json_object, normalize_article_analysis
    from news.summaries import build_news_summary_prompt, normalize_ai_summary
    from news.article_extract import article_text_is_useful, extract_article_text
    from news.text import url_hash
    from news.url_resolver import is_google_news_url, resolve_google_news_url
    from core.settings import AI_ARTICLE_ANALYSIS_PROMPT_VERSION, AI_SUMMARY_PROMPT_VERSION
    from providers.ai import ai_model_name_from_env, ai_provider_name_from_env, create_ai_text_provider


OLLAMA_SUMMARY_NUM_PREDICT = int(os.environ.get("OLLAMA_SUMMARY_NUM_PREDICT", "320"))

MAX_AI_SUMMARY_WORKERS = int(os.environ.get("MAX_AI_SUMMARY_WORKERS", "1"))
MAX_AI_SUMMARY_ARTICLES = int(os.environ.get("MAX_AI_SUMMARY_ARTICLES", "500"))
AI_SUMMARY_SYNC_ARTICLES = int(os.environ.get("AI_SUMMARY_SYNC_ARTICLES", "0"))
AI_SUMMARY_WINDOW_SECONDS = int(float(os.environ.get("AI_SUMMARY_WINDOW_HOURS", "24")) * 3600)
AI_SUMMARY_RETRY_AFTER_SECONDS = float(os.environ.get("AI_SUMMARY_RETRY_AFTER_SECONDS", "45"))
AI_SUMMARY_QUEUE_RETRY_BASE_SECONDS = float(os.environ.get("AI_SUMMARY_QUEUE_RETRY_BASE_SECONDS", "120"))
AI_SUMMARY_QUEUE_MAX_BACKOFF_SECONDS = float(os.environ.get("AI_SUMMARY_QUEUE_MAX_BACKOFF_SECONDS", "1800"))
AI_SUMMARY_MAX_INPUT_CHARS = int(os.environ.get("AI_SUMMARY_MAX_INPUT_CHARS", "4500"))

ARTICLE_EXTRACTION_TIMEOUT_SECONDS = float(os.environ.get("ARTICLE_EXTRACTION_TIMEOUT_SECONDS", "3"))
ARTICLE_EXTRACTION_MIN_CHARS = int(os.environ.get("ARTICLE_EXTRACTION_MIN_CHARS", "500"))
ARTICLE_EXTRACTION_MAX_CHARS = int(os.environ.get("ARTICLE_EXTRACTION_MAX_CHARS", "7000"))
ARTICLE_EXTRACTION_CACHE_TTL = 12 * 3600.0


class NewsAiSummaryService:
    """Coordinates Ollama calls, article extraction, caching, and in-memory updates."""

    def __init__(
        self,
        *,
        http_session_factory,
        load_persisted_summary,
        load_persisted_analysis,
        persist_summary,
        persist_analysis,
        articles_factory,
        articles_lock,
        on_analysis_applied=None,
    ) -> None:
        self.http_session_factory = http_session_factory
        self.load_persisted_summary = load_persisted_summary
        self.load_persisted_analysis = load_persisted_analysis
        self.persist_summary = persist_summary
        self.persist_analysis = persist_analysis
        self.articles_factory = articles_factory
        self.articles_lock = articles_lock
        self.on_analysis_applied = on_analysis_applied
        self.summary_cache: dict[str, tuple[str, float]] = {}
        self.analysis_cache: dict[str, tuple[dict, float]] = {}
        self.summary_cache_ttl = 24 * 3600.0
        self.summary_retry_after_ts = 0.0
        self.summary_lock = threading.Lock()
        self.summary_inflight: set[str] = set()
        self.summary_queued: set[str] = set()
        self.summary_failures: dict[str, dict] = {}
        self.summary_generation_errors: dict[str, str] = {}
        self.summary_queue: queue.PriorityQueue = queue.PriorityQueue()
        self.summary_sequence = 0
        self.summary_workers_started = False
        self.summary_worker_threads: list[threading.Thread] = []
        self.summary_executor: ThreadPoolExecutor | None = None
        self.article_text_cache: dict[str, tuple[str, float]] = {}
        self.article_url_cache: dict[str, tuple[str, float]] = {}
        self.article_text_lock = threading.Lock()
        self.provider_lock = threading.Lock()
        self._ai_text_provider = None
        self._ai_text_provider_signature = ""

    def ollama_api_base(self) -> str:
        return os.environ.get("OLLAMA_API_BASE", "http://127.0.0.1:11434/api").strip().rstrip("/")

    def ai_provider_name(self) -> str:
        return ai_provider_name_from_env()

    def ai_news_summary_model(self) -> str:
        return ai_model_name_from_env(self.ai_provider_name())

    def ai_text_provider_signature(self) -> str:
        selected = self.ai_provider_name()
        return "|".join(
            [
                selected,
                self.ai_news_summary_model(),
                os.environ.get("OLLAMA_API_BASE", ""),
                os.environ.get("BEDROCK_REGION", ""),
                os.environ.get("AWS_DEFAULT_REGION", ""),
                os.environ.get("BEDROCK_OPENAI_BASE_URL", ""),
                os.environ.get("OPENAI_BASE_URL", ""),
                "bedrock-key" if os.environ.get("BEDROCK_API_KEY") or os.environ.get("OPENAI_API_KEY") else "",
            ]
        )

    def ai_text_provider(self):
        signature = self.ai_text_provider_signature()
        with self.provider_lock:
            if self._ai_text_provider is None or self._ai_text_provider_signature != signature:
                self._ai_text_provider = create_ai_text_provider(
                    http_session_factory=self.http_session_factory,
                    provider_name=self.ai_provider_name(),
                )
                self._ai_text_provider_signature = signature
            return self._ai_text_provider

    def ai_news_summaries_enabled(self) -> bool:
        raw = os.environ.get("ENABLE_AI_NEWS_SUMMARIES", "1").strip().lower()
        if raw in {"0", "false", "no", "off"}:
            return False
        if time.time() < self.summary_retry_after_ts:
            return False
        try:
            return bool(self.ai_text_provider().is_configured())
        except Exception:
            return False

    def article_extraction_enabled(self) -> bool:
        raw = os.environ.get("ENABLE_ARTICLE_EXTRACTION", "1").strip().lower()
        return raw not in {"0", "false", "no", "off"}

    @staticmethod
    def article_link_supports_direct_extraction(link: str) -> bool:
        try:
            host = urlparse(link).netloc.lower()
        except Exception:
            return False
        # Google News RSS links are article wrappers, not the publisher body.
        if host.endswith("news.google.com"):
            return False
        return True

    def resolve_article_extraction_link(self, link: str) -> str:
        link = str(link or "").strip()
        if not is_google_news_url(link):
            return link
        cache_key = url_hash(link)
        now = time.time()
        with self.article_text_lock:
            cached = self.article_url_cache.get(cache_key)
            if cached and now - cached[1] < ARTICLE_EXTRACTION_CACHE_TTL:
                return cached[0]
        resolved = resolve_google_news_url(
            link,
            self.http_session_factory(),
            timeout=ARTICLE_EXTRACTION_TIMEOUT_SECONDS,
        )
        with self.article_text_lock:
            self.article_url_cache[cache_key] = (resolved, now)
        return resolved

    def fetch_accessible_article_text(self, article: dict) -> str:
        if not self.article_extraction_enabled():
            return ""
        link = str(article.get("link") or "").strip()
        if not link.startswith(("http://", "https://")):
            return ""
        extraction_link = self.resolve_article_extraction_link(link)
        if extraction_link and extraction_link != link:
            article["resolvedLink"] = extraction_link
        if not extraction_link or not self.article_link_supports_direct_extraction(extraction_link):
            return ""
        cache_key = url_hash(extraction_link)
        now = time.time()
        with self.article_text_lock:
            cached = self.article_text_cache.get(cache_key)
            if cached and now - cached[1] < ARTICLE_EXTRACTION_CACHE_TTL:
                return cached[0]
        try:
            response = self.http_session_factory().get(
                extraction_link,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 Chrome/124 Safari/537.36"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-IN,en;q=0.9",
                    "Referer": "https://www.google.com/",
                },
                timeout=ARTICLE_EXTRACTION_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            content_type = response.headers.get("content-type", "").lower()
            if "html" not in content_type and "text/" not in content_type:
                return ""
            text = extract_article_text(
                response.text,
                title=str(article.get("title") or ""),
                max_chars=ARTICLE_EXTRACTION_MAX_CHARS,
            )
            if not article_text_is_useful(
                text,
                feed_text=str(article.get("sourceSummary") or article.get("summary") or ""),
                min_chars=ARTICLE_EXTRACTION_MIN_CHARS,
            ):
                text = ""
        except Exception:
            text = ""
        with self.article_text_lock:
            self.article_text_cache[cache_key] = (text, now)
        return text

    def prepare_article_for_ai_summary(self, article: dict) -> dict:
        prepared = dict(article)
        feed_text = str(prepared.get("sourceSummary") or prepared.get("summary") or "").strip()
        full_text = self.fetch_accessible_article_text(prepared)
        if full_text:
            prepared["articleText"] = self.truncate_ai_summary_input(full_text)
            prepared["articleTextSource"] = "article-page"
        else:
            prepared["articleText"] = self.truncate_ai_summary_input(feed_text)
            prepared["articleTextSource"] = "rss-feed"
        return prepared

    @staticmethod
    def truncate_ai_summary_input(text: str) -> str:
        clean = re.sub(r"\s+", " ", str(text or "")).strip()
        if AI_SUMMARY_MAX_INPUT_CHARS <= 0 or len(clean) <= AI_SUMMARY_MAX_INPUT_CHARS:
            return clean
        truncated = clean[:AI_SUMMARY_MAX_INPUT_CHARS].rsplit(" ", 1)[0].strip()
        return truncated or clean[:AI_SUMMARY_MAX_INPUT_CHARS].strip()

    @staticmethod
    def ai_summary_cache_key(article: dict) -> str:
        seed = {
            "id": article.get("id") or "",
            "link": article.get("link") or "",
            "title": article.get("title") or "",
            "source": article.get("source") or "",
            "published": article.get("published") or "",
            "promptVersion": AI_SUMMARY_PROMPT_VERSION,
            "sourceSummary": article.get("sourceSummary") or article.get("summary") or "",
        }
        return url_hash(json.dumps(seed, ensure_ascii=True, sort_keys=True))

    @staticmethod
    def ai_analysis_cache_key(article: dict) -> str:
        seed = {
            "id": article.get("id") or "",
            "link": article.get("link") or "",
            "title": article.get("title") or "",
            "source": article.get("source") or "",
            "published": article.get("published") or "",
            "promptVersion": AI_ARTICLE_ANALYSIS_PROMPT_VERSION,
            "sourceSummary": article.get("sourceSummary") or article.get("summary") or "",
        }
        return url_hash(json.dumps(seed, ensure_ascii=True, sort_keys=True))

    def get_cached_ai_news_summary(self, cache_key: str) -> str:
        if not cache_key:
            return ""
        now = time.time()
        with self.summary_lock:
            cached = self.summary_cache.get(cache_key)
            if cached and now - cached[1] < self.summary_cache_ttl:
                return cached[0]
        persisted = self.load_persisted_summary(cache_key)
        if persisted:
            with self.summary_lock:
                self.summary_cache[cache_key] = (persisted, now)
            return persisted
        return ""

    def get_cached_ai_article_analysis(self, cache_key: str) -> dict:
        if not cache_key:
            return {}
        now = time.time()
        with self.summary_lock:
            cached = self.analysis_cache.get(cache_key)
            if cached and now - cached[1] < self.summary_cache_ttl:
                return dict(cached[0])
        persisted = self.load_persisted_analysis(cache_key)
        if persisted:
            with self.summary_lock:
                self.analysis_cache[cache_key] = (dict(persisted), now)
            return dict(persisted)
        return {}

    def set_ai_generation_error(self, cache_key: str, reason: str) -> None:
        if not cache_key:
            return
        clean_reason = str(reason or "").replace("\n", " ").replace("\r", " ").strip()
        if len(clean_reason) > 900:
            clean_reason = clean_reason[:900].rstrip() + "..."
        if not clean_reason:
            return
        with self.summary_lock:
            self.summary_generation_errors[cache_key] = clean_reason

    def get_ai_generation_error(self, cache_key: str) -> str:
        if not cache_key:
            return ""
        with self.summary_lock:
            return str(self.summary_generation_errors.get(cache_key) or "")

    @staticmethod
    def article_is_in_ai_summary_window(article: dict, now: float | None = None) -> bool:
        if AI_SUMMARY_WINDOW_SECONDS <= 0:
            return True
        try:
            ts = float(article.get("ts") or 0)
        except (TypeError, ValueError):
            return False
        if ts <= 0:
            return False
        return ((now or time.time()) - ts) <= AI_SUMMARY_WINDOW_SECONDS

    @staticmethod
    def article_has_ai_summary(article: dict) -> bool:
        return article.get("summarySource") == "ai" and bool(str(article.get("summary") or "").strip())

    @staticmethod
    def ai_summary_update_payload(article: dict) -> dict:
        return {
            "id": article.get("id"),
            "summary": article.get("summary") or "",
            "summarySource": article.get("summarySource") or "",
            "analysisSource": article.get("analysisSource") or "",
            "sentiment": article.get("sentiment") or {},
            "impact": article.get("impact"),
            "impactMeta": article.get("impactMeta") or {},
            "sector": article.get("sector") or "General",
            "aiAnalysis": article.get("aiAnalysis") or {},
        }

    def ai_summary_progress_for_articles(self, articles: list[dict], now: float | None = None) -> dict:
        current_time = now or time.time()
        candidates = [article for article in articles if self.article_is_in_ai_summary_window(article, current_time)]
        complete = sum(1 for article in candidates if self.article_has_ai_summary(article))
        analysis_complete = sum(
            1
            for article in candidates
            if article.get("analysisSource") == "ai" and self.article_has_ai_summary(article)
        )
        queue_status = self.ai_summary_queue_status(current_time)
        total = len(candidates)
        return {
            "total": total,
            "complete": complete,
            "analysisComplete": analysis_complete,
            "pending": max(total - analysis_complete, 0),
            "analysisPending": max(total - analysis_complete, 0),
            "queued": queue_status["queued"],
            "inflight": queue_status["inflight"],
            "failed": queue_status["failed"],
            "retryReady": queue_status["retryReady"],
            "nextRetrySeconds": queue_status["nextRetrySeconds"],
            "workers": queue_status["workers"],
            "enabled": self.ai_news_summaries_enabled(),
            "provider": self.ai_provider_name(),
            "model": self.ai_news_summary_model(),
            "lastError": queue_status.get("lastError") or "",
            "recentErrors": queue_status.get("recentErrors") or [],
        }

    def ai_summary_queue_status(self, now: float | None = None) -> dict:
        current_time = now or time.time()
        with self.summary_lock:
            recent_failures = sorted(
                self.summary_failures.values(),
                key=lambda item: float(item.get("lastFailedAt") or 0.0),
                reverse=True,
            )
            recent_errors = []
            for failure in recent_failures:
                error = str(failure.get("lastError") or "").strip()
                if error and error not in recent_errors:
                    recent_errors.append(error)
                if len(recent_errors) >= 3:
                    break
            retry_waits = [
                max(0.0, float(meta.get("nextRetryAt") or 0.0) - current_time)
                for meta in self.summary_failures.values()
                if float(meta.get("nextRetryAt") or 0.0) > current_time
            ]
            retry_ready = sum(
                1
                for meta in self.summary_failures.values()
                if float(meta.get("nextRetryAt") or 0.0) <= current_time
            )
            return {
                "queued": len(self.summary_queued),
                "inflight": len(self.summary_inflight),
                "failed": len(self.summary_failures),
                "retryReady": retry_ready,
                "nextRetrySeconds": int(min(retry_waits)) if retry_waits else 0,
                "workers": len(self.summary_worker_threads),
                "enabled": self.ai_news_summaries_enabled(),
                "provider": self.ai_provider_name(),
                "model": self.ai_news_summary_model(),
                "lastError": recent_errors[0] if recent_errors else "",
                "recentErrors": recent_errors,
            }

    def ai_summary_executor_instance(self) -> ThreadPoolExecutor:
        with self.summary_lock:
            if self.summary_executor is None:
                workers = max(1, min(MAX_AI_SUMMARY_WORKERS, 4))
                self.summary_executor = ThreadPoolExecutor(max_workers=workers)
            return self.summary_executor

    def apply_ai_summary_to_article(self, article: dict, summary: str) -> None:
        if not summary:
            return
        article_id = str(article.get("id") or "")
        with self.articles_lock:
            article["summary"] = summary
            article["summarySource"] = "ai"
            if article_id:
                for current in self.articles_factory():
                    if current is not article and str(current.get("id") or "") == article_id:
                        current["summary"] = summary
                        current["summarySource"] = "ai"

    def apply_ai_analysis_to_article(self, article: dict, analysis: dict) -> None:
        if not analysis or not analysis.get("summary"):
            return
        article_id = str(article.get("id") or "")
        summary = str(analysis.get("summary") or "").strip()
        sentiment_label = str(analysis.get("sentiment") or "neutral")
        confidence = analysis.get("confidence", 0.5)
        impact_score = analysis.get("impactScore", article.get("impact", 0))
        sector = analysis.get("sector") or article.get("sector") or "General"
        ai_meta = {
            "confidence": confidence,
            "indexImpact": analysis.get("indexImpact") or {},
            "reasons": analysis.get("reasons") or [],
            "textSource": analysis.get("_inputTextSource") or analysis.get("textSource") or "unknown",
            "inputChars": analysis.get("_inputCharCount") or analysis.get("inputChars") or 0,
            "resolvedLink": analysis.get("_resolvedLink") or analysis.get("resolvedLink") or article.get("resolvedLink") or article.get("link") or "",
        }

        def apply(target: dict) -> None:
            impact_meta = dict(target.get("impactMeta") or {})
            existing_reasons = impact_meta.get("reasons") if isinstance(impact_meta.get("reasons"), list) else []
            ai_reasons = [f"AI: {reason}" for reason in ai_meta["reasons"]]
            impact_meta["ai"] = ai_meta
            if ai_reasons:
                impact_meta["reasons"] = ai_reasons[:5]
            elif not existing_reasons:
                impact_meta["reasons"] = ["AI analysis applied"]
            target["summary"] = summary
            target["summarySource"] = "ai"
            target["analysisSource"] = "ai"
            target["sentiment"] = {"label": sentiment_label, "score": confidence}
            target["impact"] = impact_score
            target["impactMeta"] = impact_meta
            target["sector"] = sector
            target["aiAnalysis"] = ai_meta

        with self.articles_lock:
            apply(article)
            if article_id:
                for current in self.articles_factory():
                    if current is not article and str(current.get("id") or "") == article_id:
                        apply(current)
        if self.on_analysis_applied:
            try:
                self.on_analysis_applied(article)
            except Exception:
                pass

    def hydrate_article_from_ai_cache(self, article: dict) -> bool:
        analysis = self.get_cached_ai_article_analysis(self.ai_analysis_cache_key(article))
        if analysis:
            self.apply_ai_analysis_to_article(article, analysis)
            return True
        summary = self.get_cached_ai_news_summary(self.ai_summary_cache_key(article))
        if summary:
            self.apply_ai_summary_to_article(article, summary)
            return True
        return False

    def generate_ai_news_summary(self, article: dict) -> str:
        cache_key = self.ai_summary_cache_key(article)
        if not cache_key:
            return ""
        cached = self.get_cached_ai_news_summary(cache_key)
        if cached:
            return cached
        now = time.time()
        prepared_article = self.prepare_article_for_ai_summary(article)
        try:
            raw_text = self.ai_text_provider().generate_text(
                prompt=build_news_summary_prompt(prepared_article),
                temperature=0.25,
                max_tokens=OLLAMA_SUMMARY_NUM_PREDICT,
                json_mode=False,
            )
            text = normalize_ai_summary(raw_text)
        except Exception as exc:
            self.set_ai_generation_error(cache_key, f"{self.ai_provider_name()}: {exc}")
            self.summary_retry_after_ts = time.time() + AI_SUMMARY_RETRY_AFTER_SECONDS
            return ""
        if not text:
            self.set_ai_generation_error(cache_key, f"{self.ai_provider_name()}: empty summary text")
            return ""
        with self.summary_lock:
            self.summary_cache[cache_key] = (text, now)
        self.persist_summary(cache_key, article, text)
        self.summary_retry_after_ts = 0.0
        return text

    def generate_ai_article_analysis(self, article: dict) -> dict:
        analysis_key = self.ai_analysis_cache_key(article)
        summary_key = self.ai_summary_cache_key(article)
        if not analysis_key:
            return {}
        cached = self.get_cached_ai_article_analysis(analysis_key)
        if cached:
            return cached
        now = time.time()
        prepared_article = self.prepare_article_for_ai_summary(article)
        raw_text = ""
        try:
            raw_text = self.ai_text_provider().generate_text(
                prompt=build_article_analysis_prompt(prepared_article),
                temperature=0.15,
                max_tokens=max(OLLAMA_SUMMARY_NUM_PREDICT, 520),
                json_mode=True,
            )
            raw_analysis = extract_json_object(raw_text)
            if not raw_analysis:
                preview = str(raw_text or "").replace("\n", " ").replace("\r", " ").strip()
                if len(preview) > 500:
                    preview = preview[:500].rstrip() + "..."
                self.set_ai_generation_error(summary_key, f"{self.ai_provider_name()}: JSON parse failed from model text: {preview or 'empty response'}")
                return {}
            analysis = normalize_article_analysis(raw_analysis, fallback_article=article)
        except Exception as exc:
            self.set_ai_generation_error(summary_key, f"{self.ai_provider_name()}: {exc}")
            self.summary_retry_after_ts = time.time() + AI_SUMMARY_RETRY_AFTER_SECONDS
            return {}
        if not analysis:
            preview = str(raw_text or "").replace("\n", " ").replace("\r", " ").strip()
            if len(preview) > 500:
                preview = preview[:500].rstrip() + "..."
            self.set_ai_generation_error(summary_key, f"{self.ai_provider_name()}: model response did not normalize: {preview or 'empty response'}")
            return {}
        analysis["_inputTextSource"] = prepared_article.get("articleTextSource") or "unknown"
        analysis["_inputCharCount"] = len(str(prepared_article.get("articleText") or ""))
        analysis["_resolvedLink"] = prepared_article.get("resolvedLink") or prepared_article.get("link") or ""
        with self.summary_lock:
            self.analysis_cache[analysis_key] = (dict(analysis), now)
            if summary_key:
                self.summary_cache[summary_key] = (analysis["summary"], now)
                self.summary_generation_errors.pop(summary_key, None)
        self.persist_analysis(analysis_key, article, analysis)
        self.persist_summary(self.ai_summary_cache_key(article), article, analysis["summary"])
        self.summary_retry_after_ts = 0.0
        return analysis

    @staticmethod
    def ai_summary_priority(article: dict, now: float | None = None) -> int:
        current_time = now or time.time()
        try:
            impact = int(article.get("impact") or 0)
        except (TypeError, ValueError):
            impact = 0
        try:
            age_seconds = max(0.0, current_time - float(article.get("ts") or 0.0))
        except (TypeError, ValueError):
            age_seconds = 24 * 3600.0

        priority = 60
        if impact >= 8:
            priority -= 30
        elif impact >= 5:
            priority -= 18
        elif impact >= 3:
            priority -= 8
        if age_seconds <= 3600:
            priority -= 14
        elif age_seconds <= 6 * 3600:
            priority -= 8
        elif age_seconds <= 24 * 3600:
            priority -= 3
        if article.get("scope") == "local":
            priority -= 5
        if article.get("summarySource") != "ai":
            priority -= 3
        return max(priority, 0)

    def start_ai_summary_workers(self) -> None:
        with self.summary_lock:
            if self.summary_workers_started:
                return
            self.summary_workers_started = True
            workers = max(1, min(MAX_AI_SUMMARY_WORKERS, 4))
            for idx in range(workers):
                thread = threading.Thread(
                    target=self.ai_summary_worker_loop,
                    name=f"market-desk-ai-summary-{idx + 1}",
                    daemon=True,
                )
                self.summary_worker_threads.append(thread)
                thread.start()

    def ai_summary_worker_loop(self) -> None:
        while True:
            _priority, _newest_first, _sequence, cache_key, article = self.summary_queue.get()
            try:
                with self.summary_lock:
                    self.summary_queued.discard(cache_key)
                    if cache_key in self.summary_inflight:
                        continue
                    self.summary_inflight.add(cache_key)
                analysis = self.generate_ai_article_analysis(article)
                if analysis:
                    self.apply_ai_analysis_to_article(article, analysis)
                    self.clear_ai_summary_failure(cache_key)
                else:
                    self.record_ai_summary_failure(cache_key, self.get_ai_generation_error(cache_key) or "empty-summary")
            except Exception as exc:
                self.record_ai_summary_failure(cache_key, str(exc)[:180])
            finally:
                with self.summary_lock:
                    self.summary_inflight.discard(cache_key)
                self.summary_queue.task_done()

    def clear_ai_summary_failure(self, cache_key: str) -> None:
        with self.summary_lock:
            self.summary_failures.pop(cache_key, None)

    def record_ai_summary_failure(self, cache_key: str, reason: str) -> None:
        now = time.time()
        with self.summary_lock:
            previous = self.summary_failures.get(cache_key) or {}
            attempts = int(previous.get("attempts") or 0) + 1
            backoff = min(AI_SUMMARY_QUEUE_MAX_BACKOFF_SECONDS, AI_SUMMARY_QUEUE_RETRY_BASE_SECONDS * (2 ** min(attempts - 1, 4)))
            self.summary_failures[cache_key] = {
                "attempts": attempts,
                "lastError": reason,
                "lastFailedAt": now,
                "nextRetryAt": now + backoff,
            }

    def ai_summary_retry_allowed(self, cache_key: str, now: float | None = None) -> bool:
        current_time = now or time.time()
        with self.summary_lock:
            failure = self.summary_failures.get(cache_key)
            if not failure:
                return True
            return current_time >= float(failure.get("nextRetryAt") or 0.0)

    def queue_ai_news_summary(self, article: dict) -> None:
        cache_key = self.ai_summary_cache_key(article)
        if not cache_key:
            return
        cached_summary = self.get_cached_ai_news_summary(cache_key)
        if cached_summary:
            self.apply_ai_summary_to_article(article, cached_summary)
        analysis_key = self.ai_analysis_cache_key(article)
        cached_analysis = self.get_cached_ai_article_analysis(analysis_key)
        if cached_analysis:
            self.apply_ai_analysis_to_article(article, cached_analysis)
            return
        now = time.time()
        if not self.ai_summary_retry_allowed(cache_key, now):
            return
        with self.summary_lock:
            if cache_key in self.summary_queued or cache_key in self.summary_inflight:
                return
            self.summary_sequence += 1
            sequence = self.summary_sequence
            self.summary_queued.add(cache_key)
        self.start_ai_summary_workers()
        try:
            newest_first = -float(article.get("ts") or 0.0)
        except (TypeError, ValueError):
            newest_first = 0.0
        self.summary_queue.put((self.ai_summary_priority(article, now), newest_first, sequence, cache_key, article))

    def enrich_articles_with_ai_summaries(self, articles: list[dict]) -> None:
        if not self.ai_news_summaries_enabled():
            return
        now = time.time()

        def article_ts(article: dict) -> float:
            try:
                return float(article.get("ts") or 0.0)
            except (TypeError, ValueError):
                return 0.0

        candidates = [
            article
            for article in sorted(articles, key=lambda item: -article_ts(item))
            if self.article_is_in_ai_summary_window(article, now)
        ][:MAX_AI_SUMMARY_ARTICLES]
        sync_remaining = max(0, AI_SUMMARY_SYNC_ARTICLES)
        for article in candidates:
            cached_analysis = self.get_cached_ai_article_analysis(self.ai_analysis_cache_key(article))
            if cached_analysis:
                self.apply_ai_analysis_to_article(article, cached_analysis)
                continue
            cache_key = self.ai_summary_cache_key(article)
            cached_summary = self.get_cached_ai_news_summary(cache_key)
            if cached_summary:
                self.apply_ai_summary_to_article(article, cached_summary)
            if sync_remaining > 0:
                analysis = self.generate_ai_article_analysis(article)
                if analysis:
                    self.apply_ai_analysis_to_article(article, analysis)
                    sync_remaining -= 1
                    continue
            self.queue_ai_news_summary(article)
