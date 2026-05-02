from __future__ import annotations

import json
import re

try:
    from backend.agents.news.text import clean_summary, normalized_headline
except ModuleNotFoundError:
    from agents.news.text import clean_summary, normalized_headline


def summary_needs_ai(title: str, summary: str, min_chars: int = 220) -> bool:
    body = re.sub(r"\s+", " ", clean_summary(summary or "")).strip()
    if not body:
        return True
    title_key = normalized_headline(title)
    body_key = normalized_headline(body)
    if not body_key:
        return True
    if body_key == title_key or body_key.startswith(title_key):
        return True
    if len(body) < min_chars and body.count(".") < 2:
        return True
    return False


def build_news_summary_prompt(article: dict) -> str:
    title = str(article.get("title") or "").strip()
    summary = str(article.get("articleText") or article.get("sourceSummary") or article.get("summary") or "").strip()
    source = str(article.get("source") or "").strip()
    sector = str(article.get("sector") or "").strip()
    sentiment = str(((article.get("sentiment") or {}).get("label")) or "neutral").strip()
    impact = article.get("impact")
    impact_meta = article.get("impactMeta") or {}
    article_data = {
        "headline": title,
        "source": source or "Unknown",
        "feed": article.get("feed") or "",
        "scope": article.get("scope") or "",
        "sector": sector or "General",
        "sentiment": sentiment,
        "impactScore": impact if impact is not None else "unknown",
        "impactReasons": impact_meta.get("reasons") or [],
        "impactComponents": impact_meta.get("components") or {},
        "matchedKeywords": impact_meta.get("matchedKeywords") or [],
        "published": article.get("published") or "",
        "textSource": article.get("articleTextSource") or "rss-feed",
        "url": article.get("resolvedLink") or article.get("link") or "",
        "availableText": summary or "No article body available beyond the headline.",
    }
    return (
        "You are a senior Indian equity-market analyst writing for an intraday trading dashboard.\n"
        "Write one dense, useful plain-text market brief in 5-6 concise sentences.\n"
        "Aim for 140 to 220 words when enough article detail is available.\n"
        "Your job is to preserve the material facts, not to make a generic summary.\n"
        "Prioritize facts in this order: the core development, exact companies/entities, material numbers, market reaction, sector/index read-through, risks, and what traders should watch.\n"
        "Do not merely repeat the headline; extract the real development and why it matters for Indian equities.\n"
        "Include every important figure present in the article, such as profit, revenue, margins, order value, stake change, price move, target price, support/resistance, index level, valuation, date, timeline, or guidance.\n"
        "Sentence 1: state what changed and who is affected.\n"
        "Sentence 2: include the most important numbers and market reaction.\n"
        "Sentence 3: explain why the development matters for the company, sector, or earnings/risk narrative.\n"
        "Sentence 4: explain likely effect on Nifty, Bank Nifty, sector indices, or broader Indian risk appetite only when supported.\n"
        "Sentence 5: mention the main uncertainty, risk, missing detail, or why the headline may not translate into price follow-through.\n"
        "Sentence 6, if useful: explain what active traders should watch next, including levels, catalysts, related sectors, or confirmation signals only when provided.\n"
        "Use only the information provided below and do not invent facts.\n"
        "Do not use vague filler like 'could impact sentiment' unless you specify the channel of impact.\n"
        "If index impact is not directly supported, say the read-through is limited instead of forcing a bullish or bearish call.\n"
        "If textSource is article-page, prioritize the availableText body over headline-only inference.\n"
        "If the source text is thin, still write a useful 3-4 sentence brief and clearly say details are limited.\n"
        "Do not use bullets, numbering, markdown, labels, or line breaks.\n\n"
        "Article data:\n"
        f"{json.dumps(article_data, ensure_ascii=False, sort_keys=True)}\n"
    )


def extract_ollama_response_text(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("response") or "").strip()


def normalize_ai_summary(text: str) -> str:
    summary = re.sub(r"\s+", " ", str(text or "")).strip(" -•\t")
    if not summary:
        return ""
    summary = re.sub(r"^(summary|brief summary)\s*:\s*", "", summary, flags=re.IGNORECASE).strip()
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", summary) if part.strip()]
    if sentences:
        summary = " ".join(sentences[:6])
    words = summary.split()
    if len(words) > 220:
        summary = " ".join(words[:220]).rstrip(" ,;:")
        if not summary.endswith((".", "!", "?")):
            summary += "."
    return summary
