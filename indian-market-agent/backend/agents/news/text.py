from __future__ import annotations

import hashlib
import re


_KEYWORD_RE_CACHE: dict[str, re.Pattern] = {}


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text or "").strip()


def url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def clean_headline(title: str, publisher: str = "") -> str:
    headline = strip_html(title)
    source = strip_html(publisher)
    suffix = f" - {source}"
    if source and headline.lower().endswith(suffix.lower()):
        headline = headline[:-len(suffix)].strip()
    return headline


def clean_summary(summary: str, publisher: str = "") -> str:
    text = strip_html(summary)
    source = strip_html(publisher)
    if source:
        text = re.sub(rf"(?:\s+[-|:]\s+|\s{{2,}}){re.escape(source)}$", "", text, flags=re.IGNORECASE).strip()
    return text


def trim_text_boundary(text: str, limit: int) -> str:
    clean = re.sub(r"\s+", " ", text or "").strip()
    if len(clean) <= limit:
        return clean
    clipped = clean[:limit].rsplit(" ", 1)[0].rstrip(" ,;:-")
    return (clipped or clean[:limit]).rstrip() + "..."


def normalized_headline(title: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (title or "").lower())).strip()


def build_article_preview(title: str, summary: str, publisher: str = "", max_sentences: int = 5, max_chars: int = 680) -> str:
    text = clean_summary(summary, publisher)
    text = re.sub(r"\s+", " ", text).strip(" -|:")
    if not text:
        return ""
    title_key = normalized_headline(title)
    pieces = []
    for sentence in re.split(r"(?<=[.!?])\s+", text):
        clean_sentence = sentence.strip(" -|:")
        if not clean_sentence:
            continue
        if normalized_headline(clean_sentence) == title_key:
            continue
        if pieces and normalized_headline(clean_sentence) == normalized_headline(pieces[-1]):
            continue
        pieces.append(clean_sentence)
        if len(pieces) >= max_sentences:
            break
    preview = " ".join(pieces) or text
    return trim_text_boundary(preview, max_chars)


def feed_publisher_label(feed_name: str) -> str:
    for suffix in (" Markets", " Companies"):
        if feed_name.endswith(suffix):
            return feed_name[:-len(suffix)]
    return feed_name


def keyword_re(keyword: str) -> re.Pattern:
    key = (keyword or "").strip().lower()
    cached = _KEYWORD_RE_CACHE.get(key)
    if cached:
        return cached
    escaped = r"\s+".join(re.escape(part) for part in key.split())
    prefix = r"(?<![a-z0-9])" if key and key[0].isalnum() else ""
    suffix = r"(?![a-z0-9])" if key and key[-1].isalnum() else ""
    pattern = re.compile(prefix + escaped + suffix, re.IGNORECASE)
    _KEYWORD_RE_CACHE[key] = pattern
    return pattern


def keyword_found(text: str, keyword: str) -> bool:
    return bool(keyword_re(keyword).search(text or ""))
