from __future__ import annotations

import html as html_lib
import json
import re
from html.parser import HTMLParser

try:
    from backend.news.text import normalized_headline, trim_text_boundary
except ModuleNotFoundError:
    from news.text import normalized_headline, trim_text_boundary


_NOISE_PHRASES = (
    "accept cookies",
    "advertisement",
    "all rights reserved",
    "already a subscriber",
    "enable javascript",
    "follow us",
    "log in to continue",
    "newsletter",
    "privacy policy",
    "sign in to continue",
    "sign up",
    "subscribe to continue",
    "terms of use",
)

_BLOCKED_PHRASES = (
    "access denied",
    "are you a robot",
    "captcha",
    "enable javascript",
    "log in to continue",
    "please subscribe",
    "sign in to continue",
    "subscribe to continue",
    "subscription required",
)


def _clean_text(text: str) -> str:
    text = html_lib.unescape(text or "")
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n-|")
    return text


def _jsonld_candidates(html: str) -> list[str]:
    candidates: list[str] = []
    for match in re.finditer(
        r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        raw = html_lib.unescape(match.group(1)).strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        stack = payload if isinstance(payload, list) else [payload]
        while stack:
            item = stack.pop(0)
            if isinstance(item, list):
                stack.extend(item)
                continue
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            if isinstance(graph, list):
                stack.extend(graph)
            article_body = item.get("articleBody")
            description = item.get("description")
            if article_body:
                candidates.append(_clean_text(str(article_body)))
            elif description:
                candidates.append(_clean_text(str(description)))
    return [text for text in candidates if text]


class ArticleHTMLTextExtractor(HTMLParser):
    def __init__(self, title: str = ""):
        super().__init__(convert_charrefs=True)
        self.title_key = normalized_headline(title)
        self._skip_depth = 0
        self._block_stack: list[str] = []
        self._current: list[str] = []
        self.blocks: list[str] = []
        self.meta: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        attrs_map = {key.lower(): (value or "") for key, value in attrs}
        if tag == "meta":
            key = (attrs_map.get("name") or attrs_map.get("property") or attrs_map.get("itemprop") or "").lower()
            if key in {"description", "og:description", "twitter:description"}:
                content = _clean_text(attrs_map.get("content", ""))
                if content:
                    self.meta.append(content)
            return
        if tag in {"script", "style", "noscript", "svg", "canvas", "iframe", "form", "button", "nav", "footer", "header", "aside"}:
            self._skip_depth += 1
            return
        if tag in {"p", "h1", "h2", "h3", "li", "blockquote"} and self._skip_depth == 0:
            self._flush_current()
            self._block_stack.append(tag)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"script", "style", "noscript", "svg", "canvas", "iframe", "form", "button", "nav", "footer", "header", "aside"}:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._block_stack and tag == self._block_stack[-1]:
            self._block_stack.pop()
            self._flush_current()

    def handle_data(self, data: str) -> None:
        if self._skip_depth or not self._block_stack:
            return
        text = _clean_text(data)
        if text:
            self._current.append(text)

    def _flush_current(self) -> None:
        if not self._current:
            return
        text = _clean_text(" ".join(self._current))
        self._current = []
        if not text:
            return
        low = text.lower()
        if len(text) < 45:
            return
        if self.title_key and normalized_headline(text) == self.title_key:
            return
        if any(phrase in low for phrase in _NOISE_PHRASES):
            return
        self.blocks.append(text)


def is_blocked_article_text(text: str) -> bool:
    low = (text or "").lower()
    return any(phrase in low for phrase in _BLOCKED_PHRASES)


def extract_article_text(html: str, title: str = "", max_chars: int = 7000) -> str:
    jsonld = _jsonld_candidates(html)
    if jsonld:
        longest = max(jsonld, key=len)
        if len(longest) >= 350 and not is_blocked_article_text(longest):
            return trim_text_boundary(longest, max_chars)

    parser = ArticleHTMLTextExtractor(title=title)
    try:
        parser.feed(html or "")
        parser.close()
    except Exception:
        return ""

    seen: set[str] = set()
    pieces: list[str] = []
    for block in parser.blocks:
        key = normalized_headline(block)
        if not key or key in seen:
            continue
        seen.add(key)
        pieces.append(block)
        if sum(len(piece) for piece in pieces) >= max_chars:
            break
    article_text = _clean_text(" ".join(pieces))
    if len(article_text) >= 350 and not is_blocked_article_text(article_text):
        return trim_text_boundary(article_text, max_chars)

    meta_text = _clean_text(" ".join(parser.meta))
    if len(meta_text) >= 160 and not is_blocked_article_text(meta_text):
        return trim_text_boundary(meta_text, max_chars)
    return ""


def article_text_is_useful(article_text: str, feed_text: str = "", min_chars: int = 500) -> bool:
    text = _clean_text(article_text)
    feed = _clean_text(feed_text)
    if len(text) < min_chars or is_blocked_article_text(text):
        return False
    if feed and len(text) < max(min_chars, int(len(feed) * 1.35)):
        return False
    return True
