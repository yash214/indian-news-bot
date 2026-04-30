"""URL resolution helpers for news article links."""

from __future__ import annotations

import json
import re
from urllib.parse import urlparse


GOOGLE_NEWS_BATCH_URL = "https://news.google.com/_/DotsSplashUi/data/batchexecute?rpcids=Fbv4je"
GOOGLE_NEWS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/124 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://news.google.com/",
}


def is_google_news_url(url: str) -> bool:
    try:
        host = urlparse(url or "").netloc.lower()
    except Exception:
        return False
    return host.endswith("news.google.com")


def _google_news_decode_context() -> list:
    return [
        [
            "en-IN",
            "IN",
            ["FINANCE_TOP_INDICES", "GENESIS_PUBLISHER_SECTION", "WEB_TEST_1_0_0"],
            None,
            None,
            1,
            1,
            "IN:en",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            False,
            5,
        ],
        "en-IN",
        "IN",
        True,
        [3, 5, 9, 19],
        1,
        True,
        "906122764",
        None,
        None,
        None,
        False,
    ]


def _google_news_batch_request(article_id: str, timestamp: str, signature: str) -> dict[str, str]:
    inner = json.dumps(
        ["garturlreq", _google_news_decode_context(), article_id, int(timestamp), signature],
        separators=(",", ":"),
    )
    outer = [[["Fbv4je", inner, None, "generic"]]]
    return {"f.req": json.dumps(outer, separators=(",", ":"))}


def extract_google_news_metadata(html: str) -> tuple[str, str, str]:
    article_id = re.search(r'data-n-a-id="([^"]+)"', html or "")
    timestamp = re.search(r'data-n-a-ts="([^"]+)"', html or "")
    signature = re.search(r'data-n-a-sg="([^"]+)"', html or "")
    if not article_id or not timestamp or not signature:
        return "", "", ""
    return article_id.group(1), timestamp.group(1), signature.group(1)


def extract_google_news_batch_url(response_text: str) -> str:
    text = (response_text or "").strip()
    if text.startswith(")]}'"):
        text = text.split("\n", 1)[1] if "\n" in text else ""
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return ""

    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, list) or len(row) < 3:
            continue
        if row[0] != "wrb.fr" or row[1] != "Fbv4je":
            continue
        try:
            decoded = json.loads(row[2])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(decoded, list) or len(decoded) < 2 or decoded[0] != "garturlres":
            continue
        candidate = str(decoded[1] or "").strip()
        if candidate.startswith(("http://", "https://")) and not is_google_news_url(candidate):
            return candidate
    return ""


def resolve_google_news_url(url: str, session, timeout: float = 5.0) -> str:
    if not is_google_news_url(url):
        return url
    try:
        page = session.get(url, headers=GOOGLE_NEWS_HEADERS, timeout=timeout)
        page.raise_for_status()
    except Exception:
        return ""

    article_id, timestamp, signature = extract_google_news_metadata(page.text)
    if not article_id or not timestamp or not signature:
        return ""

    try:
        response = session.post(
            GOOGLE_NEWS_BATCH_URL,
            headers={
                "Content-Type": "application/x-www-form-urlencoded;charset=utf-8",
                "User-Agent": GOOGLE_NEWS_HEADERS["User-Agent"],
                "Referer": "https://news.google.com/",
            },
            data=_google_news_batch_request(article_id, timestamp, signature),
            timeout=timeout,
        )
        response.raise_for_status()
    except Exception:
        return ""
    return extract_google_news_batch_url(response.text)
