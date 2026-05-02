"""Free Stooq quote helpers for global symbols and cross-assets."""

from __future__ import annotations

import csv
import html
import io
import re
import time
from urllib.parse import quote


STOOQ_DEFAULT_BASE_URL = "https://stooq.com/q/l/"
STOOQ_DEFAULT_PAGE_BASE_URL = "https://stooq.com/q/"
STOOQ_QUOTE_FIELDS = "sd2t2ohlcvc1p2"


def stooq_quote_url(symbol: str, base_url: str = STOOQ_DEFAULT_BASE_URL) -> str:
    encoded = quote(str(symbol or "").strip().lower(), safe=".")
    return f"{base_url}?s={encoded}&f={STOOQ_QUOTE_FIELDS}&h&e=csv"


def stooq_page_url(symbol: str, base_url: str = STOOQ_DEFAULT_PAGE_BASE_URL) -> str:
    encoded = quote(str(symbol or "").strip().lower(), safe=".")
    return f"{base_url}?s={encoded}&c=1d&t=l&a=lg&b=0"


def _to_float(value, default=None):
    try:
        text = str(value).replace(",", "").strip()
        if not text or text.upper() == "N/D":
            return default
        return float(text)
    except Exception:
        return default


def _row_float(row: dict, names: tuple[str, ...], default=None):
    normalized = {
        str(key or "").lower().replace(" ", "").replace("_", "").replace("%", "pct"): value
        for key, value in row.items()
    }
    for name in names:
        value = row.get(name)
        if value is None:
            value = normalized.get(name.lower().replace(" ", "").replace("_", "").replace("%", "pct"))
        parsed = _to_float(value, None)
        if parsed is not None:
            return parsed
    return default


def _clean_html_value(value: str | None) -> str:
    text = re.sub(r"<[^>]+>", "", value or "")
    return html.unescape(text).replace("\xa0", " ").strip()


def _span_text(html_text: str, span_id: str) -> str:
    escaped_id = re.escape(span_id)
    pattern = rf'id\s*=\s*(?:"{escaped_id}"|\'{escaped_id}\'|{escaped_id})(?:\s[^>]*)?>(.*?)</span>'
    match = re.search(pattern, html_text or "", flags=re.IGNORECASE | re.DOTALL)
    return _clean_html_value(match.group(1)) if match else ""


def _plain_text(html_text: str) -> str:
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", html_text or "", flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(text).replace("\xa0", " ")).strip()


def _summary_quote_values(html_text: str) -> dict:
    text = _plain_text(html_text)
    match = re.search(
        r"(?P<date>\d{1,2}\s+[A-Za-z]{3}),\s*"
        r"(?P<time>\d{1,2}:\d{2})\s+"
        r"(?P<price>[+-]?\d[\d,]*(?:\.\d+)?)\s+"
        r"(?P<change>[+-]\d[\d,]*(?:\.\d+)?)\s+"
        r"\((?P<pct>[+-]\d[\d,]*(?:\.\d+)?)%\)",
        text,
    )
    if not match:
        return {}
    return {
        "date": match.group("date"),
        "time": match.group("time"),
        "price": _to_float(match.group("price")),
        "change": _to_float(match.group("change")),
        "pct": _to_float(match.group("pct")),
    }


def _to_percent(value, default=None):
    text = str(value or "").replace("%", "").replace("(", "").replace(")", "").strip()
    return _to_float(text, default)


def stooq_quote_from_csv(
    label: str,
    stooq_symbol: str,
    csv_text: str,
    *,
    name: str | None = None,
    currency_symbol: str = "",
    received_at: float | None = None,
) -> dict | None:
    received_at = time.time() if received_at is None else received_at
    rows = list(csv.DictReader(io.StringIO(csv_text or "")))
    if not rows:
        return None

    row = rows[0]
    close = _to_float(row.get("Close"))
    if close is None:
        return None

    open_price = _to_float(row.get("Open"), close)
    high = _to_float(row.get("High"), close)
    low = _to_float(row.get("Low"), close)
    volume = _to_float(row.get("Volume"))
    raw_change = _row_float(row, ("Change", "Chg", "Change 1", "C1", "Net Change"))
    raw_pct = _row_float(row, ("%Change", "Change%", "% Change", "Change %", "PctChange", "Percent Change", "P2"))
    change = round(raw_change if raw_change is not None else close - open_price, 4)
    pct = round(raw_pct if raw_pct is not None else ((change / open_price * 100) if open_price else 0.0), 2)
    previous_close = round(close - change, 4) if raw_change is not None else round(open_price, 4)

    provider_date = str(row.get("Date") or "").strip()
    provider_time = str(row.get("Time") or "").strip()
    provider_timestamp = " ".join(part for part in [provider_date, provider_time] if part and part.upper() != "N/D")

    return {
        "symbol": label,
        "name": name or label,
        "price": round(close, 4),
        "previous_close": previous_close,
        "change": change,
        "pct": pct,
        "day_high": round(high, 4),
        "day_low": round(low, 4),
        "open": round(open_price, 4),
        "volume": volume,
        "fetchedAt": received_at,
        "receivedAt": received_at,
        "providerTimestamp": provider_timestamp,
        "source": "Stooq",
        "stooqSymbol": stooq_symbol,
        "sym": currency_symbol,
    }


def stooq_quote_from_html(
    label: str,
    stooq_symbol: str,
    html_text: str,
    *,
    name: str | None = None,
    currency_symbol: str = "",
    received_at: float | None = None,
) -> dict | None:
    """Parse the same AutoQuote values displayed on Stooq's quote page.

    Stooq's lightweight CSV endpoint can rate-limit while the public quote page
    still exposes the live values that users compare against visually. This
    parser lets the app stay aligned with the website before falling back to
    another provider.
    """
    received_at = time.time() if received_at is None else received_at
    prefix = f"aq_{str(stooq_symbol or '').strip().lower()}_"
    summary = _summary_quote_values(html_text)

    close = _to_float(_span_text(html_text, f"{prefix}c2|3"))
    if close is None:
        close = _to_float(_span_text(html_text, f"{prefix}c1"))
    if close is None:
        close = summary.get("price")
    if close is None:
        return None

    raw_change = _to_float(_span_text(html_text, f"{prefix}m2"), summary.get("change"))
    raw_pct = _to_percent(_span_text(html_text, f"{prefix}m3"), summary.get("pct"))
    previous_close = _to_float(_span_text(html_text, f"{prefix}p"))
    open_price = _to_float(_span_text(html_text, f"{prefix}o"), close)
    high = _to_float(_span_text(html_text, f"{prefix}h"), close)
    low = _to_float(_span_text(html_text, f"{prefix}l"), close)

    if raw_change is None and previous_close is not None:
        raw_change = close - previous_close
    change = round(raw_change if raw_change is not None else close - open_price, 4)
    if raw_pct is None:
        pct_base = previous_close if previous_close is not None else open_price
        raw_pct = (change / pct_base * 100) if pct_base else 0.0
    pct = round(raw_pct, 2)
    if previous_close is None:
        previous_close = close - change if raw_change is not None else open_price

    provider_date = _span_text(html_text, f"{prefix}d2") or _span_text(html_text, f"{prefix}d1") or summary.get("date", "")
    provider_time = _span_text(html_text, f"{prefix}t1") or summary.get("time", "")
    provider_timestamp = " ".join(part for part in [provider_date, provider_time] if part)

    return {
        "symbol": label,
        "name": name or label,
        "price": round(close, 4),
        "previous_close": round(previous_close, 4),
        "change": change,
        "pct": pct,
        "day_high": round(high, 4),
        "day_low": round(low, 4),
        "open": round(open_price, 4),
        "volume": _to_float(_span_text(html_text, f"{prefix}v2")),
        "fetchedAt": received_at,
        "receivedAt": received_at,
        "providerTimestamp": provider_timestamp,
        "source": "Stooq",
        "sourceDetail": "Stooq page",
        "stooqSymbol": stooq_symbol,
        "sym": currency_symbol,
    }
