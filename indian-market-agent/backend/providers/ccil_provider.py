"""Safe low-frequency CCIL provider for bond and money-market context.

Important:
- This provider is disabled by default.
- CCIL public pages can be dynamic or change HTML structure.
- Live HTML parsing here is best-effort.
- Provider failures must update last_error and return None, not crash callers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
import re
import time
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from backend.core.settings import (
        CCIL_PROVIDER_CACHE_TTL_SECONDS,
        CCIL_PROVIDER_ENABLED,
        CCIL_PROVIDER_TIMEOUT_SECONDS,
    )
except ModuleNotFoundError:
    from core.settings import (
        CCIL_PROVIDER_CACHE_TTL_SECONDS,
        CCIL_PROVIDER_ENABLED,
        CCIL_PROVIDER_TIMEOUT_SECONDS,
    )


class CCILProvider:
    """Best-effort CCIL provider for Indian bond/yield and money-market context."""

    # Landing/menu pages
    BOND_DAILY_DATA_URL = "https://www.ccilindia.com/daily-data"
    MONEY_MARKET_DAILY_DATA_URL = "https://www.ccilindia.com/moneymarketsummury_daily_data"

    # Specific bond / market report pages
    TRADING_SUMMARY_URL = "https://www.ccilindia.com/trading-summary"
    DAY_SUMMARY_URL = "https://www.ccilindia.com/day-s-summary"
    MARKET_WATCH_URL = "https://www.ccilindia.com/market-watch"

    # Specific money-market report page
    MONEY_MARKET_RATES_URL = "https://www.ccilindia.com/money-market-rates-and-volumes-most-liquid-tenor-"

    # Yield curve / rates context
    ZCYC_ZERO_RATES_URL = "https://www.ccilindia.com/zero-rates"
    ZCYC_PARAMETERS_URL = "https://www.ccilindia.com/zcyc-parameters"

    def __init__(
        self,
        enabled: bool | None = None,
        timeout_seconds: int | None = None,
        cache_ttl_seconds: int | None = None,
        session: requests.Session | None = None,
    ):
        self.enabled = CCIL_PROVIDER_ENABLED if enabled is None else bool(enabled)
        self.timeout_seconds = int(timeout_seconds or CCIL_PROVIDER_TIMEOUT_SECONDS)
        self.cache_ttl_seconds = int(cache_ttl_seconds or CCIL_PROVIDER_CACHE_TTL_SECONDS)
        self.session = session or requests.Session()

        self.last_success_at: datetime | None = None
        self.last_error = ""
        self._cache: dict[str, tuple[float, object]] = {}

    def get_india_10y_yield(self) -> dict | None:
        """Return normalized India 10Y yield context if available.

        Note:
        CCIL does not always expose a clean direct "India 10Y yield" table through
        public HTML. This method uses get_bond_market_snapshot(), which is
        best-effort and may return None.
        """
        snapshot = self.get_bond_market_snapshot()
        if snapshot is None:
            return None

        return {
            "source": "ccil",
            "as_of_date": snapshot.get("as_of_date"),
            "india_10y_yield": snapshot.get("india_10y_yield"),
            "india_10y_change_bps": snapshot.get("india_10y_change_bps"),
            "yield_bias": snapshot.get("yield_bias"),
            "impact": snapshot.get("impact"),
            "confidence": snapshot.get("confidence"),
            "reason": snapshot.get("reason"),
        }

    def get_bond_market_snapshot(self) -> dict | None:
        """Fetch and normalize Indian bond/yield context.

        Prefer more specific CCIL pages instead of the generic daily-data landing page.
        We try yield-curve pages first, then bond trading summary as fallback.
        """
        if not self.enabled:
            return None

        cached = self._cache_get("bond")
        if cached is not None:
            return cached

        candidate_urls = [
            self.ZCYC_ZERO_RATES_URL,
            self.ZCYC_PARAMETERS_URL,
            self.TRADING_SUMMARY_URL,
            self.DAY_SUMMARY_URL,
        ]

        last_error = ""
        for url in candidate_urls:
            raw = self._fetch_text(url)
            if raw is None:
                last_error = self.last_error
                continue

            normalized = self.normalize_bond_snapshot(raw)
            if normalized is not None:
                normalized["source_url"] = url
                self._mark_success()
                return self._cache_set("bond", normalized)

            last_error = self.last_error

        self.last_error = last_error or "Unable to parse CCIL bond/yield snapshot from candidate URLs."
        return None

    def get_money_market_snapshot(self) -> dict | None:
        """Fetch and normalize CCIL money-market context."""
        if not self.enabled:
            return None

        cached = self._cache_get("money")
        if cached is not None:
            return cached

        # Prefer the direct money-market rates/volumes page.
        candidate_urls = [
            self.MONEY_MARKET_RATES_URL,
            self.MONEY_MARKET_DAILY_DATA_URL,
        ]

        last_error = ""
        for url in candidate_urls:
            raw = self._fetch_text(url)
            if raw is None:
                last_error = self.last_error
                continue

            normalized = self.normalize_money_market_snapshot(raw)
            if normalized is not None:
                normalized["source_url"] = url
                self._mark_success()
                return self._cache_set("money", normalized)

            last_error = self.last_error

        self.last_error = last_error or "Unable to parse CCIL money-market snapshot from candidate URLs."
        return None

    def normalize_bond_snapshot(self, raw: Any) -> dict | None:
        """Normalize a raw CCIL bond/yield response into stable macro-agent shape."""
        try:
            parsed = self._parse_bond_snapshot(raw)
            if not parsed:
                self.last_error = "Unable to parse CCIL bond snapshot."
                return None

            change_bps = _safe_float(parsed.get("india_10y_change_bps"))

            bias = "NEUTRAL"
            impact = 2
            confidence = 0.55
            reason = "India bond/yield context is not showing a strong macro signal."

            if change_bps is not None and change_bps >= 20:
                bias = "BEARISH"
                impact = 8
                confidence = 0.80
                reason = "A sharp rise in India 10Y yield can tighten financial conditions and pressure equities."
            elif change_bps is not None and change_bps >= 10:
                bias = "BEARISH"
                impact = 6
                confidence = 0.72
                reason = "India 10Y yield is rising enough to be a macro headwind."
            elif change_bps is not None and change_bps <= -10:
                bias = "BULLISH"
                impact = 5
                confidence = 0.68
                reason = "A meaningful fall in India 10Y yield can ease rate pressure for equities."

            return {
                "source": "ccil",
                "as_of_date": parsed.get("as_of_date"),
                "india_10y_yield": _safe_float(parsed.get("india_10y_yield")),
                "india_10y_change_bps": change_bps,
                "yield_bias": bias,
                "impact": impact,
                "confidence": confidence,
                "reason": reason,
            }

        except Exception as exc:
            self.last_error = f"CCIL bond normalize error: {str(exc)[:240]}"
            return None

    def normalize_money_market_snapshot(self, raw: Any) -> dict | None:
        """Normalize a raw CCIL money-market response into stable macro-agent shape."""
        try:
            parsed = self._parse_money_market_snapshot(raw)
            if not parsed:
                self.last_error = "Unable to parse CCIL money-market snapshot."
                return None

            stress = str(parsed.get("money_market_stress") or "UNKNOWN").upper()

            bias = "NEUTRAL"
            impact = 2
            confidence = 0.50
            reason = "Money-market conditions look broadly normal."

            if stress == "HIGH":
                bias = "CAUTION"
                impact = 7
                confidence = 0.75
                reason = "High money-market stress can signal tighter liquidity and justify caution."
            elif stress == "CAUTION":
                bias = "CAUTION"
                impact = 5
                confidence = 0.65
                reason = "Money-market conditions deserve some caution."

            return {
                "source": "ccil",
                "as_of_date": parsed.get("as_of_date"),
                "money_market_stress": stress,
                "liquidity_bias": bias,
                "impact": impact,
                "confidence": confidence,
                "reason": reason,
            }

        except Exception as exc:
            self.last_error = f"CCIL money normalize error: {str(exc)[:240]}"
            return None

    def source_status(self) -> dict:
        return {
            "provider": "ccil",
            "enabled": self.enabled,
            "configured": self.enabled,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_error": self.last_error,
            "using_fallback": False,
            "stale": False,
        }

    def _fetch_text(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                timeout=self.timeout_seconds,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; IndianMarketAgent/1.0; "
                        "+https://github.com/yash214/indian-market-agent)"
                    )
                },
            )
            response.raise_for_status()
            return response.text

        except requests.RequestException as exc:
            self.last_error = f"CCIL request error for {url}: {str(exc)[:240]}"
            return None

    def _parse_bond_snapshot(self, raw: Any) -> dict | None:
        if isinstance(raw, dict):
            return raw

        if not isinstance(raw, str) or not raw.strip():
            return None

        # First try explicit HTML table row extraction.
        html_parsed = _extract_bond_snapshot_from_html(raw)
        if html_parsed:
            return html_parsed

        # Then try pandas table extraction.
        tables: list[pd.DataFrame] = []
        try:
            tables = pd.read_html(StringIO(raw))
        except ValueError:
            tables = []
        except Exception as exc:
            self.last_error = f"CCIL pandas table parse error: {str(exc)[:240]}"
            tables = []

        for table in tables:
            rows = table.fillna("").astype(str).values.tolist()

            for row in rows:
                text = " ".join(str(cell).strip() for cell in row if str(cell).strip())
                lower = text.lower()

                if _looks_like_10y_yield_row(lower):
                    value_cells = [_safe_float(cell) for cell in row[1:]]
                    cleaned_numbers = [value for value in value_cells if value is not None]
                    if not cleaned_numbers:
                        numbers = re.findall(r"[-+]?\d[\d,]*\.?\d*", text)
                        cleaned_numbers = _drop_10y_tenor_number([n for n in (_safe_float(number) for number in numbers) if n is not None])

                    if cleaned_numbers:
                        return {
                            "as_of_date": _extract_date(raw),
                            "india_10y_yield": cleaned_numbers[0],
                            "india_10y_change_bps": cleaned_numbers[1] if len(cleaned_numbers) > 1 else None,
                        }

        # Last fallback: regex over page text.
        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)

        yield_match = re.search(
            r"(?:10y|10-year|10 year|10\s*yr|10\s*year\s*yield)"
            r"[^-+0-9]{0,60}"
            r"(\d+(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )

        change_match = re.search(
            r"(?:change|bps|basis points)"
            r"[^-+0-9]{0,30}"
            r"([-+]?\d+(?:\.\d+)?)",
            text,
            flags=re.IGNORECASE,
        )

        if not yield_match:
            return None

        return {
            "as_of_date": _extract_date(text),
            "india_10y_yield": _safe_float(yield_match.group(1)),
            "india_10y_change_bps": _safe_float(change_match.group(1)) if change_match else None,
        }

    def _parse_money_market_snapshot(self, raw: Any) -> dict | None:
        if isinstance(raw, dict):
            return raw

        if not isinstance(raw, str) or not raw.strip():
            return None

        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
        upper = text.upper()

        stress = "UNKNOWN"

        # Very simple best-effort text detection. Later this should parse actual
        # Call / Market Repo / TREPS rates and compare them to historical ranges.
        if any(flag in upper for flag in {"HIGH STRESS", "SEVERE", "TIGHT LIQUIDITY", "LIQUIDITY DEFICIT"}):
            stress = "HIGH"
        elif any(flag in upper for flag in {"CAUTION", "TIGHT", "ELEVATED"}):
            stress = "CAUTION"
        elif any(flag in upper for flag in {"NORMAL", "COMFORTABLE", "EASY", "AMPLE LIQUIDITY"}):
            stress = "NORMAL"

        # Try table-based heuristic if text labels are not explicit.
        if stress == "UNKNOWN":
            table_stress = self._infer_money_market_stress_from_tables(raw)
            if table_stress:
                stress = table_stress

        if stress == "UNKNOWN" and not text:
            return None

        return {
            "as_of_date": _extract_date(text),
            "money_market_stress": stress,
        }

    def _infer_money_market_stress_from_tables(self, raw: str) -> str | None:
        """Best-effort money-market stress inference from table values.

        This intentionally stays conservative. If we cannot confidently infer stress,
        return None instead of inventing a signal.
        """
        try:
            tables = pd.read_html(StringIO(raw))
        except ValueError:
            return None
        except Exception as exc:
            self.last_error = f"CCIL money-market table parse error: {str(exc)[:240]}"
            return None

        text_chunks: list[str] = []
        for table in tables:
            rows = table.fillna("").astype(str).values.tolist()
            for row in rows:
                row_text = " ".join(str(cell).strip() for cell in row if str(cell).strip())
                if row_text:
                    text_chunks.append(row_text)

        combined = " ".join(text_chunks).upper()

        if not combined:
            return None

        if any(flag in combined for flag in {"LIQUIDITY DEFICIT", "HIGH STRESS", "SEVERE"}):
            return "HIGH"

        if any(flag in combined for flag in {"TIGHT", "ELEVATED", "CAUTION"}):
            return "CAUTION"

        if any(flag in combined for flag in {"NORMAL", "COMFORTABLE", "EASY"}):
            return "NORMAL"

        return None

    def _cache_get(self, key: str):
        cached = self._cache.get(key)
        if not cached:
            return None

        stored_at, value = cached
        if (time.time() - stored_at) > self.cache_ttl_seconds:
            return None

        return value

    def _cache_set(self, key: str, value):
        self._cache[key] = (time.time(), value)
        return value

    def _mark_success(self) -> None:
        self.last_success_at = datetime.now(timezone.utc)
        self.last_error = ""


def _extract_date(text: str) -> str | None:
    """Extract common DD-MM-YYYY / DD/MM/YYYY / YYYY-MM-DD date formats."""
    if not text:
        return None

    patterns = [
        r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})",
        r"(\d{4}-\d{1,2}-\d{1,2})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue

        value = match.group(1)
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                continue

    return None


def _extract_bond_snapshot_from_html(raw: str) -> dict | None:
    """Best-effort extraction of 10Y yield-like rows from HTML tables."""
    soup = BeautifulSoup(raw, "html.parser")

    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]

        if len(cells) < 2:
            continue

        row_text = " ".join(cells)
        label = row_text.lower()

        if not _looks_like_10y_yield_row(label):
            continue

        value_cells = [_safe_float(cell) for cell in cells[1:]]
        cleaned_numbers = [value for value in value_cells if value is not None]
        if not cleaned_numbers:
            numbers = re.findall(r"[-+]?\d[\d,]*\.?\d*", row_text)
            cleaned_numbers = _drop_10y_tenor_number([n for n in (_safe_float(number) for number in numbers) if n is not None])

        if not cleaned_numbers:
            continue

        return {
            "as_of_date": _extract_date(raw),
            "india_10y_yield": cleaned_numbers[0],
            "india_10y_change_bps": cleaned_numbers[1] if len(cleaned_numbers) > 1 else None,
        }

    return None


def _drop_10y_tenor_number(numbers: list[float]) -> list[float]:
    if len(numbers) > 1 and numbers[0] == 10:
        return numbers[1:]
    return numbers


def _looks_like_10y_yield_row(text: str) -> bool:
    if not text:
        return False

    lower = text.lower()

    ten_year_terms = {
        "10y",
        "10 year",
        "10-year",
        "10 yr",
        "10-yr",
        "10 years",
        "10-year yield",
        "10 year yield",
    }

    has_ten_year = any(term in lower for term in ten_year_terms)
    has_yield_or_rate = any(term in lower for term in {"yield", "rate", "g-sec", "gsec", "government security"})

    return has_ten_year and has_yield_or_rate


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None

        text = str(value).strip()
        if not text:
            return None

        text = text.replace(",", "")
        text = text.replace("%", "")
        text = text.replace("bps", "")
        text = text.replace("BPS", "")
        text = text.strip()

        return float(text)

    except (TypeError, ValueError):
        return None
