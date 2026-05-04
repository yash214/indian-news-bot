"""Safe low-frequency MOSPI provider for CPI/GDP/IIP context.

Important:
- This provider is disabled by default.
- MoSPI pages can be dynamic or change HTML structure.
- Live public HTML parsing here is best-effort.
- Provider failures must update last_error and return None, not crash callers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
import re
import time
from typing import Any, Callable

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from backend.core.settings import (
        MOSPI_PROVIDER_CACHE_TTL_SECONDS,
        MOSPI_PROVIDER_ENABLED,
        MOSPI_PROVIDER_TIMEOUT_SECONDS,
    )
except ModuleNotFoundError:
    from core.settings import (
        MOSPI_PROVIDER_CACHE_TTL_SECONDS,
        MOSPI_PROVIDER_ENABLED,
        MOSPI_PROVIDER_TIMEOUT_SECONDS,
    )


class MOSPIProvider:
    """Best-effort MoSPI provider for India CPI, GDP, IIP and release-calendar context."""

    # Landing / legacy pages
    CPI_HOME_URL = "https://www.cpi.mospi.gov.in/"
    MOSPI_CPI_URL = "https://www.mospi.gov.in/cpi"
    MOSPI_IIP_URL = "https://www.mospi.gov.in/iip"
    MOSPI_DATAVIZ_URL = "https://www.mospi.gov.in/dataviz-landing-page-1"

    # Better product/data pages for parsing
    CPI_PRODUCT_URL = "https://www.mospi.gov.in/themes/product/9-consumer-price-index-cpi"
    GDP_PRODUCT_URL = "https://www.mospi.gov.in/themes/product/6-gross-domestic-product"
    IIP_PRODUCT_URL = "https://www.mospi.gov.in/themes/product/54-index-of-industrial-production"
    IIP_DATA_URL = "https://www.mospi.gov.in/iipdata"

    # eSankhyiki pages
    ESANKHYIKI_CPI_URL = "https://esankhyiki.mospi.gov.in/macroindicators?product=cpi"

    # Calendar
    RELEASE_CALENDAR_URL = "https://www.mospi.gov.in/release-calendar"

    def __init__(
        self,
        enabled: bool | None = None,
        timeout_seconds: int | None = None,
        cache_ttl_seconds: int | None = None,
        session: requests.Session | None = None,
    ):
        self.enabled = MOSPI_PROVIDER_ENABLED if enabled is None else bool(enabled)
        self.timeout_seconds = int(timeout_seconds or MOSPI_PROVIDER_TIMEOUT_SECONDS)
        self.cache_ttl_seconds = int(cache_ttl_seconds or MOSPI_PROVIDER_CACHE_TTL_SECONDS)
        self.session = session or requests.Session()

        self.last_success_at: datetime | None = None
        self.last_error = ""
        self._cache: dict[str, tuple[float, object]] = {}

    def get_latest_cpi(self) -> dict | None:
        """Return latest CPI/inflation context."""
        return self._get_metric(
            cache_key="cpi",
            urls=[
                self.CPI_PRODUCT_URL,
                self.ESANKHYIKI_CPI_URL,
                self.MOSPI_CPI_URL,
                self.CPI_HOME_URL,
                self.MOSPI_DATAVIZ_URL,
            ],
            normalizer=self.normalize_cpi,
        )

    def get_latest_gdp(self) -> dict | None:
        """Return latest GDP growth context."""
        return self._get_metric(
            cache_key="gdp",
            urls=[
                self.GDP_PRODUCT_URL,
                self.MOSPI_DATAVIZ_URL,
            ],
            normalizer=self.normalize_gdp,
        )

    def get_latest_iip(self) -> dict | None:
        """Return latest IIP context."""
        return self._get_metric(
            cache_key="iip",
            urls=[
                self.IIP_PRODUCT_URL,
                self.IIP_DATA_URL,
                self.MOSPI_IIP_URL,
            ],
            normalizer=self.normalize_iip,
        )

    def get_release_calendar(self) -> list[dict]:
        """Return best-effort MoSPI release-calendar events.

        This intentionally stays simple because MoSPI calendar pages/PDFs can change.
        """
        if not self.enabled:
            return []

        cached = self._cache_get("release_calendar")
        if cached is not None:
            return cached

        raw = self._fetch_text(self.RELEASE_CALENDAR_URL)
        if raw is None:
            return []

        try:
            events = self._parse_release_calendar(raw)
            if events:
                self._mark_success()
                return self._cache_set("release_calendar", events)

            self.last_error = "Unable to parse MOSPI release calendar."
            return []

        except Exception as exc:
            self.last_error = f"MOSPI release calendar parse error: {str(exc)[:240]}"
            return []

    def normalize_cpi(self, raw: Any) -> dict | None:
        try:
            parsed = self._parse_metric(raw, "cpi")
            if not parsed:
                self.last_error = "Unable to parse MOSPI CPI snapshot."
                return None

            cpi = _safe_float(parsed.get("cpi_yoy"))

            bias = "NEUTRAL"
            impact = 3
            confidence = 0.58
            reason = "CPI is not showing a strong inflation warning."

            if cpi is not None and cpi > 6:
                bias = "BEARISH"
                impact = 7
                confidence = 0.76
                reason = "CPI above 6% keeps inflation pressure elevated for markets."
            elif cpi is not None and cpi < 5:
                bias = "BULLISH"
                impact = 4
                confidence = 0.64
                reason = "Cooling CPI below 5% is relatively supportive for market sentiment."

            return {
                "source": "mospi",
                "as_of_date": parsed.get("as_of_date"),
                "cpi_yoy": cpi,
                "gdp_growth_yoy": None,
                "iip_yoy": None,
                "inflation_bias": bias,
                "growth_bias": "NEUTRAL",
                "impact": impact,
                "confidence": confidence,
                "reason": reason,
                "source_url": parsed.get("source_url"),
            }

        except Exception as exc:
            self.last_error = f"MOSPI CPI normalize error: {str(exc)[:240]}"
            return None

    def normalize_gdp(self, raw: Any) -> dict | None:
        try:
            parsed = self._parse_metric(raw, "gdp")
            if not parsed:
                self.last_error = "Unable to parse MOSPI GDP snapshot."
                return None

            gdp = _safe_float(parsed.get("gdp_growth_yoy"))
            forecast = _safe_float(parsed.get("forecast"))

            bias = "NEUTRAL"
            impact = 3
            confidence = 0.56
            reason = "GDP growth is not signaling a strong macro shift."

            if gdp is not None and forecast is not None and gdp > forecast:
                bias = "BULLISH"
                impact = 6
                confidence = 0.72
                reason = "GDP growth above forecast is supportive for domestic risk appetite."
            elif gdp is not None and forecast is not None and gdp < forecast:
                bias = "BEARISH"
                impact = 6
                confidence = 0.72
                reason = "GDP growth below forecast is a macro disappointment."
            elif gdp is not None and gdp >= 7:
                bias = "BULLISH"
                impact = 5
                confidence = 0.66
                reason = "GDP growth above 7% is generally supportive for domestic risk appetite."
            elif gdp is not None and gdp < 5.5:
                bias = "CAUTION"
                impact = 5
                confidence = 0.64
                reason = "GDP growth below 5.5% can justify growth caution."

            return {
                "source": "mospi",
                "as_of_date": parsed.get("as_of_date"),
                "cpi_yoy": None,
                "gdp_growth_yoy": gdp,
                "iip_yoy": None,
                "inflation_bias": "NEUTRAL",
                "growth_bias": bias,
                "impact": impact,
                "confidence": confidence,
                "reason": reason,
                "source_url": parsed.get("source_url"),
            }

        except Exception as exc:
            self.last_error = f"MOSPI GDP normalize error: {str(exc)[:240]}"
            return None

    def normalize_iip(self, raw: Any) -> dict | None:
        try:
            parsed = self._parse_metric(raw, "iip")
            if not parsed:
                self.last_error = "Unable to parse MOSPI IIP snapshot."
                return None

            iip = _safe_float(parsed.get("iip_yoy"))

            bias = "NEUTRAL"
            impact = 2
            confidence = 0.52
            reason = "IIP is not flashing a material macro stress signal."

            if iip is not None and iip < 2:
                bias = "CAUTION"
                impact = 4
                confidence = 0.62
                reason = "Weak IIP can justify mild growth caution."
            elif iip is not None and iip >= 5:
                bias = "BULLISH"
                impact = 4
                confidence = 0.60
                reason = "Strong IIP can support domestic growth sentiment."

            return {
                "source": "mospi",
                "as_of_date": parsed.get("as_of_date"),
                "cpi_yoy": None,
                "gdp_growth_yoy": None,
                "iip_yoy": iip,
                "inflation_bias": "NEUTRAL",
                "growth_bias": bias,
                "impact": impact,
                "confidence": confidence,
                "reason": reason,
                "source_url": parsed.get("source_url"),
            }

        except Exception as exc:
            self.last_error = f"MOSPI IIP normalize error: {str(exc)[:240]}"
            return None

    def source_status(self) -> dict:
        return {
            "provider": "mospi",
            "enabled": self.enabled,
            "configured": self.enabled,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_error": self.last_error,
            "using_fallback": False,
            "stale": False,
        }

    def _get_metric(
        self,
        cache_key: str,
        urls: list[str],
        normalizer: Callable[[Any], dict | None],
    ) -> dict | None:
        if not self.enabled:
            return None

        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        last_error = ""
        for url in urls:
            raw = self._fetch_text(url)
            if raw is None:
                last_error = self.last_error
                continue

            normalized = normalizer({"html": raw, "source_url": url})
            if normalized is not None:
                self._mark_success()
                return self._cache_set(cache_key, normalized)

            last_error = self.last_error

        self.last_error = last_error or f"Unable to parse MOSPI {cache_key} from candidate URLs."
        return None

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
            self.last_error = f"MOSPI request error for {url}: {str(exc)[:240]}"
            return None

    def _parse_metric(self, raw: Any, metric: str) -> dict | None:
        if isinstance(raw, dict) and "html" in raw:
            source_url = raw.get("source_url")
            html = raw.get("html") or ""
            parsed = self._parse_metric_from_html(html, metric)
            if parsed:
                parsed["source_url"] = source_url
            return parsed

        if isinstance(raw, dict):
            return raw

        if not isinstance(raw, str) or not raw.strip():
            return None

        return self._parse_metric_from_html(raw, metric)

    def _parse_metric_from_html(self, raw: str, metric: str) -> dict | None:
        if not raw or not raw.strip():
            return None

        # Prefer visible page text first.
        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)

        parsed = self._parse_metric_from_text(text, metric)
        if parsed:
            return parsed

        # Then parse HTML tables.
        try:
            tables = pd.read_html(StringIO(raw))
        except ValueError:
            tables = []
        except Exception as exc:
            self.last_error = f"MOSPI pandas table parse error: {str(exc)[:240]}"
            tables = []

        for table in tables:
            table_text = table.fillna("").astype(str).to_string(index=False)
            parsed = self._parse_metric_from_text(table_text, metric)
            if parsed:
                return parsed

        return None

    def _parse_metric_from_text(self, text: str, metric: str) -> dict | None:
        if not text:
            return None

        if metric == "cpi":
            value = _extract_cpi_value(text)
            if value is None:
                return None

            return {
                "as_of_date": _extract_date(text),
                "cpi_yoy": value,
            }

        if metric == "gdp":
            value = _extract_gdp_value(text)
            forecast = _extract_forecast(text)

            if value is None:
                return None

            return {
                "as_of_date": _extract_date(text),
                "gdp_growth_yoy": value,
                "forecast": forecast,
            }

        if metric == "iip":
            value = _extract_iip_value(text)
            if value is None:
                return None

            return {
                "as_of_date": _extract_date(text),
                "iip_yoy": value,
            }

        return None

    def _parse_release_calendar(self, raw: str) -> list[dict]:
        if not isinstance(raw, str) or not raw.strip():
            return []

        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)

        events: list[dict] = []
        for keyword in ("CPI", "GDP", "IIP", "Consumer Price Index", "Gross Domestic Product", "Index of Industrial Production"):
            if keyword.lower() in text.lower():
                events.append(
                    {
                        "source": "mospi",
                        "event": keyword,
                        "country": "India",
                        "importance": "high" if keyword in {"CPI", "GDP", "IIP"} else "medium",
                        "event_time": None,
                        "raw_match": keyword,
                    }
                )

        # Deduplicate by event.
        deduped: dict[str, dict] = {}
        for event in events:
            deduped[event["event"]] = event

        return list(deduped.values())

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


def _extract_cpi_value(text: str) -> float | None:
    patterns = [
        r"(?:inflation\s*\(cpi\s*based\)|cpi\s*based\s*inflation|retail\s*inflation|inflation)"
        r"[^-+0-9]{0,80}"
        r"([-+]?\d+(?:\.\d+)?)\s*%?",
        r"(?:cpi)[^-+0-9]{0,80}([-+]?\d+(?:\.\d+)?)\s*%?",
    ]
    return _extract_first_number_by_patterns(text, patterns)


def _extract_gdp_value(text: str) -> float | None:
    patterns = [
        r"(?:gdp\s*growth\s*rate|growth\s*rates?.{0,20}gdp|real\s*gdp.*?growth)"
        r"[^-+0-9]{0,120}"
        r"([-+]?\d+(?:\.\d+)?)\s*%?",
        r"(?:gross\s*domestic\s*product|gdp)"
        r"[^-+0-9]{0,120}"
        r"([-+]?\d+(?:\.\d+)?)\s*%?",
    ]
    return _extract_first_number_by_patterns(text, patterns)


def _extract_iip_value(text: str) -> float | None:
    patterns = [
        r"(?:iip\s*\(general\)|index\s*of\s*industrial\s*production|iip)"
        r"[^-+0-9]{0,100}"
        r"([-+]?\d+(?:\.\d+)?)\s*%?",
        r"(?:industrial\s*production)"
        r"[^-+0-9]{0,100}"
        r"([-+]?\d+(?:\.\d+)?)\s*%?",
    ]
    return _extract_first_number_by_patterns(text, patterns)


def _extract_first_number_by_patterns(text: str, patterns: list[str]) -> float | None:
    if not text:
        return None

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return _safe_float(match.group(1))

    return None


def _extract_forecast(text: str) -> float | None:
    if not text:
        return None

    patterns = [
        r"forecast[^-+0-9]{0,30}([-+]?\d+(?:\.\d+)?)\s*%",
        r"estimated[^-+0-9]{0,30}([-+]?\d+(?:\.\d+)?)\s*%",
    ]

    return _extract_first_number_by_patterns(text, patterns)


def _extract_date(text: str) -> str | None:
    if not text:
        return None

    patterns = [
        r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})",
        r"(\d{4}-\d{1,2}-\d{1,2})",
        r"(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue

        value = match.group(1)
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y", "%Y-%m-%d", "%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(value, fmt).date().isoformat()
            except ValueError:
                continue

    return None


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None

        text = str(value).strip()
        if not text:
            return None

        text = text.replace(",", "")
        text = text.replace("%", "")
        text = text.strip()

        return float(text)

    except (TypeError, ValueError):
        return None
