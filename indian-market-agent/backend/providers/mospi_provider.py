"""Safe low-frequency MOSPI provider for CPI/GDP/IIP context."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import time
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from backend.core.settings import MOSPI_PROVIDER_CACHE_TTL_SECONDS, MOSPI_PROVIDER_ENABLED, MOSPI_PROVIDER_TIMEOUT_SECONDS
except ModuleNotFoundError:
    from core.settings import MOSPI_PROVIDER_CACHE_TTL_SECONDS, MOSPI_PROVIDER_ENABLED, MOSPI_PROVIDER_TIMEOUT_SECONDS


class MOSPIProvider:
    CPI_URL = "https://cpi.mospi.gov.in/"
    GDP_URL = "https://www.mospi.gov.in/dataviz-landing-page-1"
    IIP_URL = "https://www.mospi.gov.in/iip"

    # Disabled by default via settings; live public HTML parsing is best-effort.
    # Provider failures must update last_error and return None, not crash callers.
    def __init__(self, enabled: bool | None = None, timeout_seconds: int | None = None, cache_ttl_seconds: int | None = None, session: requests.Session | None = None):
        self.enabled = MOSPI_PROVIDER_ENABLED if enabled is None else bool(enabled)
        self.timeout_seconds = int(timeout_seconds or MOSPI_PROVIDER_TIMEOUT_SECONDS)
        self.cache_ttl_seconds = int(cache_ttl_seconds or MOSPI_PROVIDER_CACHE_TTL_SECONDS)
        self.session = session or requests.Session()
        self.last_success_at: datetime | None = None
        self.last_error = ""
        self._cache: dict[str, tuple[float, object]] = {}

    def get_latest_cpi(self):
        return self._get_metric("cpi", self.CPI_URL, self.normalize_cpi)

    def get_latest_gdp(self):
        return self._get_metric("gdp", self.GDP_URL, self.normalize_gdp)

    def get_latest_iip(self):
        return self._get_metric("iip", self.IIP_URL, self.normalize_iip)

    def get_release_calendar(self):
        # TODO: add a stable release-calendar source if MOSPI exposes one without brittle scraping.
        return []

    def normalize_cpi(self, raw) -> dict | None:
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
                bias, impact, confidence, reason = ("BEARISH", 7, 0.76, "CPI above 6% keeps inflation pressure elevated for markets.")
            elif cpi is not None and cpi < 5:
                bias, impact, confidence, reason = ("BULLISH", 4, 0.64, "Cooling CPI below 5% is relatively supportive for market sentiment.")
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
            }
        except Exception as exc:
            self.last_error = f"MOSPI CPI normalize error: {str(exc)[:240]}"
            return None

    def normalize_gdp(self, raw) -> dict | None:
        try:
            parsed = self._parse_metric(raw, "gdp")
            if not parsed:
                self.last_error = "Unable to parse MOSPI GDP snapshot."
                return None
            gdp = _safe_float(parsed.get("gdp_growth_yoy"))
            bias = "NEUTRAL"
            impact = 3
            confidence = 0.56
            reason = "GDP growth is not signaling a strong macro shift."
            forecast = _safe_float(parsed.get("forecast"))
            if gdp is not None and forecast is not None and gdp > forecast:
                bias, impact, confidence, reason = ("BULLISH", 6, 0.72, "GDP growth above forecast is supportive for domestic risk appetite.")
            elif gdp is not None and forecast is not None and gdp < forecast:
                bias, impact, confidence, reason = ("BEARISH", 6, 0.72, "GDP growth below forecast is a macro disappointment.")
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
            }
        except Exception as exc:
            self.last_error = f"MOSPI GDP normalize error: {str(exc)[:240]}"
            return None

    def normalize_iip(self, raw) -> dict | None:
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
                bias, impact, confidence, reason = ("CAUTION", 4, 0.62, "Weak IIP can justify mild growth caution.")
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

    def _get_metric(self, cache_key: str, url: str, normalizer):
        if not self.enabled:
            return None
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        raw = self._fetch_text(url)
        if raw is None:
            return None
        normalized = normalizer(raw)
        if normalized is not None:
            self._mark_success()
            return self._cache_set(cache_key, normalized)
        return None

    def _fetch_text(self, url: str) -> str | None:
        try:
            response = self.session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            self.last_error = f"MOSPI request error: {str(exc)[:240]}"
            return None

    def _parse_metric(self, raw, metric: str) -> dict | None:
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str) or not raw.strip():
            return None
        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
        if metric == "cpi":
            value = _extract_metric(text, ("cpi", "inflation"))
            return {"as_of_date": _extract_date(text), "cpi_yoy": value} if value is not None else None
        if metric == "gdp":
            value = _extract_metric(text, ("gdp", "growth"))
            forecast = _extract_forecast(text)
            return {"as_of_date": _extract_date(text), "gdp_growth_yoy": value, "forecast": forecast} if value is not None else None
        if metric == "iip":
            value = _extract_metric(text, ("iip", "index of industrial production"))
            return {"as_of_date": _extract_date(text), "iip_yoy": value} if value is not None else None

        try:
            tables = pd.read_html(StringIO(raw))
        except ValueError:
            tables = []
        for table in tables:
            candidate = self._parse_metric(table.to_string(), metric)
            if candidate:
                return candidate
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


def _extract_metric(text: str, labels: tuple[str, ...]) -> float | None:
    for label in labels:
        match = re.search(rf"{re.escape(label)}[^-+0-9]{{0,40}}([-+]?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
        if match:
            return _safe_float(match.group(1))
    return None


def _extract_forecast(text: str) -> float | None:
    match = re.search(r"forecast[^-+0-9]{0,20}([-+]?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
    return _safe_float(match.group(1)) if match else None


def _extract_date(text: str) -> str | None:
    match = re.search(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})", text)
    if not match:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(match.group(1), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
