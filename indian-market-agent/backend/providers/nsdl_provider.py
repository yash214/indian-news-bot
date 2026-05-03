"""Safe low-frequency NSDL provider for FPI/FII flows."""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from backend.core.settings import (
        NSDL_PROVIDER_CACHE_TTL_SECONDS,
        NSDL_PROVIDER_ENABLED,
        NSDL_PROVIDER_TIMEOUT_SECONDS,
    )
except ModuleNotFoundError:
    from core.settings import (
        NSDL_PROVIDER_CACHE_TTL_SECONDS,
        NSDL_PROVIDER_ENABLED,
        NSDL_PROVIDER_TIMEOUT_SECONDS,
    )


class NSDLProvider:
    LATEST_URL = "https://www.fpi.nsdl.co.in/Reports/Latest.aspx"

    # Disabled by default via settings; live public HTML parsing is best-effort.
    # Provider failures must update last_error and return None, not crash callers.
    def __init__(self, enabled: bool | None = None, timeout_seconds: int | None = None, cache_ttl_seconds: int | None = None, session: requests.Session | None = None):
        self.enabled = NSDL_PROVIDER_ENABLED if enabled is None else bool(enabled)
        self.timeout_seconds = int(timeout_seconds or NSDL_PROVIDER_TIMEOUT_SECONDS)
        self.cache_ttl_seconds = int(cache_ttl_seconds or NSDL_PROVIDER_CACHE_TTL_SECONDS)
        self.session = session or requests.Session()
        self.last_success_at: datetime | None = None
        self.last_error = ""
        self._cache: dict[str, tuple[float, object]] = {}

    def get_latest_fpi_flows(self) -> dict | None:
        if not self.enabled:
            return None
        cached = self._cache_get("latest")
        if cached is not None:
            return cached
        raw = self._fetch_text(self.LATEST_URL)
        if raw is None:
            return None
        normalized = self.normalize_latest_flows(raw)
        if normalized is not None:
            self._mark_success()
            return self._cache_set("latest", normalized)
        return None

    def get_fpi_flows_by_date(self, date: str) -> dict | None:
        if not self.enabled:
            return None
        cache_key = f"date:{date}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        raw = self._fetch_text(self.LATEST_URL, params={"date": date})
        if raw is None:
            return None
        normalized = self.normalize_latest_flows(raw)
        if normalized is not None:
            normalized["as_of_date"] = normalized.get("as_of_date") or date
            self._mark_success()
            return self._cache_set(cache_key, normalized)
        return None

    def normalize_latest_flows(self, raw) -> dict | None:
        try:
            parsed = self._parse_latest_flows(raw)
            if not parsed:
                self.last_error = "Unable to parse NSDL latest flows."
                return None
            equity = _safe_float(parsed.get("equity_net_inr_cr"))
            debt = _safe_float(parsed.get("debt_net_inr_cr"))
            total = _safe_float(parsed.get("total_net_inr_cr"))
            if total is None:
                total = (equity or 0.0) + (debt or 0.0) if equity is not None or debt is not None else None
            bias, impact, confidence, reason = _flow_bias(equity)
            return {
                "source": "nsdl",
                "as_of_date": parsed.get("as_of_date"),
                "equity_net_inr_cr": equity,
                "debt_net_inr_cr": debt,
                "total_net_inr_cr": total,
                "flow_bias": bias,
                "impact": impact,
                "confidence": confidence,
                "reason": reason,
            }
        except Exception as exc:
            self.last_error = f"NSDL normalize error: {str(exc)[:240]}"
            return None

    def source_status(self) -> dict:
        return {
            "provider": "nsdl",
            "enabled": self.enabled,
            "configured": self.enabled,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_error": self.last_error,
            "using_fallback": False,
            "stale": False,
        }

    def get_fii_fpi_flows(self):
        return self.get_latest_fpi_flows()

    def get_dii_flows_if_available(self):
        # TODO: add a stable DII source once a reliable official dataset is selected.
        return None

    def _fetch_text(self, url: str, params: dict | None = None) -> str | None:
        try:
            response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            self.last_error = f"NSDL request error: {str(exc)[:240]}"
            return None

    def _parse_latest_flows(self, raw) -> dict | None:
        if isinstance(raw, dict):
            return {
                "as_of_date": _normalize_date(raw.get("as_of_date") or raw.get("date")),
                "equity_net_inr_cr": _safe_float(raw.get("equity_net_inr_cr")),
                "debt_net_inr_cr": _safe_float(raw.get("debt_net_inr_cr")),
                "total_net_inr_cr": _safe_float(raw.get("total_net_inr_cr")),
            }
        if not isinstance(raw, str) or not raw.strip():
            return None

        extracted = _extract_flow_values_from_html(raw)
        if extracted:
            extracted["as_of_date"] = extracted.get("as_of_date") or _extract_date_from_text(raw)
            return extracted

        tables = []
        try:
            tables = pd.read_html(StringIO(raw))
        except ValueError:
            tables = []
        extracted = _extract_flow_values_from_tables(tables)
        if extracted:
            extracted["as_of_date"] = extracted.get("as_of_date") or _extract_date_from_text(raw)
            return extracted

        soup = BeautifulSoup(raw, "html.parser")
        text = soup.get_text(" ", strip=True)
        equity = _extract_metric_from_text(text, "equity")
        debt = _extract_metric_from_text(text, "debt")
        total = _extract_metric_from_text(text, "total")
        if equity is None and debt is None and total is None:
            return None
        return {
            "as_of_date": _extract_date_from_text(text),
            "equity_net_inr_cr": equity,
            "debt_net_inr_cr": debt,
            "total_net_inr_cr": total,
        }

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


def _extract_flow_values_from_tables(tables: list[pd.DataFrame]) -> dict | None:
    for table in tables:
        rows = table.fillna("").astype(str).values.tolist()
        equity = debt = total = None
        for row in rows:
            text = " ".join(cell.strip() for cell in row if cell and str(cell).strip())
            lower = text.lower()
            value = _last_number(text)
            if value is None:
                continue
            if "equity" in lower:
                equity = value
            elif "debt" in lower:
                debt = value
            elif "total" in lower:
                total = value
        if equity is not None or debt is not None or total is not None:
            return {
                "equity_net_inr_cr": equity,
                "debt_net_inr_cr": debt,
                "total_net_inr_cr": total,
            }
    return None


def _extract_flow_values_from_html(raw: str) -> dict | None:
    soup = BeautifulSoup(raw, "html.parser")
    equity = debt = total = None
    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        label = cells[0].lower()
        value = _safe_float(cells[-1])
        if value is None:
            continue
        if "equity" in label:
            equity = value
        elif "debt" in label:
            debt = value
        elif "total" in label:
            total = value
    if equity is None and debt is None and total is None:
        return None
    return {
        "equity_net_inr_cr": equity,
        "debt_net_inr_cr": debt,
        "total_net_inr_cr": total,
    }


def _extract_metric_from_text(text: str, keyword: str) -> float | None:
    match = re.search(rf"{keyword}[^-+0-9]{{0,40}}([-+]?\d[\d,]*\.?\d*)", text, flags=re.IGNORECASE)
    return _safe_float(match.group(1)) if match else None


def _extract_date_from_text(text: str) -> str | None:
    match = re.search(r"(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})", text)
    if not match:
        return None
    return _normalize_date(match.group(1))


def _normalize_date(value) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%m-%y", "%d/%m/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _last_number(text: str) -> float | None:
    matches = re.findall(r"[-+]?\d[\d,]*\.?\d*", text)
    if not matches:
        return None
    return _safe_float(matches[-1])


def _flow_bias(equity_net_inr_cr: float | None) -> tuple[str, int, float, str]:
    if equity_net_inr_cr is None:
        return ("NEUTRAL", 2, 0.35, "NSDL flow data is incomplete.")
    if equity_net_inr_cr >= 7000:
        return ("BULLISH", 8, 0.82, "Strong positive FPI equity flows can support Indian index sentiment.")
    if equity_net_inr_cr >= 3000:
        return ("BULLISH", 6, 0.74, "Positive FPI equity flows are supportive for broad market risk appetite.")
    if equity_net_inr_cr <= -7000:
        return ("BEARISH", 8, 0.82, "Heavy FPI equity outflows can pressure Indian indices and sentiment.")
    if equity_net_inr_cr <= -3000:
        return ("BEARISH", 6, 0.74, "FPI equity outflows are a bearish macro flow signal.")
    return ("NEUTRAL", 2, 0.6, "FPI equity flows are not signaling a strong macro bias.")


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
