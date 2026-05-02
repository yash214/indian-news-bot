"""Safe low-frequency RBI provider for policy-rate context."""

from __future__ import annotations

from datetime import datetime
import re
import time
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from backend.core.settings import RBI_PROVIDER_CACHE_TTL_SECONDS, RBI_PROVIDER_ENABLED, RBI_PROVIDER_TIMEOUT_SECONDS
except ModuleNotFoundError:
    from core.settings import RBI_PROVIDER_CACHE_TTL_SECONDS, RBI_PROVIDER_ENABLED, RBI_PROVIDER_TIMEOUT_SECONDS


class RBIProvider:
    DBIE_URL = "https://data.rbi.org.in/DBIE/"
    HISTORY_URL = "https://www.rbi.org.in/commonman/english/History/Scripts/BankrateCRRandSLRChanges.aspx"

    def __init__(self, enabled: bool | None = None, timeout_seconds: int | None = None, cache_ttl_seconds: int | None = None, session: requests.Session | None = None):
        self.enabled = RBI_PROVIDER_ENABLED if enabled is None else bool(enabled)
        self.timeout_seconds = int(timeout_seconds or RBI_PROVIDER_TIMEOUT_SECONDS)
        self.cache_ttl_seconds = int(cache_ttl_seconds or RBI_PROVIDER_CACHE_TTL_SECONDS)
        self.session = session or requests.Session()
        self.last_success_at: datetime | None = None
        self.last_error = ""
        self._cache: dict[str, tuple[float, object]] = {}
        self._previous_policy_snapshot: dict | None = None

    def get_policy_rate_snapshot(self) -> dict | None:
        if not self.enabled:
            return None
        cached = self._cache_get("policy")
        if cached is not None:
            return cached
        raw = self._fetch_text(self.HISTORY_URL) or self._fetch_text(self.DBIE_URL)
        if raw is None:
            return None
        normalized = self.normalize_policy_rate_snapshot(raw, previous_snapshot=self._previous_policy_snapshot)
        if normalized is not None:
            self._previous_policy_snapshot = dict(normalized)
            self._mark_success()
            return self._cache_set("policy", normalized)
        return None

    def get_liquidity_snapshot(self):
        if not self.enabled:
            return None
        # TODO: add a stable RBI liquidity endpoint/CSV integration when a reliable non-interactive source is chosen.
        return None

    def get_policy_calendar(self):
        # TODO: integrate RBI/MPC calendar once a stable official machine-readable source is chosen.
        return []

    def normalize_policy_rate_snapshot(self, raw, previous_snapshot: dict | None = None) -> dict | None:
        try:
            parsed = self._parse_policy_snapshot(raw)
            if not parsed:
                self.last_error = "Unable to parse RBI policy snapshot."
                return None
            repo = _safe_float(parsed.get("repo_rate"))
            reverse_repo = _safe_float(parsed.get("reverse_repo_rate"))
            crr = _safe_float(parsed.get("crr"))
            slr = _safe_float(parsed.get("slr"))
            sdf = _safe_float(parsed.get("sdf_rate"))
            msf = _safe_float(parsed.get("msf_rate"))
            bank_rate = _safe_float(parsed.get("bank_rate"))

            bias = "NEUTRAL"
            event_risk = False
            impact = 3
            confidence = 0.58
            reason = "RBI policy rates are steady from the current snapshot."

            previous_snapshot = previous_snapshot if isinstance(previous_snapshot, dict) else {}
            prev_repo = _safe_float(previous_snapshot.get("repo_rate"))
            prev_crr = _safe_float(previous_snapshot.get("crr"))
            if repo is not None and prev_repo is not None:
                if repo > prev_repo:
                    bias, impact, confidence, reason = ("BEARISH", 8, 0.78, "A repo-rate hike can tighten financial conditions and weigh on risk appetite.")
                elif repo < prev_repo:
                    bias, impact, confidence, reason = ("BULLISH", 7, 0.74, "A repo-rate cut can support liquidity and equity sentiment.")
            if crr is not None and prev_crr is not None:
                if crr > prev_crr:
                    bias, impact, confidence, reason = ("BEARISH", 8, 0.8, "A CRR hike is a tightening signal and can pressure market liquidity.")
                elif crr < prev_crr:
                    bias, impact, confidence, reason = ("BULLISH", 7, 0.76, "A CRR cut can ease liquidity conditions.")

            return {
                "source": "rbi_dbie",
                "as_of_date": parsed.get("as_of_date"),
                "repo_rate": repo,
                "reverse_repo_rate": reverse_repo,
                "crr": crr,
                "slr": slr,
                "sdf_rate": sdf,
                "msf_rate": msf,
                "bank_rate": bank_rate,
                "policy_bias": bias,
                "event_risk": event_risk,
                "impact": impact,
                "confidence": confidence,
                "reason": reason,
            }
        except Exception as exc:
            self.last_error = f"RBI normalize error: {str(exc)[:240]}"
            return None

    def source_status(self) -> dict:
        return {
            "provider": "rbi",
            "enabled": self.enabled,
            "configured": self.enabled,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_error": self.last_error,
            "using_fallback": False,
            "stale": False,
        }

    def get_policy_rate(self):
        return self.get_policy_rate_snapshot()

    def _fetch_text(self, url: str) -> str | None:
        try:
            response = self.session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            self.last_error = f"RBI request error: {str(exc)[:240]}"
            return None

    def _parse_policy_snapshot(self, raw) -> dict | None:
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str) or not raw.strip():
            return None
        parsed = _extract_policy_metrics(raw)
        if parsed:
            return parsed
        try:
            tables = pd.read_html(StringIO(raw))
        except ValueError:
            tables = []
        for table in tables:
            candidate = _extract_policy_metrics(table.to_string())
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
        self.last_success_at = datetime.utcnow()
        self.last_error = ""


def _extract_policy_metrics(raw: str) -> dict | None:
    text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    if not text:
        return None
    metrics = {
        "repo_rate": _extract_labeled_number(text, "repo"),
        "reverse_repo_rate": _extract_labeled_number(text, "reverse repo"),
        "crr": _extract_labeled_number(text, "crr"),
        "slr": _extract_labeled_number(text, "slr"),
        "sdf_rate": _extract_labeled_number(text, "sdf"),
        "msf_rate": _extract_labeled_number(text, "msf"),
        "bank_rate": _extract_labeled_number(text, "bank rate"),
        "as_of_date": _extract_date(text),
    }
    if not any(value is not None for key, value in metrics.items() if key != "as_of_date"):
        return None
    return metrics


def _extract_labeled_number(text: str, label: str) -> float | None:
    match = re.search(rf"{re.escape(label)}[^-+0-9]{{0,30}}([-+]?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
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
