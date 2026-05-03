"""Safe low-frequency CCIL provider for bond and money-market context."""

from __future__ import annotations

from datetime import datetime, timezone
import re
import time
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from backend.core.settings import CCIL_PROVIDER_CACHE_TTL_SECONDS, CCIL_PROVIDER_ENABLED, CCIL_PROVIDER_TIMEOUT_SECONDS
except ModuleNotFoundError:
    from core.settings import CCIL_PROVIDER_CACHE_TTL_SECONDS, CCIL_PROVIDER_ENABLED, CCIL_PROVIDER_TIMEOUT_SECONDS


class CCILProvider:
    BOND_URL = "https://www.ccilindia.com/daily-data"
    MONEY_MARKET_URL = "https://www.ccilindia.com/moneymarketsummury_daily_data"

    # Disabled by default via settings; live public HTML parsing is best-effort.
    # Provider failures must update last_error and return None, not crash callers.
    def __init__(self, enabled: bool | None = None, timeout_seconds: int | None = None, cache_ttl_seconds: int | None = None, session: requests.Session | None = None):
        self.enabled = CCIL_PROVIDER_ENABLED if enabled is None else bool(enabled)
        self.timeout_seconds = int(timeout_seconds or CCIL_PROVIDER_TIMEOUT_SECONDS)
        self.cache_ttl_seconds = int(cache_ttl_seconds or CCIL_PROVIDER_CACHE_TTL_SECONDS)
        self.session = session or requests.Session()
        self.last_success_at: datetime | None = None
        self.last_error = ""
        self._cache: dict[str, tuple[float, object]] = {}

    def get_india_10y_yield(self):
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

    def get_bond_market_snapshot(self):
        if not self.enabled:
            return None
        cached = self._cache_get("bond")
        if cached is not None:
            return cached
        raw = self._fetch_text(self.BOND_URL)
        if raw is None:
            return None
        normalized = self.normalize_bond_snapshot(raw)
        if normalized is not None:
            self._mark_success()
            return self._cache_set("bond", normalized)
        return None

    def get_money_market_snapshot(self) -> dict | None:
        if not self.enabled:
            return None
        cached = self._cache_get("money")
        if cached is not None:
            return cached
        raw = self._fetch_text(self.MONEY_MARKET_URL)
        if raw is None:
            return None
        normalized = self.normalize_money_market_snapshot(raw)
        if normalized is not None:
            self._mark_success()
            return self._cache_set("money", normalized)
        return None

    def normalize_bond_snapshot(self, raw) -> dict | None:
        try:
            parsed = self._parse_bond_snapshot(raw)
            if not parsed:
                self.last_error = "Unable to parse CCIL bond snapshot."
                return None
            change_bps = _safe_float(parsed.get("india_10y_change_bps"))
            bias = "NEUTRAL"
            impact = 2
            confidence = 0.55
            reason = "India 10Y yield is not showing a strong macro signal."
            if change_bps is not None and change_bps >= 20:
                bias, impact, confidence, reason = ("BEARISH", 8, 0.8, "A sharp rise in India 10Y yield can tighten financial conditions and pressure equities.")
            elif change_bps is not None and change_bps >= 10:
                bias, impact, confidence, reason = ("BEARISH", 6, 0.72, "India 10Y yield is rising enough to be a macro headwind.")
            elif change_bps is not None and change_bps <= -10:
                bias, impact, confidence, reason = ("BULLISH", 5, 0.68, "A meaningful fall in India 10Y yield can ease rate pressure for equities.")
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

    def normalize_money_market_snapshot(self, raw) -> dict | None:
        try:
            parsed = self._parse_money_market_snapshot(raw)
            if not parsed:
                self.last_error = "Unable to parse CCIL money-market snapshot."
                return None
            stress = str(parsed.get("money_market_stress") or "UNKNOWN").upper()
            bias = "NEUTRAL"
            impact = 2
            confidence = 0.5
            reason = "Money-market conditions look broadly normal."
            if stress == "HIGH":
                bias, impact, confidence, reason = ("CAUTION", 7, 0.75, "High money-market stress can signal tighter liquidity and justify caution.")
            elif stress == "CAUTION":
                bias, impact, confidence, reason = ("CAUTION", 5, 0.65, "Money-market conditions deserve some caution.")
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
            response = self.session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            self.last_error = f"CCIL request error: {str(exc)[:240]}"
            return None

    def _parse_bond_snapshot(self, raw) -> dict | None:
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str) or not raw.strip():
            return None
        html_parsed = _extract_bond_snapshot_from_html(raw)
        if html_parsed:
            return html_parsed
        tables = []
        try:
            tables = pd.read_html(StringIO(raw))
        except ValueError:
            tables = []
        for table in tables:
            rows = table.fillna("").astype(str).values.tolist()
            for row in rows:
                text = " ".join(str(cell).strip() for cell in row if str(cell).strip())
                lower = text.lower()
                if "10" in lower and "yield" in lower:
                    numbers = re.findall(r"[-+]?\d[\d,]*\.?\d*", text)
                    if numbers:
                        return {
                            "as_of_date": _extract_date(raw),
                            "india_10y_yield": _safe_float(numbers[0]),
                            "india_10y_change_bps": _safe_float(numbers[1]) if len(numbers) > 1 else None,
                        }
        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
        yield_match = re.search(r"(?:10y|10-year|10 year)[^-+0-9]{0,40}(\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
        change_match = re.search(r"(?:change|bps)[^-+0-9]{0,20}([-+]?\d+(?:\.\d+)?)", text, flags=re.IGNORECASE)
        if not yield_match:
            return None
        return {
            "as_of_date": _extract_date(text),
            "india_10y_yield": _safe_float(yield_match.group(1)),
            "india_10y_change_bps": _safe_float(change_match.group(1)) if change_match else None,
        }

    def _parse_money_market_snapshot(self, raw) -> dict | None:
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str) or not raw.strip():
            return None
        text = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
        upper = text.upper()
        stress = "UNKNOWN"
        if any(flag in upper for flag in {"HIGH STRESS", "SEVERE", "TIGHT LIQUIDITY"}):
            stress = "HIGH"
        elif any(flag in upper for flag in {"CAUTION", "TIGHT"}):
            stress = "CAUTION"
        elif any(flag in upper for flag in {"NORMAL", "COMFORTABLE", "EASY"}):
            stress = "NORMAL"
        if stress == "UNKNOWN" and not text:
            return None
        return {
            "as_of_date": _extract_date(text),
            "money_market_stress": stress,
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


def _extract_bond_snapshot_from_html(raw: str) -> dict | None:
    soup = BeautifulSoup(raw, "html.parser")
    for row in soup.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
        if len(cells) < 2:
            continue
        label = cells[0].lower()
        if "10" in label and "yield" in label:
            return {
                "as_of_date": _extract_date(raw),
                "india_10y_yield": _safe_float(cells[1]),
                "india_10y_change_bps": _safe_float(cells[2]) if len(cells) > 2 else None,
            }
    return None


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None
