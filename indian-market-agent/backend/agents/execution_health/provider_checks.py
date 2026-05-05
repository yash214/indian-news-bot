"""Provider health normalization for the Execution Health Agent."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

try:
    from backend.agents.execution_health.schemas import (
        CRITICAL,
        FAIL,
        INFO,
        PASS,
        PROVIDER_DEGRADED,
        PROVIDER_FAIL,
        PROVIDER_OK,
        PROVIDER_UNKNOWN,
        UNKNOWN_CHECK,
        WARN,
        WARNING,
        HealthCheck,
        ProviderHealth,
    )
except ModuleNotFoundError:
    from agents.execution_health.schemas import CRITICAL, FAIL, INFO, PASS, PROVIDER_DEGRADED, PROVIDER_FAIL, PROVIDER_OK, PROVIDER_UNKNOWN, UNKNOWN_CHECK, WARN, WARNING, HealthCheck, ProviderHealth


SECRET_KEY_PARTS = ("token", "secret", "api_key", "apikey", "authorization", "password")


def normalize_provider_status(name: str, status: dict | None) -> ProviderHealth:
    provider = str(name or "unknown").strip() or "unknown"
    if not isinstance(status, Mapping) or not status:
        return ProviderHealth(
            provider=provider,
            status=PROVIDER_UNKNOWN,
            enabled=None,
            configured=None,
            last_success_at=None,
            last_error=None,
            using_fallback=False,
            stale=True,
            message=f"{provider} status unavailable.",
        )

    clean = sanitize_provider_status(status)
    requested = _safe_text(clean.get("requested"))
    active = _safe_text(clean.get("active"))
    enabled = _first_bool(clean, "enabled", "authConfigured", "connected", "upstoxAuthConfigured")
    configured = _first_bool(clean, "configured", "authConfigured", "upstoxConfigured", "upstoxAuthConfigured")
    if configured is None and provider.startswith("upstox"):
        configured = _first_bool(clean, "connected")
    last_error = _first_text(clean, "last_error", "lastError", "error")
    last_success_at = _first_text(clean, "last_success_at", "lastSuccessAt", "last_ok_at", "lastOkAt")

    rest = clean.get("rest") if isinstance(clean.get("rest"), Mapping) else {}
    stream = clean.get("stream") if isinstance(clean.get("stream"), Mapping) else {}
    if not last_error:
        last_error = _first_text(rest, "lastError", "last_error") or _first_text(stream, "lastError", "last_error")
    if not last_success_at:
        last_success_at = _first_text(rest, "lastOkAt", "last_ok_at") or _first_text(stream, "lastMessageAt", "lastConnectAt")

    using_fallback = bool(clean.get("using_fallback") or clean.get("usingFallback"))
    if requested and active and requested != active and clean.get("fallbackToNse") is not False:
        using_fallback = True
    stale = bool(clean.get("stale") or clean.get("staleData"))
    degraded = bool(clean.get("degraded"))
    if isinstance(rest, Mapping):
        degraded = degraded or bool(rest.get("lastError"))
    if isinstance(stream, Mapping):
        stale = stale or bool(stream.get("stale"))

    raw_status = _safe_text(clean.get("status")).upper()
    if raw_status in {PROVIDER_OK, PROVIDER_DEGRADED, PROVIDER_FAIL, PROVIDER_UNKNOWN}:
        provider_status = raw_status
    elif stale:
        provider_status = PROVIDER_DEGRADED
    elif last_error or degraded or using_fallback:
        provider_status = PROVIDER_DEGRADED
    elif enabled is False or configured is False:
        provider_status = PROVIDER_UNKNOWN
    else:
        provider_status = PROVIDER_OK

    message = _provider_message(provider, provider_status, last_error, using_fallback, stale, requested, active)
    return ProviderHealth(
        provider=provider,
        status=provider_status,
        enabled=enabled,
        configured=configured,
        last_success_at=last_success_at,
        last_error=last_error,
        using_fallback=using_fallback,
        stale=stale,
        message=message,
    )


def classify_provider_health(provider: ProviderHealth, critical: bool = False) -> HealthCheck:
    details = {
        "category": "provider",
        "provider": provider.provider,
        "critical": critical,
        "using_fallback": provider.using_fallback,
        "provider_status": provider.status,
    }
    if provider.status == PROVIDER_OK:
        return HealthCheck(
            name=f"provider:{provider.provider}",
            status=PASS,
            severity=INFO,
            age_seconds=None,
            message=provider.message,
            details=details,
        )
    if provider.status == PROVIDER_FAIL or (critical and provider.status == PROVIDER_DEGRADED and not provider.using_fallback):
        return HealthCheck(
            name=f"provider:{provider.provider}",
            status=FAIL,
            severity=CRITICAL if critical else WARNING,
            age_seconds=None,
            message=provider.message,
            details=details,
        )
    if provider.status == PROVIDER_UNKNOWN:
        return HealthCheck(
            name=f"provider:{provider.provider}",
            status=UNKNOWN_CHECK,
            severity=CRITICAL if critical else WARNING,
            age_seconds=None,
            message=provider.message,
            details=details,
        )
    return HealthCheck(
        name=f"provider:{provider.provider}",
        status=WARN,
        severity=WARNING,
        age_seconds=None,
        message=provider.message,
        details=details,
    )


def check_provider_statuses(provider_statuses: dict) -> tuple[list[HealthCheck], dict, list[str], list[str]]:
    checks: list[HealthCheck] = []
    normalized: dict[str, dict] = {}
    blockers: list[str] = []
    warnings: list[str] = []
    for name, status in (provider_statuses or {}).items():
        provider = normalize_provider_status(name, status if isinstance(status, dict) else None)
        critical = _provider_is_critical(name, provider)
        check = classify_provider_health(provider, critical=critical)
        checks.append(check)
        normalized[name] = provider.to_dict()
        if check.status == FAIL and check.severity == CRITICAL:
            blockers.append(check.message)
        elif check.status in {WARN, FAIL, UNKNOWN_CHECK}:
            warnings.append(check.message)
    return checks, normalized, blockers, warnings


def sanitize_provider_status(value: Any):
    if isinstance(value, Mapping):
        out = {}
        for key, item in value.items():
            key_text = str(key)
            if _looks_secret_key(key_text):
                if key_text.lower() in {"tokenpreview", "token_preview"}:
                    out[key_text] = item
                else:
                    out[key_text] = "[redacted]"
            else:
                out[key_text] = sanitize_provider_status(item)
        return out
    if isinstance(value, list):
        return [sanitize_provider_status(item) for item in value]
    return value


def _provider_is_critical(name: str, provider: ProviderHealth) -> bool:
    lower = str(name or provider.provider).lower()
    if "market_data_provider" in lower and provider.status == PROVIDER_FAIL:
        return True
    if provider.provider.lower().startswith("upstox") and provider.status == PROVIDER_FAIL:
        return True
    return False


def _provider_message(provider: str, status: str, error: str | None, fallback: bool, stale: bool, requested: str, active: str) -> str:
    if fallback:
        return f"{provider} is using fallback provider {active or 'unknown'} instead of {requested or 'requested provider'}."
    if error:
        return f"{provider} reports error: {error}"
    if stale:
        return f"{provider} status is stale."
    if status == PROVIDER_OK:
        return f"{provider} provider status OK."
    if status == PROVIDER_UNKNOWN:
        return f"{provider} provider status unknown or not configured."
    return f"{provider} provider status degraded."


def _first_bool(status: Mapping, *keys: str) -> bool | None:
    for key in keys:
        if key in status and status.get(key) is not None:
            return bool(status.get(key))
    return None


def _first_text(status: Mapping, *keys: str) -> str | None:
    for key in keys:
        value = status.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _safe_text(value) -> str:
    return str(value or "").strip().lower()


def _looks_secret_key(key: str) -> bool:
    lower = key.lower()
    return any(part in lower for part in SECRET_KEY_PARTS)
