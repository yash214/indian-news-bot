"""Snapshot builder for macro-context inputs."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from backend.agents.macro_context.schemas import MacroEvent, MacroFactorInput, MacroSnapshot
    from backend.agents.macro_context.source_status import SourceStatus
    from backend.core.settings import IST
    from backend.providers.ccil_provider import CCILProvider
    from backend.providers.fmp import FMPProvider
    from backend.providers.india_vix_provider import IndiaVixProvider
    from backend.providers.mospi_provider import MOSPIProvider
    from backend.providers.nsdl_provider import NSDLProvider
    from backend.providers.rbi_provider import RBIProvider
except ModuleNotFoundError:
    from agents.macro_context.schemas import MacroEvent, MacroFactorInput, MacroSnapshot
    from agents.macro_context.source_status import SourceStatus
    from core.settings import IST
    from providers.ccil_provider import CCILProvider
    from providers.fmp import FMPProvider
    from providers.india_vix_provider import IndiaVixProvider
    from providers.mospi_provider import MOSPIProvider
    from providers.nsdl_provider import NSDLProvider
    from providers.rbi_provider import RBIProvider


class MacroSnapshotBuilder:
    """Collects low-frequency macro factors into a normalized snapshot."""

    def __init__(
        self,
        fmp_provider: FMPProvider | None = None,
        india_vix_provider: IndiaVixProvider | None = None,
        rbi_provider: RBIProvider | None = None,
        mospi_provider: MOSPIProvider | None = None,
        nsdl_provider: NSDLProvider | None = None,
        ccil_provider: CCILProvider | None = None,
    ):
        self.fmp_provider = fmp_provider or FMPProvider()
        self.india_vix_provider = india_vix_provider or IndiaVixProvider()
        self.rbi_provider = rbi_provider or RBIProvider()
        self.mospi_provider = mospi_provider or MOSPIProvider()
        self.nsdl_provider = nsdl_provider or NSDLProvider()
        self.ccil_provider = ccil_provider or CCILProvider()

    def build(self) -> MacroSnapshot:
        now = datetime.now(IST)
        factors: dict[str, MacroFactorInput] = {}
        events: list[MacroEvent] = []
        source_status: dict[str, dict] = {}

        fmp_status = SourceStatus(
            provider="fmp",
            enabled=bool(getattr(self.fmp_provider, "enabled", False)),
            configured=bool(self.fmp_provider.is_configured()),
        )
        try:
            usd_inr = self.fmp_provider.get_usd_inr()
            gold = self.fmp_provider.get_gold()
            crude = self.fmp_provider.get_crude()
            global_cues = self.fmp_provider.get_us_indices()
            calendar = self.fmp_provider.get_economic_calendar(
                from_date=now.date().isoformat(),
                to_date=(now.date() + timedelta(days=1)).isoformat(),
            )
            if usd_inr:
                factors["usd_inr"] = _factor_from_provider("usd_inr", usd_inr)
            if gold:
                factors["gold"] = _factor_from_provider("gold", gold)
            if crude:
                factors["crude"] = _factor_from_provider("crude", crude)
            if global_cues:
                factors["global_cues"] = _factor_from_provider("global_cues", global_cues)
            for event in calendar or []:
                macro_event = _event_from_provider(event)
                if macro_event:
                    events.append(macro_event)
            if any([usd_inr, gold, crude, global_cues, calendar]):
                fmp_status.mark_success(now, stale=False)
            else:
                fmp_status.mark_error("FMP returned no usable macro data.", using_fallback=True)
        except Exception as exc:
            fmp_status.mark_error(str(exc), using_fallback=True)
        source_status["fmp"] = fmp_status.to_dict()

        vix_status = SourceStatus(provider="india_vix", enabled=True, configured=True)
        try:
            india_vix = self.india_vix_provider.get_india_vix()
            if india_vix:
                factors["india_vix"] = _factor_from_provider("india_vix", india_vix)
                vix_status.mark_success(now, stale=bool(india_vix.get("stale")))
            else:
                vix_status.mark_error("India VIX was not available from the current provider.", using_fallback=True)
        except Exception as exc:
            vix_status.mark_error(str(exc), using_fallback=True)
        source_status["india_vix"] = vix_status.to_dict()

        for provider_name, provider in {
            "rbi": self.rbi_provider,
            "mospi": self.mospi_provider,
            "nsdl": self.nsdl_provider,
            "ccil": self.ccil_provider,
        }.items():
            source_status[provider_name] = SourceStatus(
                provider=provider_name,
                enabled=False,
                configured=False,
                last_error="Provider stub only. TODO: implement source integration.",
                using_fallback=True,
                stale=True,
            ).to_dict()

        return MacroSnapshot(
            market="INDIA",
            timestamp=now,
            factors=factors,
            events=events,
            source_status=source_status,
        )

    def build_mock_snapshot(self) -> MacroSnapshot:
        now = datetime.now(IST)
        return MacroSnapshot(
            market="INDIA",
            timestamp=now,
            factors={
                "usd_inr": MacroFactorInput(name="usd_inr", symbol="USDINR", value=83.15, change_pct_1d=0.15, source="mock"),
                "gold": MacroFactorInput(name="gold", symbol="GCUSD", value=2300.0, change_pct_1d=0.2, source="mock"),
                "crude": MacroFactorInput(name="crude", symbol="CLUSD", value=82.1, change_pct_1d=-0.4, source="mock"),
                "india_vix": MacroFactorInput(name="india_vix", symbol="INDIAVIX", value=14.2, change_pct_1d=-1.0, source="mock"),
                "global_cues": MacroFactorInput(name="global_cues", symbol="US_INDEX_BASKET", value=0.3, change_pct_1d=0.25, source="mock"),
            },
            events=[
                MacroEvent(
                    country="United States",
                    event="US CPI",
                    importance="medium",
                    event_time=now.replace(hour=18, minute=0, second=0, microsecond=0),
                    source="mock",
                )
            ],
            source_status={
                "mock": SourceStatus(provider="mock", enabled=True, configured=True, last_success_at=now).to_dict(),
            },
        )


def _factor_from_provider(name: str, payload: dict) -> MacroFactorInput:
    return MacroFactorInput(
        name=name,
        symbol=payload.get("symbol"),
        value=_safe_float(payload.get("value", payload.get("average_change_pct_1d"))),
        change_pct_1d=_safe_float(payload.get("change_pct_1d", payload.get("average_change_pct_1d"))),
        change_pct_5d=_safe_float(payload.get("change_pct_5d")),
        source=payload.get("source"),
        stale=bool(payload.get("stale", False)),
        raw=dict(payload),
    )


def _event_from_provider(payload: dict | None) -> MacroEvent | None:
    if not isinstance(payload, dict):
        return None
    event_name = str(payload.get("event") or "").strip()
    if not event_name:
        return None
    event_time = payload.get("event_time")
    if isinstance(event_time, str):
        try:
            event_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
        except ValueError:
            event_time = None
    return MacroEvent(
        country=str(payload.get("country") or ""),
        event=event_name,
        importance=str(payload.get("importance") or "low"),
        event_time=event_time,
        actual=_safe_float(payload.get("actual")),
        forecast=_safe_float(payload.get("forecast")),
        previous=_safe_float(payload.get("previous")),
        source=payload.get("source"),
    )


def _safe_float(value) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
