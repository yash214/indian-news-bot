"""Runtime context passed from the Flask entrypoint into route modules."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class AppRuntimeContext:
    # Keep this flexible. It is used to pass runtime functions/state into route modules.
    current_india_vix_quote: Callable | None = None

    build_macro_snapshot: Callable | None = None
    run_macro_context_cycle: Callable | None = None
    get_latest_macro_context_report: Callable | None = None

    build_fo_snapshot: Callable | None = None
    run_fo_structure_cycle: Callable | None = None
    get_latest_fo_structure_report: Callable | None = None

    build_market_regime_snapshot: Callable | None = None
    run_market_regime_cycle: Callable | None = None
    get_latest_market_regime_report: Callable | None = None

    refresh_news_now: Callable | None = None
    get_latest_news_items: Callable | None = None
    run_news_agent_report: Callable | None = None
    get_latest_news_agent_report: Callable | None = None

    get_tickers_snapshot: Callable | None = None
    get_price_history: Callable | None = None
    refresh_market_snapshot: Callable | None = None

    upstox_runtime_status: Callable | None = None
    provider_status: Callable | None = None

    runtime_state: Any | None = None

    def __getattr__(self, name: str) -> Any:
        if self.runtime_state is None:
            raise AttributeError(name)
        if isinstance(self.runtime_state, Mapping):
            try:
                return self.runtime_state[name]
            except KeyError as exc:
                raise AttributeError(name) from exc
        return getattr(self.runtime_state, name)


def build_runtime_context(**kwargs) -> AppRuntimeContext:
    return AppRuntimeContext(**kwargs)
