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
    fetch_news: Callable | None = None
    get_news_refresh_seconds: Callable | None = None
    set_news_refresh_seconds: Callable | None = None
    runtime_news_payload_from_db: Callable | None = None
    persist_runtime_news_payload: Callable | None = None
    run_news_agent_report: Callable | None = None
    get_latest_news_agent_report: Callable | None = None

    generate_ai_chat_response: Callable | None = None
    ai_summary_progress_for_articles: Callable | None = None
    ai_summary_update_payload: Callable | None = None
    hydrate_article_from_ai_cache: Callable | None = None
    article_has_ai_summary: Callable | None = None

    build_live_only_signal: Callable | None = None
    build_symbol_signal: Callable | None = None
    build_market_analytics_payload: Callable | None = None
    build_derivatives_analysis_payload: Callable | None = None
    analytics_runtime_status: Callable | None = None

    get_tickers_snapshot: Callable | None = None
    get_price_history: Callable | None = None
    refresh_market_snapshot: Callable | None = None
    format_quote_for_client: Callable | None = None
    format_quotes_for_client: Callable | None = None
    refresh_quote_cache_for_symbols: Callable | None = None
    refresh_tracked_symbol_quotes: Callable | None = None
    rebuild_computed_payloads: Callable | None = None
    market_data_snapshot: Callable | None = None
    fetch_live_quote: Callable | None = None
    fetch_tickers: Callable | None = None
    broadcast_market_snapshot: Callable | None = None
    broadcast_tickers: Callable | None = None
    update_price_history: Callable | None = None
    runtime_snapshot_from_db: Callable | None = None
    persist_runtime_snapshot_payload: Callable | None = None
    market_runtime_status: Callable | None = None

    upstox_runtime_status: Callable | None = None
    upstox_search_instruments: Callable | None = None
    upstox_symbol_search_results: Callable | None = None
    resolve_upstox_instrument_key: Callable | None = None
    fetch_upstox_quote_batch: Callable | None = None
    fetch_upstox_quotes_by_label: Callable | None = None
    fetch_upstox_stream_quotes_by_label: Callable | None = None
    fetch_upstox_index_quotes: Callable | None = None
    fetch_upstox_option_chain: Callable | None = None
    upstox_stream_subscription_map: Callable | None = None
    upstox_stream_loop: Callable | None = None
    upstox_integration_status: Callable | None = None
    provider_status: Callable | None = None
    market_data_provider_status: Callable | None = None
    active_market_data_provider: Callable | None = None
    upstox_stream_runtime_status: Callable | None = None
    upstox_rest_runtime_status: Callable | None = None
    ticker_refresh_interval: Callable | None = None
    nse_quote_cache_ttl: Callable | None = None
    quote_age_seconds: Callable | None = None

    initialize_runtime_state: Callable | None = None
    start_background_workers: Callable | None = None
    background_threads_enabled: Callable | None = None
    external_worker_mode: Callable | None = None
    macro_background_thread_enabled: Callable | None = None
    refresh_loop: Callable | None = None
    ticker_loop: Callable | None = None
    macro_context_loop: Callable | None = None
    global_quote_loop: Callable | None = None
    background_runtime_status: Callable | None = None

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
