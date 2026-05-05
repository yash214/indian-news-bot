"""Background loop, startup, and worker runtime helpers."""

from __future__ import annotations

from collections.abc import Mapping
import os
import sys
import threading
import time
from typing import Any, Callable

try:
    from backend.services import news_runtime
except ModuleNotFoundError:
    from services import news_runtime


GLOBAL_QUOTE_REFRESH_SECONDS = max(2.0, float(os.environ.get("GLOBAL_QUOTE_REFRESH_SECONDS", "5.0") or "5.0"))

_background_threads_started = False
_background_threads_lock = threading.Lock()


def background_threads_enabled() -> bool:
    raw = os.environ.get("MARKET_DESK_DISABLE_THREADS", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return False
    return "unittest" not in sys.modules and "pytest" not in sys.modules


def external_worker_mode() -> bool:
    return os.environ.get("MARKET_DESK_DISABLE_THREADS", "").strip().lower() in {"1", "true", "yes", "on"}


def macro_background_thread_enabled() -> bool:
    return "unittest" not in sys.modules and "pytest" not in sys.modules


def initialize_runtime_state(context=None) -> None:
    load_holiday_calendar = _context_callable(context, "load_holiday_calendar")
    load_persisted_app_state = _context_callable(context, "load_persisted_app_state")
    load_refresh_settings = _context_callable(context, "load_refresh_settings")

    calendar = load_holiday_calendar() if load_holiday_calendar is not None else {}
    if load_persisted_app_state is not None:
        state, has_stored_state = load_persisted_app_state()
    else:
        state, has_stored_state = {}, False
    refresh_seconds = load_refresh_settings() if load_refresh_settings is not None else news_runtime.get_news_refresh_seconds()

    lock = _context_value(context, "_lock")
    if lock is not None:
        with lock:
            _set_context_value(context, "NSE_HOLIDAY_CALENDAR", calendar)
            _set_context_value(context, "_app_state", state)
            _set_context_value(context, "_has_persisted_state", has_stored_state)
            _set_context_value(context, "_news_refresh_seconds", refresh_seconds)
    else:
        _set_context_value(context, "NSE_HOLIDAY_CALENDAR", calendar)
        _set_context_value(context, "_app_state", state)
        _set_context_value(context, "_has_persisted_state", has_stored_state)
        _set_context_value(context, "_news_refresh_seconds", refresh_seconds)

    news_runtime.configure_news_runtime(
        refresh_seconds=refresh_seconds,
        refresh_wakeup=_context_value(context, "_refresh_wakeup"),
        ai_summary_progress_for_articles=_context_callable(context, "ai_summary_progress_for_articles"),
        enrich_articles_with_ai_summaries=_context_callable(context, "enrich_articles_with_ai_summaries"),
    )


def start_background_workers(context=None) -> bool:
    global _background_threads_started
    enabled = _context_callable(context, "background_threads_enabled") or background_threads_enabled
    macro_enabled = _context_callable(context, "macro_background_thread_enabled") or macro_background_thread_enabled
    if not enabled():
        return False
    with _background_threads_lock:
        if _background_threads_started:
            return False
        threading.Thread(target=_loop_target(context, "refresh_loop", refresh_loop), daemon=True, name="market-desk-refresh").start()
        threading.Thread(target=_loop_target(context, "ticker_loop", ticker_loop), daemon=True, name="market-desk-ticker").start()
        threading.Thread(target=_loop_target(context, "upstox_stream_loop", None), daemon=True, name="market-desk-upstox-v3").start()
        threading.Thread(target=_loop_target(context, "global_quote_loop", global_quote_loop), daemon=True, name="market-desk-global-quotes").start()
        if macro_enabled():
            threading.Thread(target=_loop_target(context, "macro_context_loop", macro_context_loop), daemon=True, name="market-desk-macro-context").start()
        _background_threads_started = True
        return True


def refresh_loop(context=None) -> None:
    nse_init = _context_callable(context, "_nse_init_session")
    if nse_init is not None:
        nse_init()
    refresh_wakeup = _context_value(context, "_refresh_wakeup")
    while True:
        try:
            print("[*] Refreshing news...")
            fetch_news = _required_context_callable(context, "fetch_news")
            articles, feed_status = fetch_news()
            refreshed_at = time.time()
            updated = _context_callable(context, "ist_now")().strftime("%H:%M:%S")
            lock = _context_value(context, "_lock")
            if lock is not None:
                with lock:
                    _set_context_value(context, "_arts", articles)
                    _set_context_value(context, "_feed_status", feed_status)
                    _set_context_value(context, "_updated", updated)
                    _set_context_value(context, "_last_news_refresh_ts", refreshed_at)
            else:
                _set_context_value(context, "_arts", articles)
                _set_context_value(context, "_feed_status", feed_status)
                _set_context_value(context, "_updated", updated)
                _set_context_value(context, "_last_news_refresh_ts", refreshed_at)
            _required_context_callable(context, "rebuild_computed_payloads")()
            _required_context_callable(context, "persist_runtime_news_payload")(articles, feed_status, updated, refreshed_at)
            _required_context_callable(context, "persist_runtime_snapshot_payload")()
            print(f"[+] {len(articles)} articles | {updated}")
        except Exception as exc:
            print(f"[!] refresh_loop error: {exc}")
        wait_seconds = _required_context_callable(context, "get_news_refresh_seconds")()
        if refresh_wakeup is not None:
            refresh_wakeup.wait(timeout=wait_seconds)
            refresh_wakeup.clear()
        else:
            time.sleep(wait_seconds)


def ticker_loop(context=None) -> None:
    while True:
        try:
            nse_init = _context_callable(context, "_nse_init_session")
            if nse_init is not None:
                nse_init()
            ticks, analytics_indices = _required_context_callable(context, "fetch_tickers")()
            refreshed_at = time.time()
            lock = _context_value(context, "_lock")
            if lock is not None:
                with lock:
                    _set_context_value(context, "_ticks", ticks)
                    _set_context_value(context, "_index_snapshot", analytics_indices)
                    _set_context_value(context, "_last_tick_refresh_ts", refreshed_at)
            else:
                _set_context_value(context, "_ticks", ticks)
                _set_context_value(context, "_index_snapshot", analytics_indices)
                _set_context_value(context, "_last_tick_refresh_ts", refreshed_at)
            _required_context_callable(context, "update_price_history")(ticks)
            _required_context_callable(context, "refresh_tracked_symbol_quotes")()
            _required_context_callable(context, "rebuild_computed_payloads")()
            _required_context_callable(context, "persist_runtime_snapshot_payload")()
            _required_context_callable(context, "broadcast_tickers")(ticks)
            print(f"[~] Tickers: {list(ticks.keys())}")
        except Exception as exc:
            print(f"[!] ticker_loop error: {exc}")
        time.sleep(_required_context_callable(context, "ticker_refresh_interval")())


def macro_context_loop(context=None) -> None:
    # FMP free-plan access is limited, so this loop only reacts to scheduled macro checkpoints.
    while True:
        try:
            now = _required_context_callable(context, "ist_now")()
            last_run_at = _context_value(context, "_last_macro_context_run_at")
            if _required_context_callable(context, "macro_refresh_due")(now, last_run_at):
                _required_context_callable(context, "run_macro_context_cycle")(
                    force_refresh=False,
                    use_mock=False,
                    context=context,
                )
                _set_context_value(context, "_last_macro_context_run_at", now)
                next_run = _required_context_callable(context, "next_macro_refresh_time")(now)
                print(f"[~] Macro context refreshed at {now.isoformat()} | next={next_run.isoformat() if next_run else 'n/a'}")
        except Exception as exc:
            print(f"[!] macro_context_loop error: {exc}")
        time.sleep(60)


def global_quote_loop(context=None) -> None:
    while True:
        try:
            # Tracked symbols are refreshed through the main NSE/Upstox ticker
            # flow instead of a separate global quote provider loop.
            pass
        except Exception as exc:
            print(f"[!] global_quote_loop error: {exc}")
        time.sleep(_context_value(context, "GLOBAL_QUOTE_REFRESH_SECONDS", GLOBAL_QUOTE_REFRESH_SECONDS))


def background_runtime_status(context=None) -> dict:
    return {
        "threadsEnabled": background_threads_enabled(),
        "externalWorkerMode": external_worker_mode(),
        "macroBackgroundThreadEnabled": macro_background_thread_enabled(),
        "started": _background_threads_started,
        "readOnly": True,
    }


def _loop_target(context, name: str, fallback: Callable | None):
    candidate = _context_callable(context, name)
    if candidate is not None:
        return candidate
    if fallback is None:
        return lambda: None
    return lambda: fallback(context=context)


def _required_context_callable(context, name: str):
    candidate = _context_callable(context, name)
    if candidate is None:
        raise RuntimeError(f"Runtime context missing callable: {name}")
    return candidate


def _context_callable(context: Any, name: str):
    value = _context_value(context, name)
    return value if callable(value) else None


def _context_value(context: Any, name: str, default=None):
    if context is None:
        return default
    try:
        value = getattr(context, name)
        return default if value is None else value
    except AttributeError:
        runtime_state = getattr(context, "runtime_state", None)
        if isinstance(runtime_state, Mapping):
            return runtime_state.get(name, default)
        return default


def _set_context_value(context: Any, name: str, value) -> bool:
    if context is None:
        return False
    runtime_state = getattr(context, "runtime_state", None)
    if isinstance(runtime_state, Mapping):
        runtime_state[name] = value
        return True
    try:
        setattr(context, name, value)
        return True
    except Exception:
        return False
