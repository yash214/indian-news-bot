#!/usr/bin/env python3
"""Background worker entrypoint for production deployments.

The Flask app can still run all loops in local development. In production,
set MARKET_DESK_DISABLE_THREADS=1 for the web process and run this worker as a
separate service so slow polling/AI work cannot block HTTP routes.
"""

from __future__ import annotations

import os
import signal
import threading
import time

os.environ.setdefault("MARKET_DESK_DISABLE_THREADS", "1")

try:
    from backend import app as market_app
    from backend.services import background_runtime
except ModuleNotFoundError:
    import app as market_app
    from services import background_runtime


STOP_EVENT = threading.Event()


def _handle_stop(_signum, _frame) -> None:
    STOP_EVENT.set()


def main() -> int:
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    context = getattr(market_app, "runtime_context", None)
    background_runtime.initialize_runtime_state(context=context)
    upstox_stream_loop = getattr(context, "upstox_stream_loop", None) if context is not None else None
    if not callable(upstox_stream_loop):
        upstox_stream_loop = market_app.upstox_stream_loop
    workers = [
        ("market-desk-refresh", lambda: background_runtime.refresh_loop(context=context)),
        ("market-desk-ticker", lambda: background_runtime.ticker_loop(context=context)),
        ("market-desk-upstox-v3", upstox_stream_loop),
        ("market-desk-macro-context", lambda: background_runtime.macro_context_loop(context=context)),
    ]
    for name, target in workers:
        threading.Thread(target=target, daemon=True, name=name).start()

    print("India Market Desk worker started: refresh, ticker, Upstox V3, macro context, AI queue")
    while not STOP_EVENT.wait(timeout=30):
        pass
    print("India Market Desk worker stopping")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
