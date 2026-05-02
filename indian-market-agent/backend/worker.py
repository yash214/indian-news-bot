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
except ModuleNotFoundError:
    import app as market_app


STOP_EVENT = threading.Event()


def _handle_stop(_signum, _frame) -> None:
    STOP_EVENT.set()


def main() -> int:
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    market_app.initialize_runtime_state()
    workers = [
        ("market-desk-refresh", market_app.refresh_loop),
        ("market-desk-ticker", market_app.ticker_loop),
        ("market-desk-upstox-v3", market_app.upstox_stream_loop),
        ("market-desk-macro-context", market_app.macro_context_loop),
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
