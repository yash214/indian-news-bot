"""Market data, ticker, history, analytics, and SSE route registration."""

from __future__ import annotations

import json
import queue
import time

from flask import Response, jsonify, request, stream_with_context


def register_market_routes(app, context) -> None:
    @app.route("/api/tickers")
    def api_tickers():
        if context.external_worker_mode():
            runtime_payload = context.runtime_snapshot_from_db(include_history=False)
            if runtime_payload and isinstance(runtime_payload.get("ticks"), dict):
                return jsonify(runtime_payload["ticks"])
        with context._lock:
            return jsonify(context._ticks)

    @app.route("/api/snapshot")
    def api_snapshot():
        include_history = request.args.get("history", "0") in {"1", "true", "yes"}
        if context.external_worker_mode():
            runtime_payload = context.runtime_snapshot_from_db(include_history=include_history)
            if runtime_payload:
                return jsonify(runtime_payload)
        return jsonify(context.market_data_snapshot(include_history=include_history))

    @app.route("/api/symbols/search")
    def api_symbol_search():
        query = request.args.get("q", "")
        try:
            limit = min(max(int(request.args.get("limit", 10)), 1), 20)
        except Exception:
            limit = 10
        results, seen = [], set()
        for item in context.search_symbols(query, limit=limit * 2):
            symbol = item.get("symbol")
            if symbol and symbol not in seen:
                seen.add(symbol)
                results.append(item)
        if len(context._clean_market_symbol(query)) >= 2:
            for item in context.upstox_symbol_search_results(query, limit=limit):
                symbol = item.get("symbol")
                if symbol and symbol not in seen:
                    seen.add(symbol)
                    results.append(item)
        return jsonify({"results": results[:limit]})

    @app.route("/api/app-state", methods=["GET", "POST"])
    def api_app_state():
        if request.method == "POST":
            payload = request.get_json(silent=True) or {}
            state = context.update_app_state(payload)
            with context._lock:
                has_stored_state = context._has_persisted_state
            return jsonify({"state": state, "hasStoredState": has_stored_state})

        with context._lock:
            has_stored_state = context._has_persisted_state
        return jsonify({
            "state": context.get_app_state_copy(),
            "hasStoredState": has_stored_state,
        })

    @app.route("/api/quotes")
    def api_quotes():
        symbols = context.sanitize_symbol_list(request.args.get("symbols", ""))
        status = context.get_market_status()
        stale_after = context.nse_quote_cache_ttl(status) * 2
        with context._lock:
            cached_quotes = {sym: context._tracked_symbol_quotes.get(sym) for sym in symbols}
        refresh_symbols = [
            sym for sym, quote in cached_quotes.items()
            if quote is None or (context.quote_age_seconds(quote) is not None and context.quote_age_seconds(quote) > stale_after)
        ]
        fresh_quotes = context.refresh_quote_cache_for_symbols(refresh_symbols)
        if fresh_quotes:
            with context._lock:
                context._tracked_symbol_quotes.update(fresh_quotes)
            try:
                context.rebuild_computed_payloads()
                context.persist_runtime_snapshot_payload()
                context.broadcast_market_snapshot()
            except Exception:
                pass
        merged = {sym: fresh_quotes.get(sym) or cached_quotes.get(sym) for sym in symbols}
        out = context.format_quotes_for_client({sym: quote for sym, quote in merged.items() if quote}, status=status)
        return jsonify(out)

    @app.route("/api/history")
    def api_history():
        if context.external_worker_mode():
            runtime_payload = context.runtime_snapshot_from_db(include_history=True)
            if runtime_payload and isinstance(runtime_payload.get("history"), dict):
                return jsonify(runtime_payload["history"])
        with context._lock:
            return jsonify(context._price_history)

    @app.route("/api/analytics")
    def api_analytics():
        if context.external_worker_mode():
            runtime_payload = context.runtime_snapshot_from_db(include_history=False)
            if runtime_payload and isinstance(runtime_payload.get("analytics"), dict):
                return jsonify(runtime_payload["analytics"])
        with context._lock:
            payload = dict(context._analytics_payload)
        return jsonify(payload)

    @app.route("/api/tickers/stream")
    def api_tickers_stream():
        if context.external_worker_mode():
            def generate_from_runtime():
                last_payload = ""
                while True:
                    payload = context.runtime_snapshot_from_db(include_history=False) or context.market_data_snapshot(include_history=False)
                    encoded = json.dumps(payload, sort_keys=True)
                    if encoded != last_payload:
                        yield "data:" + encoded + "\n\n"
                        last_payload = encoded
                    else:
                        yield ": keepalive\n\n"
                    time.sleep(context.ticker_refresh_interval())

            return Response(
                stream_with_context(generate_from_runtime()),
                mimetype="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        q: queue.Queue[str] = queue.Queue(maxsize=20)
        with context._sse_lock:
            context._sse_queues.append(q)
        initial = context.market_data_snapshot(include_history=True)

        def generate():
            try:
                yield "data:" + json.dumps(initial) + "\n\n"
                while True:
                    try:
                        msg = q.get(timeout=30)
                        yield msg
                    except queue.Empty:
                        yield ": keepalive\n\n"
            finally:
                with context._sse_lock:
                    try:
                        context._sse_queues.remove(q)
                    except ValueError:
                        pass

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
