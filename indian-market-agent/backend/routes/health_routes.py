"""Health and runtime status route registration."""

from __future__ import annotations

from flask import jsonify


def register_health_routes(app, context) -> None:
    @app.route("/api/health")
    def api_health():
        market_status = context.get_market_status()
        with context._lock:
            news_count = len(context._arts)
            ticker_count = len(context._ticks)
            analytics_ready = bool(context._analytics_payload.get("generatedAt"))
            derivatives_ready = bool(context._derivatives_payload.get("generatedAt"))
        status = "ok" if not market_status["staleData"] else "degraded"
        return jsonify({
            "status": status,
            "dataProvider": context.market_data_provider_status(),
            "upstox": context.upstox_integration_status(),
            "marketStatus": market_status,
            "newsCount": news_count,
            "tickerCount": ticker_count,
            "analyticsReady": analytics_ready,
            "derivativesReady": derivatives_ready,
            "refreshInterval": context.get_news_refresh_seconds(),
        })
