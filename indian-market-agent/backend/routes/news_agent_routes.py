"""News Agent route registration."""

from __future__ import annotations

from flask import jsonify, request

try:
    from backend.shared.serialization import to_jsonable
except ModuleNotFoundError:
    from shared.serialization import to_jsonable


def register_news_agent_routes(app, context) -> None:
    @app.route("/api/news/agent/report")
    def api_news_agent_report():
        index = request.args.get("index", "NIFTY")
        try:
            lookback_hours = int(request.args.get("lookback_hours", "24") or 24)
        except (TypeError, ValueError):
            lookback_hours = 24
        lookback_hours = int(context.clamp(lookback_hours, 1, 168))
        analyses = context.load_recent_article_ai_analyses(lookback_hours=lookback_hours)
        report = context.NewsReportAggregator(analyses).build_report(index=index, lookback_hours=lookback_hours)
        try:
            context.save_index_news_report(report)
        except Exception as exc:
            print(f"[!] news agent report persist error: {exc}")
        return jsonify(to_jsonable(report))

    @app.route("/api/news/agent/articles")
    def api_news_agent_articles():
        try:
            lookback_hours = int(request.args.get("lookback_hours", "24") or 24)
        except (TypeError, ValueError):
            lookback_hours = 24
        lookback_hours = int(context.clamp(lookback_hours, 1, 168))
        analyses = context.load_recent_article_ai_analyses(lookback_hours=lookback_hours)
        return jsonify({
            "lookback_hours": lookback_hours,
            "count": len(analyses),
            "articles": to_jsonable(analyses),
        })
