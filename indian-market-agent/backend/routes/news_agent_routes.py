"""News Agent route registration."""

from __future__ import annotations

from flask import jsonify, request

try:
    from backend.core.settings import NEWS_AGENT_LOOKBACK_HOURS
    from backend.shared.serialization import to_jsonable
except ModuleNotFoundError:
    from core.settings import NEWS_AGENT_LOOKBACK_HOURS
    from shared.serialization import to_jsonable


def _clamp_lookback_hours(context, value: int) -> int:
    clamp = getattr(context, "clamp", None)
    if callable(clamp):
        return int(clamp(value, 1, 168))
    return max(1, min(168, int(value)))


def _request_lookback_hours(context) -> int:
    raw = request.args.get("lookback_hours")
    try:
        lookback_hours = int(raw) if raw not in (None, "") else NEWS_AGENT_LOOKBACK_HOURS
    except (TypeError, ValueError):
        lookback_hours = NEWS_AGENT_LOOKBACK_HOURS
    return _clamp_lookback_hours(context, lookback_hours)


def register_news_agent_routes(app, context) -> None:
    @app.route("/api/news/agent/report")
    def api_news_agent_report():
        index = request.args.get("index", "NIFTY")
        lookback_hours = _request_lookback_hours(context)
        analyses = context.load_recent_article_ai_analyses(lookback_hours=lookback_hours)
        report = context.NewsReportAggregator(analyses).build_report(index=index, lookback_hours=lookback_hours)
        try:
            context.save_index_news_report(report)
        except Exception as exc:
            print(f"[!] news agent report persist error: {exc}")
        return jsonify(to_jsonable(report))

    @app.route("/api/news/agent/articles")
    def api_news_agent_articles():
        lookback_hours = _request_lookback_hours(context)
        analyses = context.load_recent_article_ai_analyses(lookback_hours=lookback_hours)
        return jsonify({
            "lookback_hours": lookback_hours,
            "count": len(analyses),
            "articles": to_jsonable(analyses),
        })
