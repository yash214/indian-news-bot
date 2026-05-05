"""Dashboard news and AI chat route registration."""

from __future__ import annotations

from flask import jsonify, request


def register_news_routes(app, context) -> None:
    @app.route("/api/news")
    def api_news():
        if context.external_worker_mode():
            runtime_payload = context.runtime_news_payload_from_db()
            if runtime_payload:
                articles = runtime_payload.get("articles") if isinstance(runtime_payload.get("articles"), list) else []
                for article in articles:
                    context.hydrate_article_from_ai_cache(article)
                payload = {
                    "articles": articles,
                    "updated": runtime_payload.get("updated") or "",
                    "feedStatus": runtime_payload.get("feedStatus") if isinstance(runtime_payload.get("feedStatus"), dict) else {},
                    "refreshInterval": context.get_news_refresh_seconds(),
                    "allowedRefreshWindows": context.ALLOWED_REFRESH_WINDOWS,
                    "marketStatus": context.get_market_status(),
                    "aiSummaryProgress": runtime_payload.get("aiSummaryProgress") or context.ai_summary_progress_for_articles(articles),
                }
                return jsonify(payload)
        with context._lock:
            articles = list(context._arts)
            payload = {
                "articles": articles,
                "updated": context._updated,
                "feedStatus": dict(context._feed_status),
                "refreshInterval": context.get_news_refresh_seconds(),
                "allowedRefreshWindows": context.ALLOWED_REFRESH_WINDOWS,
            }
        payload["marketStatus"] = context.get_market_status()
        payload["aiSummaryProgress"] = context.ai_summary_progress_for_articles(articles)
        return jsonify(payload)

    @app.route("/api/news/ai-summaries")
    def api_news_ai_summaries():
        if context.external_worker_mode():
            runtime_payload = context.runtime_news_payload_from_db() or {}
            articles = runtime_payload.get("articles") if isinstance(runtime_payload.get("articles"), list) else []
            for article in articles:
                context.hydrate_article_from_ai_cache(article)
            updates = [context.ai_summary_update_payload(article) for article in articles if context.article_has_ai_summary(article)]
            return jsonify({
                "updates": updates,
                "progress": runtime_payload.get("aiSummaryProgress") or context.ai_summary_progress_for_articles(articles),
                "updated": runtime_payload.get("updated") or "",
            })
        with context._lock:
            articles = list(context._arts)
            updated = context._updated
        updates = [context.ai_summary_update_payload(article) for article in articles if context.article_has_ai_summary(article)]
        return jsonify({
            "updates": updates,
            "progress": context.ai_summary_progress_for_articles(articles),
            "updated": updated,
        })

    @app.route("/api/ai-chat", methods=["POST"])
    def api_ai_chat():
        payload = request.get_json(silent=True) or {}
        question = context._trim_text(payload.get("message"), 900)
        history = payload.get("history") if isinstance(payload.get("history"), list) else []
        if not question:
            return jsonify({"error": "Ask a market question first."}), 400
        try:
            answer, provider_name, model_name = context.generate_ai_chat_response(question, history)
        except context.AiProviderConfigurationError as exc:
            return jsonify({"error": str(exc), "provider": context.ai_chat_provider_name()}), 400
        except Exception as exc:
            return jsonify({"error": f"AI chat failed: {exc}", "provider": context.ai_chat_provider_name()}), 502
        return jsonify({
            "answer": answer,
            "provider": provider_name,
            "model": model_name,
            "generatedAt": context.ist_now().isoformat(),
        })

    @app.route("/api/settings/refresh", methods=["GET", "POST"])
    def api_settings_refresh():
        if request.method == "POST":
            data = request.get_json(silent=True) or {}
            seconds = int(data.get("seconds", 0) or 0)
            try:
                current = context.set_news_refresh_seconds(seconds)
            except ValueError:
                return jsonify({"error": "Unsupported refresh interval", "allowed": context.ALLOWED_REFRESH_WINDOWS}), 400
            return jsonify({"refreshInterval": current, "allowedRefreshWindows": context.ALLOWED_REFRESH_WINDOWS})

        return jsonify({
            "refreshInterval": context.get_news_refresh_seconds(),
            "allowedRefreshWindows": context.ALLOWED_REFRESH_WINDOWS,
        })
