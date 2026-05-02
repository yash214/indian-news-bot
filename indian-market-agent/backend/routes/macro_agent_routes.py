"""Macro Context Agent route registration."""

from __future__ import annotations

from flask import jsonify, request


def register_macro_agent_routes(app, run_macro_context_cycle) -> None:
    @app.route("/api/agents/macro-context")
    def api_macro_context_report():
        force_refresh = _bool_arg(request.args.get("force_refresh"))
        use_mock = _bool_arg(request.args.get("mock"))
        report = run_macro_context_cycle(force_refresh=force_refresh, use_mock=use_mock)
        payload = report.to_dict() if hasattr(report, "to_dict") else report
        return jsonify(payload)


def _bool_arg(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
