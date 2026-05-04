"""Macro Context Agent route registration."""

from __future__ import annotations

from flask import jsonify, request

try:
    from backend.shared.serialization import to_jsonable
except ModuleNotFoundError:
    from shared.serialization import to_jsonable


def register_macro_agent_routes(app, context) -> None:
    @app.route("/api/agents/macro-context")
    def api_macro_context_report():
        force_refresh = _bool_arg(request.args.get("force_refresh"))
        use_mock = _bool_arg(request.args.get("mock"))
        report = context.run_macro_context_cycle(force_refresh=force_refresh, use_mock=use_mock, context=context)
        return jsonify(to_jsonable(report))


def _bool_arg(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
