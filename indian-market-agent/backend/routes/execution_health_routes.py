"""Execution Health Agent route registration."""

from __future__ import annotations

from flask import jsonify, request

try:
    from backend.agents.execution_health import ExecutionHealthAgent
    from backend.shared.serialization import to_jsonable
except ModuleNotFoundError:
    from agents.execution_health import ExecutionHealthAgent
    from shared.serialization import to_jsonable


def register_execution_health_routes(app, context=None) -> None:
    @app.route("/api/agents/execution-health")
    def api_execution_health_report():
        use_mock = _bool_arg(request.args.get("mock"))
        scenario = request.args.get("scenario") or None
        try:
            report = context.run_execution_health_cycle(use_mock=use_mock, context=context, scenario=scenario)
        except Exception:
            report = ExecutionHealthAgent().analyze(None)
        return jsonify(to_jsonable(report))


def _bool_arg(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
