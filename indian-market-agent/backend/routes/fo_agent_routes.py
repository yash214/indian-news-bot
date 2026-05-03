"""F&O Structure Agent route registration."""

from __future__ import annotations

from flask import jsonify, request

try:
    from backend.agents.fo_structure.agent import FOStructureAgent
    from backend.agents.fo_structure.schemas import normalize_fo_symbol
    from backend.agents.fo_structure.snapshot_builder import build_mock_option_chain_snapshot
    from backend.shared.serialization import to_jsonable
except ModuleNotFoundError:
    from agents.fo_structure.agent import FOStructureAgent
    from agents.fo_structure.schemas import normalize_fo_symbol
    from agents.fo_structure.snapshot_builder import build_mock_option_chain_snapshot
    from shared.serialization import to_jsonable


def register_fo_agent_routes(app, context) -> None:
    @app.route("/api/agents/fo-structure")
    def api_fo_structure_report():
        symbol = normalize_fo_symbol(request.args.get("symbol", "NIFTY"))
        expiry = request.args.get("expiry") or None
        use_mock = _bool_arg(request.args.get("mock"))
        if use_mock:
            if symbol not in {"NIFTY", "SENSEX"}:
                report = FOStructureAgent().analyze(None, symbol=symbol)
            else:
                report = FOStructureAgent().analyze(build_mock_option_chain_snapshot(symbol=symbol), symbol=symbol)
        else:
            report = context.run_fo_structure_cycle(symbol=symbol, expiry=expiry)
        return jsonify(to_jsonable(report))


def _bool_arg(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
