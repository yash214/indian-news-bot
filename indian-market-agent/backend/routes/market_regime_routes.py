"""Market Regime Agent route registration."""

from __future__ import annotations

from flask import jsonify, request

try:
    from backend.agents.market_regime.schemas import normalize_market_symbol
    from backend.core.settings import MARKET_REGIME_TIMEFRAME_MINUTES
    from backend.shared.serialization import to_jsonable
except ModuleNotFoundError:
    from agents.market_regime.schemas import normalize_market_symbol
    from core.settings import MARKET_REGIME_TIMEFRAME_MINUTES
    from shared.serialization import to_jsonable


def register_market_regime_routes(app, context) -> None:
    @app.route("/api/agents/market-regime")
    def api_market_regime_report():
        symbol = normalize_market_symbol(request.args.get("symbol", "NIFTY"))
        timeframe = _int_arg(request.args.get("timeframe"), MARKET_REGIME_TIMEFRAME_MINUTES)
        use_mock = _bool_arg(request.args.get("mock"))
        regime_hint = request.args.get("regime_hint") or None
        report = context.run_market_regime_cycle(
            symbol=symbol,
            timeframe_minutes=timeframe,
            use_mock=use_mock,
            context=context,
            regime_hint=regime_hint,
        )
        return jsonify(to_jsonable(report))


def _bool_arg(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _int_arg(value, default: int) -> int:
    try:
        number = int(value)
        return number if number > 0 else default
    except (TypeError, ValueError):
        return default
