"""Chart and trading-workspace API route registration."""

from __future__ import annotations

from flask import jsonify, request


def register_chart_routes(app, context=None) -> None:
    @app.route("/api/chart/candles")
    def api_chart_candles():
        symbol = request.args.get("symbol", "NIFTY")
        interval = request.args.get("interval", "5m")
        range_ = request.args.get("range", "1d")
        use_mock = _bool_arg(request.args.get("mock"))
        payload = context.get_chart_candles(
            symbol=symbol,
            interval=interval,
            range_=range_,
            use_mock=use_mock,
            context=context,
        )
        return jsonify(payload)

    @app.route("/api/chart/overlays")
    def api_chart_overlays():
        symbol = request.args.get("symbol", "NIFTY")
        interval = request.args.get("interval", "5m")
        use_mock = _bool_arg(request.args.get("mock"))
        payload = context.get_chart_overlays(
            symbol=symbol,
            interval=interval,
            use_mock=use_mock,
            context=context,
        )
        return jsonify(payload)

    @app.route("/api/workspace/summary")
    def api_workspace_summary():
        symbol = request.args.get("symbol", "NIFTY")
        use_mock = _bool_arg(request.args.get("mock"))
        payload = context.get_workspace_summary(symbol=symbol, use_mock=use_mock, context=context)
        return jsonify(payload)


def _bool_arg(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
