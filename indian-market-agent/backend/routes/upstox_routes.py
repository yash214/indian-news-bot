"""Upstox integration route registration."""

from __future__ import annotations

from flask import jsonify


def register_upstox_routes(app, context) -> None:
    @app.route("/api/integrations/upstox/status")
    def api_upstox_status():
        return jsonify(context.upstox_integration_status())
