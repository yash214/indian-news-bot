"""Derivatives and option-chain route registration."""

from __future__ import annotations

import os

import requests
from flask import jsonify, request


def register_derivatives_routes(app, context) -> None:
    @app.route("/api/derivatives/overview")
    def api_derivatives_overview():
        if context.external_worker_mode():
            runtime_payload = context.runtime_snapshot_from_db(include_history=False)
            if runtime_payload and isinstance(runtime_payload.get("derivatives"), dict):
                return jsonify(runtime_payload["derivatives"])
        with context._lock:
            payload = dict(context._derivatives_payload)
        return jsonify(payload)

    @app.route("/api/derivatives/option-chain")
    def api_derivatives_option_chain():
        underlying = request.args.get("underlying", "NIFTY")
        expiry = request.args.get("expiry") or os.environ.get("UPSTOX_OPTION_EXPIRY", "")
        max_rows = int(request.args.get("maxRows", "80") or 80)
        try:
            payload = context.fetch_upstox_option_chain(
                underlying=underlying,
                expiry_date=expiry,
                max_rows=max(10, min(max_rows, 200)),
            )
        except ValueError as exc:
            return jsonify({"error": str(exc), "provider": "Upstox"}), 400
        except RuntimeError as exc:
            return jsonify({"error": str(exc), "provider": "Upstox"}), 400
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else 502
            return jsonify({"error": "Upstox option-chain request failed", "provider": "Upstox"}), status_code
        except Exception as exc:
            return jsonify({"error": str(exc), "provider": "Upstox"}), 502
        return jsonify(payload)
