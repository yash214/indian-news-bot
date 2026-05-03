"""Frontend route registration."""

from __future__ import annotations


def register_frontend_routes(app, context) -> None:
    @app.route("/")
    def index():
        return app.send_static_file("index.html")
