"""Backward-compatible bridge to backend.agents.news.report_store."""

try:
    from backend.agents.news.report_store import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.report_store import *  # noqa: F401,F403
