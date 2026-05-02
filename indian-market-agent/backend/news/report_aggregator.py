"""Backward-compatible bridge to backend.agents.news.report_aggregator."""

try:
    from backend.agents.news.report_aggregator import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.report_aggregator import *  # noqa: F401,F403
