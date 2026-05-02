"""Backward-compatible bridge to backend.agents.news.summaries."""

try:
    from backend.agents.news.summaries import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.summaries import *  # noqa: F401,F403
