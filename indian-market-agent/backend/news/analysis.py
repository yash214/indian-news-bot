"""Backward-compatible bridge to backend.agents.news.analysis."""

try:
    from backend.agents.news.analysis import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.analysis import *  # noqa: F401,F403
