"""Backward-compatible bridge to backend.agents.news.sources."""

try:
    from backend.agents.news.sources import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.sources import *  # noqa: F401,F403
