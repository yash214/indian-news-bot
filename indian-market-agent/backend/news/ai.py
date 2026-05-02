"""Backward-compatible bridge to backend.agents.news.ai."""

try:
    from backend.agents.news.ai import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.ai import *  # noqa: F401,F403
