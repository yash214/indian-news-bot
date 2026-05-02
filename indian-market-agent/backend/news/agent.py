"""Backward-compatible bridge to backend.agents.news.agent."""

try:
    from backend.agents.news.agent import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.agent import *  # noqa: F401,F403
