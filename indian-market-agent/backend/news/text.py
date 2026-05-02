"""Backward-compatible bridge to backend.agents.news.text."""

try:
    from backend.agents.news.text import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.text import *  # noqa: F401,F403
