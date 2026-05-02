"""Backward-compatible bridge to backend.agents.news.scoring."""

try:
    from backend.agents.news.scoring import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.scoring import *  # noqa: F401,F403
