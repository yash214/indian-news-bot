"""Backward-compatible bridge to backend.agents.news.schemas."""

try:
    from backend.agents.news.schemas import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.schemas import *  # noqa: F401,F403
