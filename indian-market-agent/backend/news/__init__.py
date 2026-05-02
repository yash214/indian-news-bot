"""Backward-compatible bridge to backend.agents.news."""

try:
    from backend.agents.news import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news import *  # noqa: F401,F403
