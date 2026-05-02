"""Backward-compatible bridge to backend.agents.news.url_resolver."""

try:
    from backend.agents.news.url_resolver import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.url_resolver import *  # noqa: F401,F403
