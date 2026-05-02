"""Backward-compatible bridge to backend.agents.news.article_extract."""

try:
    from backend.agents.news.article_extract import *  # noqa: F401,F403
except ModuleNotFoundError:
    from agents.news.article_extract import *  # noqa: F401,F403
