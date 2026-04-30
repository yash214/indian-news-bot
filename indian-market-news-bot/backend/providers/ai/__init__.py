"""Factory for text-generation providers used by StockTerminal."""

from __future__ import annotations

import os

from .base import AiProviderConfigurationError, AiProviderError, AiTextProvider
from .bedrock import (
    BEDROCK_DEFAULT_MANTLE_MODEL_ID,
    BEDROCK_DEFAULT_MODEL_ID,
    BedrockConverseTextProvider,
    BedrockResponsesApiTextProvider,
)
from .ollama import OLLAMA_DEFAULT_SUMMARY_MODEL, OllamaTextProvider


def ai_provider_name_from_env() -> str:
    raw = os.environ.get("AI_PROVIDER", "ollama").strip().lower()
    aliases = {
        "aws": "bedrock",
        "aws-bedrock": "bedrock",
        "bedrock-converse": "bedrock",
        "bedrock_native": "bedrock",
        "bedrock-api": "bedrock-api-key",
        "bedrock-api-key": "bedrock-api-key",
        "bedrock-openai": "bedrock-api-key",
        "bedrock-responses": "bedrock-api-key",
        "local": "ollama",
    }
    return aliases.get(raw, raw or "ollama")


def ai_model_name_from_env(provider_name: str | None = None) -> str:
    provider_name = provider_name or ai_provider_name_from_env()
    if provider_name == "bedrock-api-key":
        configured = (
            os.environ.get("BEDROCK_MANTLE_MODEL_ID")
            or os.environ.get("OPENAI_MODEL")
            or os.environ.get("BEDROCK_MODEL_ID")
            or BEDROCK_DEFAULT_MANTLE_MODEL_ID
        ).strip()
        if configured == BEDROCK_DEFAULT_MODEL_ID:
            return BEDROCK_DEFAULT_MANTLE_MODEL_ID
        return configured or BEDROCK_DEFAULT_MANTLE_MODEL_ID
    if provider_name.startswith("bedrock"):
        return os.environ.get("BEDROCK_MODEL_ID", BEDROCK_DEFAULT_MODEL_ID).strip() or BEDROCK_DEFAULT_MODEL_ID
    return os.environ.get("OLLAMA_NEWS_SUMMARY_MODEL", OLLAMA_DEFAULT_SUMMARY_MODEL).strip() or OLLAMA_DEFAULT_SUMMARY_MODEL


def create_ai_text_provider(*, http_session_factory, provider_name: str | None = None) -> AiTextProvider:
    selected = provider_name or ai_provider_name_from_env()
    if selected == "ollama":
        return OllamaTextProvider(http_session_factory=http_session_factory)
    if selected == "bedrock":
        return BedrockConverseTextProvider()
    if selected == "bedrock-api-key":
        return BedrockResponsesApiTextProvider(http_session_factory=http_session_factory)
    raise AiProviderConfigurationError(
        f"Unsupported AI_PROVIDER={selected!r}. Use 'ollama', 'bedrock', or 'bedrock-api-key'."
    )


__all__ = [
    "AiProviderConfigurationError",
    "AiProviderError",
    "AiTextProvider",
    "ai_model_name_from_env",
    "ai_provider_name_from_env",
    "create_ai_text_provider",
]
