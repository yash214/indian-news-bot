"""Shared interfaces and errors for AI text providers."""

from __future__ import annotations

from typing import Protocol


class AiProviderError(RuntimeError):
    """Raised when an AI provider cannot complete a generation request."""


class AiProviderConfigurationError(AiProviderError):
    """Raised when a selected AI provider is missing required configuration."""


class AiTextProvider(Protocol):
    """Minimal interface used by the news AI enrichment service."""

    name: str

    @property
    def model(self) -> str:
        """Return the configured model name/id."""

    def is_configured(self) -> bool:
        """Return whether the provider has enough config to be called."""

    def generate_text(
        self,
        *,
        prompt: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool = False,
    ) -> str:
        """Generate text for the supplied prompt."""
