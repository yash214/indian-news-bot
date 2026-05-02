"""Ollama-backed local AI provider."""

from __future__ import annotations

import os

from .base import AiProviderConfigurationError, AiProviderError


OLLAMA_DEFAULT_API_BASE = "http://127.0.0.1:11434/api"
OLLAMA_DEFAULT_SUMMARY_MODEL = "qwen2.5:3b"


def _extract_ollama_response_text(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(payload.get("response") or "").strip()


class OllamaTextProvider:
    """Generate text using a locally running Ollama server."""

    name = "ollama"

    def __init__(self, *, http_session_factory) -> None:
        self.http_session_factory = http_session_factory

    @property
    def api_base(self) -> str:
        return os.environ.get("OLLAMA_API_BASE", OLLAMA_DEFAULT_API_BASE).strip().rstrip("/")

    @property
    def model(self) -> str:
        return os.environ.get("OLLAMA_NEWS_SUMMARY_MODEL", OLLAMA_DEFAULT_SUMMARY_MODEL).strip() or OLLAMA_DEFAULT_SUMMARY_MODEL

    @property
    def keep_alive(self) -> str:
        return os.environ.get("OLLAMA_SUMMARY_KEEP_ALIVE", "15m").strip()

    @property
    def timeout_seconds(self) -> float:
        return float(os.environ.get("OLLAMA_SUMMARY_TIMEOUT_SECONDS", "90"))

    def is_configured(self) -> bool:
        return bool(self.api_base and self.model)

    def generate_text(
        self,
        *,
        prompt: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool = False,
    ) -> str:
        if not self.is_configured():
            raise AiProviderConfigurationError("Ollama provider is missing OLLAMA_API_BASE or OLLAMA_NEWS_SUMMARY_MODEL")
        request_body = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": self.keep_alive,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
        }
        if json_mode:
            request_body["format"] = "json"
        try:
            response = self.http_session_factory().post(
                f"{self.api_base}/generate",
                headers={"Content-Type": "application/json"},
                json=request_body,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
            return _extract_ollama_response_text(response.json())
        except Exception as exc:
            raise AiProviderError(f"Ollama generation failed: {exc}") from exc
