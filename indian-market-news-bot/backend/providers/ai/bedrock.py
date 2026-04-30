"""Amazon Bedrock AI providers."""

from __future__ import annotations

import os
import json

from .base import AiProviderConfigurationError, AiProviderError


BEDROCK_DEFAULT_REGION = "ap-south-1"
BEDROCK_DEFAULT_MODEL_ID = "qwen.qwen3-next-80b-a3b"
BEDROCK_DEFAULT_MANTLE_MODEL_ID = "qwen.qwen3-next-80b-a3b-instruct"


def _bedrock_model_id() -> str:
    return os.environ.get("BEDROCK_MODEL_ID", BEDROCK_DEFAULT_MODEL_ID).strip() or BEDROCK_DEFAULT_MODEL_ID


def _bedrock_mantle_model_id() -> str:
    configured = (
        os.environ.get("BEDROCK_MANTLE_MODEL_ID")
        or os.environ.get("OPENAI_MODEL")
        or os.environ.get("BEDROCK_MODEL_ID")
        or BEDROCK_DEFAULT_MANTLE_MODEL_ID
    ).strip()
    if configured == BEDROCK_DEFAULT_MODEL_ID:
        return BEDROCK_DEFAULT_MANTLE_MODEL_ID
    return configured or BEDROCK_DEFAULT_MANTLE_MODEL_ID


def _bedrock_region() -> str:
    return os.environ.get("BEDROCK_REGION", os.environ.get("AWS_DEFAULT_REGION", BEDROCK_DEFAULT_REGION)).strip() or BEDROCK_DEFAULT_REGION


def _extract_converse_text(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    message = ((payload.get("output") or {}).get("message") or {})
    content = message.get("content") if isinstance(message, dict) else []
    parts = []
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("text"):
                parts.append(str(block.get("text") or ""))
    return "\n".join(part.strip() for part in parts if part.strip()).strip()


def _extract_responses_api_text(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    direct = str(payload.get("output_text") or "").strip()
    if direct:
        return direct
    parts = []
    output = payload.get("output") if isinstance(payload.get("output"), list) else []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content") if isinstance(item.get("content"), list) else []
        for block in content:
            if not isinstance(block, dict):
                continue
            text = block.get("text") or block.get("output_text")
            if text:
                parts.append(str(text))
    return "\n".join(part.strip() for part in parts if part.strip()).strip()


def _extract_chat_completion_text(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices") if isinstance(payload.get("choices"), list) else []
    parts = []
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            parts.append(content.strip())
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("text"):
                    parts.append(str(block.get("text") or "").strip())
                elif isinstance(block, dict) and block.get("type") in {"text", "output_text"} and block.get("content"):
                    parts.append(str(block.get("content") or "").strip())
        text = choice.get("text")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
        delta = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
        delta_content = delta.get("content")
        if isinstance(delta_content, str) and delta_content.strip():
            parts.append(delta_content.strip())
    return "\n".join(part for part in parts if part).strip()


def _raise_for_bad_response(response, label: str) -> None:
    if getattr(response, "ok", False):
        return
    status = getattr(response, "status_code", "unknown")
    try:
        body = str(response.text or "")
    except Exception:
        body = ""
    body = body.replace("\n", " ").replace("\r", " ").strip()
    if len(body) > 500:
        body = body[:500].rstrip() + "..."
    raise AiProviderError(f"{label} returned HTTP {status}: {body or 'no response body'}")


def _payload_preview(payload: dict, max_chars: int = 900) -> str:
    try:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = str(payload)
    text = text.replace("\n", " ").replace("\r", " ").strip()
    if len(text) > max_chars:
        text = text[:max_chars].rstrip() + "..."
    return text


class BedrockConverseTextProvider:
    """Generate text through the native Bedrock Runtime Converse API."""

    name = "bedrock"

    def __init__(self) -> None:
        self._client = None

    @property
    def region(self) -> str:
        return _bedrock_region()

    @property
    def model(self) -> str:
        return _bedrock_model_id()

    @property
    def timeout_seconds(self) -> float:
        return float(os.environ.get("BEDROCK_TIMEOUT_SECONDS", os.environ.get("OLLAMA_SUMMARY_TIMEOUT_SECONDS", "120")))

    def is_configured(self) -> bool:
        return bool(self.region and self.model)

    def _runtime_client(self):
        if self._client is not None:
            return self._client
        try:
            import boto3
            from botocore.config import Config
        except Exception as exc:
            raise AiProviderConfigurationError("Install boto3 to use AI_PROVIDER=bedrock") from exc

        profile = os.environ.get("BEDROCK_PROFILE_NAME", os.environ.get("AWS_PROFILE", "")).strip()
        session_kwargs = {"region_name": self.region}
        if profile:
            session_kwargs["profile_name"] = profile
        session = boto3.Session(**session_kwargs)
        config = Config(
            connect_timeout=10,
            read_timeout=self.timeout_seconds,
            retries={"max_attempts": 2, "mode": "standard"},
        )
        self._client = session.client("bedrock-runtime", region_name=self.region, config=config)
        return self._client

    def generate_text(
        self,
        *,
        prompt: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool = False,
    ) -> str:
        if not self.is_configured():
            raise AiProviderConfigurationError("Bedrock provider is missing BEDROCK_REGION or BEDROCK_MODEL_ID")
        request = {
            "modelId": self.model,
            "messages": [{"role": "user", "content": [{"text": prompt}]}],
            "inferenceConfig": {
                "temperature": float(temperature),
                "maxTokens": int(max_tokens),
            },
        }
        if json_mode:
            request["system"] = [{"text": "Return valid JSON only. Do not include markdown, code fences, or commentary."}]
        try:
            response = self._runtime_client().converse(**request)
            return _extract_converse_text(response)
        except Exception as exc:
            raise AiProviderError(f"Bedrock Converse generation failed: {exc}") from exc


class BedrockResponsesApiTextProvider:
    """Generate text through Bedrock's OpenAI-compatible API-key path."""

    name = "bedrock-api-key"

    def __init__(self, *, http_session_factory) -> None:
        self.http_session_factory = http_session_factory

    @property
    def region(self) -> str:
        return _bedrock_region()

    @property
    def api_base(self) -> str:
        configured = (
            os.environ.get("BEDROCK_OPENAI_BASE_URL")
            or os.environ.get("BEDROCK_RESPONSES_BASE_URL")
            or os.environ.get("OPENAI_BASE_URL")
            or ""
        ).strip()
        return configured.rstrip("/") or f"https://bedrock-mantle.{self.region}.api.aws/v1"

    @property
    def api_key(self) -> str:
        return (os.environ.get("BEDROCK_API_KEY") or os.environ.get("OPENAI_API_KEY") or "").strip()

    @property
    def model(self) -> str:
        return _bedrock_mantle_model_id()

    @property
    def timeout_seconds(self) -> float:
        return float(os.environ.get("BEDROCK_TIMEOUT_SECONDS", os.environ.get("OLLAMA_SUMMARY_TIMEOUT_SECONDS", "120")))

    @property
    def api_mode(self) -> str:
        raw = os.environ.get("BEDROCK_OPENAI_API", "chat_completions").strip().lower().replace("-", "_")
        if raw in {"responses", "response"}:
            return "responses"
        return "chat_completions"

    def is_configured(self) -> bool:
        return bool(self.api_base and self.api_key and self.model)

    def generate_text(
        self,
        *,
        prompt: str,
        temperature: float,
        max_tokens: int,
        json_mode: bool = False,
    ) -> str:
        if not self.is_configured():
            raise AiProviderConfigurationError(
                "Bedrock API-key provider needs BEDROCK_API_KEY or OPENAI_API_KEY plus BEDROCK_MODEL_ID"
            )
        instructions = "Return valid JSON only. Do not include markdown, code fences, or commentary." if json_mode else ""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
            if self.api_mode == "responses":
                payload = {
                    "model": self.model,
                    "input": [{"role": "user", "content": f"{instructions}\n\n{prompt}".strip()}],
                    "temperature": float(temperature),
                    "max_output_tokens": int(max_tokens),
                }
                response = self.http_session_factory().post(
                    f"{self.api_base}/responses",
                    headers=headers,
                    json=payload,
                    timeout=self.timeout_seconds,
                )
                _raise_for_bad_response(response, "Bedrock Responses API")
                payload = response.json()
                text = _extract_responses_api_text(payload)
                if not text:
                    raise AiProviderError(f"Bedrock Responses API returned no text: {_payload_preview(payload)}")
                return text

            messages = []
            if instructions:
                messages.append({"role": "system", "content": instructions})
            messages.append({"role": "user", "content": prompt})
            payload = {
                "model": self.model,
                "messages": messages,
                "temperature": float(temperature),
                "max_tokens": int(max_tokens),
            }
            response = self.http_session_factory().post(
                f"{self.api_base}/chat/completions",
                headers=headers,
                json=payload,
                timeout=self.timeout_seconds,
            )
            _raise_for_bad_response(response, "Bedrock Chat Completions API")
            payload = response.json()
            text = _extract_chat_completion_text(payload)
            if not text:
                raise AiProviderError(f"Bedrock Chat Completions API returned no text: {_payload_preview(payload)}")
            return text
        except Exception as exc:
            raise AiProviderError(f"Bedrock OpenAI-compatible {self.api_mode} generation failed: {exc}") from exc
