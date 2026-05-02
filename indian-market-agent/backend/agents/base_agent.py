"""Small base contract for future market agents.

This is intentionally lightweight for now: the current refactor is structural
only, so existing agent behavior remains inside the concrete modules.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AgentRunContext:
    agent_name: str
    run_id: str = ""


class BaseAgent:
    name = "base"

    def run(self, *_args, **_kwargs):
        raise NotImplementedError

