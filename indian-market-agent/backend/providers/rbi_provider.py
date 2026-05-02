"""Future RBI macro provider stub."""

from __future__ import annotations


class RBIProvider:
    def get_policy_rate(self):
        # TODO: integrate RBI policy/rate feed with respectful low-frequency polling.
        return None

    def get_liquidity_snapshot(self):
        # TODO: integrate RBI liquidity and system liquidity context.
        return None

    def get_policy_calendar(self):
        # TODO: integrate RBI/MPC event calendar.
        return []
