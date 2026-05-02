"""Future CCIL/bond-market provider stub."""

from __future__ import annotations


class CCILProvider:
    def get_india_10y_yield(self):
        # TODO: integrate India 10Y yield snapshot with respectful polling.
        return None

    def get_bond_market_snapshot(self):
        # TODO: integrate broader bond-market context from CCIL or a stable official source.
        return None
