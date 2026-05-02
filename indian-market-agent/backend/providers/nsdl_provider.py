"""Future NSDL/NSE flow provider stub."""

from __future__ import annotations


class NSDLProvider:
    def get_fii_fpi_flows(self):
        # TODO: integrate low-frequency FII/FPI flow data.
        return None

    def get_dii_flows_if_available(self):
        # TODO: integrate DII flow data when a stable source is chosen.
        return None
