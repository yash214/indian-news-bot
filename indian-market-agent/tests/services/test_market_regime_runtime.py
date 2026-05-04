import unittest
from unittest import mock

from backend.services.market_regime_runtime import (
    build_market_regime_snapshot,
    market_regime_runtime_status,
    run_market_regime_cycle,
)


class MarketRegimeRuntimeTests(unittest.TestCase):
    def test_build_market_regime_snapshot_mock_returns_snapshot(self):
        snapshot = build_market_regime_snapshot(symbol="NIFTY", use_mock=True)
        self.assertEqual(snapshot.symbol, "NIFTY")

    def test_run_market_regime_cycle_nifty_mock_returns_report(self):
        with mock.patch("backend.agents.market_regime.agent.save_agent_report"):
            report = run_market_regime_cycle(symbol="NIFTY", use_mock=True)
        self.assertEqual(report.symbol, "NIFTY")
        self.assertEqual(report.agent_name, "market_regime_agent")

    def test_run_market_regime_cycle_sensex_mock_returns_report(self):
        with mock.patch("backend.agents.market_regime.agent.save_agent_report"):
            report = run_market_regime_cycle(symbol="SENSEX", use_mock=True, regime_hint="bearish")
        self.assertEqual(report.symbol, "SENSEX")
        self.assertIn(report.primary_regime, {"TRENDING_DOWN", "BREAKDOWN"})

    def test_run_market_regime_cycle_banknifty_mock_returns_safe_unsupported_report(self):
        with mock.patch("backend.agents.market_regime.agent.save_agent_report"):
            report = run_market_regime_cycle(symbol="BANKNIFTY", use_mock=True)
        self.assertEqual(report.symbol, "BANKNIFTY")
        self.assertEqual(report.primary_regime, "UNCLEAR")
        self.assertEqual(report.trade_filter, "WAIT")
        self.assertTrue(report.strategy_engine_guidance.avoid_directional_trade)
        self.assertIn("Unsupported symbol: BANKNIFTY. Supported symbols: NIFTY, SENSEX.", report.warnings)

    def test_market_regime_runtime_status_returns_dict(self):
        status = market_regime_runtime_status()
        self.assertIsInstance(status, dict)
        self.assertEqual(status["agent"], "market_regime_agent")
        self.assertIn("supported_symbols", status)


if __name__ == "__main__":
    unittest.main()
