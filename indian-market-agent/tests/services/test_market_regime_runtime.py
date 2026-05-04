import unittest
from unittest import mock

from backend.services.market_regime_runtime import run_market_regime_cycle


class MarketRegimeRuntimeTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
