import os
import unittest
from unittest import mock

from backend.agents.fo_structure.schemas import FOStructureReport, OptionChainSnapshot
from backend.services.fo_runtime import build_fo_snapshot, fo_runtime_status, run_fo_structure_cycle


class FORuntimeTests(unittest.TestCase):
    def test_build_fo_snapshot_mock_returns_snapshot(self):
        snapshot = build_fo_snapshot(symbol="NIFTY", use_mock=True)
        self.assertIsInstance(snapshot, OptionChainSnapshot)
        self.assertEqual(snapshot.symbol, "NIFTY")

    def test_run_fo_structure_cycle_nifty_mock_returns_report(self):
        with mock.patch("backend.agents.fo_structure.agent.save_agent_report"):
            report = run_fo_structure_cycle(symbol="NIFTY", use_mock=True)
        self.assertIsInstance(report, FOStructureReport)
        self.assertEqual(report.symbol, "NIFTY")

    def test_run_fo_structure_cycle_sensex_mock_returns_report(self):
        with mock.patch("backend.agents.fo_structure.agent.save_agent_report"):
            report = run_fo_structure_cycle(symbol="SENSEX", use_mock=True)
        self.assertIsInstance(report, FOStructureReport)
        self.assertEqual(report.symbol, "SENSEX")

    def test_run_fo_structure_cycle_banknifty_mock_returns_safe_unsupported_report(self):
        with mock.patch("backend.agents.fo_structure.agent.save_agent_report"):
            report = run_fo_structure_cycle(symbol="BANKNIFTY", use_mock=True)
        self.assertEqual(report.symbol, "BANKNIFTY")
        self.assertEqual(report.bias, "NEUTRAL")
        self.assertTrue(report.strategy_engine_guidance.avoid_directional_trade)
        self.assertIn("Unsupported symbol: BANKNIFTY. Supported symbols: NIFTY, SENSEX.", report.warnings)

    def test_no_upstox_token_required_for_mock_cycle(self):
        with mock.patch.dict(os.environ, {"UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            with mock.patch("backend.agents.fo_structure.agent.save_agent_report"):
                report = run_fo_structure_cycle(symbol="NIFTY", use_mock=True)
        self.assertEqual(report.symbol, "NIFTY")

    def test_fo_runtime_status_returns_dict(self):
        status = fo_runtime_status()
        self.assertIsInstance(status, dict)
        self.assertEqual(status["agent"], "fo_structure_agent")
        self.assertIn("upstox_options_configured", status)


if __name__ == "__main__":
    unittest.main()
