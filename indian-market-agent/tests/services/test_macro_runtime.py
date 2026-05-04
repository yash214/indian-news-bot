import os
import unittest
from unittest import mock

from backend.agents.macro_context.schemas import MacroContextReport, MacroSnapshot
from backend.services.macro_runtime import (
    build_macro_snapshot,
    macro_runtime_status,
    run_macro_context_cycle,
)


class MacroRuntimeTests(unittest.TestCase):
    def test_build_macro_snapshot_mock_returns_snapshot(self):
        snapshot = build_macro_snapshot(use_mock=True)
        self.assertIsInstance(snapshot, MacroSnapshot)
        self.assertEqual(snapshot.market, "INDIA")

    def test_run_macro_context_cycle_mock_returns_report(self):
        with mock.patch("backend.agents.macro_context.agent.save_agent_report"):
            report = run_macro_context_cycle(use_mock=True)
        self.assertIsInstance(report, MacroContextReport)
        self.assertEqual(report.agent_name, "macro_context_agent")
        self.assertIsInstance(report.to_dict(), dict)

    def test_no_fmp_key_required_for_mock_cycle(self):
        with mock.patch.dict(os.environ, {"FMP_API_KEY": ""}, clear=False):
            with mock.patch("backend.agents.macro_context.agent.save_agent_report"):
                report = run_macro_context_cycle(use_mock=True)
        self.assertEqual(report.market, "INDIA")

    def test_macro_runtime_status_returns_dict(self):
        status = macro_runtime_status()
        self.assertIsInstance(status, dict)
        self.assertEqual(status["agent"], "macro_context_agent")
        self.assertIn("fmp_configured", status)


if __name__ == "__main__":
    unittest.main()
