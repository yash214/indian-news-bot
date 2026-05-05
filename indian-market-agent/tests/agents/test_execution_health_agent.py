import json
import tempfile
import unittest
from pathlib import Path

from backend.agents import agent_output_store as store
from backend.agents.execution_health import ExecutionHealthAgent
from backend.agents.execution_health.schemas import HEALTHY, UNHEALTHY, UNKNOWN
from backend.agents.execution_health.snapshot_builder import ExecutionHealthSnapshotBuilder
from backend.core.persistence import init_state_db


class ExecutionHealthAgentTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "execution_health.db"
        self.old_state_db_path = store.STATE_DB_PATH
        store.STATE_DB_PATH = self.db_path
        init_state_db(self.db_path)

    def tearDown(self):
        store.STATE_DB_PATH = self.old_state_db_path
        self.tmpdir.cleanup()

    def test_healthy_mock_snapshot_returns_healthy(self):
        snapshot = ExecutionHealthSnapshotBuilder(mock_scenario="healthy").build(use_mock=True)

        report = ExecutionHealthAgent().analyze(snapshot)

        self.assertEqual(report.overall_health, HEALTHY)
        self.assertTrue(report.trade_allowed)
        self.assertTrue(report.strategy_engine_guidance.allow_trade_proposal)
        self.assertFalse(report.strategy_engine_guidance.allow_live_execution)

    def test_stale_fo_report_blocks_fresh_trade_proposal(self):
        snapshot = ExecutionHealthSnapshotBuilder(mock_scenario="unhealthy").build(use_mock=True)

        report = ExecutionHealthAgent().analyze(snapshot)

        self.assertEqual(report.overall_health, UNHEALTHY)
        self.assertTrue(report.fresh_trade_blocked)
        self.assertFalse(report.strategy_engine_guidance.allow_trade_proposal)

    def test_missing_snapshot_returns_unknown(self):
        report = ExecutionHealthAgent().analyze(None)

        self.assertEqual(report.overall_health, UNKNOWN)
        self.assertFalse(report.trade_allowed)
        self.assertTrue(report.fresh_trade_blocked)

    def test_report_is_json_serializable(self):
        snapshot = ExecutionHealthSnapshotBuilder(mock_scenario="healthy").build(use_mock=True)
        report = ExecutionHealthAgent().analyze(snapshot)

        encoded = json.dumps(report.to_dict(), sort_keys=True)

        self.assertIn("execution_health_agent", encoded)

    def test_report_is_saved_to_agent_output_store(self):
        snapshot = ExecutionHealthSnapshotBuilder(mock_scenario="healthy").build(use_mock=True)

        ExecutionHealthAgent().analyze(snapshot)

        latest = store.load_latest_agent_report("execution_health_agent", "SYSTEM", "EXECUTION_HEALTH_REPORT")
        history = store.load_agent_report_history("execution_health_agent", "SYSTEM", "EXECUTION_HEALTH_REPORT")
        self.assertEqual(latest["agent_name"], "execution_health_agent")
        self.assertEqual(latest["overall_health"], HEALTHY)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["payload"]["overall_health"], HEALTHY)

    def test_blockers_and_warnings_are_populated(self):
        snapshot = ExecutionHealthSnapshotBuilder(mock_scenario="unhealthy").build(use_mock=True)

        report = ExecutionHealthAgent().analyze(snapshot)

        self.assertTrue(report.blockers)
        self.assertTrue(any("FO_STRUCTURE_REPORT" in item or "MARKET_REGIME_REPORT" in item for item in report.blockers))


if __name__ == "__main__":
    unittest.main()
