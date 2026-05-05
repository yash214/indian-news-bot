import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.agents import agent_output_store as store
from backend.core.persistence import init_state_db
from backend.services import execution_health_runtime


class ExecutionHealthRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "execution_health_runtime.db"
        self.old_state_db_path = store.STATE_DB_PATH
        store.STATE_DB_PATH = self.db_path
        init_state_db(self.db_path)

    def tearDown(self):
        store.STATE_DB_PATH = self.old_state_db_path
        self.tmpdir.cleanup()

    def test_run_execution_health_cycle_mock_returns_report(self):
        with mock.patch.dict("os.environ", {"UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            report = execution_health_runtime.run_execution_health_cycle(use_mock=True)

        self.assertEqual(report.agent_name, "execution_health_agent")
        self.assertIn(report.overall_health, {"HEALTHY", "DEGRADED", "UNHEALTHY", "UNKNOWN"})
        self.assertFalse(report.strategy_engine_guidance.allow_live_execution)

    def test_get_latest_execution_health_report_works_after_run(self):
        execution_health_runtime.run_execution_health_cycle(use_mock=True)

        latest = execution_health_runtime.get_latest_execution_health_report()

        self.assertIsInstance(latest, dict)
        self.assertEqual(latest["agent_name"], "execution_health_agent")

    def test_execution_health_runtime_status_returns_dict(self):
        status = execution_health_runtime.execution_health_runtime_status()

        self.assertIsInstance(status, dict)
        self.assertEqual(status["agent"], "execution_health_agent")
        self.assertTrue(status["read_only"])

    def test_execution_health_mock_route_returns_report(self):
        from backend import app as market_app

        response = market_app.app.test_client().get("/api/agents/execution-health?mock=true")
        payload = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["agent_name"], "execution_health_agent")
        self.assertIn("overall_health", payload)


if __name__ == "__main__":
    unittest.main()
