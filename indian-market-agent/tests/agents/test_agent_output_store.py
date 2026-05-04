import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from backend.agents import agent_output_store as store
from backend.core.persistence import db_connect, init_state_db


class AgentOutputStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "agent_outputs.db"
        self.old_state_db_path = store.STATE_DB_PATH
        store.STATE_DB_PATH = self.db_path
        init_state_db(self.db_path)

    def tearDown(self):
        store.STATE_DB_PATH = self.old_state_db_path
        self.tmpdir.cleanup()

    def test_save_agent_report_writes_latest_app_state_value(self):
        payload = {"generated_at": "2026-05-04T09:20:00+05:30", "macro_bias": "NEUTRAL", "confidence": 0.66}

        store.save_agent_report("macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT", payload)

        latest = store.load_agent_output("macro_context_agent:INDIA:MACRO_CONTEXT_REPORT")
        self.assertEqual(latest["macro_bias"], "NEUTRAL")
        self.assertEqual(
            store.load_latest_agent_report("macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT")["confidence"],
            0.66,
        )

    def test_save_agent_report_inserts_history_newest_first(self):
        first = {"generated_at": "2026-05-04T09:20:00+05:30", "bias": "BULLISH", "confidence": 0.7}
        second = {"generated_at": "2026-05-04T09:25:00+05:30", "bias": "BEARISH", "confidence": 0.8}

        store.save_agent_report("fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT", first)
        store.save_agent_report("fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT", second)

        history = store.load_agent_report_history("fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT")
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["payload"]["bias"], "BEARISH")
        self.assertEqual(history[1]["payload"]["bias"], "BULLISH")

    def test_bias_derivation_works(self):
        samples = [
            ("macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT", {"macro_bias": "MIXED_BEARISH", "confidence": 0.7}, "MIXED_BEARISH"),
            ("fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT", {"bias": "BULLISH", "confidence": 0.8}, "BULLISH"),
            ("market_regime_agent", "NIFTY", "MARKET_REGIME_REPORT", {"primary_regime": "TRENDING_UP", "confidence": 0.75}, "TRENDING_UP"),
        ]
        for agent_name, symbol, report_type, payload, expected_bias in samples:
            store.save_agent_report(agent_name, symbol, report_type, payload)
            history = store.load_agent_report_history(agent_name, symbol, report_type, limit=1)
            self.assertEqual(history[0]["bias"], expected_bias)

    def test_missing_generated_at_does_not_crash(self):
        store.save_agent_report("market_regime_agent", "NIFTY", "MARKET_REGIME_REPORT", {"primary_regime": "UNCLEAR"})

        history = store.load_agent_report_history("market_regime_agent", "NIFTY", "MARKET_REGIME_REPORT")
        self.assertEqual(len(history), 1)
        self.assertGreater(history[0]["generated_ts"], 0)

    def test_invalid_generated_at_does_not_crash(self):
        store.save_agent_report("market_regime_agent", "SENSEX", "MARKET_REGIME_REPORT", {"generated_at": "bad-date"})

        history = store.load_agent_report_history("market_regime_agent", "SENSEX", "MARKET_REGIME_REPORT")
        self.assertEqual(len(history), 1)
        self.assertGreater(history[0]["generated_ts"], 0)

    def test_json_serialization_works_for_datetime(self):
        generated_at = datetime(2026, 5, 4, 9, 30, tzinfo=timezone.utc)

        store.save_agent_report(
            "macro_context_agent",
            "INDIA",
            "MACRO_CONTEXT_REPORT",
            {"generated_at": generated_at, "macro_bias": "NEUTRAL"},
        )

        latest = store.load_latest_agent_report("macro_context_agent", "INDIA", "MACRO_CONTEXT_REPORT")
        self.assertEqual(latest["generated_at"], generated_at.isoformat())

    def test_load_agent_reports_since_returns_only_recent_rows(self):
        old_payload = {"generated_at": "2026-05-04T09:20:00+05:30", "bias": "NEUTRAL"}
        new_payload = {"generated_at": "2026-05-04T09:40:00+05:30", "bias": "BULLISH"}
        since_ts = datetime.fromisoformat("2026-05-04T09:30:00+05:30").timestamp()

        store.save_agent_report("fo_structure_agent", "SENSEX", "FO_STRUCTURE_REPORT", old_payload)
        store.save_agent_report("fo_structure_agent", "SENSEX", "FO_STRUCTURE_REPORT", new_payload)

        rows = store.load_agent_reports_since("fo_structure_agent", "SENSEX", "FO_STRUCTURE_REPORT", since_ts)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload"]["bias"], "BULLISH")

    def test_existing_save_and_load_agent_output_still_work(self):
        store.save_agent_output("custom_agent:NIFTY:CUSTOM_REPORT", {"answer": 42})

        self.assertEqual(store.load_agent_output("custom_agent:NIFTY:CUSTOM_REPORT")["answer"], 42)
        self.assertEqual(store.load_agent_output("agent_output:custom_agent:NIFTY:CUSTOM_REPORT")["answer"], 42)

    def test_future_learning_tables_are_created(self):
        expected_tables = {
            "agent_outputs",
            "agent_outcomes",
            "error_analysis",
            "tuning_suggestions",
            "ruleset_versions",
            "audit_logs",
        }
        with db_connect(self.db_path) as conn:
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        table_names = {row["name"] for row in rows}
        self.assertTrue(expected_tables.issubset(table_names))


if __name__ == "__main__":
    unittest.main()
