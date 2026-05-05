import unittest
from datetime import datetime, timedelta

from backend.agents.execution_health.freshness import (
    age_seconds,
    check_report_freshness,
    is_timestamp_in_future,
    parse_timestamp,
)
from backend.agents.execution_health.schemas import FAIL, PASS, UNKNOWN_CHECK, WARN
from backend.core.settings import IST


class ExecutionHealthFreshnessTests(unittest.TestCase):
    def test_parse_timestamp_handles_iso_string(self):
        parsed = parse_timestamp("2026-05-05T09:20:00+05:30")

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.hour, 9)
        self.assertIsNotNone(parsed.tzinfo)

    def test_parse_timestamp_handles_datetime(self):
        value = datetime(2026, 5, 5, 9, 20, tzinfo=IST)

        self.assertEqual(parse_timestamp(value), value)

    def test_parse_timestamp_bad_string_returns_none(self):
        self.assertIsNone(parse_timestamp("not-a-date"))

    def test_age_seconds_works(self):
        now = datetime(2026, 5, 5, 9, 21, tzinfo=IST)
        value = datetime(2026, 5, 5, 9, 20, tzinfo=IST)

        self.assertEqual(age_seconds(value, now=now), 60.0)

    def test_future_timestamp_detection_works(self):
        now = datetime(2026, 5, 5, 9, 20, tzinfo=IST)

        self.assertTrue(is_timestamp_in_future(now + timedelta(seconds=90), now=now))
        self.assertFalse(is_timestamp_in_future(now + timedelta(seconds=30), now=now))

    def test_fresh_report_passes(self):
        now = datetime(2026, 5, 5, 9, 20, tzinfo=IST)
        report = {
            "generated_at": (now - timedelta(seconds=60)).isoformat(),
            "valid_until": (now + timedelta(seconds=60)).isoformat(),
        }

        check, freshness = check_report_freshness("fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT", report, 360, True, now=now)

        self.assertEqual(check.status, PASS)
        self.assertEqual(freshness.status, PASS)

    def test_stale_critical_report_fails(self):
        now = datetime(2026, 5, 5, 9, 20, tzinfo=IST)
        report = {"generated_at": (now - timedelta(seconds=500)).isoformat()}

        check, freshness = check_report_freshness("fo_structure_agent", "NIFTY", "FO_STRUCTURE_REPORT", report, 360, True, now=now)

        self.assertEqual(check.status, FAIL)
        self.assertEqual(freshness.status, FAIL)

    def test_missing_critical_report_fails(self):
        check, freshness = check_report_freshness("market_regime_agent", "NIFTY", "MARKET_REGIME_REPORT", None, 360, True)

        self.assertEqual(check.status, FAIL)
        self.assertEqual(freshness.status, FAIL)

    def test_missing_noncritical_report_warns_or_unknown(self):
        check, freshness = check_report_freshness("news_agent", "INDIA", "NEWS_INDEX_REPORT", None, 3600, False)

        self.assertIn(check.status, {WARN, UNKNOWN_CHECK})
        self.assertIn(freshness.status, {WARN, UNKNOWN_CHECK})


if __name__ == "__main__":
    unittest.main()
