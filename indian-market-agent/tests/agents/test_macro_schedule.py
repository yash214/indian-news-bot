import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from backend.agents.macro_context.schedule import (
    get_next_macro_refresh_time,
    is_macro_refresh_due,
    should_run_midday,
    should_run_post_open,
    should_run_postmarket,
    should_run_pre_close,
    should_run_premarket,
)


TZ = ZoneInfo("Asia/Kolkata")


class MacroScheduleTests(unittest.TestCase):
    def test_premarket_window(self):
        now = datetime(2026, 5, 4, 8, 35, tzinfo=TZ)
        self.assertTrue(should_run_premarket(now))
        self.assertTrue(is_macro_refresh_due(now, None))

    def test_post_open_window(self):
        now = datetime(2026, 5, 4, 9, 25, tzinfo=TZ)
        self.assertTrue(should_run_post_open(now))
        self.assertTrue(is_macro_refresh_due(now, datetime(2026, 5, 4, 8, 35, tzinfo=TZ)))

    def test_midday_window(self):
        now = datetime(2026, 5, 4, 12, 30, tzinfo=TZ)
        self.assertTrue(should_run_midday(now))

    def test_pre_close_window(self):
        now = datetime(2026, 5, 4, 15, 0, tzinfo=TZ)
        self.assertTrue(should_run_pre_close(now))

    def test_postmarket_window(self):
        now = datetime(2026, 5, 4, 15, 45, tzinfo=TZ)
        self.assertTrue(should_run_postmarket(now))

    def test_random_time_not_due(self):
        now = datetime(2026, 5, 4, 11, 17, tzinfo=TZ)
        self.assertFalse(is_macro_refresh_due(now, None))

    def test_next_refresh_time(self):
        now = datetime(2026, 5, 4, 11, 17, tzinfo=TZ)
        next_run = get_next_macro_refresh_time(now)
        self.assertEqual(next_run.hour, 12)
        self.assertEqual(next_run.minute, 30)


if __name__ == "__main__":
    unittest.main()
