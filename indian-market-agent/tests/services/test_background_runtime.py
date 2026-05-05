import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from backend.services import background_runtime


class BackgroundRuntimeServiceTests(unittest.TestCase):
    def setUp(self):
        self._started = background_runtime._background_threads_started
        background_runtime._background_threads_started = False

    def tearDown(self):
        background_runtime._background_threads_started = self._started

    def test_background_threads_enabled_false_when_disabled(self):
        with mock.patch.dict("os.environ", {"MARKET_DESK_DISABLE_THREADS": "1"}, clear=False):
            self.assertFalse(background_runtime.background_threads_enabled())

    def test_external_worker_mode_true_when_disabled(self):
        with mock.patch.dict("os.environ", {"MARKET_DESK_DISABLE_THREADS": "1"}, clear=False):
            self.assertTrue(background_runtime.external_worker_mode())

    def test_macro_background_thread_enabled_false_under_tests(self):
        self.assertFalse(background_runtime.macro_background_thread_enabled())

    def test_background_runtime_status_returns_dict(self):
        status = background_runtime.background_runtime_status()

        self.assertIsInstance(status, dict)
        self.assertIn("threadsEnabled", status)
        self.assertIn("externalWorkerMode", status)
        self.assertIn("started", status)

    def test_start_background_workers_returns_false_when_disabled(self):
        with mock.patch.dict("os.environ", {"MARKET_DESK_DISABLE_THREADS": "1"}, clear=False):
            self.assertFalse(background_runtime.start_background_workers(context=None))

    def test_initialize_runtime_state_accepts_minimal_context(self):
        runtime_state = {}
        context = SimpleNamespace(
            runtime_state=runtime_state,
            _lock=threading.Lock(),
            _refresh_wakeup=threading.Event(),
            load_holiday_calendar=lambda: {"2026-01-26": "Republic Day"},
            load_persisted_app_state=lambda: ({"watchlist": ["INFY"]}, True),
            load_refresh_settings=lambda: 45,
            ai_summary_progress_for_articles=lambda articles, now=None: {},
            enrich_articles_with_ai_summaries=lambda articles: articles,
        )

        with mock.patch.object(background_runtime.news_runtime, "configure_news_runtime") as configure:
            background_runtime.initialize_runtime_state(context=context)

        self.assertEqual(runtime_state["NSE_HOLIDAY_CALENDAR"], {"2026-01-26": "Republic Day"})
        self.assertEqual(runtime_state["_app_state"], {"watchlist": ["INFY"]})
        self.assertTrue(runtime_state["_has_persisted_state"])
        self.assertEqual(runtime_state["_news_refresh_seconds"], 45)
        configure.assert_called_once()


if __name__ == "__main__":
    unittest.main()
