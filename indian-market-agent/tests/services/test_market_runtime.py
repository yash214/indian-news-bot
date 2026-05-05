import time
import unittest
from types import SimpleNamespace
from unittest import mock

from backend.services import market_runtime


class MarketRuntimeServiceTests(unittest.TestCase):
    def test_format_quote_for_client_returns_safe_structure_when_quote_is_none(self):
        payload = market_runtime.format_quote_for_client(
            "INFY",
            None,
            status={"session": "postclose", "isMarketOpen": False},
        )

        self.assertEqual(payload["symbol"], "INFY")
        self.assertIsNone(payload["price"])
        self.assertFalse(payload["live"])
        self.assertTrue(payload["stale"])

    def test_format_quote_for_client_handles_price_change_fields(self):
        fetched_at = time.time()
        payload = market_runtime.format_quote_for_client(
            "INFY",
            {
                "price": 1500.0,
                "change": 12.5,
                "pct": 0.84,
                "fetchedAt": fetched_at,
                "source": "NSE",
                "previous_close": 1487.5,
            },
            status={"session": "open", "isMarketOpen": True},
        )

        self.assertEqual(payload["price"], 1500.0)
        self.assertEqual(payload["change"], 12.5)
        self.assertEqual(payload["pct"], 0.84)
        self.assertEqual(payload["previous_close"], 1487.5)
        self.assertFalse(payload["stale"])

    def test_runtime_snapshot_from_db_returns_none_or_dict_safely(self):
        with mock.patch.object(market_runtime, "db_get_json", return_value=None):
            self.assertIsNone(market_runtime.runtime_snapshot_from_db())

        payload = {"ticks": {}, "history": {"Nifty 50": [1.0]}}
        with mock.patch.object(market_runtime, "db_get_json", return_value=payload):
            without_history = market_runtime.runtime_snapshot_from_db(include_history=False)
            with_history = market_runtime.runtime_snapshot_from_db(include_history=True)

        self.assertIsInstance(without_history, dict)
        self.assertNotIn("history", without_history)
        self.assertEqual(with_history["history"], {"Nifty 50": [1.0]})

    def test_market_runtime_status_returns_dict(self):
        status = market_runtime.market_runtime_status()

        self.assertIsInstance(status, dict)
        self.assertIn("tickCount", status)
        self.assertIn("dataProvider", status)

    def test_fetch_tickers_no_data_context_does_not_crash(self):
        context = SimpleNamespace()

        with mock.patch.dict("os.environ", {"MARKET_DATA_PROVIDER": "nse"}, clear=False):
            ticks, analytics_indices = market_runtime.fetch_tickers(context=context)

        self.assertEqual(ticks, {})
        self.assertEqual(analytics_indices, {})


if __name__ == "__main__":
    unittest.main()
