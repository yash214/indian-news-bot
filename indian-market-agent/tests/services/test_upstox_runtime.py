import json
import unittest
from unittest import mock

from backend.services import provider_status, upstox_runtime


class UpstoxRuntimeServiceTests(unittest.TestCase):
    def setUp(self):
        provider_status.patch_upstox_rest_status(
            lastError=None,
            lastErrorAt=None,
            lastOkAt=None,
            failedKeys=[],
        )

    def test_upstox_runtime_status_returns_dict_without_token(self):
        with mock.patch.dict("os.environ", {"MARKET_DATA_PROVIDER": "nse", "UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            status = upstox_runtime.upstox_runtime_status()

        self.assertIsInstance(status, dict)
        self.assertFalse(status["configured"])
        self.assertEqual(status["tokenSource"], "none")

    def test_upstox_integration_status_does_not_expose_raw_token(self):
        token = "super-secret-token"
        with mock.patch.dict("os.environ", {"MARKET_DATA_PROVIDER": "upstox", "UPSTOX_ANALYTICS_TOKEN": token}, clear=False):
            status = upstox_runtime.upstox_integration_status()

        self.assertIsInstance(status, dict)
        self.assertNotIn(token, json.dumps(status))
        self.assertEqual(status["credential"], "UPSTOX_ANALYTICS_TOKEN")
        self.assertEqual(status["tokenMode"], "analytics")

    def test_resolve_upstox_instrument_key_returns_catalog_key(self):
        key = upstox_runtime.resolve_upstox_instrument_key("INFY")

        self.assertEqual(key, "NSE_EQ|INE009A01021")

    def test_upstox_request_json_raises_safe_missing_token_error(self):
        with mock.patch.dict("os.environ", {"UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "UPSTOX_ANALYTICS_TOKEN is not configured"):
                upstox_runtime.upstox_request_json("https://api.upstox.com/v2/market-quote/quotes")

    def test_fetch_upstox_quotes_by_label_returns_empty_without_token(self):
        with mock.patch.dict("os.environ", {"UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            quotes = upstox_runtime.fetch_upstox_quotes_by_label({"INFY": "NSE_EQ|INE009A01021"})

        self.assertEqual(quotes, {})

    def test_fetch_upstox_option_chain_returns_none_without_token(self):
        with mock.patch.dict("os.environ", {"UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            payload = upstox_runtime.fetch_upstox_option_chain("NIFTY", expiry="2026-04-30")

        self.assertIsNone(payload)

    def test_upstox_stream_subscription_map_includes_default_indexes(self):
        subscriptions = upstox_runtime.upstox_stream_subscription_map(state={"tickerSelections": [], "watchlist": [], "portfolio": {}})

        self.assertIsInstance(subscriptions, dict)
        self.assertIn("Nifty 50", subscriptions)
        self.assertIn("Nifty Bank", subscriptions)
        self.assertTrue(subscriptions["Nifty 50"].startswith("NSE_INDEX|"))


if __name__ == "__main__":
    unittest.main()
