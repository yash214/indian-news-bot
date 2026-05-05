import json
import unittest
from unittest import mock

from backend.services import provider_status


class ProviderStatusServiceTests(unittest.TestCase):
    def setUp(self):
        provider_status.patch_upstox_rest_status(
            lastError=None,
            lastErrorAt=None,
            lastOkAt=None,
            failedKeys=[],
        )

    def test_requested_market_data_provider_returns_string(self):
        with mock.patch.dict("os.environ", {"MARKET_DATA_PROVIDER": "upstox"}, clear=False):
            self.assertIsInstance(provider_status.requested_market_data_provider(), str)
            self.assertEqual(provider_status.requested_market_data_provider(), "upstox")

    def test_upstox_configured_returns_bool(self):
        with mock.patch.dict("os.environ", {"UPSTOX_ANALYTICS_TOKEN": ""}, clear=False):
            self.assertIsInstance(provider_status.upstox_configured(), bool)
            self.assertFalse(provider_status.upstox_configured())

    def test_market_data_provider_status_does_not_expose_token(self):
        with mock.patch.dict(
            "os.environ",
            {
                "MARKET_DATA_PROVIDER": "upstox",
                "UPSTOX_ANALYTICS_TOKEN": "super-secret-token",
            },
            clear=False,
        ):
            status = provider_status.market_data_provider_status()

        self.assertIsInstance(status, dict)
        self.assertNotIn("super-secret-token", json.dumps(status))
        self.assertEqual(status["upstoxTokenSource"], "analytics_env")

    def test_upstox_rest_runtime_status_returns_dict(self):
        status = provider_status.upstox_rest_runtime_status()

        self.assertIsInstance(status, dict)
        self.assertIn("transport", status)
        self.assertIn("curlPreferred", status)

    def test_upstox_stream_runtime_status_returns_dict(self):
        status = provider_status.upstox_stream_runtime_status()

        self.assertIsInstance(status, dict)
        self.assertIn("connected", status)
        self.assertIn("dependencyReady", status)


if __name__ == "__main__":
    unittest.main()
