import unittest
from unittest import mock

import requests

from backend.providers.fmp import FMPProvider


def mock_response(*, json_payload=None, status_code=200, json_side_effect=None, raise_error=None):
    response = mock.Mock()
    response.status_code = status_code
    if json_side_effect is not None:
        response.json.side_effect = json_side_effect
    else:
        response.json.return_value = json_payload
    if raise_error is not None:
        response.raise_for_status.side_effect = raise_error
    else:
        response.raise_for_status.return_value = None
    return response


class FMPProviderTests(unittest.TestCase):
    def test_no_api_key_returns_safe_empty_payloads(self):
        provider = FMPProvider(api_key=None, enabled=False)

        self.assertFalse(provider.is_configured())
        self.assertIsNone(provider.get_usd_inr())
        self.assertEqual(provider.get_us_indices(), {})
        self.assertEqual(provider.get_economic_calendar("2026-05-03", "2026-05-04"), [])

    def test_http_error_is_handled_gracefully(self):
        provider = FMPProvider(api_key="key", enabled=True)
        provider.session.get = mock.Mock(return_value=mock_response(raise_error=requests.HTTPError("boom")))

        self.assertIsNone(provider.get_usd_inr())

    def test_malformed_json_is_handled_gracefully(self):
        provider = FMPProvider(api_key="key", enabled=True)
        provider.session.get = mock.Mock(return_value=mock_response(json_side_effect=ValueError("bad json")))

        self.assertIsNone(provider.get_gold())

    def test_provider_normalizes_sample_quote_data(self):
        provider = FMPProvider(api_key="key", enabled=True)
        provider.session.get = mock.Mock(return_value=mock_response(
            json_payload=[{
                "symbol": "USDINR",
                "name": "USD/INR",
                "price": 83.12,
                "changesPercentage": 0.45,
            }]
        ))

        payload = provider.get_usd_inr()

        self.assertIsNotNone(payload)
        self.assertEqual(payload["source"], "fmp")
        self.assertEqual(payload["symbol"], "USDINR")
        self.assertEqual(payload["value"], 83.12)
        self.assertEqual(payload["change_pct_1d"], 0.45)


if __name__ == "__main__":
    unittest.main()
