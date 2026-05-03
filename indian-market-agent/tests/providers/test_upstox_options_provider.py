import unittest
from unittest import mock

from backend.agents.fo_structure.schemas import OptionChainSnapshot
from backend.providers.upstox.options_provider import UpstoxOptionsProvider


def response_with_json(payload, status_code=200):
    response = mock.Mock()
    response.status_code = status_code
    response.json.return_value = payload
    return response


def option_chain_payload(underlying_key="NSE_INDEX|Nifty 50"):
    return {
        "status": "success",
        "data": [
            {
                "strike_price": 22400,
                "underlying_key": underlying_key,
                "underlying_spot_price": 22450,
                "call_options": {
                    "market_data": {"ltp": 70, "volume": 1000, "oi": 100000, "prev_oi": 90000, "bid_price": 69, "ask_price": 71, "bid_qty": 100, "ask_qty": 150},
                    "option_greeks": {"iv": 14.2, "delta": 0.55, "gamma": 0.01, "theta": -10, "vega": 8},
                },
                "put_options": {
                    "market_data": {"ltp": 40, "volume": 1200, "oi": 130000, "prev_oi": 100000, "bid_price": 39, "ask_price": 41, "bid_qty": 120, "ask_qty": 160},
                    "option_greeks": {"iv": 15.1, "delta": -0.45, "gamma": 0.01, "theta": -11, "vega": 8.5},
                },
            },
            {
                "strike_price": 22500,
                "underlying_key": underlying_key,
                "underlying_spot_price": 22450,
                "call_options": {"market_data": {"ltp": 30, "volume": 900, "oi": 160000, "prev_oi": 140000}},
                "put_options": {"market_data": {"ltp": 90, "volume": 850, "oi": 90000, "prev_oi": 95000}},
            },
            {
                "strike_price": 22600,
                "underlying_key": underlying_key,
                "underlying_spot_price": 22450,
                "call_options": {"market_data": {"ltp": 15, "volume": 700, "oi": 180000, "prev_oi": 170000}},
                "put_options": {"market_data": {"ltp": 120, "volume": 650, "oi": 70000, "prev_oi": 72000}},
            },
        ],
    }


class UpstoxOptionsProviderTests(unittest.TestCase):
    def test_disabled_provider_returns_none(self):
        provider = UpstoxOptionsProvider(token="token", enabled=False)
        provider.session.get = mock.Mock()
        self.assertIsNone(provider.get_put_call_option_chain("NSE_INDEX|Nifty 50", "2026-05-07"))
        provider.session.get.assert_not_called()

    def test_missing_token_returns_none(self):
        provider = UpstoxOptionsProvider(token="", enabled=True)
        provider.session.get = mock.Mock()
        self.assertIsNone(provider.get_option_contracts("NSE_INDEX|Nifty 50"))
        provider.session.get.assert_not_called()

    def test_mocked_nifty_option_chain_normalizes(self):
        provider = UpstoxOptionsProvider(token="token", enabled=True)
        snapshot = provider.normalize_option_chain(option_chain_payload(), symbol="NIFTY", expiry="2026-05-07")
        self.assertIsInstance(snapshot, OptionChainSnapshot)
        self.assertEqual(snapshot.symbol, "NIFTY")
        self.assertEqual(snapshot.spot, 22450)
        self.assertEqual(snapshot.strikes[0].call_change_oi, 10000)
        self.assertEqual(snapshot.strikes[0].put_change_oi, 30000)
        self.assertEqual(snapshot.strikes[0].call_iv, 14.2)

    def test_mocked_sensex_option_chain_normalizes(self):
        provider = UpstoxOptionsProvider(token="token", enabled=True)
        snapshot = provider.normalize_option_chain(option_chain_payload("BSE_INDEX|SENSEX"), symbol="SENSEX", expiry="2026-05-07")
        self.assertEqual(snapshot.symbol, "SENSEX")
        self.assertEqual(snapshot.underlying_key, "BSE_INDEX|SENSEX")

    def test_http_error_returns_none_and_sets_last_error(self):
        provider = UpstoxOptionsProvider(token="token", enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_json({"status": "error"}, status_code=403))
        self.assertIsNone(provider.get_put_call_option_chain("NSE_INDEX|Nifty 50", "2026-05-07"))
        self.assertIn("HTTP 403", provider.last_error)

    def test_malformed_json_returns_none_and_sets_last_error(self):
        provider = UpstoxOptionsProvider(token="token", enabled=True)
        response = mock.Mock(status_code=200)
        response.json.side_effect = ValueError("bad json")
        provider.session.get = mock.Mock(return_value=response)
        self.assertIsNone(provider.get_option_contracts("NSE_INDEX|Nifty 50"))
        self.assertIn("JSON parse", provider.last_error)

    def test_source_status_fields(self):
        provider = UpstoxOptionsProvider(token="token", enabled=True)
        status = provider.source_status()
        self.assertEqual(status["provider"], "upstox_options")
        self.assertTrue(status["enabled"])
        self.assertTrue(status["configured"])
        self.assertIn("last_error", status)

    def test_sensex_unavailable_returns_none_safely(self):
        provider = UpstoxOptionsProvider(token="token", enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_json({"status": "error"}, status_code=404))
        self.assertIsNone(provider.get_put_call_option_chain("BSE_INDEX|SENSEX", "2026-05-07"))
        self.assertIn("HTTP 404", provider.last_error)


if __name__ == "__main__":
    unittest.main()
