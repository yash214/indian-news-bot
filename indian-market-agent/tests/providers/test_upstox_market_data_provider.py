import unittest
from unittest import mock

from backend.agents.market_regime.schemas import MarketCandle
from backend.providers.upstox.market_data_provider import UpstoxMarketDataProvider


def response_with_json(payload, status_code=200):
    response = mock.Mock()
    response.status_code = status_code
    response.json.return_value = payload
    return response


def candle_payload():
    return {
        "status": "success",
        "data": {
            "candles": [
                ["2026-05-04T09:15:00+05:30", 22400, 22425, 22390, 22420, 1000, 0],
                ["2026-05-04T09:20:00+05:30", 22420, 22450, 22410, 22440, 1200, 0],
            ]
        },
    }


def daily_candle_payload():
    return {
        "status": "success",
        "data": {
            "candles": [
                ["2026-05-01T00:00:00+05:30", 22300, 22500, 22200, 22450, 0],
            ]
        },
    }


class UpstoxMarketDataProviderTests(unittest.TestCase):
    def test_disabled_provider_returns_none(self):
        provider = UpstoxMarketDataProvider(token="token", enabled=False)
        provider.session.get = mock.Mock()
        self.assertIsNone(provider.get_intraday_candles("NSE_INDEX|Nifty 50"))
        provider.session.get.assert_not_called()

    def test_missing_token_returns_none_where_auth_required(self):
        provider = UpstoxMarketDataProvider(token="", enabled=True)
        provider.session.get = mock.Mock()
        self.assertIsNone(provider.get_ohlc_quote(["NSE_INDEX|Nifty 50"]))
        provider.session.get.assert_not_called()

    def test_mocked_intraday_candle_response_normalizes(self):
        provider = UpstoxMarketDataProvider(token="token", enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_json(candle_payload()))
        raw = provider.get_intraday_candles("NSE_INDEX|Nifty 50", interval=5)
        candles = provider.normalize_candles(raw, "NIFTY", "NSE_INDEX|Nifty 50", 5)
        self.assertEqual(len(candles), 2)
        self.assertIsInstance(candles[0], MarketCandle)
        self.assertEqual(candles[0].close, 22420)
        self.assertEqual(candles[0].volume, 1000)

    def test_mocked_historical_daily_candle_response_normalizes_previous_day_data(self):
        provider = UpstoxMarketDataProvider(token="token", enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_json(daily_candle_payload()))
        raw = provider.get_historical_candles("NSE_INDEX|Nifty 50", unit="days", interval=1, to_date="2026-05-04", from_date="2026-04-25")
        candles = provider.normalize_candles(raw, "NIFTY", "NSE_INDEX|Nifty 50", 5)
        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].high, 22500)
        self.assertEqual(candles[0].low, 22200)
        self.assertEqual(candles[0].close, 22450)

    def test_http_error_returns_none_and_sets_last_error(self):
        provider = UpstoxMarketDataProvider(token="token", enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_json({"status": "error"}, status_code=403))
        self.assertIsNone(provider.get_intraday_candles("NSE_INDEX|Nifty 50"))
        self.assertIn("HTTP 403", provider.last_error)

    def test_malformed_json_returns_none_and_sets_last_error(self):
        provider = UpstoxMarketDataProvider(token="token", enabled=True)
        response = mock.Mock(status_code=200)
        response.json.side_effect = ValueError("bad json")
        provider.session.get = mock.Mock(return_value=response)
        self.assertIsNone(provider.get_ltp_quote(["NSE_INDEX|Nifty 50"]))
        self.assertIn("JSON parse", provider.last_error)

    def test_source_status_returns_expected_fields(self):
        provider = UpstoxMarketDataProvider(token="token", enabled=True)
        status = provider.source_status()
        self.assertEqual(status["provider"], "upstox_market_data")
        self.assertTrue(status["enabled"])
        self.assertTrue(status["configured"])
        self.assertTrue(status["read_only"])
        self.assertIn("last_error", status)

    def test_sensex_unavailable_returns_none_safely(self):
        provider = UpstoxMarketDataProvider(token="token", enabled=True)
        provider.session.get = mock.Mock(return_value=response_with_json({"status": "error"}, status_code=404))
        self.assertIsNone(provider.discover_instrument_key("SENSEX"))
        self.assertIn("HTTP 404", provider.last_error)


if __name__ == "__main__":
    unittest.main()
