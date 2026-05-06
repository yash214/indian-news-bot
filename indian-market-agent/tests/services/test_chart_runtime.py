import unittest
from types import SimpleNamespace
from unittest import mock

from backend.services import chart_runtime


class ChartRuntimeTests(unittest.TestCase):
    def test_get_chart_candles_mock_returns_candles(self):
        payload = chart_runtime.get_chart_candles(symbol="NIFTY", interval="5m", range_="1d", use_mock=True)

        self.assertEqual(payload["symbol"], "NIFTY")
        self.assertEqual(payload["source"], "mock")
        self.assertTrue(payload["candles"])
        self.assertIn("time", payload["candles"][0])

    def test_mock_candles_are_sorted_ascending(self):
        payload = chart_runtime.get_chart_candles(symbol="SENSEX", interval="15m", range_="1d", use_mock=True)
        times = [candle["time"] for candle in payload["candles"]]

        self.assertEqual(times, sorted(times))
        self.assertEqual(len(times), len(set(times)))

    def test_unsupported_symbol_returns_empty_with_warning(self):
        payload = chart_runtime.get_chart_candles(symbol="BANKNIFTY", interval="5m", range_="1d", use_mock=True)

        self.assertEqual(payload["symbol"], "BANKNIFTY")
        self.assertEqual(payload["candles"], [])
        self.assertTrue(payload["warnings"])

    def test_get_chart_overlays_mock_returns_overlay_keys(self):
        payload = chart_runtime.get_chart_overlays(symbol="NIFTY", interval="5m", use_mock=True)
        overlays = payload["overlays"]

        for key in ["vwap", "ema_9", "ema_21", "opening_range", "support_zones", "resistance_zones", "price_lines", "markers"]:
            self.assertIn(key, overlays)
        self.assertTrue(overlays["price_lines"])

    def test_missing_agent_reports_do_not_crash_overlays(self):
        payload = chart_runtime.get_chart_overlays(symbol="NIFTY", interval="5m", use_mock=False)

        self.assertEqual(payload["symbol"], "NIFTY")
        self.assertIsInstance(payload["overlays"], dict)
        self.assertIn("warnings", payload)

    def test_runtime_history_fallback_returns_candles(self):
        context = SimpleNamespace(
            get_price_history=lambda: {"Nifty 50": [22400, 22410, 22405, 22440]},
        )

        payload = chart_runtime.get_chart_candles(symbol="NIFTY", interval="5m", range_="1d", context=context)

        self.assertEqual(payload["source"], "history")
        self.assertEqual(len(payload["candles"]), 4)

    def test_workspace_summary_mock_returns_market_bar_and_agents(self):
        payload = chart_runtime.get_workspace_summary(symbol="NIFTY", use_mock=True)

        self.assertIn("market_bar", payload)
        self.assertIn("agents", payload)
        self.assertIn("strategy_suggestions", payload)
        self.assertTrue(payload["strategy_suggestions"][0]["manual_approval_required"])

    def test_workspace_summary_fills_market_bar_when_ticks_missing(self):
        def latest_report(agent_name, symbol, report_type):
            if agent_name == "market_regime_agent" and symbol == "NIFTY":
                return {
                    "key_levels": {
                        "latest_close": 22500,
                        "previous_day_close": 22400,
                        "india_vix": 15.5,
                    }
                }
            if agent_name == "macro_context_agent":
                return {
                    "factors": {
                        "usd_inr": {"value": 83.2, "change_pct_1d": 0.1},
                        "crude": {"value": 82.0, "change_pct_1d": -0.3},
                    }
                }
            return {}

        context = SimpleNamespace(
            market_data_snapshot=lambda include_history=False: {"ticks": {}, "marketStatus": {"session": "open"}},
        )
        with mock.patch.object(chart_runtime, "load_latest_agent_report", side_effect=latest_report):
            payload = chart_runtime.get_workspace_summary(symbol="NIFTY", use_mock=False, context=context)

        bar = payload["market_bar"]
        self.assertEqual(bar["nifty"]["price"], 22500)
        self.assertEqual(bar["india_vix"]["price"], 15.5)
        self.assertEqual(bar["usd_inr"]["price"], 83.2)
        self.assertTrue(bar["sensex"]["price"])

    def test_chart_routes_return_200_with_mock(self):
        from backend import app as market_app

        client = market_app.app.test_client()
        cases = [
            "/api/chart/candles?symbol=NIFTY&interval=5m&range=1d&mock=true",
            "/api/chart/overlays?symbol=NIFTY&interval=5m&mock=true",
            "/api/workspace/summary?symbol=NIFTY&mock=true",
            "/workspace",
        ]
        for path in cases:
            response = client.get(path)
            self.assertEqual(response.status_code, 200, path)


if __name__ == "__main__":
    unittest.main()
