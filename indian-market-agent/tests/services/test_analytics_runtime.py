import unittest

from backend.services import analytics_runtime


class AnalyticsRuntimeTests(unittest.TestCase):
    def test_build_live_only_signal_positive_quote(self):
        signal = analytics_runtime.build_live_only_signal(
            "NIFTY",
            {"price": 22500.0, "change": 160.0, "pct": 0.72, "day_high": 22520.0, "day_low": 22320.0},
        )

        self.assertEqual(signal["symbol"], "NIFTY")
        self.assertEqual(signal["price"], 22500.0)
        self.assertIn("trend", signal)
        self.assertIn("signal", signal)

    def test_build_live_only_signal_negative_quote(self):
        signal = analytics_runtime.build_live_only_signal(
            "BANKNIFTY",
            {"price": 48900.0, "change": -420.0, "pct": -0.85, "day_high": 49400.0, "day_low": 48880.0},
        )

        self.assertEqual(signal["symbol"], "BANKNIFTY")
        self.assertEqual(signal["trend"], "Range")
        self.assertIn(signal["signal"], {"Near day low", "Seller pressure"})

    def test_build_symbol_signal_uses_live_quote_only(self):
        quote = {"price": 1500.0, "change": 10.0, "pct": 0.67, "day_high": 1510.0, "day_low": 1488.0}

        self.assertIsInstance(analytics_runtime.build_symbol_signal("INFY", live_quote=quote), dict)
        self.assertIsNone(analytics_runtime.build_symbol_signal("INFY"))

    def test_build_market_analytics_payload_minimal_shape(self):
        ticks = {
            "Nifty 50": {"price": 22500.0, "change": 120.0, "pct": 0.54},
            "Nifty Bank": {"price": 49000.0, "change": 400.0, "pct": 0.82},
            "VIX": {"price": 13.2, "change": -0.2, "pct": -1.49},
        }
        index_snapshot = {
            "Nifty 50": ticks["Nifty 50"],
            "Nifty Bank": ticks["Nifty Bank"],
            "India VIX": ticks["VIX"],
        }

        payload = analytics_runtime.build_market_analytics_payload(
            articles=[],
            ticks=ticks,
            index_snapshot=index_snapshot,
            symbols=["INFY"],
            quote_map={"INFY": {"price": 1500.0, "change": 12.0, "pct": 0.81, "day_high": 1510.0, "day_low": 1488.0}},
        )

        self.assertIsInstance(payload, dict)
        for key in ["generatedAt", "overviewCards", "alerts", "sectorBoard", "sectorMap", "keyLevels", "watchlistSignals", "symbolMap", "regime", "primary"]:
            self.assertIn(key, payload)
        self.assertEqual(payload["watchlistSignals"][0]["symbol"], "INFY")

    def test_build_derivatives_analysis_payload_minimal_shape(self):
        ticks = {
            "Nifty 50": {"price": 22500.0, "change": 120.0, "pct": 0.54, "day_high": 22540.0, "day_low": 22310.0},
            "Nifty Bank": {"price": 49000.0, "change": 400.0, "pct": 0.82, "day_high": 49120.0, "day_low": 48520.0},
            "Nifty IT": {"price": 38000.0, "change": 100.0, "pct": 0.26},
            "VIX": {"price": 13.2, "change": -0.2, "pct": -1.49},
            "Crude Oil": {"price": 81.2, "change": -0.9, "pct": -1.10},
            "USD/INR": {"price": 82.7, "change": -0.2, "pct": -0.24},
        }
        history = {
            "Nifty 50": [22400.0, 22440.0, 22480.0, 22500.0],
            "Nifty Bank": [48700.0, 48800.0, 48900.0, 49000.0],
            "VIX": [13.8, 13.6, 13.4, 13.2],
        }

        payload = analytics_runtime.build_derivatives_analysis_payload(
            articles=[],
            ticks=ticks,
            index_snapshot={},
            option_chain=None,
            price_history=history,
            market_status={"staleData": False},
        )

        self.assertIsInstance(payload, dict)
        for key in ["generatedAt", "overviewCards", "predictionCards", "contextNotes", "riskFlags", "crossAssetRows", "relativeValueRows", "scoreBreakdown", "tradeScenarios", "signalMatrix", "triggerMap"]:
            self.assertIn(key, payload)
        self.assertEqual(len(payload["predictionCards"]), 6)

    def test_analytics_runtime_status_returns_dict(self):
        status = analytics_runtime.analytics_runtime_status()

        self.assertIsInstance(status, dict)
        self.assertTrue(status["readOnly"])


if __name__ == "__main__":
    unittest.main()
