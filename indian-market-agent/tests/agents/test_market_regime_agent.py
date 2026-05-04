import json
import unittest
from datetime import datetime, timezone
from unittest import mock

from backend.agents.market_regime.agent import MarketRegimeAgent
from backend.agents.market_regime.schemas import MarketFeatureSnapshot
from backend.agents.market_regime.snapshot_builder import build_mock_market_feature_snapshot


def invalid_snapshot():
    return MarketFeatureSnapshot(
        symbol="NIFTY",
        instrument_key="NSE_INDEX|Nifty 50",
        timestamp=datetime.now(timezone.utc),
        timeframe_minutes=5,
        candles=[],
        source="test",
        source_status={"provider": "test"},
    )


class MarketRegimeAgentTests(unittest.TestCase):
    def setUp(self):
        self.agent = MarketRegimeAgent()

    def test_nifty_mock_bullish_works(self):
        report = self.agent.analyze(build_mock_market_feature_snapshot("NIFTY", "bullish"), symbol="NIFTY")
        self.assertEqual(report.symbol, "NIFTY")
        self.assertIn(report.primary_regime, {"TRENDING_UP", "BREAKOUT_UP"})
        self.assertEqual(report.directional_bias, "BULLISH")

    def test_sensex_mock_bearish_works(self):
        report = self.agent.analyze(build_mock_market_feature_snapshot("SENSEX", "bearish"), symbol="SENSEX")
        self.assertEqual(report.symbol, "SENSEX")
        self.assertIn(report.primary_regime, {"TRENDING_DOWN", "BREAKDOWN"})
        self.assertEqual(report.directional_bias, "BEARISH")

    def test_banknifty_unsupported_returns_safe_unclear(self):
        report = self.agent.analyze(None, symbol="BANKNIFTY")
        self.assertEqual(report.symbol, "BANKNIFTY")
        self.assertEqual(report.primary_regime, "UNCLEAR")
        self.assertEqual(report.directional_bias, "NEUTRAL")
        self.assertEqual(report.confidence, 0.35)
        self.assertEqual(report.trade_filter, "WAIT")
        self.assertTrue(report.strategy_engine_guidance.avoid_directional_trade)
        self.assertIn("Unsupported symbol: BANKNIFTY. Supported symbols: NIFTY, SENSEX.", report.warnings)

    def test_invalid_snapshot_returns_unclear_with_warnings(self):
        report = self.agent.analyze(invalid_snapshot(), symbol="NIFTY")
        self.assertEqual(report.primary_regime, "UNCLEAR")
        self.assertTrue(report.warnings)
        self.assertTrue(report.strategy_engine_guidance.avoid_directional_trade)

    def test_json_serialization_works(self):
        report = self.agent.analyze(build_mock_market_feature_snapshot("NIFTY", "range"), symbol="NIFTY")
        payload = report.to_dict()
        self.assertIsInstance(payload["generated_at"], str)
        json.dumps(payload)

    def test_report_persisted_to_agent_output_store(self):
        with mock.patch("backend.agents.market_regime.agent.save_agent_report") as save_mock:
            report = self.agent.analyze(build_mock_market_feature_snapshot("NIFTY", "bullish"), symbol="NIFTY")
        save_mock.assert_called_once()
        self.assertEqual(save_mock.call_args.kwargs["agent_name"], "market_regime_agent")
        self.assertEqual(save_mock.call_args.kwargs["symbol"], "NIFTY")
        self.assertEqual(save_mock.call_args.kwargs["report_type"], "MARKET_REGIME_REPORT")
        self.assertEqual(save_mock.call_args.kwargs["payload"]["symbol"], report.symbol)


if __name__ == "__main__":
    unittest.main()
