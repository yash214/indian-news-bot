import json
import unittest
from datetime import datetime, timezone
from unittest import mock

from backend.agents.fo_structure.agent import FOStructureAgent
from backend.agents.fo_structure.schemas import OptionChainSnapshot, OptionStrike
from backend.agents.fo_structure.snapshot_builder import build_mock_option_chain_snapshot


def make_snapshot(*, symbol="NIFTY", spot=100.0, expiry="2026-05-07", strikes=None):
    return OptionChainSnapshot(
        symbol=symbol,
        underlying_key="NSE_INDEX|Nifty 50" if symbol == "NIFTY" else "BSE_INDEX|SENSEX",
        spot=spot,
        expiry=expiry,
        timestamp=datetime.now(timezone.utc),
        strikes=strikes or [],
        source="test",
        source_status={"provider": "test", "enabled": True, "configured": True},
    )


class FOStructureAgentTests(unittest.TestCase):
    def setUp(self):
        self.agent = FOStructureAgent()

    def test_nifty_mock_option_chain_works(self):
        report = self.agent.analyze(build_mock_option_chain_snapshot("NIFTY"), symbol="NIFTY")
        self.assertEqual(report.symbol, "NIFTY")
        self.assertIn(report.bias, {"BULLISH", "BEARISH", "NEUTRAL", "MIXED"})
        self.assertIsNotNone(report.pcr)

    def test_sensex_mock_option_chain_works(self):
        report = self.agent.analyze(build_mock_option_chain_snapshot("SENSEX"), symbol="SENSEX")
        self.assertEqual(report.symbol, "SENSEX")
        self.assertIsNotNone(report.atm_strike)

    def test_banknifty_is_unsupported(self):
        report = self.agent.analyze(None, symbol="BANKNIFTY")
        self.assertEqual(report.symbol, "BANKNIFTY")
        self.assertEqual(report.bias, "NEUTRAL")
        self.assertEqual(report.confidence, 0.35)
        self.assertIsNone(report.pcr)
        self.assertEqual(report.pcr_state, "UNKNOWN")
        self.assertTrue(report.strategy_engine_guidance.avoid_directional_trade)
        self.assertIn("Unsupported symbol: BANKNIFTY. Supported symbols: NIFTY, SENSEX.", report.warnings)

    def test_bullish_scenario(self):
        snapshot = make_snapshot(strikes=[
            OptionStrike(strike=95, call_oi=50, call_prev_oi=70, call_change_oi=-20, put_oi=300, put_prev_oi=200, put_change_oi=100, call_ltp=6, put_ltp=3, call_bid_price=5, call_ask_price=6, put_bid_price=2, put_ask_price=3),
            OptionStrike(strike=100, call_oi=80, call_prev_oi=120, call_change_oi=-40, put_oi=220, put_prev_oi=170, put_change_oi=50, call_ltp=5, put_ltp=5, call_bid_price=4.8, call_ask_price=5.1, put_bid_price=4.8, put_ask_price=5.1),
            OptionStrike(strike=105, call_oi=120, call_prev_oi=115, call_change_oi=5, put_oi=120, put_prev_oi=110, put_change_oi=10, call_ltp=3, put_ltp=7, call_bid_price=2.8, call_ask_price=3.1, put_bid_price=6.8, put_ask_price=7.1),
            OptionStrike(strike=110, call_oi=140, call_prev_oi=130, call_change_oi=10, put_oi=80, put_prev_oi=75, put_change_oi=5, call_ltp=2, put_ltp=9, call_bid_price=1.9, call_ask_price=2.1, put_bid_price=8.8, put_ask_price=9.1),
            OptionStrike(strike=115, call_oi=100, call_prev_oi=95, call_change_oi=5, put_oi=50, put_prev_oi=45, put_change_oi=5, call_ltp=1, put_ltp=12, call_bid_price=0.9, call_ask_price=1.1, put_bid_price=11.8, put_ask_price=12.1),
        ])
        report = self.agent.analyze(snapshot, symbol="NIFTY")
        self.assertEqual(report.bias, "BULLISH")
        self.assertGreater(report.pcr, 1.1)

    def test_bearish_scenario(self):
        snapshot = make_snapshot(strikes=[
            OptionStrike(strike=85, call_oi=60, call_prev_oi=55, call_change_oi=5, put_oi=80, put_prev_oi=70, put_change_oi=10, call_ltp=15, put_ltp=1, call_bid_price=14.8, call_ask_price=15.1, put_bid_price=0.9, put_ask_price=1.1),
            OptionStrike(strike=90, call_oi=90, call_prev_oi=85, call_change_oi=5, put_oi=120, put_prev_oi=130, put_change_oi=-10, call_ltp=10, put_ltp=2, call_bid_price=9.8, call_ask_price=10.1, put_bid_price=1.9, put_ask_price=2.1),
            OptionStrike(strike=95, call_oi=120, call_prev_oi=110, call_change_oi=10, put_oi=130, put_prev_oi=180, put_change_oi=-50, call_ltp=7, put_ltp=3, call_bid_price=6.8, call_ask_price=7.1, put_bid_price=2.9, put_ask_price=3.1),
            OptionStrike(strike=100, call_oi=220, call_prev_oi=150, call_change_oi=70, put_oi=90, put_prev_oi=120, put_change_oi=-30, call_ltp=5, put_ltp=5, call_bid_price=4.8, call_ask_price=5.1, put_bid_price=4.8, put_ask_price=5.1),
            OptionStrike(strike=105, call_oi=320, call_prev_oi=200, call_change_oi=120, put_oi=60, put_prev_oi=80, put_change_oi=-20, call_ltp=3, put_ltp=8, call_bid_price=2.8, call_ask_price=3.1, put_bid_price=7.8, put_ask_price=8.1),
        ])
        report = self.agent.analyze(snapshot, symbol="NIFTY")
        self.assertEqual(report.bias, "BEARISH")
        self.assertLess(report.pcr, 0.9)

    def test_mixed_scenario(self):
        snapshot = make_snapshot(strikes=[
            OptionStrike(strike=90, call_oi=100, call_prev_oi=90, call_change_oi=10, put_oi=100, put_prev_oi=90, put_change_oi=10, call_ltp=10, put_ltp=1, call_bid_price=9.8, call_ask_price=10.1, put_bid_price=0.9, put_ask_price=1.1),
            OptionStrike(strike=95, call_oi=120, call_prev_oi=110, call_change_oi=10, put_oi=120, put_prev_oi=110, put_change_oi=10, call_ltp=7, put_ltp=3, call_bid_price=6.8, call_ask_price=7.1, put_bid_price=2.9, put_ask_price=3.1),
            OptionStrike(strike=100, call_oi=150, call_prev_oi=145, call_change_oi=5, put_oi=150, put_prev_oi=145, put_change_oi=5, call_ltp=5, put_ltp=5, call_bid_price=4.8, call_ask_price=5.1, put_bid_price=4.8, put_ask_price=5.1),
            OptionStrike(strike=105, call_oi=120, call_prev_oi=110, call_change_oi=10, put_oi=120, put_prev_oi=110, put_change_oi=10, call_ltp=3, put_ltp=7, call_bid_price=2.8, call_ask_price=3.1, put_bid_price=6.8, put_ask_price=7.1),
            OptionStrike(strike=110, call_oi=100, call_prev_oi=90, call_change_oi=10, put_oi=100, put_prev_oi=90, put_change_oi=10, call_ltp=1, put_ltp=10, call_bid_price=0.9, call_ask_price=1.1, put_bid_price=9.8, put_ask_price=10.1),
        ])
        report = self.agent.analyze(snapshot, symbol="NIFTY")
        self.assertIn(report.bias, {"MIXED", "NEUTRAL"})

    def test_extreme_expiry_risk_reduces_directional_confidence(self):
        snapshot = build_mock_option_chain_snapshot("NIFTY")
        with mock.patch("backend.agents.fo_structure.agent.calculate_expiry_risk", return_value=("EXTREME", [])):
            report = self.agent.analyze(snapshot, symbol="NIFTY")
        self.assertEqual(report.expiry_risk, "EXTREME")
        self.assertTrue(report.strategy_engine_guidance.reduce_position_size)
        self.assertTrue(report.strategy_engine_guidance.avoid_directional_trade)

    def test_invalid_snapshot_does_not_crash(self):
        report = self.agent.analyze(make_snapshot(strikes=[], spot=0), symbol="NIFTY")
        self.assertEqual(report.bias, "NEUTRAL")
        self.assertEqual(report.confidence, 0.35)
        self.assertTrue(report.warnings)

    def test_json_serialization(self):
        report = self.agent.analyze(build_mock_option_chain_snapshot("NIFTY"), symbol="NIFTY")
        payload = report.to_dict()
        self.assertIsInstance(payload["generated_at"], str)
        json.dumps(payload)


if __name__ == "__main__":
    unittest.main()
