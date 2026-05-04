import unittest
from datetime import datetime, timedelta, timezone

from backend.agents.market_regime.feature_builder import build_market_feature_snapshot
from backend.agents.market_regime.schemas import MarketCandle
from backend.agents.market_regime.scoring import score_market_regime
from backend.agents.market_regime.snapshot_builder import build_mock_market_feature_snapshot


def insufficient_snapshot():
    now = datetime.now(timezone.utc)
    candles = [
        MarketCandle(now + timedelta(minutes=idx * 5), 100 + idx, 101 + idx, 99 + idx, 100 + idx, 1000)
        for idx in range(3)
    ]
    return build_market_feature_snapshot("NIFTY", "NSE_INDEX|Nifty 50", candles, source_status={"provider": "test", "timeframe_minutes": 5})


class MarketRegimeScoringTests(unittest.TestCase):
    def test_bullish_snapshot_gives_trending_or_breakout(self):
        score = score_market_regime(build_mock_market_feature_snapshot("NIFTY", "bullish"))
        self.assertIn(score["primary_regime"], {"TRENDING_UP", "BREAKOUT_UP"})
        self.assertEqual(score["directional_bias"], "BULLISH")

    def test_bearish_snapshot_gives_trending_or_breakdown(self):
        score = score_market_regime(build_mock_market_feature_snapshot("SENSEX", "bearish"))
        self.assertIn(score["primary_regime"], {"TRENDING_DOWN", "BREAKDOWN"})
        self.assertEqual(score["directional_bias"], "BEARISH")

    def test_range_snapshot_gives_range_bound(self):
        score = score_market_regime(build_mock_market_feature_snapshot("NIFTY", "range"))
        self.assertEqual(score["primary_regime"], "RANGE_BOUND")
        self.assertEqual(score["trade_filter"], "WAIT")

    def test_choppy_snapshot_gives_choppy(self):
        score = score_market_regime(build_mock_market_feature_snapshot("NIFTY", "choppy"))
        self.assertEqual(score["primary_regime"], "CHOPPY")
        self.assertEqual(score["trade_filter"], "AVOID_DIRECTIONAL")

    def test_high_vix_snapshot_gives_high_volatility_or_reduce_size(self):
        score = score_market_regime(build_mock_market_feature_snapshot("NIFTY", "high_vol"))
        self.assertEqual(score["primary_regime"], "HIGH_VOLATILITY")
        self.assertIn(score["trade_filter"], {"REDUCE_POSITION_SIZE", "AVOID_DIRECTIONAL", "BLOCK_FRESH_TRADES"})
        self.assertGreaterEqual(score["volatility_score"], 85)

    def test_insufficient_data_gives_unclear(self):
        score = score_market_regime(insufficient_snapshot())
        self.assertEqual(score["primary_regime"], "UNCLEAR")
        self.assertEqual(score["confidence"], 0.35)
        self.assertTrue(score["warnings"])


if __name__ == "__main__":
    unittest.main()
