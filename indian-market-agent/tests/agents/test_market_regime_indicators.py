import unittest
from datetime import datetime, timedelta, timezone

from backend.agents.market_regime.indicators import (
    calculate_atr,
    calculate_day_high_low,
    calculate_ema,
    calculate_opening_range,
    calculate_rsi,
    calculate_vwap,
    count_vwap_crosses,
    detect_higher_highs_lows,
    detect_lower_highs_lows,
)
from backend.agents.market_regime.schemas import MarketCandle


def make_candles(closes, *, start=None, volume=1000):
    start = start or datetime.now(timezone.utc)
    candles = []
    previous = closes[0]
    for idx, close in enumerate(closes):
        open_price = previous
        candles.append(MarketCandle(
            timestamp=start + timedelta(minutes=idx * 5),
            open=float(open_price),
            high=float(max(open_price, close) + 2),
            low=float(min(open_price, close) - 2),
            close=float(close),
            volume=volume,
        ))
        previous = close
    return candles


class MarketRegimeIndicatorTests(unittest.TestCase):
    def test_ema_calculation(self):
        self.assertAlmostEqual(calculate_ema([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 3), 9.0)

    def test_rsi_calculation(self):
        self.assertEqual(calculate_rsi(list(range(1, 21)), 14), 100.0)

    def test_atr_calculation(self):
        candles = make_candles(list(range(100, 121)))
        self.assertAlmostEqual(calculate_atr(candles, 14), 5.0, places=3)

    def test_vwap_with_volume(self):
        candles = [
            MarketCandle(datetime.now(timezone.utc), 10, 12, 9, 11, 100),
            MarketCandle(datetime.now(timezone.utc) + timedelta(minutes=5), 11, 13, 10, 12, 200),
        ]
        expected = ((((12 + 9 + 11) / 3) * 100) + (((13 + 10 + 12) / 3) * 200)) / 300
        self.assertAlmostEqual(calculate_vwap(candles), expected)

    def test_vwap_returns_none_without_volume(self):
        candles = make_candles([100, 101, 102], volume=0)
        self.assertIsNone(calculate_vwap(candles))

    def test_opening_range_calculation(self):
        candles = make_candles([100, 105, 102, 99, 101])
        opening_high, opening_low = calculate_opening_range(candles, minutes=15)
        self.assertEqual(opening_high, max(c.high for c in candles[:3]))
        self.assertEqual(opening_low, min(c.low for c in candles[:3]))

    def test_day_high_low(self):
        candles = make_candles([100, 105, 99, 103])
        self.assertEqual(calculate_day_high_low(candles), (107.0, 97.0))

    def test_higher_highs_lows_detection(self):
        candles = [
            MarketCandle(datetime.now(timezone.utc) + timedelta(minutes=idx * 5), 100 + idx, 105 + idx, 99 + idx, 102 + idx, 1000)
            for idx in range(6)
        ]
        self.assertTrue(detect_higher_highs_lows(candles, lookback=5))

    def test_lower_highs_lows_detection(self):
        candles = [
            MarketCandle(datetime.now(timezone.utc) + timedelta(minutes=idx * 5), 110 - idx, 115 - idx, 104 - idx, 108 - idx, 1000)
            for idx in range(6)
        ]
        self.assertTrue(detect_lower_highs_lows(candles, lookback=5))

    def test_vwap_cross_count(self):
        candles = make_candles([99, 101, 98, 102, 97, 103])
        self.assertEqual(count_vwap_crosses(candles, vwap=100, lookback=6), 5)


if __name__ == "__main__":
    unittest.main()
