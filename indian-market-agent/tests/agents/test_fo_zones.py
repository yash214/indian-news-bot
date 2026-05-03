import unittest
from datetime import datetime, timezone

from backend.agents.fo_structure.schemas import OptionChainSnapshot, OptionStrike
from backend.agents.fo_structure.strike_zones import find_atm_strike, find_resistance_zones, find_support_zones


def zone_snapshot():
    return OptionChainSnapshot(
        symbol="NIFTY",
        underlying_key="NSE_INDEX|Nifty 50",
        spot=100.0,
        expiry="2026-05-07",
        timestamp=datetime.now(timezone.utc),
        strikes=[
            OptionStrike(strike=90, put_oi=80, put_change_oi=20, call_oi=30, call_change_oi=5),
            OptionStrike(strike=95, put_oi=300, put_change_oi=100, call_oi=40, call_change_oi=-10),
            OptionStrike(strike=100, put_oi=160, put_change_oi=30, call_oi=150, call_change_oi=20),
            OptionStrike(strike=105, put_oi=60, put_change_oi=-10, call_oi=320, call_change_oi=120),
            OptionStrike(strike=110, put_oi=30, put_change_oi=-15, call_oi=220, call_change_oi=80),
        ],
    )


class FOZoneTests(unittest.TestCase):
    def test_support_zone_detection(self):
        zones = find_support_zones(zone_snapshot())
        self.assertEqual(zones[0].strike, 95)
        self.assertGreaterEqual(zones[0].strength, zones[1].strength)
        self.assertIn("support", zones[0].reason)

    def test_resistance_zone_detection(self):
        zones = find_resistance_zones(zone_snapshot())
        self.assertEqual(zones[0].strike, 105)
        self.assertGreaterEqual(zones[0].strength, zones[1].strength)
        self.assertIn("resistance", zones[0].reason)

    def test_atm_strike_detection(self):
        self.assertEqual(find_atm_strike(zone_snapshot()), 100)


if __name__ == "__main__":
    unittest.main()
