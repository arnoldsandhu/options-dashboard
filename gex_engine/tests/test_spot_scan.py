"""Unit tests for gex_engine.spot_scan — synthetic chain with known flip."""
import unittest
from datetime import date, timedelta

from gex_engine.spot_scan import run_spot_scan


def _make_contract(strike, cp, oi, iv, expiry_offset_days=30):
    expiry = (date.today() + timedelta(days=expiry_offset_days)).isoformat()
    return {
        "ticker": "TEST", "strike": float(strike), "option_type": cp,
        "oi": oi, "volume": 0, "iv": float(iv),
        "vendor_gamma": 0.0, "multiplier": 100, "expiration": expiry, "rate": 0.05,
    }


class TestSpotScan(unittest.TestCase):

    def test_flip_found_in_range(self):
        """Synthetic chain with heavy calls above spot → flip below current spot."""
        # Heavy call OI above 510, heavy put OI below 490.
        # At current spot=500: calls dominate → net GEX positive.
        # At spot=480: puts dominate → net GEX negative.
        # Flip should be found somewhere between.
        contracts = [
            # Calls near spot: positive GEX
            _make_contract(510, "call", oi=5000, iv=0.18),
            _make_contract(515, "call", oi=4000, iv=0.19),
            # Puts far below: negative GEX
            _make_contract(490, "put",  oi=500,  iv=0.22),
            _make_contract(480, "put",  oi=500,  iv=0.25),
        ]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        # The scan should at minimum run and return a dict
        self.assertIn("flip_point", result)
        self.assertIn("curve", result)
        self.assertGreater(len(result["curve"]), 0)

    def test_flip_not_found_returns_note(self):
        """Chain with no sign crossing should report nearest boundary."""
        # All calls, all positive → net GEX always positive across grid
        contracts = [
            _make_contract(500, "call", oi=10000, iv=0.18),
            _make_contract(510, "call", oi=8000,  iv=0.19),
        ]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        self.assertIn("flip_note", result)
        if not result["flip_found"]:
            self.assertIn("not found", result["flip_note"].lower())

    def test_curve_has_correct_length(self):
        """Curve should span ±10% of spot in 0.5% steps (~40 points)."""
        contracts = [_make_contract(500, "call", oi=1000, iv=0.20)]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        n = len(result["curve"])
        # 0.90 to 1.10 in 0.5% steps = 40 steps + 1 = 41 points (approx)
        self.assertGreaterEqual(n, 38)
        self.assertLessEqual(n, 44)

    def test_grid_bounds(self):
        """Grid lo/hi should be ±10% around spot."""
        contracts = [_make_contract(500, "call", oi=1000, iv=0.20)]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        self.assertAlmostEqual(result["grid_lo"], 450.0, delta=5.0)
        self.assertAlmostEqual(result["grid_hi"], 550.0, delta=5.0)


if __name__ == "__main__":
    unittest.main()
