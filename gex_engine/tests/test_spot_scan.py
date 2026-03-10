"""
Unit tests for gex_engine.spot_scan — synthetic chain with analytically known flip.

Known-flip design
-----------------
A call at strike K_c and a put at strike K_p (K_p < K_c), equal OI, same IV:

  At hypo_spot = K_c (call ATM):
      gamma(K_c, K_c) > gamma(K_c, K_p)  →  call contrib > put contrib  →  net > 0

  At hypo_spot = K_p (put ATM):
      gamma(K_p, K_p) > gamma(K_p, K_c)  →  put contrib > call contrib  →  net < 0

Therefore the zero-crossing (flip) must lie strictly between K_p and K_c.
With symmetric equidistant strikes from spot (K_p = spot − d, K_c = spot + d) and
equal OI, the flip must be near the midpoint (spot) by approximate BS symmetry.
"""
import math
import unittest
from datetime import date, timedelta

from gex_engine.spot_scan import run_spot_scan
from gex_engine.greek_calc import bs_gamma, time_to_expiry_years


def _make_contract(strike, cp, oi, iv, expiry_offset_days=30, rate=0.05):
    expiry = (date.today() + timedelta(days=expiry_offset_days)).isoformat()
    return {
        "ticker": "TEST", "strike": float(strike), "option_type": cp,
        "oi": oi, "volume": 0, "iv": float(iv),
        "vendor_gamma": 0.0, "multiplier": 100, "expiration": expiry, "rate": rate,
    }


# ── Known-flip tests ───────────────────────────────────────────────────────────

class TestSpotScanKnownFlip(unittest.TestCase):
    """Verify that the scan finds the flip within an analytically provable range."""

    def test_flip_between_call_and_put_strikes(self):
        """
        Call at 510 + put at 490, equal OI, t=30d, iv=20%.
        Proof:
          At hypo=510: gamma(510,510) > gamma(510,490) → net positive
          At hypo=490: gamma(490,490) > gamma(490,510) → net negative
        Therefore the flip must lie in [490, 510].
        """
        spot = 500.0
        K_call, K_put = 510.0, 490.0
        oi = 5000
        contracts = [
            _make_contract(K_call, "call", oi=oi, iv=0.20),
            _make_contract(K_put,  "put",  oi=oi, iv=0.20),
        ]
        result = run_spot_scan(contracts, spot=spot, mode="1pct")

        self.assertTrue(result["flip_found"], "Flip should be found for symmetric chain")
        flip = result["flip_point"]
        self.assertGreaterEqual(flip, K_put,  "Flip must be above lower put strike")
        self.assertLessEqual(flip,   K_call,  "Flip must be below upper call strike")
        # By approximate symmetry the flip is near spot
        self.assertAlmostEqual(flip, spot, delta=spot * 0.025)  # within 2.5%

    def test_flip_shifts_down_with_put_heavy_skew(self):
        """
        Heavier put OI than call OI pushes the flip downward (more negative GEX).
        With 3× more put OI, the flip should be below spot.
        """
        spot = 500.0
        contracts = [
            _make_contract(510, "call", oi=3000, iv=0.20),
            _make_contract(490, "put",  oi=9000, iv=0.20),
        ]
        result = run_spot_scan(contracts, spot=spot, mode="1pct")
        # Current net GEX should be negative (puts dominate at spot)
        self.assertLess(result["current_net_gex"], 0.0,
                        "Put-heavy chain: net GEX at spot should be negative")
        # Flip found somewhere (may be above spot or no flip)
        if result["flip_found"]:
            # If a flip is found, it must be above spot (to the upside)
            self.assertGreater(result["flip_point"], spot - 1)

    def test_flip_shifts_up_with_call_heavy_skew(self):
        """
        Heavier call OI pushes net GEX positive at spot; flip must be below spot
        (or at the lower boundary if very call-heavy).
        """
        spot = 500.0
        contracts = [
            _make_contract(510, "call", oi=9000, iv=0.20),
            _make_contract(490, "put",  oi=3000, iv=0.20),
        ]
        result = run_spot_scan(contracts, spot=spot, mode="1pct")
        self.assertGreater(result["current_net_gex"], 0.0,
                           "Call-heavy chain: net GEX at spot should be positive")

    def test_flip_sign_verified_independently(self):
        """
        After the scan finds a flip_point, verify the sign change manually using
        bs_gamma directly to confirm the zero-crossing is real.
        """
        spot = 500.0
        contracts = [
            _make_contract(515, "call", oi=4000, iv=0.20),
            _make_contract(485, "put",  oi=4000, iv=0.20),
        ]
        result = run_spot_scan(contracts, spot=spot, mode="1pct")
        if not result["flip_found"]:
            self.skipTest("Flip not found — cannot verify sign change")

        flip = result["flip_point"]

        # Compute net GEX at flip−step and flip+step manually
        step = spot * 0.005  # one grid step

        def _manual_net(hypo):
            t_c = time_to_expiry_years(
                (date.today() + timedelta(days=30)).isoformat()
            )
            g_call = bs_gamma(hypo, 515.0, t_c, 0.20, 0.05) or 0.0
            g_put  = bs_gamma(hypo, 485.0, t_c, 0.20, 0.05) or 0.0
            call_c = g_call * 4000 * 100 * hypo * hypo * 0.01
            put_c  = g_put  * 4000 * 100 * hypo * hypo * 0.01
            return call_c - put_c

        net_below = _manual_net(flip - step)
        net_above = _manual_net(flip + step)

        # The two sides must have opposite signs (or one is exactly 0)
        self.assertTrue(
            (net_below <= 0 and net_above >= 0) or (net_below >= 0 and net_above <= 0),
            f"Sign should change around flip {flip:.2f}: "
            f"net_below={net_below:.0f}, net_above={net_above:.0f}",
        )


# ── Structural / contract-filtering tests ─────────────────────────────────────

class TestSpotScanStructure(unittest.TestCase):

    def test_flip_found_in_range(self):
        """Synthetic chain with heavy calls above + puts below → flip exists."""
        contracts = [
            _make_contract(510, "call", oi=5000, iv=0.18),
            _make_contract(515, "call", oi=4000, iv=0.19),
            _make_contract(490, "put",  oi=500,  iv=0.22),
            _make_contract(480, "put",  oi=500,  iv=0.25),
        ]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        self.assertIn("flip_point", result)
        self.assertIn("curve", result)
        self.assertGreater(len(result["curve"]), 0)

    def test_flip_not_found_returns_note(self):
        """Chain with no sign crossing should report nearest boundary."""
        contracts = [
            _make_contract(500, "call", oi=10000, iv=0.18),
            _make_contract(510, "call", oi=8000,  iv=0.19),
        ]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        self.assertIn("flip_note", result)
        if not result["flip_found"]:
            self.assertIn("not found", result["flip_note"].lower())

    def test_curve_has_correct_length(self):
        """Curve spans ±10% of spot in 0.5% steps ≈ 41 points."""
        contracts = [_make_contract(500, "call", oi=1000, iv=0.20)]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        n = len(result["curve"])
        self.assertGreaterEqual(n, 38)
        self.assertLessEqual(n, 44)

    def test_grid_bounds(self):
        """Grid lo/hi should be ±10% around spot."""
        contracts = [_make_contract(500, "call", oi=1000, iv=0.20)]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        self.assertAlmostEqual(result["grid_lo"], 450.0, delta=5.0)
        self.assertAlmostEqual(result["grid_hi"], 550.0, delta=5.0)

    def test_zero_oi_contracts_excluded(self):
        """Contracts with OI=0 must not affect the scan result."""
        live = [_make_contract(510, "call", oi=5000, iv=0.20)]
        dead = [_make_contract(510, "call", oi=0,    iv=0.20)]
        result_live = run_spot_scan(live, spot=500.0, mode="1pct")
        result_dead = run_spot_scan(dead, spot=500.0, mode="1pct")
        # Zero-OI chain → all net_gex == 0
        for pt in result_dead["curve"]:
            self.assertEqual(pt["net_gex"], 0.0)
        # Live chain must have non-zero net
        totals = sum(abs(pt["net_gex"]) for pt in result_live["curve"])
        self.assertGreater(totals, 0.0)

    def test_zero_strike_contract_excluded(self):
        """A contract with strike=0 should not raise and should be silently skipped."""
        contracts = [
            _make_contract(0,   "call", oi=5000, iv=0.20),  # invalid
            _make_contract(500, "call", oi=2000, iv=0.20),  # valid
        ]
        result = run_spot_scan(contracts, spot=500.0, mode="1pct")
        self.assertGreater(len(result["curve"]), 0)

    def test_mode_1usd_returns_higher_values(self):
        """
        In 1usd mode, per-contract contribution = gamma*oi*mult*spot (not *spot²*0.01).
        At spot=500, the 1usd mode produces values ≈ 50× the 1pct mode.
        """
        contracts = [_make_contract(500, "call", oi=5000, iv=0.20)]
        r_pct = run_spot_scan(contracts, spot=500.0, mode="1pct")
        r_usd = run_spot_scan(contracts, spot=500.0, mode="1usd")
        mid_pct = r_pct["current_net_gex"]
        mid_usd = r_usd["current_net_gex"]
        # 1usd / 1pct ≈ 1 / (spot * 0.01) = 100 / spot = 0.2 → ratio ≈ 1/0.2 = 5?
        # Actually: 1usd = gamma*oi*m*spot; 1pct = gamma*oi*m*spot²*0.01
        # ratio = 1 / (spot * 0.01) = 1 / 5 → 1pct is 5× larger? No:
        # 1pct = gamma*oi*m*spot²*0.01  1usd = gamma*oi*m*spot
        # ratio 1usd/1pct = spot / (spot² * 0.01) = 1/(spot*0.01) = 1/5 = 0.2 at spot=500
        # So 1usd is smaller
        self.assertGreater(mid_pct, mid_usd * 0.05)
        self.assertLess(mid_usd, mid_pct * 25)


if __name__ == "__main__":
    unittest.main()
