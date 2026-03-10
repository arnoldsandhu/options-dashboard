"""
Unit tests for gex_engine.exposure_calc — calc_chain_metrics with known fixture.

All expected values are derived from fixtures.compute_expected(), which is an
independent reference implementation that mirrors the production logic using only
standard-library math (no import of exposure_calc or greek_calc).

Sign mode variants:
  classic      — calls +1, puts −1
  unsigned     — all contracts +1 (no sign flip for puts)
  Both are verified here; heuristic/customer_long follow classic sign so they
  need only order-of-magnitude checks.
"""
import math
import unittest

from gex_engine.exposure_calc import calc_chain_metrics
from gex_engine.tests.fixtures import CHAIN, SPOT, EXPECTED, compute_expected


# ── Aggregate metrics against reference implementation ─────────────────────────

class TestCalcChainMetricsVsFixture(unittest.TestCase):
    """calc_chain_metrics must match the independently computed EXPECTED dict."""

    def setUp(self):
        self.result = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct")

    def test_net_gex_matches_reference(self):
        """net_gex must match reference implementation within floating-point tolerance."""
        self.assertAlmostEqual(
            self.result["net_gex"],
            EXPECTED["net_gex"],
            delta=abs(EXPECTED["net_gex"]) * 1e-4 + 1.0,   # 0.01% + $1 tolerance
        )

    def test_abs_gex_matches_reference(self):
        self.assertAlmostEqual(
            self.result["abs_gex"],
            EXPECTED["abs_gex"],
            delta=abs(EXPECTED["abs_gex"]) * 1e-4 + 1.0,
        )

    def test_abs_gex_ge_abs_net_gex(self):
        """Absolute GEX must always be ≥ |net GEX|."""
        self.assertGreaterEqual(
            self.result["abs_gex"],
            abs(self.result["net_gex"]) - 1e-6,
        )

    def test_net_gex_0dte_matches_reference(self):
        self.assertAlmostEqual(
            self.result["net_gex_0dte"],
            EXPECTED["net_gex_0dte"],
            delta=abs(EXPECTED["net_gex_0dte"]) * 1e-3 + 1.0,
        )

    def test_abs_gex_0dte_matches_reference(self):
        self.assertAlmostEqual(
            self.result["abs_gex_0dte"],
            EXPECTED["abs_gex_0dte"],
            delta=abs(EXPECTED["abs_gex_0dte"]) * 1e-3 + 1.0,
        )

    def test_0dte_abs_le_total_abs(self):
        """0DTE abs GEX cannot exceed total abs GEX."""
        self.assertLessEqual(
            abs(self.result["abs_gex_0dte"]),
            self.result["abs_gex"] + 1e-6,
        )


# ── Diagnostics counters ───────────────────────────────────────────────────────

class TestDiagnostics(unittest.TestCase):

    def setUp(self):
        self.result = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct")
        self.diag   = self.result["diagnostics"]

    def test_zero_oi_count_matches_fixture(self):
        """Fixture has 2 zero-OI contracts (indices 13 and 14)."""
        self.assertEqual(self.diag["zero_oi"], EXPECTED["n_zero_oi"])

    def test_included_count_matches_fixture(self):
        """Included = total valid contracts with non-zero gamma."""
        self.assertEqual(self.diag["included"], EXPECTED["n_included"])

    def test_bs_computed_count_matches_fixture(self):
        """BS-computed count must equal reference n_bs from fixture."""
        self.assertEqual(self.diag["bs_computed"], EXPECTED["n_bs"])

    def test_vendor_fallback_count_matches_fixture(self):
        """Vendor-fallback count must equal reference n_vendor from fixture."""
        self.assertEqual(self.diag["vendor_fallback"], EXPECTED["n_vendor"])

    def test_bs_plus_vendor_equals_included(self):
        """bs_computed + vendor_fallback should equal included contracts."""
        self.assertEqual(
            self.diag["bs_computed"] + self.diag["vendor_fallback"],
            self.diag["included"],
        )

    def test_total_is_included_plus_zero_oi(self):
        self.assertEqual(
            self.diag["total"],
            self.diag["included"] + self.diag["zero_oi"],
        )


# ── Strike-level GEX dictionaries ─────────────────────────────────────────────

class TestStrikeLevelGex(unittest.TestCase):

    def setUp(self):
        self.result = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct")

    def test_strike_gex_values_are_nonnegative(self):
        """strike_gex stores absolute values (always ≥ 0)."""
        for strike, val in self.result["strike_gex"].items():
            self.assertGreaterEqual(val, 0.0, f"strike {strike} has negative strike_gex")

    def test_call_gex_values_are_positive(self):
        """call_gex_by_s entries should be positive (calls = +sign)."""
        for strike, val in self.result["call_gex_by_s"].items():
            self.assertGreater(val, 0.0, f"call strike {strike} has non-positive gex")

    def test_put_gex_values_are_negative(self):
        """put_gex_by_s entries should be negative (puts = −sign) in classic mode."""
        for strike, val in self.result["put_gex_by_s"].items():
            self.assertLess(val, 0.0, f"put strike {strike} has non-negative gex")

    def test_atm_strike_has_highest_call_gex(self):
        """Strike 500 (ATM, highest OI) should be the largest call GEX contributor."""
        call_gex = self.result["call_gex_by_s"]
        if 500.0 not in call_gex:
            self.skipTest("ATM call not in chain")
        max_strike = max(call_gex, key=lambda k: abs(call_gex[k]))
        self.assertEqual(max_strike, 500.0)

    def test_atm_strike_has_highest_put_gex(self):
        """Strike 500 (ATM put, second highest OI) should be the largest put GEX contributor."""
        put_gex = self.result["put_gex_by_s"]
        if 500.0 not in put_gex:
            self.skipTest("ATM put not in chain")
        max_strike = max(put_gex, key=lambda k: abs(put_gex[k]))
        self.assertEqual(max_strike, 500.0)

    def test_zero_oi_strikes_absent_from_gex_dicts(self):
        """Strikes 505 and 498 (OI=0) must not appear in any GEX dict."""
        for d in (self.result["strike_gex"],
                  self.result["call_gex_by_s"],
                  self.result["put_gex_by_s"]):
            self.assertNotIn(505.0, d, "Zero-OI strike 505 must be excluded")
            self.assertNotIn(498.0, d, "Zero-OI strike 498 must be excluded")

    def test_zero_strike_absent_from_gex_dicts(self):
        """Strike=0 (invalid contract) must not appear in any GEX dict."""
        for d in (self.result["strike_gex"],
                  self.result["call_gex_by_s"],
                  self.result["put_gex_by_s"]):
            self.assertNotIn(0.0, d, "Strike=0 contract must be excluded")


# ── Sign mode switching ────────────────────────────────────────────────────────

class TestSignModes(unittest.TestCase):

    def test_unsigned_abs_gex_matches_classic(self):
        """
        In unsigned mode all puts get sign +1, so abs_gex equals net_gex.
        abs_gex in unsigned mode should equal sum of all |contributions|,
        which is the same as abs_gex in classic mode (absolute values don't depend on sign).
        """
        classic  = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="classic")
        unsigned = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="unsigned")
        self.assertAlmostEqual(
            classic["abs_gex"],
            unsigned["abs_gex"],
            delta=classic["abs_gex"] * 1e-4 + 1.0,
        )

    def test_unsigned_net_gex_equals_abs_gex(self):
        """In unsigned mode, all contributions are positive, so net == abs."""
        result = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="unsigned")
        self.assertAlmostEqual(
            result["net_gex"],
            result["abs_gex"],
            delta=result["abs_gex"] * 1e-6,
        )

    def test_unsigned_put_gex_is_positive(self):
        """In unsigned mode, put_gex_by_s entries should be positive."""
        result = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="unsigned")
        for strike, val in result["put_gex_by_s"].items():
            self.assertGreater(val, 0.0,
                               f"unsigned put at {strike} should be positive, got {val}")

    def test_classic_and_unsigned_same_call_gex(self):
        """Call GEX is the same in both modes (calls always get +1 sign)."""
        classic  = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="classic")
        unsigned = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="unsigned")
        for strike in classic["call_gex_by_s"]:
            self.assertAlmostEqual(
                classic["call_gex_by_s"][strike],
                unsigned["call_gex_by_s"][strike],
                delta=abs(classic["call_gex_by_s"][strike]) * 1e-6 + 1e-3,
            )

    def test_classic_net_gex_less_than_unsigned(self):
        """
        In classic mode, put contributions subtract from net_gex.
        Therefore classic net_gex < unsigned net_gex (which adds all contributions).
        """
        classic  = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="classic")
        unsigned = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="unsigned")
        self.assertLessEqual(classic["net_gex"], unsigned["net_gex"])

    def test_bs_vendor_counts_identical_across_modes(self):
        """
        Sign mode should not affect gamma source counts — same contracts are included.
        """
        classic  = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="classic")
        unsigned = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct", sign_mode="unsigned")
        self.assertEqual(
            classic["diagnostics"]["bs_computed"],
            unsigned["diagnostics"]["bs_computed"],
        )
        self.assertEqual(
            classic["diagnostics"]["vendor_fallback"],
            unsigned["diagnostics"]["vendor_fallback"],
        )


# ── 0DTE filter ────────────────────────────────────────────────────────────────

class TestZeroDteFilter(unittest.TestCase):

    def setUp(self):
        self.result = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct")

    def test_0dte_gex_is_subset_of_total(self):
        """0DTE abs GEX should be a subset (≤) of total abs GEX."""
        self.assertLessEqual(
            self.result["abs_gex_0dte"],
            self.result["abs_gex"] + 1e-6,
        )

    def test_0dte_net_gex_nonzero_when_contracts_present(self):
        """
        Fixture includes two 0DTE contracts (indices 8 and 9) with OI > 0 and IV > 0.
        0DTE metrics should be non-zero.
        """
        # Reference implementation confirms 0DTE contracts have non-zero contribution
        if EXPECTED["net_gex_0dte"] != 0.0:
            self.assertNotEqual(self.result["net_gex_0dte"], 0.0)

    def test_today_str_override(self):
        """Passing a past today_str means no contracts match 0DTE filter."""
        result = calc_chain_metrics(
            CHAIN, spot=SPOT, mode="1pct", today_str="1900-01-01"
        )
        self.assertEqual(result["net_gex_0dte"], 0.0)
        self.assertEqual(result["abs_gex_0dte"], 0.0)

    def test_flow_gex_0dte_present(self):
        """flow_gex_0dte key must be present in result."""
        self.assertIn("flow_gex_0dte", self.result)


# ── 1usd mode ─────────────────────────────────────────────────────────────────

class TestCalcMode1Usd(unittest.TestCase):

    def test_1usd_mode_produces_different_values(self):
        """
        In 1usd mode the formula is gamma*oi*mult*spot (not *spot²*0.01).
        At spot=500, 1pct / 1usd ≈ spot * 0.01 = 5.
        """
        r1pct = calc_chain_metrics(CHAIN, spot=SPOT, mode="1pct")
        r1usd = calc_chain_metrics(CHAIN, spot=SPOT, mode="1usd")
        # 1pct = gamma*oi*m*spot²*0.01; 1usd = gamma*oi*m*spot
        # ratio = spot * 0.01 = 5 for spot=500
        ratio = r1pct["abs_gex"] / r1usd["abs_gex"]
        self.assertAlmostEqual(ratio, SPOT * 0.01, delta=1.0)  # ≈ 5 ± 1

    def test_mode_key_in_result(self):
        r = calc_chain_metrics(CHAIN, spot=SPOT, mode="1usd")
        self.assertEqual(r["mode"], "1usd")

    def test_default_mode_is_1pct(self):
        r = calc_chain_metrics(CHAIN, spot=SPOT)
        self.assertEqual(r["mode"], "1pct")


# ── Edge cases ─────────────────────────────────────────────────────────────────

class TestEdgeCases(unittest.TestCase):

    def test_empty_chain_returns_zeros(self):
        result = calc_chain_metrics([], spot=500.0)
        self.assertEqual(result["net_gex"], 0.0)
        self.assertEqual(result["abs_gex"], 0.0)
        self.assertEqual(result["net_gex_0dte"], 0.0)
        self.assertEqual(result["abs_gex_0dte"], 0.0)

    def test_empty_chain_diagnostics(self):
        result = calc_chain_metrics([], spot=500.0)
        d = result["diagnostics"]
        self.assertEqual(d["included"], 0)
        self.assertEqual(d["zero_oi"], 0)
        self.assertEqual(d["bs_computed"], 0)
        self.assertEqual(d["vendor_fallback"], 0)

    def test_all_zero_oi_chain(self):
        from datetime import date, timedelta
        exp = (date.today() + timedelta(days=30)).isoformat()
        chain = [
            {"ticker": "T", "option_type": "call", "strike": 500.0, "oi": 0,
             "volume": 0, "iv": 0.20, "vendor_gamma": 0.0,
             "multiplier": 100, "expiration": exp, "rate": 0.05},
        ]
        result = calc_chain_metrics(chain, spot=500.0)
        self.assertEqual(result["net_gex"], 0.0)
        self.assertEqual(result["diagnostics"]["zero_oi"], 1)
        self.assertEqual(result["diagnostics"]["included"], 0)

    def test_unknown_option_type_skipped(self):
        """A contract with option_type='X' must be silently skipped."""
        from datetime import date, timedelta
        exp = (date.today() + timedelta(days=30)).isoformat()
        chain = [
            {"ticker": "T", "option_type": "X", "strike": 500.0, "oi": 1000,
             "volume": 10, "iv": 0.20, "vendor_gamma": 0.01,
             "multiplier": 100, "expiration": exp, "rate": 0.05},
        ]
        result = calc_chain_metrics(chain, spot=500.0)
        self.assertEqual(result["net_gex"], 0.0)
        self.assertEqual(result["diagnostics"]["included"], 0)

    def test_net_gex_signed_correctly(self):
        """
        Single call → net_gex positive.
        Single put  → net_gex negative.
        """
        from datetime import date, timedelta
        exp = (date.today() + timedelta(days=30)).isoformat()
        call_c = [{"ticker": "T", "option_type": "call", "strike": 500.0, "oi": 1000,
                   "volume": 10, "iv": 0.20, "vendor_gamma": 0.0,
                   "multiplier": 100, "expiration": exp, "rate": 0.05}]
        put_c  = [{"ticker": "T", "option_type": "put",  "strike": 500.0, "oi": 1000,
                   "volume": 10, "iv": 0.20, "vendor_gamma": 0.0,
                   "multiplier": 100, "expiration": exp, "rate": 0.05}]
        r_call = calc_chain_metrics(call_c, spot=500.0)
        r_put  = calc_chain_metrics(put_c,  spot=500.0)
        self.assertGreater(r_call["net_gex"], 0.0)
        self.assertLess(r_put["net_gex"],  0.0)

    def test_call_and_put_same_params_net_zero(self):
        """
        Equal call + put at same strike, same OI, same IV → net_gex = 0 (puts cancel calls).
        """
        from datetime import date, timedelta
        exp = (date.today() + timedelta(days=30)).isoformat()
        template = {"ticker": "T", "strike": 500.0, "oi": 1000, "volume": 10,
                    "iv": 0.20, "vendor_gamma": 0.0, "multiplier": 100,
                    "expiration": exp, "rate": 0.05}
        chain = [
            {**template, "option_type": "call"},
            {**template, "option_type": "put"},
        ]
        result = calc_chain_metrics(chain, spot=500.0)
        self.assertAlmostEqual(result["net_gex"], 0.0, places=2)
        self.assertGreater(result["abs_gex"], 0.0)


if __name__ == "__main__":
    unittest.main()
