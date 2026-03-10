"""
Unit tests for gex_engine.greek_calc

Known BS gamma values are computed by hand and verified independently.

Hand-calculation reference
--------------------------
gamma = N'(d1) / (S × iv × sqrt(t))
d1    = (ln(S/K) + (r + 0.5×iv²)×t) / (iv×sqrt(t))
N'(x) = exp(-0.5×x²) / sqrt(2π)

Case A: S=K=100, t=1, iv=0.20, r=0.05
    d1 = (0 + 0.07×1) / 0.20 = 0.35
    N'(0.35) = exp(-0.0613)/2.5066 = 0.9406/2.5066 = 0.37524
    gamma = 0.37524 / (100 × 0.20 × 1) = 0.018762

Case B: S=K=100, t=0.25, iv=0.20, r=0.0
    d1 = (0 + 0.5×0.04×0.25) / (0.20×0.5) = 0.005/0.10 = 0.05
    N'(0.05) = exp(-0.00125)/2.5066 = 0.99875/2.5066 = 0.39844
    gamma = 0.39844 / (100 × 0.20 × 0.5) = 0.039844

Case C: S=200, K=190, t=0.5, iv=0.25, r=0.05
    d1 = (ln(200/190) + (0.05+0.03125)×0.5) / (0.25×0.70711)
       = (0.051293 + 0.040625) / 0.17678 = 0.091918/0.17678 = 0.52002
    N'(0.52002) = exp(-0.13521)/2.5066 = 0.87354/2.5066 = 0.34849
    gamma = 0.34849 / (200 × 0.25 × 0.70711) = 0.34849/35.355 = 0.009858
"""
import math
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from gex_engine.greek_calc import bs_gamma, time_to_expiry_years, get_gamma, reset_fallback_counter


# ── Black-Scholes Gamma ────────────────────────────────────────────────────────

class TestBsGammaKnownValues(unittest.TestCase):
    """Verify bs_gamma against independently hand-calculated values."""

    def test_case_a_atm_1yr(self):
        """Case A: S=K=100, t=1yr, iv=20%, r=5% → gamma ≈ 0.018762"""
        g = bs_gamma(spot=100.0, strike=100.0, t=1.0, iv=0.20, rate=0.05)
        self.assertIsNotNone(g)
        self.assertAlmostEqual(g, 0.018762, places=4)

    def test_case_b_atm_3month_zero_rate(self):
        """Case B: S=K=100, t=0.25yr, iv=20%, r=0 → gamma ≈ 0.039844"""
        g = bs_gamma(spot=100.0, strike=100.0, t=0.25, iv=0.20, rate=0.0)
        self.assertIsNotNone(g)
        self.assertAlmostEqual(g, 0.039844, places=4)

    def test_case_c_itm_6month(self):
        """Case C: S=200, K=190, t=0.5yr, iv=25%, r=5% → gamma ≈ 0.009858"""
        g = bs_gamma(spot=200.0, strike=190.0, t=0.5, iv=0.25, rate=0.05)
        self.assertIsNotNone(g)
        self.assertAlmostEqual(g, 0.009858, places=4)

    def test_atm_always_positive_finite(self):
        """ATM options at any reasonable parameter set must produce positive finite gamma."""
        params = [
            (500.0, 500.0, 30/365, 0.20),
            (100.0, 100.0, 7/365,  0.50),
            (2000.0, 2000.0, 90/365, 0.15),
            (1.0,   1.0,   0.5,   0.80),
        ]
        for spot, strike, t, iv in params:
            with self.subTest(spot=spot, t=t, iv=iv):
                g = bs_gamma(spot, strike, t, iv)
                self.assertIsNotNone(g)
                self.assertGreater(g, 0.0)
                self.assertTrue(math.isfinite(g))

    def test_gamma_higher_for_atm_than_otm(self):
        """ATM must have higher gamma than equidistant OTM by equal distance."""
        spot = 500.0
        g_atm  = bs_gamma(spot, 500.0, 30/365, 0.20)
        g_itm  = bs_gamma(spot, 480.0, 30/365, 0.20)  # 4% ITM
        g_dotm = bs_gamma(spot, 540.0, 30/365, 0.20)  # 8% OTM
        self.assertGreater(g_atm, g_itm)
        self.assertGreater(g_atm, g_dotm)

    def test_gamma_decreases_as_expiry_approaches_for_otm(self):
        """Far-OTM option gamma decreases as expiry approaches (less time to recover)."""
        strike = 600.0   # 20% OTM
        spot   = 500.0
        g_1yr  = bs_gamma(spot, strike, 1.0,    0.20)
        g_1m   = bs_gamma(spot, strike, 30/365, 0.20)
        self.assertIsNotNone(g_1yr)
        self.assertIsNotNone(g_1m)
        self.assertGreater(g_1yr, g_1m)

    def test_gamma_approximately_symmetric_around_atm(self):
        """
        BS gamma is approximately (not exactly) symmetric for equidistant strikes.
        Due to log-normal distribution, gamma(S, S+d) ≠ gamma(S, S-d) exactly.
        We verify the relative difference is small (< 10%).
        """
        spot = 500.0
        g_call_otm = bs_gamma(spot, 510.0, 30/365, 0.20)   # 2% OTM call
        g_put_otm  = bs_gamma(spot, 490.0, 30/365, 0.20)   # 2% OTM put
        self.assertIsNotNone(g_call_otm)
        self.assertIsNotNone(g_put_otm)
        rel_diff = abs(g_call_otm - g_put_otm) / max(g_call_otm, g_put_otm)
        self.assertLess(rel_diff, 0.10, "Gamma should be within 10% for equidistant strikes")


class TestBsGammaInvalidInputs(unittest.TestCase):

    def test_zero_spot(self):
        self.assertIsNone(bs_gamma(0.0, 100.0, 0.1, 0.20))

    def test_zero_strike(self):
        self.assertIsNone(bs_gamma(100.0, 0.0, 0.1, 0.20))

    def test_zero_time(self):
        self.assertIsNone(bs_gamma(100.0, 100.0, 0.0, 0.20))

    def test_zero_iv(self):
        self.assertIsNone(bs_gamma(100.0, 100.0, 0.1, 0.0))

    def test_negative_spot(self):
        self.assertIsNone(bs_gamma(-100.0, 100.0, 0.1, 0.20))

    def test_negative_strike(self):
        self.assertIsNone(bs_gamma(100.0, -100.0, 0.1, 0.20))

    def test_negative_time(self):
        self.assertIsNone(bs_gamma(100.0, 100.0, -0.1, 0.20))

    def test_negative_iv(self):
        self.assertIsNone(bs_gamma(100.0, 100.0, 0.1, -0.20))


# ── Time-to-Expiry ─────────────────────────────────────────────────────────────

class TestTimeToExpiry(unittest.TestCase):

    def test_30_calendar_days(self):
        exp = (date.today() + timedelta(days=30)).isoformat()
        t = time_to_expiry_years(exp)
        self.assertAlmostEqual(t, 30 / 365.0, places=4)

    def test_1_calendar_day(self):
        exp = (date.today() + timedelta(days=1)).isoformat()
        t = time_to_expiry_years(exp)
        self.assertAlmostEqual(t, 1 / 365.0, places=4)

    def test_365_calendar_days(self):
        exp = (date.today() + timedelta(days=365)).isoformat()
        t = time_to_expiry_years(exp)
        self.assertAlmostEqual(t, 1.0, places=3)

    def test_expired_yesterday_returns_zero(self):
        exp = (date.today() - timedelta(days=1)).isoformat()
        self.assertEqual(time_to_expiry_years(exp), 0.0)

    def test_expired_last_week_returns_zero(self):
        exp = (date.today() - timedelta(days=7)).isoformat()
        self.assertEqual(time_to_expiry_years(exp), 0.0)

    def test_same_day_during_market_hours_returns_positive(self):
        """0DTE during market hours must return a positive TTE."""
        from zoneinfo import ZoneInfo
        ET = ZoneInfo("America/New_York")
        fake_now = datetime.now(ET).replace(hour=11, minute=30, second=0, microsecond=0)
        with patch("gex_engine.greek_calc.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            t = time_to_expiry_years(date.today().isoformat())
        self.assertGreater(t, 0.0)
        # 4.5 hours remaining out of 252*6.5 trading hours per year
        expected_max = 5.0 / (252 * 6.5)
        self.assertLess(t, expected_max)

    def test_same_day_after_market_close_returns_zero(self):
        """0DTE after market close must return 0."""
        from zoneinfo import ZoneInfo
        ET = ZoneInfo("America/New_York")
        fake_now = datetime.now(ET).replace(hour=16, minute=1, second=0, microsecond=0)
        with patch("gex_engine.greek_calc.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            t = time_to_expiry_years(date.today().isoformat())
        self.assertEqual(t, 0.0)

    def test_same_day_at_open_returns_near_full_day(self):
        """0DTE at market open should be close to 6.5 hours / trading year."""
        from zoneinfo import ZoneInfo
        ET = ZoneInfo("America/New_York")
        fake_now = datetime.now(ET).replace(hour=9, minute=30, second=0, microsecond=0)
        with patch("gex_engine.greek_calc.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            t = time_to_expiry_years(date.today().isoformat())
        expected = 6.5 / (252 * 6.5)
        self.assertAlmostEqual(t, expected, places=5)

    def test_invalid_string_returns_zero(self):
        self.assertEqual(time_to_expiry_years("not-a-date"), 0.0)

    def test_empty_string_returns_zero(self):
        self.assertEqual(time_to_expiry_years(""), 0.0)

    def test_reference_date_override(self):
        """reference_date param should override date.today()."""
        ref  = date(2026, 6, 1)
        exp  = "2026-07-01"
        t = time_to_expiry_years(exp, reference_date=ref)
        self.assertAlmostEqual(t, 30 / 365.0, places=4)


# ── get_gamma priority logic ───────────────────────────────────────────────────

class TestGetGamma(unittest.TestCase):

    def setUp(self):
        reset_fallback_counter()

    def _contract(self, iv, vendor_gamma, offset_days=30, cp="call"):
        return {
            "ticker":       "TEST",
            "strike":       500.0,
            "option_type":  cp,
            "expiration":   (date.today() + timedelta(days=offset_days)).isoformat(),
            "iv":           iv,
            "vendor_gamma": vendor_gamma,
        }

    def test_bs_preferred_over_vendor_when_iv_present(self):
        """With iv > 0 and t > 0, BS gamma should be used (not vendor)."""
        c = self._contract(iv=0.20, vendor_gamma=0.9999)  # vendor is absurdly large
        g = get_gamma(c, spot=500.0)
        # BS gamma at ATM 30-DTE is ~0.013; vendor=0.9999 would be obvious if used
        self.assertLess(g, 0.1)

    def test_vendor_used_when_iv_zero(self):
        """With iv == 0, vendor_gamma should be returned exactly."""
        c = self._contract(iv=0.0, vendor_gamma=0.007777)
        g = get_gamma(c, spot=500.0)
        self.assertAlmostEqual(g, 0.007777, places=6)

    def test_zero_returned_when_no_iv_and_no_vendor(self):
        """With iv=0 and vendor_gamma=0, must return 0.0."""
        c = self._contract(iv=0.0, vendor_gamma=0.0)
        g = get_gamma(c, spot=500.0)
        self.assertEqual(g, 0.0)

    def test_bs_and_vendor_same_order_of_magnitude_at_atm(self):
        """BS and vendor should agree in order of magnitude for reasonable inputs."""
        c = self._contract(iv=0.20, vendor_gamma=0.014)  # plausible vendor value
        g = get_gamma(c, spot=500.0)
        self.assertGreater(g, 0.005)
        self.assertLess(g, 0.10)

    def test_0dte_expired_falls_to_vendor(self):
        """Expired 0DTE (t=0) cannot use BS; vendor should be returned."""
        c = self._contract(iv=0.30, vendor_gamma=0.05, offset_days=-1)
        g = get_gamma(c, spot=500.0)
        # iv > 0 but t = 0, so bs_gamma returns None → vendor 0.05 used
        self.assertAlmostEqual(g, 0.05, places=4)

    def test_gamma_always_nonnegative(self):
        """get_gamma should never return a negative value."""
        for iv, vg in [(0.20, 0.01), (0.0, 0.005), (0.0, 0.0)]:
            c = self._contract(iv=iv, vendor_gamma=vg)
            self.assertGreaterEqual(get_gamma(c, spot=500.0), 0.0)


if __name__ == "__main__":
    unittest.main()
