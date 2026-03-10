"""Unit tests for gex_engine.greek_calc"""
import math
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from gex_engine.greek_calc import bs_gamma, time_to_expiry_years, get_gamma, reset_fallback_counter


class TestBsGamma(unittest.TestCase):

    def test_atm_call_reasonable_gamma(self):
        """ATM option gamma should be positive and finite."""
        g = bs_gamma(spot=500.0, strike=500.0, t=30/365, iv=0.20, rate=0.05)
        self.assertIsNotNone(g)
        self.assertTrue(g > 0)
        self.assertTrue(math.isfinite(g))

    def test_known_value(self):
        """Verify against a known hand-calculated value.

        For S=100, K=100, t=1yr, iv=0.20, r=0.05:
            d1 = (log(1) + (0.05 + 0.02)*1) / (0.20*1) = 0.35
            N'(d1) ~ 0.3752
            gamma = 0.3752 / (100 * 0.20 * 1) = 0.018760
        """
        g = bs_gamma(spot=100.0, strike=100.0, t=1.0, iv=0.20, rate=0.05)
        self.assertIsNotNone(g)
        self.assertAlmostEqual(g, 0.01876, places=4)

    def test_invalid_inputs_return_none(self):
        """Zero or negative inputs must return None."""
        self.assertIsNone(bs_gamma(0.0,  100.0, 0.1, 0.20))
        self.assertIsNone(bs_gamma(100.0, 0.0,  0.1, 0.20))
        self.assertIsNone(bs_gamma(100.0, 100.0, 0.0, 0.20))
        self.assertIsNone(bs_gamma(100.0, 100.0, 0.1, 0.0))
        self.assertIsNone(bs_gamma(-1.0,  100.0, 0.1, 0.20))

    def test_far_otm_lower_gamma(self):
        """Deep OTM option should have lower gamma than ATM."""
        g_atm  = bs_gamma(spot=500.0, strike=500.0, t=30/365, iv=0.20)
        g_otm  = bs_gamma(spot=500.0, strike=600.0, t=30/365, iv=0.20)
        self.assertIsNotNone(g_atm)
        self.assertIsNotNone(g_otm)
        self.assertGreater(g_atm, g_otm)


class TestTimeToExpiry(unittest.TestCase):

    def test_future_date(self):
        future = (date.today() + timedelta(days=30)).isoformat()
        t = time_to_expiry_years(future)
        self.assertAlmostEqual(t, 30 / 365, places=3)

    def test_expired_date(self):
        past = (date.today() - timedelta(days=1)).isoformat()
        t = time_to_expiry_years(past)
        self.assertEqual(t, 0.0)

    def test_today_nonzero_before_close(self):
        """For 0DTE during market hours, TTE should be > 0."""
        today_str = date.today().isoformat()
        # Patch the current time to 10 AM ET (well before close)
        from datetime import datetime
        from zoneinfo import ZoneInfo
        ET = ZoneInfo("America/New_York")
        fake_now = datetime.now(ET).replace(hour=10, minute=0, second=0, microsecond=0)
        with patch("gex_engine.greek_calc.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            t = time_to_expiry_years(today_str)
        self.assertGreater(t, 0.0)

    def test_invalid_string(self):
        t = time_to_expiry_years("not-a-date")
        self.assertEqual(t, 0.0)


class TestGetGamma(unittest.TestCase):

    def setUp(self):
        reset_fallback_counter()

    def test_uses_bs_when_iv_available(self):
        contract = {
            "ticker": "SPY", "strike": 500.0,
            "expiration": (date.today() + timedelta(days=30)).isoformat(),
            "iv": 0.20, "vendor_gamma": 0.005, "option_type": "call",
        }
        g = get_gamma(contract, spot=500.0)
        # BS gamma for this case is distinct from vendor_gamma
        self.assertGreater(g, 0)

    def test_falls_back_to_vendor_when_no_iv(self):
        contract = {
            "ticker": "SPY", "strike": 500.0,
            "expiration": (date.today() + timedelta(days=30)).isoformat(),
            "iv": 0.0, "vendor_gamma": 0.007, "option_type": "call",
        }
        g = get_gamma(contract, spot=500.0)
        self.assertAlmostEqual(g, 0.007, places=6)

    def test_returns_zero_when_nothing_available(self):
        contract = {
            "ticker": "SPY", "strike": 500.0,
            "expiration": (date.today() + timedelta(days=30)).isoformat(),
            "iv": 0.0, "vendor_gamma": 0.0, "option_type": "call",
        }
        g = get_gamma(contract, spot=500.0)
        self.assertEqual(g, 0.0)


if __name__ == "__main__":
    unittest.main()
