"""
Unit tests for db.database._pctile_and_zscore — historical percentile math.

Reference formulas
------------------
  percentile = (count of series values STRICTLY LESS than value) / n * 100
  mean       = sum(series) / n
  variance   = sum((v − mean)²) / n          [population variance]
  z-score    = (value − mean) / std           [std = sqrt(variance)]

Edge cases
----------
  - Empty series      → (50.0, 0.0)
  - All identical     → z = 0.0, percentile depends on strict-less count
  - value < all       → percentile = 0.0
  - value > all       → percentile = 100.0

All reference values are computed by hand and annotated below.
"""
import math
import unittest

from db.database import _pctile_and_zscore


class TestPctileAndZscoreKnownValues(unittest.TestCase):
    """Verify against hand-computed reference results."""

    def test_middle_value_of_5(self):
        """
        series = [1, 2, 3, 4, 5], value = 3
        count < 3 = 2  →  pctile = 2/5*100 = 40.0
        mean = 3.0,  variance = (4+1+0+1+4)/5 = 2.0,  std = √2 ≈ 1.4142
        z = (3 − 3) / 1.4142 = 0.0
        """
        pct, z = _pctile_and_zscore(3.0, [1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertAlmostEqual(pct, 40.0, places=1)
        self.assertAlmostEqual(z,   0.0,  places=4)

    def test_highest_value_of_5(self):
        """
        series = [1, 2, 3, 4, 5], value = 5
        count < 5 = 4  →  pctile = 4/5*100 = 80.0
        z = (5 − 3) / 1.4142 ≈ 1.4142  [function rounds to 2 decimal places → 1.41]
        """
        pct, z = _pctile_and_zscore(5.0, [1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertAlmostEqual(pct, 80.0, places=1)
        # Function returns round(z, 2), so we compare at 2 decimal places
        self.assertAlmostEqual(z, round(math.sqrt(2), 2), places=2)

    def test_lowest_value_of_5(self):
        """
        series = [1, 2, 3, 4, 5], value = 1
        count < 1 = 0  →  pctile = 0.0
        z = (1 − 3) / 1.4142 ≈ −1.4142  [rounded → −1.41]
        """
        pct, z = _pctile_and_zscore(1.0, [1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertAlmostEqual(pct, 0.0, places=1)
        self.assertAlmostEqual(z, round(-math.sqrt(2), 2), places=2)

    def test_value_above_all(self):
        """
        series = [10, 20, 30], value = 40
        count < 40 = 3  →  pctile = 3/3*100 = 100.0
        mean = 20, variance = (100+0+100)/3 ≈ 66.67, std ≈ 8.165
        z = (40 − 20) / 8.165 ≈ 2.449  [rounded → 2.45]
        """
        pct, z = _pctile_and_zscore(40.0, [10.0, 20.0, 30.0])
        self.assertAlmostEqual(pct, 100.0, places=1)
        expected_std = math.sqrt((100 + 0 + 100) / 3)
        self.assertAlmostEqual(z, round(20.0 / expected_std, 2), places=2)

    def test_value_below_all(self):
        """
        series = [10, 20, 30], value = 5
        count < 5 = 0  →  pctile = 0.0
        z = (5 − 20) / std ≈ −1.837  [rounded → −1.84]
        """
        pct, z = _pctile_and_zscore(5.0, [10.0, 20.0, 30.0])
        self.assertAlmostEqual(pct, 0.0, places=1)
        expected_std = math.sqrt((100 + 0 + 100) / 3)
        self.assertAlmostEqual(z, round(-15.0 / expected_std, 2), places=2)

    def test_single_element_series_same_as_value(self):
        """
        series = [42], value = 42
        count < 42 = 0  →  pctile = 0.0
        std = 0 → z = 0.0 (guard against division by zero)
        """
        pct, z = _pctile_and_zscore(42.0, [42.0])
        self.assertAlmostEqual(pct, 0.0, places=1)
        self.assertAlmostEqual(z,   0.0, places=4)

    def test_single_element_series_value_greater(self):
        """
        series = [10], value = 20
        count < 20 = 1  →  pctile = 100.0,  z = 0.0 (std=0)
        """
        pct, z = _pctile_and_zscore(20.0, [10.0])
        self.assertAlmostEqual(pct, 100.0, places=1)
        self.assertAlmostEqual(z,   0.0,   places=4)

    def test_two_element_series(self):
        """
        series = [0, 100], value = 50
        count < 50 = 1  →  pctile = 1/2*100 = 50.0
        mean = 50, variance = (2500+2500)/2 = 2500, std = 50
        z = (50−50)/50 = 0.0
        """
        pct, z = _pctile_and_zscore(50.0, [0.0, 100.0])
        self.assertAlmostEqual(pct, 50.0, places=1)
        self.assertAlmostEqual(z,   0.0,  places=4)

    def test_large_series_median(self):
        """
        series = [1..100], value = 50
        count < 50 = 49  →  pctile = 49/100*100 = 49.0
        mean = 50.5, std = sqrt(sum of (i-50.5)² / 100)
        """
        series = list(range(1, 101))  # [1, 2, ..., 100]
        pct, z = _pctile_and_zscore(50.0, series)
        self.assertAlmostEqual(pct, 49.0, places=1)
        mean = sum(series) / len(series)   # = 50.5
        var  = sum((v - mean) ** 2 for v in series) / len(series)
        std  = math.sqrt(var)
        expected_z = (50.0 - mean) / std
        self.assertAlmostEqual(z, expected_z, places=2)

    def test_negative_values_in_series(self):
        """
        series = [-3, -1, 0, 1, 3], value = 1
        count < 1 = 3  →  pctile = 3/5*100 = 60.0
        mean = 0, variance = (9+1+0+1+9)/5 = 4.0, std = 2.0
        z = (1−0)/2.0 = 0.5
        """
        series = [-3.0, -1.0, 0.0, 1.0, 3.0]
        pct, z = _pctile_and_zscore(1.0, series)
        self.assertAlmostEqual(pct, 60.0, places=1)
        self.assertAlmostEqual(z,   0.5,  places=4)

    def test_all_identical_values_z_zero(self):
        """
        series = [5, 5, 5, 5], value = 5
        std = 0 → z = 0.0 (no division by zero)
        count < 5 = 0 → pctile = 0.0
        """
        pct, z = _pctile_and_zscore(5.0, [5.0, 5.0, 5.0, 5.0])
        self.assertAlmostEqual(pct, 0.0, places=1)
        self.assertAlmostEqual(z,   0.0, places=4)

    def test_value_between_all_identical(self):
        """
        series = [5, 5, 5, 5], value = 7
        count < 7 = 4 → pctile = 100.0, z = 0.0 (std=0)
        """
        pct, z = _pctile_and_zscore(7.0, [5.0, 5.0, 5.0, 5.0])
        self.assertAlmostEqual(pct, 100.0, places=1)
        self.assertAlmostEqual(z,     0.0, places=4)


class TestPctileAndZscoreEdgeCases(unittest.TestCase):
    """Boundary conditions: empty series, extreme values."""

    def test_empty_series_returns_defaults(self):
        """Empty series must return (50.0, 0.0) — neutral defaults."""
        pct, z = _pctile_and_zscore(42.0, [])
        self.assertEqual(pct, 50.0)
        self.assertEqual(z,   0.0)

    def test_return_is_two_tuple(self):
        """Return value must be a 2-tuple of floats."""
        result = _pctile_and_zscore(1.0, [0.0, 2.0])
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], float)
        self.assertIsInstance(result[1], float)

    def test_percentile_range_0_to_100(self):
        """Percentile must be in [0, 100] for any input."""
        series = list(range(10))
        for value in [-1.0, 0.0, 4.5, 9.0, 100.0]:
            pct, _ = _pctile_and_zscore(value, series)
            self.assertGreaterEqual(pct, 0.0)
            self.assertLessEqual(pct, 100.0)

    def test_z_score_is_finite(self):
        """z-score must be finite (no inf/nan) for reasonable inputs."""
        series = [float(i) for i in range(1, 21)]
        for value in [0.0, 5.0, 10.0, 20.0, 25.0]:
            _, z = _pctile_and_zscore(value, series)
            self.assertTrue(math.isfinite(z), f"z={z} is not finite for value={value}")

    def test_percentile_is_strictly_less_not_le(self):
        """
        The formula counts STRICTLY less than value — not less than or equal.
        series = [1, 2, 3, 4, 5], value = 3
        Strictly < 3: {1, 2}  → count = 2 → pctile = 40 (not 60)
        """
        pct, _ = _pctile_and_zscore(3.0, [1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertAlmostEqual(pct, 40.0, places=1)

    def test_large_series_extreme_high(self):
        """Value above 99% of 1000-element series → pctile close to 100."""
        series = [float(i) for i in range(1000)]
        pct, _ = _pctile_and_zscore(999.0, series)
        self.assertAlmostEqual(pct, 99.9, places=1)

    def test_large_series_extreme_low(self):
        """Value at position 0 in 1000-element series → pctile = 0."""
        series = [float(i) for i in range(1000)]
        pct, _ = _pctile_and_zscore(0.0, series)
        self.assertAlmostEqual(pct, 0.0, places=1)


class TestPctileMonotonicity(unittest.TestCase):
    """Percentile must be non-decreasing as value increases."""

    def test_monotone_increasing(self):
        """For a fixed series, percentile should be non-decreasing in value."""
        series = [10.0, 20.0, 30.0, 40.0, 50.0]
        values = [5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0]
        prev_pct = -1.0
        for v in values:
            pct, _ = _pctile_and_zscore(v, series)
            self.assertGreaterEqual(pct, prev_pct,
                                    f"pctile not monotone at value={v}: {pct} < {prev_pct}")
            prev_pct = pct

    def test_z_score_monotone_increasing(self):
        """z-score must be non-decreasing as value increases (for fixed series)."""
        series = [1.0, 2.0, 3.0, 4.0, 5.0]
        values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
        prev_z = float("-inf")
        for v in values:
            _, z = _pctile_and_zscore(v, series)
            self.assertGreaterEqual(z, prev_z - 1e-9,
                                    f"z not monotone at value={v}: {z} < {prev_z}")
            prev_z = z


if __name__ == "__main__":
    unittest.main()
