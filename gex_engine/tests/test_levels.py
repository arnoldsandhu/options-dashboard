"""Unit tests for gex_engine.levels — wall ranking and regime classification."""
import unittest

from gex_engine.levels import find_walls, classify_regime, WallLevel


class TestFindWalls(unittest.TestCase):

    def setUp(self):
        # Signed call GEX (positive), signed put GEX (negative)
        self.call_gex = {500.0: 10e6, 510.0: 5e6, 520.0: 2e6, 530.0: 0.5e6}
        self.put_gex  = {490.0: -8e6, 480.0: -3e6, 470.0: -1e6}
        self.spot     = 500.0

    def test_top_call_wall_is_highest_call(self):
        result = find_walls(self.call_gex, self.put_gex, self.spot, top_n=3)
        self.assertEqual(result["call_walls"][0].strike, 500.0)

    def test_top_put_wall_is_highest_put(self):
        result = find_walls(self.call_gex, self.put_gex, self.spot, top_n=3)
        self.assertEqual(result["put_walls"][0].strike, 490.0)

    def test_returns_top_n(self):
        result = find_walls(self.call_gex, self.put_gex, self.spot, top_n=2)
        self.assertLessEqual(len(result["call_walls"]), 2)
        self.assertLessEqual(len(result["put_walls"]), 2)

    def test_magnet_score_positive(self):
        result = find_walls(self.call_gex, self.put_gex, self.spot, top_n=3)
        self.assertGreater(result["magnet_score"], 1.0)

    def test_wall_distance_sign(self):
        result = find_walls(self.call_gex, self.put_gex, self.spot, top_n=3)
        # Call wall at 500 = same as spot → distance ≈ 0
        self.assertAlmostEqual(result["call_walls"][0].distance_pct, 0.0, places=1)
        # 510 is above spot → positive distance
        self.assertGreater(result["call_walls"][1].distance_pct, 0.0)
        # Put wall at 490 → negative distance
        self.assertLess(result["put_walls"][0].distance_pct, 0.0)

    def test_empty_dicts(self):
        result = find_walls({}, {}, spot=500.0)
        self.assertEqual(result["call_walls"], [])
        self.assertEqual(result["put_walls"], [])


class TestClassifyRegime(unittest.TestCase):

    def test_strong_positive_regime(self):
        r = classify_regime(net_gex_m=500.0, abs_gex_m=600.0, abs_gex_0dte_m=10.0,
                            flip_point=None, spot=500.0, net_gex_pctile=85.0)
        self.assertIn("positive", r.label.lower())

    def test_strong_negative_regime(self):
        r = classify_regime(net_gex_m=-300.0, abs_gex_m=400.0, abs_gex_0dte_m=10.0,
                            flip_point=None, spot=500.0, net_gex_pctile=15.0)
        self.assertIn("negative", r.label.lower())

    def test_0dte_dominant_subflag(self):
        r = classify_regime(net_gex_m=100.0, abs_gex_m=100.0, abs_gex_0dte_m=40.0,
                            flip_point=None, spot=500.0, net_gex_pctile=60.0)
        self.assertIn("0DTE dominant", r.subflags)

    def test_near_flip_neutral(self):
        r = classify_regime(net_gex_m=10.0, abs_gex_m=200.0, abs_gex_0dte_m=5.0,
                            flip_point=500.3, spot=500.0, net_gex_pctile=50.0)
        self.assertIn("Neutral", r.label)

    def test_pinning_subflag(self):
        r = classify_regime(net_gex_m=200.0, abs_gex_m=300.0, abs_gex_0dte_m=5.0,
                            flip_point=500.4, spot=500.0, net_gex_pctile=65.0)
        self.assertIn("Pinning likely", r.subflags)


if __name__ == "__main__":
    unittest.main()
