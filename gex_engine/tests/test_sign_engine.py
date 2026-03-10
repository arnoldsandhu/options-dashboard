"""Unit tests for gex_engine.sign_engine"""
import unittest

from gex_engine.sign_engine import get_sign, apply_sign_to_chain, MODES


_CALL = {"option_type": "call", "strike": 500.0, "oi": 1000, "volume": 100, "expiration": "2099-12-31", "iv": 0.20}
_PUT  = {"option_type": "put",  "strike": 480.0, "oi": 1000, "volume": 50,  "expiration": "2099-12-31", "iv": 0.22}


class TestGetSign(unittest.TestCase):

    def test_classic_call_is_positive(self):
        sign, _ = get_sign(_CALL, spot=500.0, mode="classic")
        self.assertEqual(sign, 1.0)

    def test_classic_put_is_negative(self):
        sign, _ = get_sign(_PUT, spot=500.0, mode="classic")
        self.assertEqual(sign, -1.0)

    def test_unsigned_is_always_positive(self):
        for cp in ("call", "put"):
            c = {**_CALL, "option_type": cp}
            sign, conf = get_sign(c, spot=500.0, mode="unsigned")
            self.assertEqual(sign, 1.0)
            self.assertEqual(conf, 100.0)

    def test_heuristic_call_sign_is_classic(self):
        sign, conf = get_sign(_CALL, spot=500.0, mode="heuristic")
        self.assertEqual(sign, 1.0)
        self.assertGreater(conf, 30.0)
        self.assertLessEqual(conf, 95.0)

    def test_invalid_mode_raises(self):
        with self.assertRaises(ValueError):
            get_sign(_CALL, spot=500.0, mode="bogus_mode")

    def test_all_modes_exist(self):
        for mode in MODES:
            sign, conf = get_sign(_CALL, spot=500.0, mode=mode)
            self.assertIn(sign, (-1.0, 1.0))


class TestApplySignToChain(unittest.TestCase):

    def test_enriches_contracts(self):
        chain = [_CALL, _PUT]
        enriched, avg_conf = apply_sign_to_chain(chain, spot=500.0, mode="classic")
        self.assertEqual(len(enriched), 2)
        self.assertIn("sign", enriched[0])
        self.assertIn("sign_confidence", enriched[0])
        self.assertEqual(enriched[0]["sign"], 1.0)
        self.assertEqual(enriched[1]["sign"], -1.0)
        self.assertGreater(avg_conf, 0)

    def test_empty_chain(self):
        enriched, avg_conf = apply_sign_to_chain([], spot=500.0, mode="classic")
        self.assertEqual(enriched, [])
        self.assertEqual(avg_conf, 0.0)


if __name__ == "__main__":
    unittest.main()
