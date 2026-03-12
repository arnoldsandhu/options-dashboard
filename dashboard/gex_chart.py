"""Per-strike GEX breakdown for the dashboard chart panel."""
from datetime import date
import math

from polygon import RESTClient

from config import POLYGON_API_KEY, GEX_HISTORY_DAYS
from db.database import (
    save_gex_snapshot, get_today_gex_snapshots, get_gex_percentile,
    save_gex_metrics, get_gex_percentiles_30d,
)
from gex_engine.vendor_ingest import fetch_chain, fetch_spot_fallback
from gex_engine.greek_calc import get_gamma, reset_fallback_counter
from gex_engine.contract_model import normalize_chain
from gex_engine.exposure_calc import calc_chain_metrics, gex_1pct, calc_probabilistic_range
from gex_engine.sign_engine import get_sign as _get_sign
from gex_engine.spot_scan import run_spot_scan
from gex_engine.levels import compute_levels

CHART_TICKERS = ["SPY", "QQQ", "SPX"]
STRIKE_BAND_PCT = 0.05   # ±5% around spot
MAX_STRIKES = 50         # cap bars per chart

# Map from options-chain underlying to price-bar ticker
_PRICE_TICKER = {"SPX": "I:SPX"}


def get_gex_by_strike(ticker: str) -> dict:
    """Compute per-strike GEX breakdown for one ticker.

    Returns a dict with:
      ticker, spot, net_gex_m, regime, regime_summary,
      strikes (list of {strike, call_gex_m, put_gex_m, net_gex_m}),
      zdtes   (same, filtered to today's expiry),
      key_strikes (list of float),
      price_bars  (list of {t, o, h, l, c}).
    """
    reset_fallback_counter()
    today_str = date.today().isoformat()

    try:
        spot, raw_contracts = fetch_chain(ticker)
    except Exception as e:
        print(f"  [gex_chart] chain error {ticker}: {e}")
        return _empty(ticker, str(e))

    if spot == 0.0:
        return _empty(ticker, "no spot price")

    contracts = normalize_chain(raw_contracts, ticker)

    # Use exposure_calc for all GEX math (1% move — primary metric)
    # Classic signed mode (calls +1, puts -1) — primary
    metrics = calc_chain_metrics(contracts, spot, mode="1pct", today_str=today_str,
                                 sign_mode="classic")
    # Unsigned mode (|gamma| only, no directional sign)
    metrics_unsigned = calc_chain_metrics(contracts, spot, mode="1pct", today_str=today_str,
                                          sign_mode="unsigned")

    call_gex_by_s  = metrics["call_gex_by_s"]
    put_gex_by_s   = metrics["put_gex_by_s"]
    strike_gex_abs = metrics["strike_gex"]
    net_gex_raw    = metrics["net_gex"]        # in 1%-move $ units
    net_gex_0dte   = metrics["net_gex_0dte"]
    flow_gex_0dte  = metrics["flow_gex_0dte"]
    abs_gex_0dte   = metrics["abs_gex_0dte"]
    diagnostics    = metrics["diagnostics"]

    lo = spot * (1 - STRIKE_BAND_PCT)
    hi = spot * (1 + STRIKE_BAND_PCT)
    all_strikes = sorted(set(call_gex_by_s) | set(put_gex_by_s))

    def _rows(c_dict: dict, p_dict: dict, strikes_list: list) -> list:
        rows = []
        for s in strikes_list:
            if not (lo <= s <= hi):
                continue
            cg = c_dict.get(s, 0.0) / 1e6
            pg = p_dict.get(s, 0.0) / 1e6
            rows.append({
                "strike":     s,
                "call_gex_m": round(cg, 4),
                "put_gex_m":  round(-pg, 4),   # put_gex is negative; store magnitude
                "net_gex_m":  round(cg + pg, 4),  # already signed
            })
        return rows

    strike_rows = _rows(call_gex_by_s, put_gex_by_s, all_strikes)

    # Unsigned rows (absolute gamma only — puts shown positive)
    u_call = metrics_unsigned["call_gex_by_s"]
    u_put  = metrics_unsigned["put_gex_by_s"]
    u_all  = sorted(set(u_call) | set(u_put))
    unsigned_strike_rows = _rows(u_call, u_put, u_all)

    # 0DTE rows: filter contracts by expiry == today
    zdte_call_by_s: dict[float, float] = {}
    zdte_put_by_s:  dict[float, float] = {}
    for c in contracts:
        if c.get("expiration", "") != today_str:
            continue
        oi     = c.get("oi", 0)
        cp     = c.get("option_type", "")
        strike = c.get("strike", 0.0)
        mult   = c.get("multiplier", 100)
        if oi == 0 or strike == 0.0:
            continue
        gamma = get_gamma(c, spot)
        if gamma == 0.0:
            continue
        from gex_engine.exposure_calc import dealer_sign
        sign   = dealer_sign(cp)
        contrib = gex_1pct(gamma, oi, mult, spot, sign)
        if cp == "call":
            zdte_call_by_s[strike] = zdte_call_by_s.get(strike, 0.0) + contrib
        else:
            zdte_put_by_s[strike]  = zdte_put_by_s.get(strike, 0.0) + contrib

    zdte_all  = sorted(set(zdte_call_by_s) | set(zdte_put_by_s))
    zdte_rows = _rows(zdte_call_by_s, zdte_put_by_s, zdte_all)

    # Unsigned 0DTE rows
    uzdte_call_by_s: dict[float, float] = {}
    uzdte_put_by_s:  dict[float, float] = {}
    for c in contracts:
        if c.get("expiration", "") != today_str:
            continue
        oi     = c.get("oi", 0)
        cp     = c.get("option_type", "")
        strike = c.get("strike", 0.0)
        mult   = c.get("multiplier", 100)
        if oi == 0 or strike == 0.0:
            continue
        gamma = get_gamma(c, spot)
        if gamma == 0.0:
            continue
        contrib = gex_1pct(gamma, oi, mult, spot, 1.0)  # sign=+1 (unsigned)
        if cp == "call":
            uzdte_call_by_s[strike] = uzdte_call_by_s.get(strike, 0.0) + contrib
        else:
            uzdte_put_by_s[strike]  = uzdte_put_by_s.get(strike, 0.0) + contrib
    uzdte_all  = sorted(set(uzdte_call_by_s) | set(uzdte_put_by_s))
    unsigned_zdte_rows = _rows(uzdte_call_by_s, uzdte_put_by_s, uzdte_all)

    # Probabilistic mode: per-contract p_dealer_short with uncertainty band
    prob_range = calc_probabilistic_range(contracts, spot, mode="1pct", today_str=today_str)
    prob_base_m  = prob_range["net_gex_base"] / 1e6
    prob_low_m   = prob_range["net_gex_low"]  / 1e6
    prob_high_m  = prob_range["net_gex_high"] / 1e6
    prob_width_m = prob_range["uncertainty_width"] / 1e6
    prob_call    = prob_range["call_gex_by_s_base"]
    prob_put     = prob_range["put_gex_by_s_base"]
    prob_all     = sorted(set(prob_call) | set(prob_put))
    prob_strike_rows = _rows(prob_call, prob_put, prob_all)
    if len(prob_strike_rows) > MAX_STRIKES:
        prob_strike_rows.sort(key=lambda r: abs(r["net_gex_m"]), reverse=True)
        prob_strike_rows = prob_strike_rows[:MAX_STRIKES]
        prob_strike_rows.sort(key=lambda r: r["strike"])

    # Probabilistic 0DTE rows
    pzdte_call_by_s: dict[float, float] = {}
    pzdte_put_by_s:  dict[float, float] = {}
    from gex_engine.sign_engine import get_p_dealer_short as _gp
    for c in contracts:
        if c.get("expiration", "") != today_str:
            continue
        oi     = c.get("oi", 0)
        cp     = c.get("option_type", "")
        strike = c.get("strike", 0.0)
        mult   = c.get("multiplier", 100)
        if oi == 0 or strike == 0.0 or cp not in ("call", "put"):
            continue
        gamma = get_gamma(c, spot)
        if gamma == 0.0:
            continue
        p = _gp(c, spot)
        sign = 2.0 * p - 1.0
        contrib = gex_1pct(gamma, oi, mult, spot, sign)
        if cp == "call":
            pzdte_call_by_s[strike] = pzdte_call_by_s.get(strike, 0.0) + contrib
        else:
            pzdte_put_by_s[strike]  = pzdte_put_by_s.get(strike, 0.0) + contrib
    pzdte_all = sorted(set(pzdte_call_by_s) | set(pzdte_put_by_s))
    prob_zdte_rows = _rows(pzdte_call_by_s, pzdte_put_by_s, pzdte_all)

    # Heuristic mode: confidence-weighted signs via sign_engine (moneyness/TTE/volume)
    metrics_heuristic = calc_chain_metrics(contracts, spot, mode="1pct",
                                           today_str=today_str, sign_mode="heuristic")
    h_call = metrics_heuristic["call_gex_by_s"]
    h_put  = metrics_heuristic["put_gex_by_s"]
    h_all  = sorted(set(h_call) | set(h_put))
    heuristic_strike_rows = _rows(h_call, h_put, h_all)

    # Heuristic 0DTE rows
    hzdte_call_by_s: dict[float, float] = {}
    hzdte_put_by_s:  dict[float, float] = {}
    for c in contracts:
        if c.get("expiration", "") != today_str:
            continue
        oi     = c.get("oi", 0)
        cp     = c.get("option_type", "")
        strike = c.get("strike", 0.0)
        mult   = c.get("multiplier", 100)
        if oi == 0 or strike == 0.0 or cp not in ("call", "put"):
            continue
        gamma = get_gamma(c, spot)
        if gamma == 0.0:
            continue
        raw_sign, conf = _get_sign(c, spot, "heuristic")
        contrib = gex_1pct(gamma, oi, mult, spot, raw_sign * (conf / 100.0))
        if cp == "call":
            hzdte_call_by_s[strike] = hzdte_call_by_s.get(strike, 0.0) + contrib
        else:
            hzdte_put_by_s[strike]  = hzdte_put_by_s.get(strike, 0.0) + contrib
    hzdte_all = sorted(set(hzdte_call_by_s) | set(hzdte_put_by_s))
    heuristic_zdte_rows = _rows(hzdte_call_by_s, hzdte_put_by_s, hzdte_all)

    # Cap to MAX_STRIKES by keeping top abs(net_gex_m), then re-sort by strike
    if len(strike_rows) > MAX_STRIKES:
        strike_rows.sort(key=lambda r: abs(r["net_gex_m"]), reverse=True)
        strike_rows = strike_rows[:MAX_STRIKES]
        strike_rows.sort(key=lambda r: r["strike"])
    if len(unsigned_strike_rows) > MAX_STRIKES:
        unsigned_strike_rows.sort(key=lambda r: abs(r["net_gex_m"]), reverse=True)
        unsigned_strike_rows = unsigned_strike_rows[:MAX_STRIKES]
        unsigned_strike_rows.sort(key=lambda r: r["strike"])
    if len(heuristic_strike_rows) > MAX_STRIKES:
        heuristic_strike_rows.sort(key=lambda r: abs(r["net_gex_m"]), reverse=True)
        heuristic_strike_rows = heuristic_strike_rows[:MAX_STRIKES]
        heuristic_strike_rows.sort(key=lambda r: r["strike"])

    net_gex_m = net_gex_raw / 1e6
    unsigned_net_gex_m  = metrics_unsigned["net_gex"] / 1e6
    heuristic_net_gex_m = metrics_heuristic["net_gex"] / 1e6

    total_abs   = sum(abs(r["net_gex_m"]) for r in strike_rows) or 1.0
    key_strikes = [r["strike"] for r in strike_rows
                   if abs(r["net_gex_m"]) / total_abs >= 0.04]

    # ── Key Levels ────────────────────────────────────────────────────────────
    # Put Wall: strike with highest absolute put GEX
    put_wall_by_s  = {s: abs(v) for s, v in put_gex_by_s.items()}
    call_wall_by_s = {s: abs(v) for s, v in call_gex_by_s.items()}
    put_wall  = max(put_wall_by_s,  key=lambda s: put_wall_by_s[s],  default=None)
    call_wall = max(call_wall_by_s, key=lambda s: call_wall_by_s[s], default=None)

    # Max Gamma Strike: strike with highest absolute net GEX in the displayed band
    max_gamma_strike = max(
        strike_rows, key=lambda r: abs(r["net_gex_m"]), default=None
    )
    max_gamma_strike = max_gamma_strike["strike"] if max_gamma_strike else None

    # True Gamma Flip: spot-scan zero-crossing (replaces legacy strike-level approx)
    scan = run_spot_scan(contracts, spot, mode="1pct")
    flip_point  = scan["flip_point"]
    flip_note   = scan["flip_note"]
    flip_curve  = scan["curve"]   # [{spot, net_gex}, ...] for JPM-style chart

    # Stability % = percentile of current net GEX vs history
    try:
        stability_pct = round(get_gex_percentile(ticker, net_gex_raw,
                                                 days=GEX_HISTORY_DAYS), 1)
    except Exception:
        stability_pct = 50.0

    # Improved regime + walls from levels module
    levels = compute_levels(
        call_gex_by_s  = call_gex_by_s,
        put_gex_by_s   = put_gex_by_s,
        net_gex_m      = net_gex_m,
        abs_gex_m      = metrics["abs_gex"] / 1e6,
        abs_gex_0dte_m = abs_gex_0dte / 1e6,
        flip_point     = flip_point,
        spot           = spot,
        net_gex_pctile = stability_pct,
    )
    regime_label   = levels["regime_label"]
    regime_subflags = levels["regime_subflags"]

    # Legacy regime/regime_summary fields kept for backward compat
    regime = "positive" if net_gex_m >= 0 else "negative"
    regime_summary = (
        f"{regime_label}"
        + (f"  [{', '.join(regime_subflags)}]" if regime_subflags else "")
        + f"  —  Net GEX ${net_gex_m:+.1f}M (1% move, est.)"
    )

    price_ticker = _PRICE_TICKER.get(ticker, ticker)
    _poly_client = RESTClient(api_key=POLYGON_API_KEY)
    price_bars   = _fetch_intraday_bars(_poly_client, price_ticker, today_str)

    # Persist per-strike snapshot for the heatmap
    try:
        save_gex_snapshot(ticker, spot, strike_rows)
    except Exception as e:
        print(f"  [gex_chart] snapshot save error {ticker}: {e}")

    # Persist enhanced metrics for 30-day historical percentiles
    try:
        save_gex_metrics(
            ticker         = ticker,
            net_gex_m      = net_gex_m,
            abs_gex_m      = metrics["abs_gex"] / 1e6,
            net_gex_0dte_m = net_gex_0dte / 1e6,
            abs_gex_0dte_m = abs_gex_0dte / 1e6,
            flow_gex_0dte_m = flow_gex_0dte / 1e6,
            flip_point     = flip_point,
            regime_label   = regime_label,
            spot           = spot,
        )
    except Exception as e:
        print(f"  [gex_chart] metrics save error {ticker}: {e}")

    # 30-day granular percentiles
    try:
        pctiles_30d = get_gex_percentiles_30d(ticker, {
            "net_gex_m":      net_gex_m,
            "abs_gex_m":      metrics["abs_gex"] / 1e6,
            "abs_gex_0dte_m": abs_gex_0dte / 1e6,
        })
    except Exception:
        pctiles_30d = {}

    # Sign mode metadata for each of the 4 modes
    sign_modes = {
        "classic": {
            "strikes":   strike_rows,
            "zdtes":     zdte_rows,
            "net_gex_m": round(net_gex_m, 2),
            "label":     "Classic",
            "note":      "Standard dealer hedging convention — calls +1, puts \u22121 (ESTIMATED)",
        },
        "customer_long": {
            "strikes":   strike_rows,          # same bars as classic
            "zdtes":     zdte_rows,
            "net_gex_m": round(net_gex_m, 2),
            "label":     "Customer Long",
            "note":      "Assumes customers net long options; dealer net short. Confidence 80% (ESTIMATED)",
        },
        "heuristic": {
            "strikes":   heuristic_strike_rows,
            "zdtes":     heuristic_zdte_rows,
            "net_gex_m": round(heuristic_net_gex_m, 2),
            "label":     "Heuristic",
            "note":      "Confidence weighted by moneyness, TTE, and volume flow (ESTIMATED)",
        },
        "unsigned": {
            "strikes":   unsigned_strike_rows,
            "zdtes":     unsigned_zdte_rows,
            "net_gex_m": round(unsigned_net_gex_m, 2),
            "label":     "Unsigned",
            "note":      "Absolute gamma concentration only \u2014 no directional dealer sign applied",
        },
        "probabilistic": {
            "strikes":           prob_strike_rows,
            "zdtes":             prob_zdte_rows,
            "net_gex_m":         round(prob_base_m, 2),
            "net_gex_low_m":     round(prob_low_m, 2),
            "net_gex_high_m":    round(prob_high_m, 2),
            "uncertainty_width_m": round(prob_width_m, 2),
            "label":             "Probabilistic",
            "note":              "Per-contract p_dealer_short via delta/DTE/vol heuristics. Band = p\u2208[0.40, 0.95] scenarios (ESTIMATED)",
        },
    }

    return {
        "ticker":            ticker,
        "spot":              round(spot, 2),
        "net_gex_m":         round(net_gex_m, 2),      # 1%-move $ units / 1M
        "gex_mode":          "1pct",
        "sign_modes":        sign_modes,
        "regime":            regime,
        "regime_summary":    regime_summary,
        "stability_pct":     stability_pct,
        "strikes":           strike_rows,
        "zdtes":             zdte_rows,
        "key_strikes":       key_strikes,
        "price_bars":        price_bars,
        # Key levels
        "flip_point":        flip_point,
        "flip_note":         flip_note,
        "flip_curve":        flip_curve,
        "put_wall":          put_wall,
        "call_wall":         call_wall,
        "call_walls":        [w._asdict() for w in levels["call_walls"]],
        "put_walls":         [w._asdict() for w in levels["put_walls"]],
        "max_gamma_strike":  levels["max_abs_strike"],
        "magnet_score":      levels["magnet_score"],
        "regime_label":      regime_label,
        "regime_subflags":   regime_subflags,
        # 0DTE metrics
        "net_gex_0dte_m":    round(net_gex_0dte / 1e6, 2),
        "flow_gex_0dte_m":   round(flow_gex_0dte / 1e6, 2),
        "abs_gex_0dte_m":    round(abs_gex_0dte / 1e6, 2),
        # 30-day historical percentiles
        "pctiles_30d":       pctiles_30d,
        # Diagnostics
        "diagnostics":       diagnostics,
    }


def _empty(ticker: str, error: str) -> dict:
    return {
        "ticker":         ticker,
        "error":          error,
        "spot":           0,
        "net_gex_m":      0,
        "regime":         "unknown",
        "regime_summary": error,
        "strikes":        [],
        "zdtes":          [],
        "key_strikes":    [],
        "price_bars":     [],
    }


def _fetch_intraday_bars(client: RESTClient, ticker: str, date_str: str) -> list:
    try:
        bars = []
        for agg in client.get_aggs(
            ticker, 5, "minute",
            from_=date_str, to=date_str,
            adjusted=True, limit=1000,
        ):
            bars.append({
                "t": agg.timestamp,
                "o": agg.open,
                "h": agg.high,
                "l": agg.low,
                "c": agg.close,
            })
        return bars
    except Exception as e:
        print(f"  [gex_chart] price bars error {ticker}: {e}")
        return []


def get_gex_chart_data(tickers: list | None = None) -> dict:
    """Fetch GEX-by-strike for all chart tickers. Returns {ticker: data}."""
    if tickers is None:
        tickers = CHART_TICKERS
    result = {}
    for t in tickers:
        print(f"  [gex_chart] Fetching {t}...")
        result[t] = get_gex_by_strike(t)
    return result


def build_heatmap_payload(tickers: list | None = None) -> dict:
    """Build time×strike heatmap matrix from today's stored snapshots.

    Returns {ticker: {timestamps, strikes, matrix, spot_trace}} where
    matrix[strike_idx][time_idx] = net_gex_m at that strike and time.
    """
    if tickers is None:
        tickers = CHART_TICKERS
    result = {}
    for ticker in tickers:
        snaps = get_today_gex_snapshots(ticker)
        if len(snaps) < 2:
            result[ticker] = {"timestamps": [], "strikes": [], "matrix": [], "spot_trace": []}
            continue

        # Union of all strike prices seen across every snapshot
        all_strikes = sorted({r["strike"] for snap in snaps for r in snap["strikes"]})
        timestamps  = [snap["timestamp"] for snap in snaps]
        spot_trace  = [snap["spot"] for snap in snaps]

        # Build per-snapshot lookup: {strike → net_gex_m}
        lookups = [
            {row["strike"]: row["net_gex_m"] for row in snap["strikes"]}
            for snap in snaps
        ]

        # matrix[si][ti] = net_gex_m for strike all_strikes[si] at time ti
        matrix = [
            [lk.get(strike, 0.0) for lk in lookups]
            for strike in all_strikes
        ]

        result[ticker] = {
            "timestamps": timestamps,
            "strikes":    all_strikes,
            "matrix":     matrix,
            "spot_trace": spot_trace,
        }
    return result
