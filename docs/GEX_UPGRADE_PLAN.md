# GEX Engine Institutional Overhaul — Staged Execution Plan

## HOW TO USE THIS FILE

Save this as `docs/GEX_UPGRADE_PLAN.md` in your project. Then feed Claude Code the
prompts below ONE AT A TIME. Wait for each to complete and push before sending the
next. The full spec is at the bottom of this file for reference.

---

## STAGE 0: AUDIT (send this first)

You are a senior quantitative options engineer. Audit my current GEX calculation code. Find all files related to gamma exposure calculation, GEX by strike, flip point, call wall, put wall, max gamma, stability percentile, and 0DTE GEX. For each file, tell me:

1. What formula is being used
2. Whether units are labeled (per $1 move vs per 1% move)
3. Whether gamma is taken directly from Polygon or recalculated
4. How the dealer sign is assigned (calls +1, puts -1, or something else)
5. How the flip point is calculated (strike-based or spot-scan)
6. Whether contract multiplier is hardcoded to 100
7. How 0DTE is filtered

Do NOT make any changes. Just audit and report back.

---

## STAGE 1: DATA ARCHITECTURE REFACTOR

Refactor the GEX data pipeline into clean modular layers. Create these files:

1. `gex-engine/vendor-ingest.js` — raw Polygon options chain fetch and normalization. Extract spot, strike, expiration, IV, option_type, OI, volume, vendor_gamma from each contract. Handle missing fields gracefully.

2. `gex-engine/greek-calc.js` — internal Black-Scholes gamma calculator. Inputs: spot, strike, time_to_expiry_years (precise, including same-day), IV, rate (default 0.05), option_type. Output: calculated_gamma. Fall back to vendor gamma if IV is missing. Log all fallbacks. Include a helper for precise time-to-expiry in years (use market hours remaining for 0DTE).

3. `gex-engine/contract-model.js` — contract normalization. Default multiplier 100 for standard equity options. Structure so different multipliers can be passed. Attach product_type and multiplier to each contract record.

Wire these into the existing data flow so they replace (or wrap) the current raw Polygon usage. Do not change the UI yet. Push when done.

---

## STAGE 2: EXPOSURE CALCULATION WITH PROPER UNITS

Create `gex-engine/exposure-calc.js` with these clearly labeled functions:

```javascript
// Per-strike hedge shares for a $1 spot move
function hedgeShares1USD(gamma, OI, multiplier, sign) {
  return gamma * OI * multiplier * sign;
}

// Per-strike hedge notional for a $1 spot move
function hedgeNotional1USD(gamma, OI, multiplier, spot, sign) {
  return gamma * OI * multiplier * spot * sign;
}

// Per-strike GEX for a 1% spot move (THIS IS THE PRIMARY METRIC)
function gex1Pct(gamma, OI, multiplier, spot, sign) {
  return gamma * OI * multiplier * spot * spot * 0.01 * sign;
}

// Net GEX = sum across all strikes
// Absolute GEX = sum of absolute values across all strikes
```

Add 0DTE versions of each (same formulas, filtered to contracts expiring today).

Update the dashboard's net GEX display to use `gex1Pct` as the default. Change the header card label from "NET GEX (SPY)" to "Net GEX (1% move)". Add a toggle or dropdown to switch between "$1 move" and "1% move" views.

Make all unit labels explicit in the code with JSDoc comments. Push when done.

---

## STAGE 3: SIGN ENGINE WITH MULTIPLE MODES

Create `gex-engine/sign-engine.js` with four configurable modes:

- **Mode 1 "Classic"**: calls = +1, puts = -1
- **Mode 2 "Customer Long Premium"**: same signs but with explicit confidence score and UI note "Assumed: customers net long options"
- **Mode 3 "Heuristic"**: use moneyness, time-to-expiry, and volume patterns to adjust confidence. Far OTM puts = higher confidence customer long. Near-ATM covered call zone = lower confidence. Return a confidence score 0–100 per contract.
- **Mode 4 "Unsigned"**: ignore sign entirely, show only absolute gamma concentration

Add a sign mode selector to the Gamma Exposure panel header (next to the SPY/QQQ/SPX tabs). Default to Mode 1. Display the active mode name and average confidence in the UI. Label all signed metrics as "estimated" — never present dealer positioning as known fact.

Update all exposure calculations to use the sign engine output. Push when done.

---

## STAGE 4: TRUE SPOT-SCAN GAMMA FLIP

This is the most important upgrade. Replace the current flip point calculation.

Create `gex-engine/spot-scan.js`:

1. Build a spot grid from `current_spot * 0.90` to `current_spot * 1.10` in 0.5% increments (about 40 grid points).
2. For each hypothetical spot level on the grid, recalculate gamma for every contract using the Black-Scholes gamma function from `greek-calc.js` with that hypothetical spot.
3. Recompute total net GEX across the entire chain at each grid point.
4. Find where net GEX crosses zero — this is the TRUE gamma flip.
5. If zero is never crossed, report the nearest boundary and label it "Flip not found in ±10% range — nearest: $XXX"

Replace the current flip point display with this spot-scan result. Label it "Gamma Flip (spot-scan)" to distinguish from the old strike-based method.

Also output the full GEX-vs-spot curve so it can be plotted (this is the JPM-style "S&P 500 gamma imbalance profile" chart). Store the curve data for the UI to render later.

Push when done.

---

## STAGE 5: IMPROVED WALL LOGIC AND REGIME FRAMEWORK

Create `gex-engine/levels.js` with improved wall and regime logic:

**WALLS:**
- Top 3 call gamma concentrations (with strike, value, distance from spot)
- Top 3 put gamma concentrations (with strike, value, distance from spot)
- Max absolute gamma concentration (the "magnet" strike)
- Compute a "magnet score" = absolute gamma at that strike / average absolute gamma across all strikes

**REGIME CLASSIFICATION** based on:
- Net GEX sign and magnitude
- Distance to flip
- 0DTE share of total gamma (> 30% = "0DTE dominant")
- Concentration near spot (within 1% of spot)
- Historical percentile (vs 30-day trailing)

Output one of these regime labels:
- "Strong positive gamma — dealer dampening"
- "Moderate positive gamma"
- "Neutral — near flip"
- "Moderate negative gamma"
- "Strong negative gamma — dealer amplifying"

Plus subflags: "0DTE dominant", "Pinning likely" (if near large absolute gamma with positive net), "Air pocket" (if near large negative gamma zone), "High convexity zone"

Update the Gamma Exposure panel's regime display to show this classification. Push when done.

---

## STAGE 6: IMPROVED 0DTE AND HISTORICAL STORAGE

**1. IMPROVED 0DTE:**

Create distinct 0DTE metrics:
- "0DTE OI GEX" = standard GEX filtered to today's expiry using OI
- "0DTE Flow Gamma" = same calculation but weighted by today's volume instead of OI (captures intraday activity)
- "0DTE Blended" = configurable weighted average of OI and volume versions

Add labels for each in the 0DTE panel. If open/close data isn't available from Polygon, note that and treat volume as activity not confirmed new positioning.

**2. HISTORICAL SNAPSHOTS:**

Store a snapshot of key metrics (net GEX, absolute GEX, flip point, top walls, 0DTE share, regime) every time data refreshes. Use localStorage or the existing storage mechanism. Keep up to 30 days of snapshots.

Compute trailing percentiles and z-scores from this history. Show:
- Net GEX percentile (30d)
- Absolute GEX percentile (30d)
- 0DTE share percentile (30d)
- Near-spot concentration percentile (30d)

Replace the single "Stability %" metric with these more granular percentiles.

Push when done.

---

## STAGE 7: DIAGNOSTICS, TESTING, AND UI LABELS

**1. DIAGNOSTICS PANEL:**

Add a collapsible "Diagnostics" section at the bottom of the Gamma Exposure panel showing:
- Contracts included / excluded (and why)
- Missing IV count
- Fallback gamma count (vendor vs internal)
- Active sign model and confidence
- Spot scan range and step size
- Data snapshot timestamp
- Staleness warning if data is > 5 min old

**2. UI LABEL CLEANUP:**

Rename all GEX-related labels to be precise:
- "Net GEX" → "Net GEX (1% move)"
- "Flip $674" → "Gamma Flip $674 (spot-scan)"
- "Call Wall $675" → "Top Call Wall $675"
- "Put Wall $675" → "Top Put Wall $675"
- "Max Γ $660" → "Max |Γ| $660 (magnet)"
- "96% stable" → "Net GEX: 96th pct (30d)"
- Add unit tooltips on hover for every metric

**3. TESTS:**

Create `gex-engine/tests/` with unit tests for:
- Black-Scholes gamma calculation (verify against known values)
- Time-to-expiry precision (including same-day)
- Spot scan flip logic (synthetic dataset with known flip)
- Sign mode switching
- Wall ranking
- Regime classification
- Historical percentile math

Create a small synthetic options chain (10–20 contracts with known correct answers) as a test fixture.

Push when done.

---

## FULL SPEC REFERENCE

*(The complete methodology spec from the original document is referenced above in each stage prompt. Key principles:)*

- **Primary GEX metric**: per-1%-move notional — `gamma × OI × multiplier × spot² × 0.01 × sign`
- **Flip point**: true spot-scan zero-crossing (not strike-based approximation)
- **Sign model**: selectable mode with explicit confidence scoring; never assert dealer intent as fact
- **0DTE**: separate OI-weighted vs volume-weighted metrics
- **History**: 30-day rolling snapshots with percentile/z-score context
- **Diagnostics**: full transparency on data quality and methodology
