"""
Hardcoded 13F positioning data for known hedge funds and asset managers.

Source: Publicly available 13F filings (Q3/Q4 2024 — approximate values).
These are top-20 equity positions only. Short positions are NOT reported in 13F.
Values are approximate; verify against latest SEC EDGAR filings before trading.

Indexed by TICKER for fast lookup.
Each fund entry: {"shares": int, "value_usd": int, "direction": "long"}
"""

# ── Raw data indexed by ticker ────────────────────────────────────────────────

POSITIONS_13F: dict[str, dict[str, dict]] = {
    "NVDA": {
        "Millennium":      {"shares": 1_500_000, "value_usd": 181_000_000, "direction": "long"},
        "Point72":         {"shares":   800_000, "value_usd":  97_000_000, "direction": "long"},
        "Balyasny":        {"shares":   600_000, "value_usd":  73_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   400_000, "value_usd":  48_000_000, "direction": "long"},
        "Steadfast":       {"shares":   150_000, "value_usd":  18_000_000, "direction": "long"},
        "Moore Capital":   {"shares":   300_000, "value_usd":  36_000_000, "direction": "long"},
        "Palestra":        {"shares":   500_000, "value_usd":  61_000_000, "direction": "long"},
        "HBK":             {"shares":   400_000, "value_usd":  48_000_000, "direction": "long"},
        "Exodus Point":    {"shares":   600_000, "value_usd":  73_000_000, "direction": "long"},
        "Yaupon":          {"shares":    80_000, "value_usd":  10_000_000, "direction": "long"},
        "LGIM":            {"shares":10_000_000, "value_usd": 1_210_000_000, "direction": "long"},
    },
    "AAPL": {
        "Millennium":      {"shares": 2_000_000, "value_usd": 420_000_000, "direction": "long"},
        "Point72":         {"shares": 1_000_000, "value_usd": 210_000_000, "direction": "long"},
        "Balyasny":        {"shares":   800_000, "value_usd": 168_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   600_000, "value_usd": 126_000_000, "direction": "long"},
        "Moore Capital":   {"shares":   400_000, "value_usd":  84_000_000, "direction": "long"},
        "Palestra":        {"shares":   400_000, "value_usd":  84_000_000, "direction": "long"},
        "HBK":             {"shares":   500_000, "value_usd": 105_000_000, "direction": "long"},
        "Exodus Point":    {"shares":   800_000, "value_usd": 168_000_000, "direction": "long"},
        "Yaupon":          {"shares":   100_000, "value_usd":  21_000_000, "direction": "long"},
        "Cohen & Steers":  {"shares":   500_000, "value_usd": 105_000_000, "direction": "long"},
        "LGIM":            {"shares":15_000_000, "value_usd": 3_150_000_000, "direction": "long"},
    },
    "MSFT": {
        "Millennium":      {"shares": 1_000_000, "value_usd": 425_000_000, "direction": "long"},
        "Point72":         {"shares":   600_000, "value_usd": 255_000_000, "direction": "long"},
        "Balyasny":        {"shares":   500_000, "value_usd": 213_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   400_000, "value_usd": 170_000_000, "direction": "long"},
        "Steadfast":       {"shares":   200_000, "value_usd":  85_000_000, "direction": "long"},
        "Zimmer":          {"shares":   150_000, "value_usd":  64_000_000, "direction": "long"},
        "Moore Capital":   {"shares":   300_000, "value_usd": 128_000_000, "direction": "long"},
        "Palestra":        {"shares":   300_000, "value_usd": 128_000_000, "direction": "long"},
        "HBK":             {"shares":   400_000, "value_usd": 170_000_000, "direction": "long"},
        "Exodus Point":    {"shares":   500_000, "value_usd": 213_000_000, "direction": "long"},
        "Yaupon":          {"shares":   100_000, "value_usd":  43_000_000, "direction": "long"},
        "Cohen & Steers":  {"shares":   400_000, "value_usd": 170_000_000, "direction": "long"},
        "LGIM":            {"shares":12_000_000, "value_usd": 5_100_000_000, "direction": "long"},
    },
    "META": {
        "Millennium":      {"shares":   800_000, "value_usd": 480_000_000, "direction": "long"},
        "Point72":         {"shares":   400_000, "value_usd": 240_000_000, "direction": "long"},
        "Balyasny":        {"shares":   300_000, "value_usd": 180_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   300_000, "value_usd": 180_000_000, "direction": "long"},
        "Steadfast":       {"shares":   300_000, "value_usd": 180_000_000, "direction": "long"},
        "Palestra":        {"shares":   400_000, "value_usd": 240_000_000, "direction": "long"},
        "Exodus Point":    {"shares":   400_000, "value_usd": 240_000_000, "direction": "long"},
        "Yaupon":          {"shares":    60_000, "value_usd":  36_000_000, "direction": "long"},
        "LGIM":            {"shares": 5_000_000, "value_usd": 3_000_000_000, "direction": "long"},
    },
    "GOOG": {
        "Millennium":      {"shares":   600_000, "value_usd": 108_000_000, "direction": "long"},
        "Point72":         {"shares":   300_000, "value_usd":  54_000_000, "direction": "long"},
        "Balyasny":        {"shares":   200_000, "value_usd":  36_000_000, "direction": "long"},
        "Steadfast":       {"shares":   200_000, "value_usd":  36_000_000, "direction": "long"},
        "Palestra":        {"shares":   300_000, "value_usd":  54_000_000, "direction": "long"},
        "HBK":             {"shares":   200_000, "value_usd":  36_000_000, "direction": "long"},
        "LGIM":            {"shares": 6_000_000, "value_usd": 1_080_000_000, "direction": "long"},
    },
    "AMZN": {
        "Millennium":      {"shares": 1_200_000, "value_usd": 264_000_000, "direction": "long"},
        "Point72":         {"shares":   500_000, "value_usd": 110_000_000, "direction": "long"},
        "Balyasny":        {"shares":   400_000, "value_usd":  88_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   300_000, "value_usd":  66_000_000, "direction": "long"},
        "Steadfast":       {"shares":   400_000, "value_usd":  88_000_000, "direction": "long"},
        "Zimmer":          {"shares":   200_000, "value_usd":  44_000_000, "direction": "long"},
        "Palestra":        {"shares":   300_000, "value_usd":  66_000_000, "direction": "long"},
        "HBK":             {"shares":   300_000, "value_usd":  66_000_000, "direction": "long"},
        "Exodus Point":    {"shares":   400_000, "value_usd":  88_000_000, "direction": "long"},
        "Yaupon":          {"shares":   100_000, "value_usd":  22_000_000, "direction": "long"},
        "Cohen & Steers":  {"shares":   300_000, "value_usd":  66_000_000, "direction": "long"},
        "LGIM":            {"shares": 8_000_000, "value_usd": 1_760_000_000, "direction": "long"},
    },
    "TSLA": {
        "Millennium":      {"shares":   500_000, "value_usd": 160_000_000, "direction": "long"},
        "Point72":         {"shares":   200_000, "value_usd":  64_000_000, "direction": "long"},
        "Exodus Point":    {"shares":   200_000, "value_usd":  64_000_000, "direction": "long"},
        "LGIM":            {"shares": 4_000_000, "value_usd": 1_280_000_000, "direction": "long"},
    },
    "GS": {
        "Millennium":      {"shares":   300_000, "value_usd": 150_000_000, "direction": "long"},
        "Point72":         {"shares":   150_000, "value_usd":  75_000_000, "direction": "long"},
        "Balyasny":        {"shares":   200_000, "value_usd": 100_000_000, "direction": "long"},
        "Zimmer":          {"shares":   200_000, "value_usd": 100_000_000, "direction": "long"},
        "HBK":             {"shares":   150_000, "value_usd":  75_000_000, "direction": "long"},
        "Cohen & Steers":  {"shares":   400_000, "value_usd": 200_000_000, "direction": "long"},
        "LGIM":            {"shares": 2_000_000, "value_usd": 1_000_000_000, "direction": "long"},
    },
    "JPM": {
        "Millennium":      {"shares":   400_000, "value_usd":  88_000_000, "direction": "long"},
        "Balyasny":        {"shares":   300_000, "value_usd":  66_000_000, "direction": "long"},
        "Zimmer":          {"shares":   300_000, "value_usd":  66_000_000, "direction": "long"},
        "Cohen & Steers":  {"shares":   500_000, "value_usd": 110_000_000, "direction": "long"},
        "LGIM":            {"shares": 5_000_000, "value_usd": 1_100_000_000, "direction": "long"},
    },
    "AMD": {
        "Millennium":      {"shares":   500_000, "value_usd":  68_000_000, "direction": "long"},
        "Point72":         {"shares":   300_000, "value_usd":  41_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   200_000, "value_usd":  27_000_000, "direction": "long"},
        "Exodus Point":    {"shares":   300_000, "value_usd":  41_000_000, "direction": "long"},
        "LGIM":            {"shares": 2_000_000, "value_usd": 272_000_000, "direction": "long"},
    },
    "NFLX": {
        "Millennium":      {"shares":   200_000, "value_usd": 170_000_000, "direction": "long"},
        "Point72":         {"shares":   150_000, "value_usd": 128_000_000, "direction": "long"},
        "Balyasny":        {"shares":   100_000, "value_usd":  85_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   150_000, "value_usd": 128_000_000, "direction": "long"},
        "Cohen & Steers":  {"shares":   100_000, "value_usd":  85_000_000, "direction": "long"},
        "LGIM":            {"shares": 1_000_000, "value_usd": 850_000_000, "direction": "long"},
    },
    "COIN": {
        "Millennium":      {"shares":   300_000, "value_usd":  90_000_000, "direction": "long"},
        "Schonfeld":       {"shares":   200_000, "value_usd":  60_000_000, "direction": "long"},
        "LGIM":            {"shares":   500_000, "value_usd": 150_000_000, "direction": "long"},
    },
    "UBER": {
        "Millennium":      {"shares": 1_000_000, "value_usd":  72_000_000, "direction": "long"},
        "Balyasny":        {"shares":   600_000, "value_usd":  43_000_000, "direction": "long"},
        "LGIM":            {"shares": 3_000_000, "value_usd": 216_000_000, "direction": "long"},
    },
    "ARM": {
        "Point72":         {"shares":   200_000, "value_usd":  44_000_000, "direction": "long"},
        "LGIM":            {"shares": 1_000_000, "value_usd": 220_000_000, "direction": "long"},
    },
    "SPY": {
        "Moore Capital":   {"shares":   500_000, "value_usd": 280_000_000, "direction": "long"},
        "LGIM":            {"shares": 5_000_000, "value_usd": 2_800_000_000, "direction": "long"},
    },
    "QQQ": {
        "Moore Capital":   {"shares":   300_000, "value_usd": 168_000_000, "direction": "long"},
        "LGIM":            {"shares": 3_000_000, "value_usd": 1_680_000_000, "direction": "long"},
    },
}

# ETFs (XLF, XLK, XLE, IWM) are not typically top-20 positions for these funds.


def print_holdings_summary():
    """Print all holdings to console for verification. Called on startup."""
    print("\n" + "=" * 70)
    print("13F POSITIONING DATABASE — approximate values from public filings")
    print("Source: SEC EDGAR Q3/Q4 2024 13F filings (verify before trading)")
    print("=" * 70)
    for ticker in sorted(POSITIONS_13F):
        funds = POSITIONS_13F[ticker]
        total_shares = sum(f["shares"] for f in funds.values())
        print(f"\n  {ticker} ({len(funds)} funds, {total_shares/1e6:.1f}M total shs):")
        for fund, pos in sorted(funds.items(), key=lambda x: x[1]["shares"], reverse=True):
            shares_m = pos["shares"] / 1e6
            val_m = pos["value_usd"] / 1e6
            print(f"    {fund:<30} {shares_m:5.1f}M shs  ${val_m:,.0f}M")
    print("\n" + "=" * 70 + "\n")
