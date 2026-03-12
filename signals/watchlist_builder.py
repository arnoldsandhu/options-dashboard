"""Dynamic OVI scan list builder: TIER1 (core) + TIER2 (ETFs) + TIER3 (S&P pool) + TIER4 (DB spikes)."""
from datetime import date, timedelta

from db.database import get_cached_watchlist, save_watchlist_daily, get_tier4_tickers

# ── Tier definitions ──────────────────────────────────────────────────────────

TIER1 = [
    # Core watchlist — used for ALL signals (GEX, SKEW, POSITIONING, OVI)
    "SPY", "QQQ", "NVDA", "AAPL", "MSFT", "META", "GOOG", "AMZN",
    "TSLA", "GS", "JPM", "XLF", "XLK", "XLE", "IWM", "COIN",
    "AMD", "NFLX", "UBER", "ARM", "MRVL",
]

TIER2 = [
    # Liquid sector + leveraged ETFs with active options markets
    # Leveraged index
    "TQQQ", "SQQQ", "SPXL", "SPXU", "UPRO", "SDOW", "UDOW",
    # Volatility products
    "UVXY", "VXX", "SVXY",
    # Sector ETFs (beyond core XLF/XLK/XLE)
    "XLV", "XLI", "XLB", "XLU", "XLRE", "XLC", "XLP", "XLY",
    # Semiconductor
    "SOXX", "SOXL", "SOXS",
    # Small cap leveraged
    "TNA", "TZA",
    # Commodity
    "GLD", "SLV", "USO", "UNG", "GDX", "GDXJ", "IAU",
    # International
    "EEM", "EFA", "FXI", "EWJ", "EWZ",
]

TIER3_POOL = [
    # High-activity S&P 500 + notable names not already in TIER1/TIER2
    # Mega cap
    "GOOGL", "LLY", "AVGO", "UNH", "V", "COST", "MA", "BRK.B",
    # Tech / Semis
    "INTC", "ORCL", "CRM", "ADBE", "QCOM", "MU", "AMAT", "LRCX", "KLAC",
    "SMCI", "ON", "MPWR", "IBM", "DELL",
    # AI / Cloud / SaaS
    "PLTR", "SNOW", "DDOG", "NET", "CRWD", "PANW", "FTNT", "APP", "RBLX",
    "ZM", "DOCU", "OKTA",
    # Consumer / Retail
    "WMT", "TGT", "HD", "LOW", "MCD", "SBUX",
    # Financials
    "BAC", "WFC", "C", "MS", "BX", "KKR", "APO", "AXP",
    # Healthcare / Biotech
    "PFE", "MRNA", "JNJ", "ABBV", "MRK", "GILD", "REGN", "AMGN", "BMY",
    "ISRG", "SYK", "BSX", "HCA", "CI", "CVS", "BNTX",
    # Energy
    "XOM", "CVX", "DVN", "FCX", "SLB",
    # Payments / Fintech
    "PYPL", "SQ", "AFRM", "HOOD", "SOFI",
    # Comms / Media
    "T", "VZ", "CMCSA", "DIS", "SNAP", "PINS",
    # EV / Autos
    "F", "GM", "RIVN", "NIO", "XPEV", "LI",
    # Chinese ADRs
    "BABA", "JD", "PDD",
    # Travel / Leisure
    "ABNB", "LYFT", "DAL", "UAL", "AAL", "MAR", "HLT", "RCL",
    # Defense
    "LMT", "NOC", "GD", "RTX",
    # Other high-vol
    "SHOP", "MELI", "SE", "NU",
    "BA", "GE", "CAT", "DE", "HON",
    "SPGI", "BLK", "MCO",
    "ENPH", "FSLR", "ENVX",
    "KO", "PG", "PEP",
    # Fixed income ETF (high options activity)
    "LQD",
    # Bitcoin / crypto ETFs
    "GBTC", "MSTR", "IBIT",
    # Single-stock leveraged ETFs (high options activity)
    "NVDL", "TSLL",
]

_MAX_TICKERS = 300


def _n_trading_days_ago(n: int) -> str:
    """Approximate: skip weekends only (no holiday calendar)."""
    d = date.today()
    count = 0
    while count < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return d.isoformat()


def build_scan_list(date_str: str | None = None) -> list[str]:
    """Build deduplicated scan list for today, cached in watchlist_daily table.
    Order: TIER1 → TIER2 → TIER3_POOL → TIER4, capped at _MAX_TICKERS.
    """
    if date_str is None:
        date_str = date.today().isoformat()

    cached = get_cached_watchlist(date_str)
    if cached:
        print(f"  [watchlist] Loaded {len(cached)} tickers from cache ({date_str})")
        return cached

    seen: set[str] = set()
    result: list[str] = []
    tiers: dict[int, list[str]] = {1: [], 2: [], 3: [], 4: []}

    def _add(ticker: str, tier: int):
        if ticker not in seen and len(result) < _MAX_TICKERS:
            seen.add(ticker)
            result.append(ticker)
            tiers[tier].append(ticker)

    for t in TIER1:
        _add(t, 1)
    for t in TIER2:
        _add(t, 2)
    for t in TIER3_POOL:
        _add(t, 3)

    # TIER4: tickers with recent vol spikes from DB history
    since = _n_trading_days_ago(3)
    try:
        for t in get_tier4_tickers(since):
            _add(t, 4)
    except Exception as e:
        print(f"  [watchlist] TIER4 error: {e}")

    print(
        f"  [watchlist] Built scan list: {len(result)} tickers "
        f"(T1={len(tiers[1])}, T2={len(tiers[2])}, T3={len(tiers[3])}, T4={len(tiers[4])})"
    )

    try:
        save_watchlist_daily(date_str, tiers)
    except Exception as e:
        print(f"  [watchlist] Cache save error: {e}")

    return result
