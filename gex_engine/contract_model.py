"""
gex_engine.contract_model
--------------------------
Contract normalization layer.

Standard equity/ETF options: multiplier = 100.
Pass a custom multiplier dict for non-standard products (e.g. mini-SPX = 10).
"""
from __future__ import annotations

# Default multipliers by product type
_MULTIPLIERS: dict[str, int] = {
    "equity":   100,
    "etf":      100,
    "index":    100,
    "mini":      10,
    "micro":     10,
}

# Tickers whose options use a non-100 multiplier
_TICKER_OVERRIDES: dict[str, tuple[int, str]] = {
    # "XSP": (100, "index"),   # Mini-SPX (same multiplier, index type)
}


def get_multiplier(ticker: str, product_type: str = "equity") -> int:
    """Return the contract multiplier for a given ticker/product_type."""
    if ticker in _TICKER_OVERRIDES:
        return _TICKER_OVERRIDES[ticker][0]
    return _MULTIPLIERS.get(product_type, 100)


def normalize_contract(raw: dict, ticker: str | None = None) -> dict:
    """
    Attach product_type and multiplier to a normalized contract dict.

    Expects the dict to already have the fields produced by vendor_ingest:
        ticker, strike, expiration, option_type, oi, volume, iv, vendor_gamma

    Adds:
        product_type  str  – "equity" | "etf" | "index" | "mini" | "micro"
        multiplier    int  – shares per contract (default 100)

    Args:
        raw:    dict from vendor_ingest.fetch_chain normalization
        ticker: override if raw dict doesn't contain one
    """
    tk = (ticker or raw.get("ticker", "")).upper()

    if tk in _TICKER_OVERRIDES:
        mult, ptype = _TICKER_OVERRIDES[tk]
    else:
        # Heuristic: index symbols start with ^ or I: prefix
        if tk.startswith("^") or tk.startswith("I:"):
            ptype = "index"
        else:
            ptype = "equity"
        mult = _MULTIPLIERS.get(ptype, 100)

    return {
        **raw,
        "product_type": ptype,
        "multiplier":   mult,
    }


def normalize_chain(contracts: list[dict], ticker: str | None = None) -> list[dict]:
    """Normalize all contracts in a chain. Mutates nothing — returns new list."""
    return [normalize_contract(c, ticker) for c in contracts]
