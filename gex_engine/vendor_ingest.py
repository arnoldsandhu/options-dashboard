"""
gex_engine.vendor_ingest
------------------------
Raw Polygon options chain fetch and normalization.

Extracts: spot, strike, expiration, iv, option_type, oi, volume, vendor_gamma
from each contract. Handles missing fields gracefully.
"""
from __future__ import annotations

from polygon import RESTClient

from config import POLYGON_API_KEY

_client: RESTClient | None = None


def _get_client() -> RESTClient:
    global _client
    if _client is None:
        _client = RESTClient(api_key=POLYGON_API_KEY)
    return _client


def fetch_chain(ticker: str) -> tuple[float, list[dict]]:
    """
    Fetch the full options chain snapshot for *ticker* from Polygon.

    Returns:
        (spot, contracts) where spot is the current underlying price and
        contracts is a list of normalized dicts:
        {
            ticker, strike, expiration, option_type,
            oi, volume, iv, vendor_gamma
        }
        Contracts with strike=0, no option_type, or no expiration are dropped.
    """
    client = _get_client()
    contracts: list[dict] = []
    spot = 0.0

    try:
        # limit=250 is Polygon's max per page; the SDK auto-paginates via next_url
        # so the full chain is always returned — this just minimises round-trips.
        for contract in client.list_snapshot_options_chain(ticker, params={"limit": 250}):
            # Extract spot from the first contract's underlying_asset.price
            if spot == 0.0:
                ua = getattr(contract, "underlying_asset", None)
                if ua:
                    p = getattr(ua, "price", None)
                    if p:
                        spot = float(p)

            details = getattr(contract, "details", None)
            greeks  = getattr(contract, "greeks",  None)
            day     = getattr(contract, "day",     None)

            strike     = float(getattr(details, "strike_price",      0.0) or 0.0) if details else 0.0
            expiration = (getattr(details, "expiration_date",          "") or "")  if details else ""
            cp         = (getattr(details, "contract_type",            "") or "").lower() if details else ""
            oi         = int(getattr(contract, "open_interest",          0) or 0)
            volume     = int(getattr(day,      "volume",                  0) or 0) if day else 0
            iv         = float(getattr(greeks,  "implied_volatility",  0.0) or 0.0) if greeks else 0.0
            vendor_gamma = float(getattr(greeks, "gamma",              0.0) or 0.0) if greeks else 0.0

            # Drop contracts missing required identifiers
            if strike == 0.0 or cp not in ("call", "put") or not expiration:
                continue

            contracts.append({
                "ticker":       ticker,
                "strike":       strike,
                "expiration":   expiration,
                "option_type":  cp,
                "oi":           oi,
                "volume":       volume,
                "iv":           iv,
                "vendor_gamma": vendor_gamma,
            })

    except Exception as e:
        print(f"  [vendor_ingest] chain fetch error {ticker}: {e}")

    print(f"  [vendor_ingest] {ticker}: {len(contracts)} contracts, spot={spot:.2f}")
    return spot, contracts


def fetch_spot_fallback(ticker: str) -> float:
    """
    Fetch spot price via last trade or previous close.
    Used when chain fetch returns no spot.
    """
    client = _get_client()
    try:
        trade = client.get_last_trade(ticker)
        price = getattr(trade, "price", None) or getattr(trade, "p", None)
        if price:
            return float(price)
        results = getattr(trade, "results", None)
        if results:
            price = getattr(results, "price", None) or getattr(results, "p", None)
            if price:
                return float(price)
    except Exception:
        pass
    try:
        pc = list(client.get_previous_close(ticker))
        if pc and hasattr(pc[0], "close") and pc[0].close:
            return float(pc[0].close)
    except Exception as e:
        print(f"  [vendor_ingest] spot fallback error {ticker}: {e}")
    return 0.0
