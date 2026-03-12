"""
Investable universe: S&P 500 + Nasdaq 100 + Russell 2000 (top 800 by weight).

Run standalone to build/refresh:
    python data/universe.py

Refreshes weekly — reads from DB if last_verified < 7 days, fetches otherwise.
"""
import os
import re
import sys
from datetime import date, datetime, timedelta

import requests

_here = os.path.dirname(os.path.abspath(__file__))
_root = os.path.dirname(_here)
if _root not in sys.path:
    sys.path.insert(0, _root)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_UNIVERSE_MAX_AGE_DAYS = 7  # refresh weekly


def get_option_root(ticker: str) -> str:
    """
    Return the option ticker root for a given equity ticker.
    Strips non-alphanumeric chars so BRK-B → BRKB, BF-B → BFB.
    This matches what appears in OCC option symbol prefixes.
    """
    return re.sub(r"[^A-Z0-9]", "", ticker.upper())


def fetch_sp500() -> list[str]:
    """Fetch S&P 500 constituents from Wikipedia."""
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            headers=_HEADERS, timeout=15,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")
        table = soup.find("table", {"id": "constituents"})
        if not table:
            # Fallback: find first wikitable
            table = soup.find("table", {"class": "wikitable"})
        tickers = []
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if cells:
                raw = cells[0].get_text(strip=True)
                # Wikipedia uses . for BRK.B — convert to -
                cleaned = raw.replace(".", "-")
                if cleaned:
                    tickers.append(cleaned)
        print(f"  [universe] SP500: {len(tickers)} tickers")
        return tickers
    except Exception as e:
        print(f"  [universe] SP500 fetch error: {e}")
        return []


def fetch_ndx100() -> list[str]:
    """Fetch Nasdaq-100 constituents from Wikipedia."""
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(
            "https://en.wikipedia.org/wiki/Nasdaq-100",
            headers=_HEADERS, timeout=15,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # Find the constituents table — look for one with Ticker column
        tickers = []
        for table in soup.find_all("table", {"class": "wikitable"}):
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            # Look for column named 'ticker' or 'symbol'
            col_idx = None
            for i, h in enumerate(headers):
                if h in ("ticker", "symbol"):
                    col_idx = i
                    break
            if col_idx is None:
                continue
            for row in table.find_all("tr")[1:]:
                cells = row.find_all("td")
                if len(cells) > col_idx:
                    raw = cells[col_idx].get_text(strip=True)
                    cleaned = raw.replace(".", "-")
                    if cleaned and re.match(r"^[A-Z\-]+$", cleaned):
                        tickers.append(cleaned)
            if tickers:
                break

        print(f"  [universe] NDX100: {len(tickers)} tickers")
        return tickers
    except Exception as e:
        print(f"  [universe] NDX100 fetch error: {e}")
        return []


def fetch_russell2000(top_n: int = 800) -> list[str]:
    """
    Fetch Russell 2000 (IWM) holdings from iShares.
    Returns top N by weight — covers the most liquid RUT names.
    Falls back to an empty list if iShares blocks the request.
    """
    try:
        import pandas as pd
        import io

        # iShares IWM holdings CSV
        url = (
            "https://www.ishares.com/us/products/239710/IWM/1467271812596.ajax"
            "?fileType=csv&fileName=IWM_holdings&dataType=fund"
        )
        headers = dict(_HEADERS)
        headers["Accept"] = "text/csv,application/csv,*/*"
        headers["Referer"] = "https://www.ishares.com/"

        resp = requests.get(url, headers=headers, timeout=20)
        resp.raise_for_status()

        raw_text = resp.text

        # The CSV has intro lines before the actual data — find the header row
        lines = raw_text.splitlines()
        header_idx = None
        for i, line in enumerate(lines):
            if "Ticker" in line and "Asset Class" in line:
                header_idx = i
                break

        if header_idx is None:
            print("  [universe] IWM: could not find header row in CSV")
            return []

        data_text = "\n".join(lines[header_idx:])
        df = pd.read_csv(io.StringIO(data_text))

        # Filter out non-equity rows
        if "Asset Class" in df.columns:
            df = df[~df["Asset Class"].str.contains(
                "Cash|Money Market|Currency|Future", case=False, na=True
            )]

        # Normalize ticker column name
        ticker_col = next(
            (c for c in df.columns if c.strip().lower() == "ticker"), None
        )
        if ticker_col is None:
            print(f"  [universe] IWM: no Ticker column found. Columns: {list(df.columns)}")
            return []

        # Sort by weight if available
        weight_col = next(
            (c for c in df.columns if "weight" in c.lower()), None
        )
        if weight_col:
            df[weight_col] = pd.to_numeric(df[weight_col].astype(str).str.replace("%", ""), errors="coerce")
            df = df.sort_values(weight_col, ascending=False)

        tickers = []
        for raw in df[ticker_col].dropna():
            cleaned = str(raw).strip().replace(".", "-")
            if cleaned and re.match(r"^[A-Z\-]+$", cleaned) and len(cleaned) <= 6:
                tickers.append(cleaned)

        tickers = tickers[:top_n]
        print(f"  [universe] RUT (IWM top {top_n}): {len(tickers)} tickers")
        return tickers

    except Exception as e:
        print(f"  [universe] RUT fetch error: {e}")
        return []


def build_universe(force_refresh: bool = False) -> dict[str, dict]:
    """
    Build the investable universe: SP500 ∪ NDX100 ∪ RUT800.
    Loads from DB if fresh (< 7 days). Fetches + saves otherwise.

    Returns {ticker: {in_sp500, in_ndx, in_rut, option_root}}
    """
    from db.database import get_universe_tickers, get_universe_last_verified, save_universe

    # Check if DB data is fresh enough
    if not force_refresh:
        last = get_universe_last_verified()
        if last:
            try:
                last_dt = datetime.fromisoformat(last).date()
                age = (date.today() - last_dt).days
                if age < _UNIVERSE_MAX_AGE_DAYS:
                    universe = get_universe_tickers()
                    if universe:
                        sp = sum(1 for v in universe.values() if v["in_sp500"])
                        ndx = sum(1 for v in universe.values() if v["in_ndx"])
                        rut = sum(1 for v in universe.values() if v["in_rut"])
                        print(
                            f"  [universe] Loaded from DB ({age}d old): "
                            f"{sp} SP500 | {ndx} NDX | {rut} RUT "
                            f"= {len(universe)} unique"
                        )
                        return universe
            except Exception:
                pass

    print("  [universe] Fetching fresh universe data...")
    sp500  = fetch_sp500()
    ndx100 = fetch_ndx100()
    rut800 = fetch_russell2000()

    sp_set  = set(sp500)
    ndx_set = set(ndx100)
    rut_set = set(rut800)
    all_tickers = list(dict.fromkeys(sp500 + ndx100 + rut800))  # preserve order, dedup

    universe: dict[str, dict] = {}
    for t in all_tickers:
        universe[t] = {
            "in_sp500":    t in sp_set,
            "in_ndx":      t in ndx_set,
            "in_rut":      t in rut_set,
            "option_root": get_option_root(t),
        }

    save_universe(universe)

    print(
        f"  [universe] Built: {len(sp_set)} SP500 | {len(ndx_set)} NDX | "
        f"{len(rut_set)} RUT = {len(universe)} unique tickers"
    )
    return universe


if __name__ == "__main__":
    print("Building investable universe...")
    u = build_universe(force_refresh=True)
    sp  = sum(1 for v in u.values() if v["in_sp500"])
    ndx = sum(1 for v in u.values() if v["in_ndx"])
    rut = sum(1 for v in u.values() if v["in_rut"])
    print(f"\nUniverse: {sp} SP500 | {ndx} NDX | {rut} RUT = {len(u)} unique tickers")
    print("\nSample tickers:", list(u.keys())[:10])
