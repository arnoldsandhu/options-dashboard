"""Signal 3: Roll Opportunity — large OI in near-expiry contracts"""
from datetime import date, datetime

from polygon import RESTClient

from config import POLYGON_API_KEY, ROLL_DTE_THRESHOLD, ROLL_OI_THRESHOLD
from db.database import has_roll_alert

_client: RESTClient | None = None


def _get_client() -> RESTClient:
    global _client
    if _client is None:
        _client = RESTClient(api_key=POLYGON_API_KEY)
    return _client


def check_rolls(ticker: str) -> list[dict]:
    """
    Returns a list of alert dicts for roll opportunities.
    """
    client = _get_client()
    contracts = []
    try:
        for contract in client.list_snapshot_options_chain(
            ticker, params={"limit": 250}
        ):
            contracts.append(contract)
    except Exception as e:
        print(f"  [rolls] Error fetching chain for {ticker}: {e}")
        return []

    today = date.today()
    alerts = []

    for c in contracts:
        details = getattr(c, "details", None)
        if details is None:
            continue

        expiry_str = getattr(details, "expiration_date", None)
        if expiry_str is None:
            continue

        try:
            expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        dte = (expiry_dt - today).days
        if dte < 0 or dte > ROLL_DTE_THRESHOLD:
            continue

        oi = getattr(c, "open_interest", 0) or 0
        if oi < ROLL_OI_THRESHOLD:
            continue

        strike = getattr(details, "strike_price", "?")
        cp = getattr(details, "contract_type", "?").upper()
        expiry_short = expiry_str[2:].replace("-", "")
        contract_key = f"{ticker}_{strike}_{expiry_short}_{cp}"

        if has_roll_alert(ticker, contract_key):
            continue

        alert_text = (
            f"{ticker} ROLL OPPORTUNITY — {strike}{cp[0]} {expiry_short} "
            f"OI: {int(oi):,} — expires in {dte} day{'s' if dte != 1 else ''}"
        )
        alerts.append({
            "ticker": ticker,
            "contract_key": contract_key,
            "strike": strike,
            "expiry": expiry_str,
            "expiry_short": expiry_short,
            "cp": cp,
            "oi": oi,
            "dte": dte,
            "alert_text": alert_text,
            "signal_data": {
                "strike": strike,
                "expiry": expiry_str,
                "contract_type": cp,
                "oi": oi,
                "dte": dte,
            },
        })

    # Sort by OI descending (largest positions most likely to roll), take top 5
    alerts.sort(key=lambda a: a["oi"], reverse=True)
    return alerts[:5]
