import sqlite3
import json
from datetime import datetime, timedelta
from contextlib import contextmanager
from config import DB_PATH, DEDUP_HOURS, GEX_HISTORY_DAYS


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS gex_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                net_gex REAL NOT NULL,
                spot_price REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS alerts_sent (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                alert_text TEXT NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                signal_data_json TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                alerted INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS skew_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                skew_value REAL NOT NULL,
                expiry TEXT,
                spot_price REAL
            );

            CREATE TABLE IF NOT EXISTS positioning_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                fund_name TEXT,
                position_direction TEXT,
                flow_direction TEXT,
                match_type TEXT,
                timestamp TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS ovi_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                date TEXT NOT NULL,
                call_vol INTEGER NOT NULL DEFAULT 0,
                put_vol INTEGER NOT NULL DEFAULT 0,
                call_oi INTEGER NOT NULL DEFAULT 0,
                put_oi INTEGER NOT NULL DEFAULT 0,
                spot_price REAL,
                px_pct_chg REAL,
                UNIQUE(ticker, date)
            );

            CREATE TABLE IF NOT EXISTS ovi_report (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                report_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS news_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                headline TEXT NOT NULL UNIQUE,
                source TEXT,
                ticker TEXT,
                published TEXT,
                fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS watchlist_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                ticker TEXT NOT NULL,
                tier INTEGER NOT NULL,
                UNIQUE(date, ticker)
            );

            CREATE TABLE IF NOT EXISTS universe (
                ticker TEXT PRIMARY KEY,
                in_sp500 INTEGER NOT NULL DEFAULT 0,
                in_ndx INTEGER NOT NULL DEFAULT 0,
                in_rut INTEGER NOT NULL DEFAULT 0,
                option_root TEXT NOT NULL,
                added_date TEXT NOT NULL,
                last_verified TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS gex_strike_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                spot REAL NOT NULL DEFAULT 0,
                strikes_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_gss_ticker_ts
                ON gex_strike_snapshots(ticker, timestamp);

            CREATE TABLE IF NOT EXISTS gex_metrics_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                net_gex_m REAL NOT NULL,
                abs_gex_m REAL NOT NULL DEFAULT 0,
                net_gex_0dte_m REAL NOT NULL DEFAULT 0,
                abs_gex_0dte_m REAL NOT NULL DEFAULT 0,
                flow_gex_0dte_m REAL NOT NULL DEFAULT 0,
                flip_point REAL,
                regime_label TEXT,
                spot REAL NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_gmh_ticker_ts
                ON gex_metrics_history(ticker, timestamp);

            CREATE TABLE IF NOT EXISTS intraday_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                data_json TEXT NOT NULL
            );
        """)
    # Add new columns to ovi_history if they don't exist yet (idempotent)
    with get_conn() as conn:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(ovi_history)").fetchall()}
        if "avg_call_iv" not in existing:
            conn.execute("ALTER TABLE ovi_history ADD COLUMN avg_call_iv REAL")
        if "avg_put_iv" not in existing:
            conn.execute("ALTER TABLE ovi_history ADD COLUMN avg_put_iv REAL")
        if "data_source" not in existing:
            conn.execute("ALTER TABLE ovi_history ADD COLUMN data_source TEXT")
    print("[DB] Initialized database.")


# ── GEX History ──────────────────────────────────────────────────────────────

def save_gex(ticker: str, net_gex: float, spot_price: float):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO gex_history (ticker, timestamp, net_gex, spot_price) VALUES (?, ?, ?, ?)",
            (ticker, ts, net_gex, spot_price),
        )


def get_gex_history(ticker: str, days: int = GEX_HISTORY_DAYS) -> list[dict]:
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM gex_history WHERE ticker = ? AND timestamp >= ? ORDER BY timestamp",
            (ticker, since),
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_gex(ticker: str) -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM gex_history WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1",
            (ticker,),
        ).fetchone()
    return dict(row) if row else None


# ── Deduplication ─────────────────────────────────────────────────────────────

def is_already_alerted(ticker: str, signal_type: str) -> bool:
    cutoff = (datetime.utcnow() - timedelta(hours=DEDUP_HOURS)).isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM alerts_sent WHERE ticker = ? AND signal_type = ? AND timestamp >= ?",
            (ticker, signal_type, cutoff),
        ).fetchone()
    return row is not None


def record_alert(ticker: str, signal_type: str, alert_text: str):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO alerts_sent (ticker, signal_type, alert_text, timestamp) VALUES (?, ?, ?, ?)",
            (ticker, signal_type, alert_text, ts),
        )


# ── Signal Log ────────────────────────────────────────────────────────────────

def log_signal(ticker: str, signal_type: str, signal_data: dict, alerted: bool):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO signal_log (ticker, signal_type, signal_data_json, timestamp, alerted) VALUES (?, ?, ?, ?, ?)",
            (ticker, signal_type, json.dumps(signal_data), ts, int(alerted)),
        )


def get_day_signals(date_str: str | None = None) -> list[dict]:
    if date_str is None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM signal_log WHERE timestamp LIKE ? ORDER BY timestamp DESC",
            (f"{date_str}%",),
        ).fetchall()
    return [dict(r) for r in rows]


# ── Skew History ──────────────────────────────────────────────────────────────

def save_skew(ticker: str, skew_value: float, spot_price: float, expiry: str):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO skew_history (ticker, timestamp, skew_value, expiry, spot_price) VALUES (?, ?, ?, ?, ?)",
            (ticker, ts, skew_value, expiry, spot_price),
        )


def get_skew_history(ticker: str, days: int = 30) -> list[dict]:
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM skew_history WHERE ticker = ? AND timestamp >= ? ORDER BY timestamp",
            (ticker, since),
        ).fetchall()
    return [dict(r) for r in rows]


def get_skew_percentile(ticker: str, current_skew: float, days: int = 30) -> float:
    history = get_skew_history(ticker, days)
    values = [r["skew_value"] for r in history]
    if not values:
        return 50.0
    below = sum(1 for v in values if v < current_skew)
    return (below / len(values)) * 100.0


def get_latest_skew_all(days: int = 30) -> list[dict]:
    """Most recent skew row for every ticker, with 30-day percentile attached."""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT s.*
            FROM skew_history s
            INNER JOIN (
                SELECT ticker, MAX(timestamp) AS max_ts
                FROM skew_history
                WHERE timestamp >= ?
                GROUP BY ticker
            ) latest ON s.ticker = latest.ticker AND s.timestamp = latest.max_ts
            ORDER BY s.ticker
            """,
            (since,),
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["percentile"] = get_skew_percentile(d["ticker"], d["skew_value"], days)
        result.append(d)
    return result


# ── Positioning Matches ────────────────────────────────────────────────────────

def save_positioning_match(ticker: str, signal_type: str, fund_name: str,
                           position_direction: str, flow_direction: str, match_type: str):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO positioning_matches
               (ticker, signal_type, fund_name, position_direction, flow_direction, match_type, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (ticker, signal_type, fund_name, position_direction, flow_direction, match_type, ts),
        )


# ── Dashboard Queries ─────────────────────────────────────────────────────────

def get_today_alerts(date_str: str | None = None) -> list[dict]:
    """All alerts sent today, newest first."""
    if date_str is None:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts_sent WHERE timestamp LIKE ? ORDER BY timestamp DESC",
            (f"{date_str}%",),
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_gex_all() -> list[dict]:
    """Most recent GEX row for every ticker that has history."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT g.*
            FROM gex_history g
            INNER JOIN (
                SELECT ticker, MAX(timestamp) AS max_ts
                FROM gex_history
                GROUP BY ticker
            ) latest ON g.ticker = latest.ticker AND g.timestamp = latest.max_ts
            ORDER BY g.ticker
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def get_gex_percentile(ticker: str, net_gex: float, days: int = GEX_HISTORY_DAYS) -> float:
    """Percentile rank of net_gex within the last `days` days for ticker."""
    history = get_gex_history(ticker, days)
    values = [r["net_gex"] for r in history]
    if not values:
        return 50.0
    below = sum(1 for v in values if v < net_gex)
    return (below / len(values)) * 100.0


# ── Enhanced GEX Metrics History (30-day rolling) ──────────────────────────────

def save_gex_metrics(
    ticker: str,
    net_gex_m: float,
    abs_gex_m: float,
    net_gex_0dte_m: float,
    abs_gex_0dte_m: float,
    flow_gex_0dte_m: float,
    flip_point: float | None,
    regime_label: str,
    spot: float,
) -> None:
    """Persist a rich GEX snapshot for the 30-day trailing history."""
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO gex_metrics_history
               (ticker, timestamp, net_gex_m, abs_gex_m,
                net_gex_0dte_m, abs_gex_0dte_m, flow_gex_0dte_m,
                flip_point, regime_label, spot)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, ts, net_gex_m, abs_gex_m,
             net_gex_0dte_m, abs_gex_0dte_m, flow_gex_0dte_m,
             flip_point, regime_label, spot),
        )
        # Prune records older than 30 days
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        conn.execute(
            "DELETE FROM gex_metrics_history WHERE ticker = ? AND timestamp < ?",
            (ticker, cutoff),
        )


def get_gex_metrics_history(ticker: str, days: int = 30) -> list[dict]:
    """Fetch up to `days` days of enhanced GEX metric snapshots."""
    since = (datetime.utcnow() - timedelta(days=days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT * FROM gex_metrics_history
               WHERE ticker = ? AND timestamp >= ?
               ORDER BY timestamp""",
            (ticker, since),
        ).fetchall()
    return [dict(r) for r in rows]


def _pctile_and_zscore(value: float, series: list[float]) -> tuple[float, float]:
    """Compute percentile rank and z-score of value vs series."""
    if not series:
        return 50.0, 0.0
    n = len(series)
    pct = sum(1 for v in series if v < value) / n * 100.0
    mean = sum(series) / n
    variance = sum((v - mean) ** 2 for v in series) / n
    std = variance ** 0.5
    z = (value - mean) / std if std > 0 else 0.0
    return round(pct, 1), round(z, 2)


def get_gex_percentiles_30d(ticker: str, current: dict) -> dict:
    """
    Compute trailing 30-day percentiles and z-scores for key GEX metrics.

    Args:
        ticker:   underlying symbol
        current:  dict with keys net_gex_m, abs_gex_m, abs_gex_0dte_m

    Returns dict with *_pctile and *_zscore keys.
    """
    history = get_gex_metrics_history(ticker, days=30)
    net_series      = [r["net_gex_m"]      for r in history]
    abs_series      = [r["abs_gex_m"]      for r in history]
    zdte_series     = [r["abs_gex_0dte_m"] for r in history]
    # 0DTE share series
    share_series    = [
        r["abs_gex_0dte_m"] / r["abs_gex_m"] if r["abs_gex_m"] > 0 else 0.0
        for r in history
    ]

    cur_net   = current.get("net_gex_m", 0.0)
    cur_abs   = current.get("abs_gex_m", 0.0)
    cur_zdte  = current.get("abs_gex_0dte_m", 0.0)
    cur_share = cur_zdte / cur_abs if cur_abs > 0 else 0.0

    net_pct,   net_z   = _pctile_and_zscore(cur_net,   net_series)
    abs_pct,   abs_z   = _pctile_and_zscore(cur_abs,   abs_series)
    zdte_pct,  zdte_z  = _pctile_and_zscore(cur_zdte,  zdte_series)
    share_pct, share_z = _pctile_and_zscore(cur_share, share_series)

    return {
        "net_gex_pctile":      net_pct,
        "net_gex_zscore":      net_z,
        "abs_gex_pctile":      abs_pct,
        "abs_gex_zscore":      abs_z,
        "zdte_abs_pctile":     zdte_pct,
        "zdte_abs_zscore":     zdte_z,
        "zdte_share_pctile":   share_pct,
        "zdte_share_zscore":   share_z,
        "history_count":       len(history),
    }


# ── OVI History ───────────────────────────────────────────────────────────────

def save_ovi_snapshot(ticker: str, date_str: str, call_vol: int, put_vol: int,
                      call_oi: int, put_oi: int, spot_price: float | None,
                      px_pct_chg: float | None):
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO ovi_history
               (ticker, date, call_vol, put_vol, call_oi, put_oi, spot_price, px_pct_chg)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(ticker, date) DO UPDATE SET
               call_vol=excluded.call_vol, put_vol=excluded.put_vol,
               call_oi=excluded.call_oi, put_oi=excluded.put_oi,
               spot_price=excluded.spot_price, px_pct_chg=excluded.px_pct_chg""",
            (ticker, date_str, call_vol, put_vol, call_oi, put_oi, spot_price, px_pct_chg),
        )


def get_ovi_history(ticker: str, days: int = 20) -> list[dict]:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    since = (datetime.utcnow() - timedelta(days=days + 14)).strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            """SELECT date, call_vol, put_vol, call_oi, put_oi
               FROM ovi_history
               WHERE ticker = ? AND date != ? AND date >= ?
               ORDER BY date DESC LIMIT ?""",
            (ticker, today, since, days),
        ).fetchall()
    return [dict(r) for r in rows]


def save_ovi_report(report_json: str):
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("DELETE FROM ovi_report")
        conn.execute("INSERT INTO ovi_report (timestamp, report_json) VALUES (?, ?)", (ts, report_json))


def get_latest_ovi_report() -> dict | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT report_json FROM ovi_report ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row:
        try:
            return json.loads(row["report_json"])
        except Exception:
            return None
    return None


# ── News Cache ────────────────────────────────────────────────────────────────

def save_news_items(items: list[dict]):
    """Insert new news items; UNIQUE headline constraint prevents duplicates."""
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for item in items:
            ticker = item.get("primary_ticker") or (
                item["tickers"][0] if item.get("tickers") else None
            )
            conn.execute(
                """INSERT OR IGNORE INTO news_cache
                   (headline, source, ticker, published, fetched_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (item.get("headline", ""), item.get("source", ""),
                 ticker, item.get("published", ""), ts),
            )
        # Clean up entries older than 24h
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        conn.execute("DELETE FROM news_cache WHERE fetched_at < ?", (cutoff,))


def get_recent_news(minutes: int = 30) -> list[dict]:
    since = (datetime.utcnow() - timedelta(minutes=minutes)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM news_cache WHERE fetched_at >= ? ORDER BY fetched_at DESC",
            (since,),
        ).fetchall()
    return [dict(r) for r in rows]


def has_new_ticker_news(tickers: list[str], since_ts: str) -> bool:
    """Return True if any ticker-specific news was first inserted after since_ts."""
    if not tickers or not since_ts:
        return False
    placeholders = ",".join("?" * len(tickers))
    with get_conn() as conn:
        row = conn.execute(
            f"SELECT id FROM news_cache WHERE ticker IN ({placeholders}) "
            f"AND fetched_at >= ? LIMIT 1",
            (*tickers, since_ts),
        ).fetchone()
    return row is not None


# ── Watchlist Daily ───────────────────────────────────────────────────────────

def save_watchlist_daily(date_str: str, tiers: dict):
    """Save daily scan list by tier. tiers: {1: [tickers], 2: [...], ...}"""
    with get_conn() as conn:
        for tier, tickers in tiers.items():
            for ticker in tickers:
                conn.execute(
                    "INSERT OR IGNORE INTO watchlist_daily (date, ticker, tier) VALUES (?, ?, ?)",
                    (date_str, ticker, int(tier)),
                )


def get_cached_watchlist(date_str: str) -> list[str]:
    """Return cached scan list for date (empty list if not yet cached)."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ticker FROM watchlist_daily WHERE date = ? ORDER BY id",
            (date_str,),
        ).fetchall()
    return [r["ticker"] for r in rows]


def get_tier4_tickers(since_date_str: str) -> list[str]:
    """Tickers with recent vol spike: max vol in recent days > 2x historical avg."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            WITH recent AS (
                SELECT ticker, MAX(call_vol) AS max_c, MAX(put_vol) AS max_p
                FROM ovi_history WHERE date >= ?
                GROUP BY ticker
            ),
            hist AS (
                SELECT ticker, AVG(call_vol) AS avg_c, AVG(put_vol) AS avg_p
                FROM ovi_history WHERE date < ?
                GROUP BY ticker
            )
            SELECT r.ticker
            FROM recent r
            JOIN hist h ON r.ticker = h.ticker
            WHERE (h.avg_c > 0 AND r.max_c > 2 * h.avg_c)
               OR (h.avg_p > 0 AND r.max_p > 2 * h.avg_p)
            """,
            (since_date_str, since_date_str),
        ).fetchall()
    return [r["ticker"] for r in rows]


# ── Roll Dedup ────────────────────────────────────────────────────────────────

def has_roll_alert(ticker: str, contract_key: str) -> bool:
    """contract_key = '{ticker}_{strike}_{expiry}_{call_put}'"""
    cutoff = (datetime.utcnow() - timedelta(hours=DEDUP_HOURS)).isoformat()
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM alerts_sent WHERE ticker = ? AND signal_type = 'ROLL' AND alert_text LIKE ? AND timestamp >= ?",
            (ticker, f"%{contract_key}%", cutoff),
        ).fetchone()
    return row is not None


# ── Universe ──────────────────────────────────────────────────────────────────

def save_universe(tickers: dict[str, dict]):
    """
    Upsert universe tickers.
    tickers: {ticker: {in_sp500, in_ndx, in_rut, option_root}}
    """
    today = datetime.utcnow().isoformat()
    with get_conn() as conn:
        for ticker, flags in tickers.items():
            conn.execute(
                """INSERT INTO universe
                   (ticker, in_sp500, in_ndx, in_rut, option_root, added_date, last_verified)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(ticker) DO UPDATE SET
                   in_sp500=excluded.in_sp500,
                   in_ndx=excluded.in_ndx,
                   in_rut=excluded.in_rut,
                   option_root=excluded.option_root,
                   last_verified=excluded.last_verified""",
                (
                    ticker,
                    int(flags.get("in_sp500", False)),
                    int(flags.get("in_ndx", False)),
                    int(flags.get("in_rut", False)),
                    flags.get("option_root", ticker),
                    today, today,
                ),
            )


def get_universe_tickers() -> dict[str, dict]:
    """Return full universe as {ticker: {in_sp500, in_ndx, in_rut, option_root}}."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ticker, in_sp500, in_ndx, in_rut, option_root FROM universe ORDER BY ticker"
        ).fetchall()
    return {
        r["ticker"]: {
            "in_sp500":    bool(r["in_sp500"]),
            "in_ndx":      bool(r["in_ndx"]),
            "in_rut":      bool(r["in_rut"]),
            "option_root": r["option_root"],
        }
        for r in rows
    }


def get_universe_last_verified() -> str | None:
    """Return the most recent last_verified timestamp across all universe rows."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT MAX(last_verified) AS lv FROM universe"
        ).fetchone()
    return row["lv"] if row else None


def get_universe_option_root_map() -> dict[str, str]:
    """Return {option_root → ticker} for fast lookup when parsing option symbols."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT ticker, option_root FROM universe"
        ).fetchall()
    return {r["option_root"]: r["ticker"] for r in rows}


# ── GEX Strike Snapshots (heatmap) ───────────────────────────────────────────

def save_gex_snapshot(ticker: str, spot: float, strikes: list) -> None:
    """Persist a per-strike GEX snapshot for the heatmap. Prunes rows > 3 days old."""
    ts = datetime.utcnow().isoformat()
    cutoff = (datetime.utcnow() - timedelta(days=3)).isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO gex_strike_snapshots (ticker, timestamp, spot, strikes_json) VALUES (?, ?, ?, ?)",
            (ticker, ts, spot, json.dumps(strikes)),
        )
        conn.execute(
            "DELETE FROM gex_strike_snapshots WHERE timestamp < ?", (cutoff,)
        )


def get_today_gex_snapshots(ticker: str) -> list[dict]:
    """Return all snapshots stored today for one ticker, oldest first."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT timestamp, spot, strikes_json FROM gex_strike_snapshots "
            "WHERE ticker = ? AND timestamp LIKE ? ORDER BY timestamp",
            (ticker, f"{today}%"),
        ).fetchall()
    result = []
    for row in rows:
        try:
            strikes = json.loads(row["strikes_json"])
        except Exception:
            strikes = []
        result.append({
            "timestamp": row["timestamp"],
            "spot": float(row["spot"]),
            "strikes": strikes,
        })
    return result


# ── Intraday Snapshots ────────────────────────────────────────────────────────

def save_intraday_snapshot(data: dict) -> None:
    """Save a dashboard snapshot (JSON blob). Keeps only the 3 most recent."""
    ts = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO intraday_snapshots (timestamp, data_json) VALUES (?, ?)",
            (ts, json.dumps(data)),
        )
        # Prune to 3 most recent
        conn.execute("""
            DELETE FROM intraday_snapshots
            WHERE id NOT IN (
                SELECT id FROM intraday_snapshots
                ORDER BY id DESC LIMIT 3
            )
        """)


def get_prior_snapshot() -> dict | None:
    """Return the second-most-recent snapshot, or None if only one exists."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT data_json FROM intraday_snapshots ORDER BY id DESC LIMIT 2"
        ).fetchall()
    if len(rows) < 2:
        return None
    try:
        return json.loads(rows[1]["data_json"])
    except Exception:
        return None
