"""Market news context compiler for OVI AI analysis."""
import json
import re
import socket
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from time import mktime

import feedparser

from config import POLYGON_API_KEY
from db.database import save_news_items

POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"

# Replaced Reuters (403-blocked) and WSJ (stale Jan-2025 feed) with live feeds.
RSS_FEEDS = [
    ("marketwatch",  "https://feeds.marketwatch.com/marketwatch/topstories/"),
    ("cnbc_markets", "https://search.cnbc.com/rs/search/combinedcms/view.xml"
                     "?partnerId=wrss01&id=20910258"),
    ("ft_markets",   "https://www.ft.com/rss/home/uk"),
]

# Word-boundary patterns prevent substring false-positives:
# "ppi" matched "shi_ppi_ng"; "rate" matched "ope_rate_; "fed" matched "of_fe_red"
_MACRO_KWS = [
    "fed", "federal reserve", "fomc", "cpi", "inflation", "gdp", "jobs", "payrolls",
    "recession", "rate hike", "rate cut", "rate", "treasury", "yield", "dollar",
    "vix", "tariff", "interest rate", "unemployment", "pce", "ppi", "debt ceiling",
    "central bank", "trade war", "sanctions", "geopolitical", "crude", "oil",
]
_VOL_KWS = [
    "volatility", "options", "puts", "calls", "hedging", "hedge", "derivatives",
    "gamma", "earnings", "short squeeze", "buyback",
]
_IRREL_KWS = [
    "sports", "weather", "celebrity", "entertainment", "lifestyle", "recipe",
    "fashion", "travel",
]

MACRO_PATTERNS  = [re.compile(r'\b' + re.escape(k) + r'\b', re.I) for k in _MACRO_KWS]
VOL_PATTERNS    = [re.compile(r'\b' + re.escape(k) + r'\b', re.I) for k in _VOL_KWS]
IRREL_PATTERNS  = [re.compile(r'\b' + re.escape(k) + r'\b', re.I) for k in _IRREL_KWS]

MIN_RELEVANCE_SCORE = 3
MAX_CONTEXT_ITEMS   = 8
RSS_MAX_AGE_HOURS   = 24   # skip RSS articles older than this (avoids stale feeds)


def _fetch_polygon_market_news() -> list[dict]:
    """Fetch top 10 market-wide headlines from Polygon (no ticker filter)."""
    if not POLYGON_API_KEY:
        return []
    try:
        url = (
            f"{POLYGON_NEWS_URL}?limit=10&order=desc&apiKey={POLYGON_API_KEY}"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "options-alert-system/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
        items = []
        for article in data.get("results", []):
            items.append({
                "headline":       article.get("title", ""),
                "source":         article.get("publisher", {}).get("name", "Polygon"),
                "tickers":        article.get("tickers", []),
                "published":      article.get("published_utc", ""),
                "type":           "polygon_market",
                "primary_ticker": None,
            })
        return items
    except Exception as e:
        print(f"  [news] Polygon market-wide error: {e}")
        return []


def _fetch_polygon_news(ovi_tickers: list[str]) -> list[dict]:
    """Ticker-specific Polygon news for all OVI tickers (last 24h)."""
    items = []
    if not POLYGON_API_KEY:
        return items

    since = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    seen_urls: set[str] = set()

    for ticker in ovi_tickers[:10]:   # top 10 OVI tickers
        try:
            url = (
                f"{POLYGON_NEWS_URL}?ticker={ticker}&limit=3"
                f"&published_utc.gte={since}&apiKey={POLYGON_API_KEY}"
            )
            req = urllib.request.Request(url, headers={"User-Agent": "options-alert-system/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode())
            for article in data.get("results", []):
                art_url = article.get("article_url", "")
                if art_url in seen_urls:
                    continue
                seen_urls.add(art_url)
                items.append({
                    "headline":      article.get("title", ""),
                    "source":        article.get("publisher", {}).get("name", "Polygon"),
                    "tickers":       article.get("tickers", []),
                    "published":     article.get("published_utc", ""),
                    "type":          "polygon_ticker",
                    "primary_ticker": ticker,
                })
        except Exception as e:
            print(f"  [news] Polygon {ticker} error: {e}")

    return items


def _fetch_rss_feeds() -> list[dict]:
    """Fetch RSS feeds with 6s socket timeout, skipping articles older than RSS_MAX_AGE_HOURS."""
    items = []
    cutoff_dt = datetime.utcnow() - timedelta(hours=RSS_MAX_AGE_HOURS)
    old_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(6)
    try:
        for feed_name, feed_url in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
                for entry in feed.entries[:15]:
                    pub_str = ""
                    pub_dt  = None
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        pub_dt  = datetime.utcfromtimestamp(mktime(entry.published_parsed))
                        pub_str = pub_dt.isoformat()

                    # Skip stale articles
                    if pub_dt and pub_dt < cutoff_dt:
                        continue

                    title = entry.get("title", "").strip()
                    if not title:
                        continue
                    items.append({
                        "headline":       title,
                        "source":         feed_name,
                        "tickers":        [],
                        "published":      pub_str,
                        "type":           "rss",
                        "primary_ticker": None,
                    })
            except Exception as e:
                print(f"  [news] RSS {feed_name} error: {e}")
    finally:
        socket.setdefaulttimeout(old_timeout)
    return items


def _score_headline(headline: str, ovi_tickers: list[str]) -> int:
    """
    Score relevance using word-boundary regex (prevents 'ppi' matching 'shipping').
    macro hit: +3 each (cap +6) | ticker hit: +2 | vol keyword: +2 each (cap +4)
    irrelevant keyword: -2 each
    """
    macro_hits = sum(1 for p in MACRO_PATTERNS  if p.search(headline))
    vol_hits   = sum(1 for p in VOL_PATTERNS    if p.search(headline))
    irrel_hits = sum(1 for p in IRREL_PATTERNS  if p.search(headline))

    score = min(macro_hits * 3, 6) + min(vol_hits * 2, 4) - (irrel_hits * 2)

    # Bonus: headline mentions one of our OVI tickers (word-boundary safe for tickers)
    hl_lower = headline.lower()
    for ticker in ovi_tickers:
        # Ticker symbols are short — check with word boundary
        pat = re.compile(r'\b' + re.escape(ticker.lower()) + r'\b')
        if pat.search(hl_lower):
            score += 2
            break

    return score


def _deduplicate(items: list[dict]) -> list[dict]:
    """Remove near-duplicates by first 60 chars of headline."""
    seen: set[str] = set()
    unique = []
    for item in items:
        key = item["headline"][:60].lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(item)
    return unique


def compile_market_context(ovi_tickers: list[str]) -> str:
    """
    Fetch, score, and format market news for Claude context injection.
    Returns formatted string or empty string if nothing relevant found.
    """
    try:
        polygon_items  = _fetch_polygon_news(ovi_tickers)
        polygon_market = _fetch_polygon_market_news()
        rss_items      = _fetch_rss_feeds()
        all_items      = _deduplicate(polygon_market + polygon_items + rss_items)

        scored = []
        for item in all_items:
            score = _score_headline(item["headline"], ovi_tickers)
            if score >= MIN_RELEVANCE_SCORE:
                scored.append((score, item))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_items = [item for _, item in scored[:MAX_CONTEXT_ITEMS]]

        if top_items:
            try:
                save_news_items(top_items)
            except Exception:
                pass

        if not top_items:
            return ""

        lines = ["MARKET CONTEXT (recent headlines):"]
        for item in top_items:
            src      = item.get("source", "")
            headline = item.get("headline", "")
            tickers  = item.get("tickers", [])
            tk_str   = f" [{', '.join(tickers[:3])}]" if tickers else ""
            lines.append(f"- [{src}]{tk_str} {headline}")

        return "\n".join(lines)

    except Exception as e:
        print(f"  [news] compile_market_context error: {e}")
        return ""
