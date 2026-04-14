"""
OpenBB data provider for Kael ICT Bot.

Provides:
  - Economic calendar with high-impact event detection and blackout windows
  - News sentiment scoring for confluence factor adjustment

OpenBB is optional. If not installed the economic calendar returns [] and
the bot continues scanning and trading normally. Install with:
    pip install openbb

Data sources (attempted in order):
  1. OpenBB Platform + econdb provider (free, no key)
  2. FMP demo API fallback (free, limited)
  3. Empty list (bot keeps running)
"""

import time as _time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

# ── OpenBB availability ────────────────────────────────────────────────────────
try:
    from openbb import obb as _obb
    _OBB_AVAILABLE = True
except ImportError:
    _obb = None
    _OBB_AVAILABLE = False

# ── Blackout window around high-impact events ──────────────────────────────────
BLACKOUT_BEFORE_MIN = 30   # Pause N minutes BEFORE event
BLACKOUT_AFTER_MIN  = 15   # Resume N minutes AFTER event

# Keywords that flag any event as high-impact regardless of the "impact" field.
# These are the ICT-relevant macro catalysts that can spike price irrationally.
HIGH_IMPACT_KEYWORDS = [
    "fomc", "fed", "federal reserve", "interest rate", "rate decision",
    "nonfarm", "nfp", "non-farm", "employment", "jobs", "payroll",
    "cpi", "consumer price index", "inflation",
    "ppi", "producer price",
    "gdp", "gross domestic product",
    "pce", "personal consumption",
    "ism manufacturing", "ism services",
    "retail sales", "jolts", "jolts job openings",
    "powell", "unemployment", "initial claims",
    "trade balance", "durable goods",
]

# ── Internal caches ────────────────────────────────────────────────────────────
_ECON_CACHE: dict = {"events": [], "fetched_at": 0.0}
_ECON_TTL   = 3600   # Refresh calendar once per hour

_NEWS_CACHE: dict = {}   # symbol -> (fetched_at, score, headlines)
_NEWS_TTL   = 900        # Refresh sentiment every 15 minutes

# ── Headline sentiment keywords ────────────────────────────────────────────────
_BULL = [
    "rally", "surge", "gain", "rise", "jump", "soar", "record high",
    "upgrade", "recovery", "rebound", "beats", "beats expectations",
    "strong", "positive", "optimism", "growth", "expansion",
]
_BEAR = [
    "crash", "fall", "drop", "decline", "plunge", "selloff", "sell-off",
    "downgrade", "concern", "warning", "risk", "recession",
    "uncertainty", "miss", "disappoints", "weak", "fear",
    "panic", "contraction", "downturn",
]


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC API — ECONOMIC CALENDAR
# ──────────────────────────────────────────────────────────────────────────────

def get_economic_calendar(hours_ahead: int = 6) -> list:
    """
    Returns upcoming economic events for the next `hours_ahead` hours.

    Each event dict:
    {
        name:            str,
        time_et:         datetime (ET, timezone-aware),
        impact:          str  ("high" | "medium" | "low"),
        currency:        str,
        is_high_impact:  bool,
        blackout_active: bool,
        blackout_start:  str  (ISO),
        blackout_end:    str  (ISO),
        minutes_away:    int,
    }

    Safe to call every scan cycle. Cached for 1 hour.
    Returns [] on any failure — never blocks the bot.
    """
    now_ts = _time.time()
    if _ECON_CACHE["events"] and (now_ts - _ECON_CACHE["fetched_at"]) < _ECON_TTL:
        return _filter_upcoming(_ECON_CACHE["events"], hours_ahead)

    events = _fetch_calendar()
    _ECON_CACHE["events"]     = events
    _ECON_CACHE["fetched_at"] = now_ts
    return _filter_upcoming(events, hours_ahead)


def is_news_blackout() -> tuple:
    """
    Returns (blackout_active: bool, reason: str).

    True when a high-impact economic event is within the blackout window
    (BLACKOUT_BEFORE_MIN before or BLACKOUT_AFTER_MIN after the event).

    Safe to call every 15 seconds — calendar is cached internally.
    """
    try:
        events = get_economic_calendar(hours_ahead=2)
        for ev in events:
            if ev.get("blackout_active"):
                name = ev["name"]
                mins = ev["minutes_away"]
                if mins >= 0:
                    return True, f"NEWS BLACKOUT: {name} in {mins}m — trading paused"
                else:
                    return True, f"NEWS BLACKOUT: {name} released {abs(mins)}m ago — cooling off"
    except Exception as e:
        print(f"[OPENBB] Blackout check error: {e}")
    return False, ""


def get_calendar_summary() -> list:
    """
    Returns upcoming events formatted for the dashboard /api/economic-calendar endpoint.
    Each event has serializable (string) fields only.
    """
    try:
        events = get_economic_calendar(hours_ahead=24)
        out = []
        for ev in events:
            out.append({
                "name":            ev["name"],
                "time_str":        ev["time_et"].strftime("%H:%M ET"),
                "impact":          ev.get("impact", "low"),
                "currency":        ev.get("currency", "USD"),
                "is_high_impact":  ev.get("is_high_impact", False),
                "blackout_active": ev.get("blackout_active", False),
                "minutes_away":    ev.get("minutes_away", 0),
            })
        return out
    except Exception:
        return []


# ──────────────────────────────────────────────────────────────────────────────
# PUBLIC API — NEWS SENTIMENT
# ──────────────────────────────────────────────────────────────────────────────

def get_news_sentiment(symbol: str) -> tuple:
    """
    Returns (sentiment_score: float, headlines: list[str]).

    sentiment_score ranges from -1.0 (strongly bearish) to +1.0 (strongly bullish).
    Uses keyword analysis on recent headlines. Cached 15 minutes per symbol.
    No API key required.
    """
    now_ts = _time.time()
    cached = _NEWS_CACHE.get(symbol)
    if cached and (now_ts - cached[0]) < _NEWS_TTL:
        return cached[1], cached[2]

    headlines = _fetch_headlines(symbol)
    score     = _score_sentiment(headlines)

    _NEWS_CACHE[symbol] = (now_ts, score, headlines[:5])
    return score, headlines[:5]


# ──────────────────────────────────────────────────────────────────────────────
# INTERNAL — CALENDAR FETCHING
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_calendar() -> list:
    events = []

    # ── 1. OpenBB + econdb (free, no API key) ─────────────────────────────
    if _OBB_AVAILABLE:
        try:
            today = datetime.now(_ET).strftime("%Y-%m-%d")
            result = _obb.economy.calendar(start_date=today, provider="econdb")
            if result and result.results:
                for ev in result.results:
                    parsed = _parse_obb_event(ev)
                    if parsed:
                        events.append(parsed)
            if events:
                print(f"[OPENBB] Calendar: {len(events)} events loaded via econdb")
                return events
        except Exception as e:
            print(f"[OPENBB] Calendar (econdb) failed: {e}")

    # ── 2. FMP free API fallback ───────────────────────────────────────────
    try:
        import requests
        from datetime import date
        today = date.today().isoformat()
        r = requests.get(
            "https://financialmodelingprep.com/api/v3/economic_calendar",
            params={"from": today, "to": today, "apikey": "demo"},
            timeout=8,
        )
        if r.status_code == 200:
            raw = r.json()
            if isinstance(raw, list) and raw:
                for ev in raw:
                    try:
                        raw_dt = ev.get("date", "")
                        event_time = None
                        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                            try:
                                event_time = datetime.strptime(raw_dt[:len(fmt)], fmt).replace(tzinfo=_ET)
                                break
                            except (ValueError, TypeError):
                                pass
                        if event_time is None:
                            continue
                        impact = (ev.get("impact") or "Low").lower()
                        events.append({
                            "name":     str(ev.get("event", "Unknown")),
                            "time_et":  event_time,
                            "impact":   impact if impact in ("high", "medium", "low") else "low",
                            "currency": str(ev.get("currency", "USD")),
                        })
                    except Exception:
                        continue
                if events:
                    print(f"[OPENBB] Calendar: {len(events)} events loaded via FMP")
                    return events
    except Exception as e:
        print(f"[OPENBB] Calendar (FMP fallback) failed: {e}")

    # ── 3. Hardcoded 2026 high-impact schedule (final fallback) ───────────
    # All times are ET. Dates sourced from Fed, BLS, and BEA public calendars.
    events = _hardcoded_2026_schedule()
    if events:
        print(f"[OPENBB] Calendar: using hardcoded 2026 schedule ({len(events)} events total, filtering to today)")
    return events


# Major 2026 US economic event schedule — hardcoded from official Fed/BLS/BEA releases.
# Used only when both OpenBB and FMP are unavailable. Times are ET.
_2026_SCHEDULE = [
    # FOMC Rate Decisions (2:00 PM ET, statement day)
    ("2026-01-29", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    ("2026-03-19", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    ("2026-04-30", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    ("2026-06-18", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    ("2026-07-30", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    ("2026-09-17", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    ("2026-11-05", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    ("2026-12-10", "14:00", "FOMC Interest Rate Decision",    "high", "USD"),
    # Nonfarm Payrolls (8:30 AM ET, first Friday of month)
    ("2026-01-09", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-02-06", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-03-06", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-04-03", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-05-01", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-06-05", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-07-10", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-08-07", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-09-04", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-10-02", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-11-06", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    ("2026-12-04", "08:30", "Nonfarm Payrolls (NFP)",         "high", "USD"),
    # CPI (8:30 AM ET, BLS schedule)
    ("2026-01-15", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-02-12", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-03-12", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-04-10", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-05-13", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-06-11", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-07-15", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-08-12", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-09-10", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-10-14", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-11-12", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    ("2026-12-10", "08:30", "CPI (Consumer Price Index)",     "high", "USD"),
    # PPI (8:30 AM ET, day after CPI)
    ("2026-01-16", "08:30", "PPI (Producer Price Index)",     "medium", "USD"),
    ("2026-02-13", "08:30", "PPI (Producer Price Index)",     "medium", "USD"),
    ("2026-03-13", "08:30", "PPI (Producer Price Index)",     "medium", "USD"),
    ("2026-04-14", "08:30", "PPI (Producer Price Index)",     "medium", "USD"),
    ("2026-05-14", "08:30", "PPI (Producer Price Index)",     "medium", "USD"),
    # PCE / Core PCE (8:30 AM ET, monthly)
    ("2026-01-30", "08:30", "PCE Price Index",                "high", "USD"),
    ("2026-02-27", "08:30", "PCE Price Index",                "high", "USD"),
    ("2026-03-27", "08:30", "PCE Price Index",                "high", "USD"),
    ("2026-04-30", "08:30", "PCE Price Index",                "high", "USD"),
    ("2026-05-29", "08:30", "PCE Price Index",                "high", "USD"),
    ("2026-06-26", "08:30", "PCE Price Index",                "high", "USD"),
    ("2026-07-31", "08:30", "PCE Price Index",                "high", "USD"),
    # GDP (8:30 AM ET, quarterly advance estimate)
    ("2026-01-29", "08:30", "GDP Advance Estimate Q4 2025",   "high", "USD"),
    ("2026-04-29", "08:30", "GDP Advance Estimate Q1 2026",   "high", "USD"),
    ("2026-07-30", "08:30", "GDP Advance Estimate Q2 2026",   "high", "USD"),
    ("2026-10-29", "08:30", "GDP Advance Estimate Q3 2026",   "high", "USD"),
    # ISM Manufacturing (10:00 AM ET, first business day of month)
    ("2026-02-02", "10:00", "ISM Manufacturing PMI",          "medium", "USD"),
    ("2026-03-02", "10:00", "ISM Manufacturing PMI",          "medium", "USD"),
    ("2026-04-01", "10:00", "ISM Manufacturing PMI",          "medium", "USD"),
    ("2026-05-01", "10:00", "ISM Manufacturing PMI",          "medium", "USD"),
    ("2026-06-01", "10:00", "ISM Manufacturing PMI",          "medium", "USD"),
    # Retail Sales (8:30 AM ET, mid-month)
    ("2026-01-16", "08:30", "Retail Sales",                   "medium", "USD"),
    ("2026-02-13", "08:30", "Retail Sales",                   "medium", "USD"),
    ("2026-03-13", "08:30", "Retail Sales",                   "medium", "USD"),
    ("2026-04-15", "08:30", "Retail Sales",                   "medium", "USD"),
    ("2026-05-15", "08:30", "Retail Sales",                   "medium", "USD"),
]


def _hardcoded_2026_schedule() -> list:
    """Build the hardcoded 2026 schedule as event dicts."""
    events = []
    for date_str, time_str, name, impact, currency in _2026_SCHEDULE:
        try:
            event_time = datetime.strptime(
                f"{date_str} {time_str}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=_ET)
            events.append({
                "name":     name,
                "time_et":  event_time,
                "impact":   impact,
                "currency": currency,
            })
        except Exception:
            continue
    return events


def _parse_obb_event(ev) -> dict:
    """Extract a standardized event dict from an OpenBB result object."""
    event_time = None
    for attr in ("date", "event_date", "time", "release_date", "datetime", "timestamp"):
        val = getattr(ev, attr, None)
        if val is None:
            continue
        if isinstance(val, datetime):
            event_time = val.astimezone(_ET) if val.tzinfo else val.replace(tzinfo=_ET)
            break
        if isinstance(val, str) and val:
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S",
                        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    trimmed = val[:len(fmt)]
                    event_time = datetime.strptime(trimmed, fmt).replace(tzinfo=_ET)
                    break
                except ValueError:
                    pass
            if event_time:
                break

    if event_time is None:
        return None

    name     = str(getattr(ev, "event",      getattr(ev, "name",     "Unknown")))
    impact   = str(getattr(ev, "importance", getattr(ev, "impact",   "low"))).lower().strip()
    currency = str(getattr(ev, "currency",   getattr(ev, "country",  "USD"))).upper()[:6]

    # Normalise importance values to standard labels
    if impact in ("3", "high", "red", "***", "very high"):
        impact = "high"
    elif impact in ("2", "medium", "orange", "**", "moderate"):
        impact = "medium"
    else:
        impact = "low"

    return {"name": name, "time_et": event_time, "impact": impact, "currency": currency}


def _filter_upcoming(events: list, hours_ahead: int) -> list:
    """Filter events to window and annotate each with blackout status."""
    now_et  = datetime.now(_ET)
    cutoff  = now_et + timedelta(hours=hours_ahead)
    lookback = now_et - timedelta(hours=1)   # Include recent events still in post-blackout
    result  = []

    for ev in events:
        t = ev.get("time_et")
        if not isinstance(t, datetime):
            continue
        if not (lookback <= t <= cutoff):
            continue

        name_lower = ev["name"].lower()
        impact     = ev.get("impact", "")

        is_high = (
            impact == "high" or
            any(kw in name_lower for kw in HIGH_IMPACT_KEYWORDS)
        )

        blackout_start  = t - timedelta(minutes=BLACKOUT_BEFORE_MIN)
        blackout_end    = t + timedelta(minutes=BLACKOUT_AFTER_MIN)
        blackout_active = is_high and (blackout_start <= now_et <= blackout_end)
        mins_away       = int((t - now_et).total_seconds() / 60)

        result.append({
            **ev,
            "is_high_impact":  is_high,
            "blackout_active": blackout_active,
            "blackout_start":  blackout_start.isoformat(),
            "blackout_end":    blackout_end.isoformat(),
            "minutes_away":    mins_away,
        })

    return sorted(result, key=lambda x: x["time_et"])


# ──────────────────────────────────────────────────────────────────────────────
# INTERNAL — NEWS SENTIMENT
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_headlines(symbol: str) -> list:
    headlines = []

    # Try OpenBB equity news (uses yfinance provider, no key required)
    if _OBB_AVAILABLE:
        try:
            sym_map = {"MNQ": "QQQ", "MGC": "GLD", "NQ": "QQQ", "GC": "GLD"}
            news_sym = sym_map.get(symbol.upper(), "SPY")
            result = _obb.equity.news(symbol=news_sym, limit=10, provider="yfinance")
            if result and result.results:
                for item in result.results:
                    title = str(getattr(item, "title", getattr(item, "headline", "")))
                    if title.strip():
                        headlines.append(title.strip())
        except Exception:
            pass

    # Fallback: yfinance direct (uses liquid ETF proxies since futures have no news feed)
    if not headlines:
        try:
            import yfinance as yf
            # Map futures symbols to ETF proxies that actually have news
            news_proxy_map = {"MNQ": "QQQ", "NQ": "QQQ", "MGC": "GLD", "GC": "GLD"}
            ticker_sym = news_proxy_map.get(symbol.upper(), "SPY")
            news_data  = yf.Ticker(ticker_sym).news or []
            for item in news_data[:10]:
                content = item.get("content", {})
                title   = content.get("title", item.get("title", ""))
                if title:
                    headlines.append(str(title).strip())
        except Exception:
            pass

    return headlines


def _score_sentiment(headlines: list) -> float:
    """Keyword-count sentiment from -1.0 (bearish) to +1.0 (bullish)."""
    total, count = 0.0, 0
    for h in headlines:
        h_low = h.lower()
        bull  = sum(1 for w in _BULL if w in h_low)
        bear  = sum(1 for w in _BEAR if w in h_low)
        if bull + bear > 0:
            total += (bull - bear) / (bull + bear)
            count += 1
    if count == 0:
        return 0.0
    return round(max(-1.0, min(1.0, total / count)), 2)
