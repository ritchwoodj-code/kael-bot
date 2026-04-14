"""
ICT Trading Bot — Main Runner
Scans for setups every 60 seconds during kill zones.
Executes on TopstepX via ProjectX API.

Run: python run_bot.py
Stop: Ctrl + C
"""

import os
import sys
import importlib
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import time
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# ─── LOG ROTATION — truncate bot_output.log if > 50MB ────────────────────────
_LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_output.log")
try:
    if os.path.exists(_LOG_FILE) and os.path.getsize(_LOG_FILE) > 50 * 1024 * 1024:
        with open(_LOG_FILE, "w", encoding="utf-8") as _lf:
            _lf.write(f"[STARTUP] Log truncated — exceeded 50MB\n")
except Exception:
    pass

# ─── SINGLE-INSTANCE LOCK ────────────────────────────────────────────────────
# PID-based lock: checks if the PID in bot.lock is still alive before blocking.
# Handles stale locks from crashed/killed processes gracefully.
_LOCK_FILE = os.path.join(os.path.dirname(__file__), "bot.lock")
_lock_fd = None

def _check_and_acquire_lock():
    if os.path.exists(_LOCK_FILE):
        try:
            with open(_LOCK_FILE) as _f:
                _old_pid = int(_f.read().strip())
            import psutil
            if psutil.pid_exists(_old_pid):
                # Double-check it's actually our bot, not a coincidental PID reuse
                try:
                    proc = psutil.Process(_old_pid)
                    cmdline = " ".join(proc.cmdline())
                    if "run_bot" in cmdline or "python" in cmdline.lower():
                        print(f"[ERROR] Bot already running (PID {_old_pid}). Stop it first.")
                        sys.exit(1)
                except Exception:
                    pass  # Can't read cmdline — assume stale
        except Exception:
            pass  # Unreadable lock file — treat as stale
    # Write our PID (overwrites any stale lock)
    try:
        with open(_LOCK_FILE, "w") as _f:
            _f.write(str(os.getpid()))
    except Exception:
        pass  # Can't write lock — non-fatal, continue anyway

_check_and_acquire_lock()

sys.path.append(".")

_BROKER = os.getenv("BROKER", "topstep").lower()
if _BROKER == "tradovate":
    from tradovate_executor import TradovateExecutor as _ExecutorClass
elif _BROKER == "paper":
    from paper_executor import PaperExecutor as _ExecutorClass
else:
    from executor import TopstepExecutor as _ExecutorClass
from ict_bot import TradingBot, Instrument, Bias, RISK_PARAMS
from journal import write_bot_entry, update_trade_exit

# ─── CONFIG ───────────────────────────────────────────────
# All micro instruments. Full-size (NQ, GC) intentionally excluded — too large for most prop accounts.
# Quantities are conservative: 1 contract each. Increase only after consistent profitability.
# FX and MCL have tighter daily loss limits set in RISK_PARAMS.
INSTRUMENTS = {
    # Equity Index Micros
    Instrument.MNQ: {"symbol": "MNQ", "quantity": 1},   # Micro Nasdaq     $2/pt
    Instrument.MES: {"symbol": "MES", "quantity": 1},   # Micro S&P 500    $5/pt
    Instrument.M2K: {"symbol": "M2K", "quantity": 1},   # Micro Russell    $5/pt
    Instrument.MYM: {"symbol": "MYM", "quantity": 1},   # Micro Dow        $0.50/pt
    # Metals Micro
    Instrument.MGC: {"symbol": "MGC", "quantity": 1},   # Micro Gold       $10/pt
    # Energy Micro
    Instrument.MCL: {"symbol": "MCL", "quantity": 1},   # Micro Crude Oil  $100/pt
    # FX Micros
    Instrument.M6E: {"symbol": "M6E", "quantity": 1},   # Micro EUR/USD    $1.25/pip
    Instrument.M6B: {"symbol": "M6B", "quantity": 1},   # Micro GBP/USD    $0.625/pip
}

# PAPER_TRADE: set to True to scan and log signals WITHOUT placing real orders.
# Set to False only when you are ready to go live with a funded account.
PAPER_TRADE = False

MIN_CONFIDENCE = 0.65          # Medium confidence threshold — more setups allowed through
SCAN_INTERVAL_SECONDS = 15
SIGNAL_COOLDOWN_SECONDS = 300  # Don't re-fire same instrument within this window
DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")
ET = ZoneInfo("America/New_York")

# ─── PLATFORM CONFIG SYNC ─────────────────────────────────
# When these are set the bot pulls risk settings from the SaaS dashboard every 5 min.
# Set PLATFORM_URL + PLATFORM_USER_ID + BOT_API_KEY in .env to enable.
PLATFORM_URL     = os.getenv("PLATFORM_URL", "").rstrip("/")   # e.g. https://kael.polarisdgtl.com
PLATFORM_USER_ID = os.getenv("PLATFORM_USER_ID", "")           # integer user id on the platform
PLATFORM_API_KEY = os.getenv("BOT_API_KEY", "")                # must match PLATFORM's BOT_API_KEY env var

# Runtime-mutable risk values — updated by _poll_platform_config() if platform is configured
_runtime = {
    "min_confidence":   MIN_CONFIDENCE,
    "daily_loss_limit": 500.0,
    "max_trades_day":   3,
    "bot_enabled":      True,
    "automation_locked": False,
    "open_symbols":     [],   # Symbols with open trades in the platform DB (duplicate guard)
}


def _poll_platform_config():
    """
    Fetches the user's risk settings from the Kael SaaS platform and updates
    _runtime so the bot enforces whatever the user set in their dashboard.
    Returns True on success, False if platform is not configured or unreachable.
    """
    if not PLATFORM_URL or not PLATFORM_USER_ID or not PLATFORM_API_KEY:
        return False
    try:
        url = f"{PLATFORM_URL}/api/user-config/{PLATFORM_USER_ID}"
        r   = requests.get(url, headers={"X-Bot-Key": PLATFORM_API_KEY}, timeout=5)
        if r.status_code != 200:
            log(f"[CONFIG SYNC] Platform returned {r.status_code} — keeping current settings")
            return False
        cfg = r.json()
        _runtime["min_confidence"]   = float(cfg.get("min_confidence",   _runtime["min_confidence"]))
        _runtime["daily_loss_limit"] = float(cfg.get("daily_loss_limit", _runtime["daily_loss_limit"]))
        _runtime["max_trades_day"]   = int(cfg.get("max_trades_day",     _runtime["max_trades_day"]))
        _runtime["bot_enabled"]      = bool(cfg.get("bot_enabled",       _runtime["bot_enabled"]))
        _runtime["automation_locked"]= bool(cfg.get("automation_locked", _runtime["automation_locked"]))
        _runtime["open_symbols"]     = list(cfg.get("open_symbols",      []))
        log(f"[CONFIG SYNC] OK — conf≥{_runtime['min_confidence']*100:.0f}% | "
            f"loss_limit=${_runtime['daily_loss_limit']} | "
            f"max_trades={_runtime['max_trades_day']} | "
            f"bot_enabled={_runtime['bot_enabled']} | "
            f"locked={_runtime['automation_locked']}")
        return True
    except Exception as e:
        log(f"[CONFIG SYNC] Failed to reach platform: {e} — keeping current settings")
        return False
# ──────────────────────────────────────────────────────────

ACTIVE_WINDOWS = [
    (3, 0, 5, 0),
    (8, 30, 12, 0),
    (10, 0, 12, 0),
    (13, 30, 16, 0),
]

# Set to True to force scan anytime regardless of kill zones
FORCE_SCAN = True
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
# ──────────────────────────────────────────────────────────


def now_et():
    return datetime.now(ET)


def in_active_window():
    et = now_et()
    mins = et.hour * 60 + et.minute
    for (sh, sm, eh, em) in ACTIVE_WINDOWS:
        start = sh * 60 + sm
        end = eh * 60 + em
        if start <= mins < end:
            return True
    return False


def save_state(signals, trades, status, last_scan, last_result="", account_id="", balance=0, daily_pnl=0, scan_log=None, balance_at_day_open=0, open_positions=None, symbol_biases=None):
    trimmed_signals = signals[-100:]
    trimmed_trades = trades[-50:]
    # Trim in-memory lists to prevent unbounded RAM growth (memory leak fix)
    if len(signals) > 110:
        del signals[:-100]
    if len(trades) > 60:
        del trades[:-50]
    # Atomic write — write to .tmp then rename to avoid corruption on Railway kill signals
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({
            "signals": trimmed_signals,
            "trades": trimmed_trades,
            "status": status,
            "last_scan": last_scan,
            "last_result": last_result,
            "account_id": account_id,
            "balance": balance,
            "daily_pnl": daily_pnl,
            "scan_log": scan_log or [],
            "balance_at_day_open": balance_at_day_open,
            "balance_date": now_et().strftime("%Y-%m-%d"),
            "last_fired": {str(k): v.isoformat() for k, v in (save_state._last_fired or {}).items()},
            "open_positions": open_positions or [],
        }, f)
    # Retry up to 5x — dashboard.py may hold a brief read lock on Windows
    for _attempt in range(5):
        try:
            os.replace(tmp, STATE_FILE)
            break
        except PermissionError:
            time.sleep(0.1)
    else:
        os.replace(tmp, STATE_FILE)  # Final attempt — raises if still locked

    # Push state to SaaS platform so the user's dashboard stays live
    if PLATFORM_URL and PLATFORM_USER_ID and PLATFORM_API_KEY:
        try:
            requests.post(
                f"{PLATFORM_URL}/api/bot-state",
                headers={"X-Bot-Key": PLATFORM_API_KEY},
                json={
                    "user_id":        int(PLATFORM_USER_ID),
                    "status":         status,
                    "balance":        balance,
                    "day_pnl":        daily_pnl,
                    "total_pnl":      daily_pnl,
                    "open_positions": open_positions or [],
                    "scan_log":       (scan_log or [])[-30:],
                    "symbol_biases":  symbol_biases or {},
                },
                timeout=5,
            )
        except Exception:
            pass  # Platform push is best-effort — never crash the bot over it


def log(msg):
    print(f"[{now_et().strftime('%Y-%m-%d %H:%M:%S ET')}] {msg}")


def preflight_check():
    """
    Run before the main loop. Validates all dependencies and connectivity.
    CRITICAL failures raise RuntimeError — bot will not start.
    Warnings are logged and skipped.
    """
    errors = []
    warnings = []

    # 1. Required packages importable
    for pkg in ["yfinance", "pandas", "numpy", "requests", "dotenv", "zoneinfo"]:
        try:
            importlib.import_module(pkg)
        except ImportError:
            errors.append(f"Required package not importable: {pkg}")

    # 2. psutil (non-critical — msvcrt handles the lock on Windows)
    try:
        import psutil  # noqa: F401
    except ImportError:
        warnings.append("psutil not installed — PID lock fallback disabled (Windows msvcrt handles this, OK)")

    # 3. Critical env vars
    if _BROKER == "tradovate":
        for var in ("TRADOVATE_USERNAME", "TRADOVATE_PASSWORD"):
            if not os.getenv(var):
                errors.append(f"Missing required env var: {var}")
    else:
        for var in ("TOPSTEP_USERNAME", "TOPSTEP_API_KEY"):
            if not os.getenv(var):
                errors.append(f"Missing required env var: {var}")
    if not os.getenv("DISCORD_WEBHOOK"):
        warnings.append("DISCORD_WEBHOOK not set — trade notifications will be silenced")

    # 4. yfinance live data feed (core dependency)
    # In paper mode, empty data just means the market is closed — not a startup blocker.
    # In live broker mode, we need confirmed data before risking real capital.
    try:
        import yfinance as yf
        for sym in ("MGC=F", "MNQ=F"):
            try:
                df = yf.Ticker(sym).history(interval="1m", period="1d")
                if df is None or len(df) == 0:
                    if _BROKER == "paper":
                        warnings.append(f"yfinance returned empty data for {sym} — market likely closed. Bot will scan normally during kill zones.")
                    else:
                        errors.append(f"yfinance returned empty data for {sym} — data feed unavailable")
                else:
                    log(f"[PREFLIGHT] yfinance OK — {len(df)} bars for {sym}")
            except Exception as exc:
                if _BROKER == "paper":
                    warnings.append(f"yfinance fetch failed for {sym}: {exc} — market likely closed, continuing anyway.")
                else:
                    errors.append(f"yfinance fetch failed for {sym}: {exc}")
    except ImportError:
        pass  # already caught in check 1

    # 5. bot_state.json integrity (auto-recover if corrupt)
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as _f:
                json.load(_f)
        except (json.JSONDecodeError, ValueError) as exc:
            warnings.append(f"bot_state.json corrupt ({exc}) — deleting and starting fresh")
            try:
                os.remove(STATE_FILE)
            except OSError:
                pass

    # 6. Log file writable
    try:
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_output.log")
        with open(log_path, "a"):
            pass
    except OSError as exc:
        warnings.append(f"bot_output.log not writable: {exc}")

    # 7. System clock sanity (catches VM clock drift)
    year = datetime.now(ET).year
    if not (2024 <= year <= 2028):
        errors.append(f"System clock appears wrong: year={year} — check VM/system time settings")

    # 8. Contract rollover warning — warn 7 days before CME expiry
    from datetime import date as _date
    _expiry_schedule = [
        (_date(2026, 6, 25), "MGC.M26 / MNQ.M26 (June 2026)"),
        (_date(2026, 9, 24), "MGC.U26 / MNQ.U26 (September 2026)"),
        (_date(2026, 12, 28), "MGC.Z26 / MNQ.Z26 (December 2026)"),
        (_date(2027, 3, 25), "MGC.H27 / MNQ.H27 (March 2027)"),
    ]
    today_d = now_et().date()
    for expiry_d, label in _expiry_schedule:
        days_left = (expiry_d - today_d).days
        if 0 < days_left <= 7:
            warnings.append(f"CONTRACT EXPIRY in {days_left} day(s): {label} — verify TopstepX rolls to next month")
        elif -2 <= days_left <= 0:
            errors.append(f"CONTRACT MAY BE EXPIRED: {label} — update contract months immediately")

    # Report
    for w in warnings:
        log(f"[PREFLIGHT WARNING] {w}")
    if errors:
        for e in errors:
            log(f"[PREFLIGHT CRITICAL] {e}")
        raise RuntimeError(f"Preflight failed ({len(errors)} critical issue(s)) — bot will not start.")
    log(f"[PREFLIGHT] All checks passed ({len(warnings)} warning(s)).")


def notify_discord(signal, symbol, side):
    direction = "LONG 📈" if side == "buy" else "SHORT 📉"
    color = 0x00FF99 if side == "buy" else 0xFF4444
    embed = {
        "title": f"⚡ KAEL TRADE FIRED — {symbol} {direction}",
        "color": color,
        "fields": [
            {"name": "Entry",      "value": str(signal.entry_price),   "inline": True},
            {"name": "Stop Loss",  "value": str(signal.stop_loss),     "inline": True},
            {"name": "Take Profit","value": str(signal.take_profit_2), "inline": True},
            {"name": "Confidence", "value": f"{signal.confidence * 100:.0f}%", "inline": True},
            {"name": "Setup",      "value": signal.setup_type.value,   "inline": True},
            {"name": "Session",    "value": signal.session,            "inline": True},
        ],
        "footer": {"text": f"Kael ICT Bot • {now_et().strftime('%Y-%m-%d %H:%M:%S ET')}"},
    }
    if not DISCORD_WEBHOOK:
        log("Discord notify skipped — no webhook configured")
        return
    try:
        r = requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=5)
        if r.status_code in (200, 204):
            log(f"Discord notified: {symbol} {side.upper()}")
        else:
            log(f"Discord notify failed: HTTP {r.status_code} — {r.text[:100]}")
    except Exception as e:
        log(f"Discord notify failed: {e}")


def run():
    log("ICT Bot starting up...")
    preflight_check()

    try:
        executor = _ExecutorClass()
    except Exception as e:
        log(f"FAILED to connect to {'Tradovate' if _BROKER == 'tradovate' else 'TopstepX'}: {e}")
        return

    # Account-level daily loss limit — computed once, never changes
    max_loss = min(RISK_PARAMS[instr]["max_daily_loss"] for instr in INSTRUMENTS)

    # One TradingBot per instrument
    bots = {instr: TradingBot(instrument=instr) for instr in INSTRUMENTS}
    symbols_str = ", ".join(cfg["symbol"] for cfg in INSTRUMENTS.values())
    log(f"Scanning {symbols_str} | Min confidence: {MIN_CONFIDENCE * 100:.0f}%")
    log("Press Ctrl+C to stop.\n")

    signal_log = []
    trade_log = []
    current_scan_log = []
    last_reset_date = now_et().date()
    last_config_poll = None   # Track last platform config fetch
    acct_id = executor.account_id or ""
    acct_balance, live_pnl = 0, 0
    last_known_pnl = None       # None until first successful balance read; loss guard skipped until established
    last_fired: dict = {}       # Tracks last trade time per instrument to prevent re-fires
    last_journal_update = None  # Tracks last journal sync time
    current_trade_id = None     # Journal ID of the currently open bot trade

    # Restore day-open balance AND last_fired cooldowns on restart
    balance_at_day_open = 0
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as _sf:
                _saved = json.load(_sf)
            if _saved.get("balance_date") == now_et().strftime("%Y-%m-%d"):
                balance_at_day_open = _saved.get("balance_at_day_open", 0)
                if balance_at_day_open:
                    log(f"Restored day-open balance from state: ${balance_at_day_open:,.2f}")
                # Restore cooldowns so restarts can't double-fire
                for k_str, ts_str in (_saved.get("last_fired") or {}).items():
                    try:
                        instr = next(i for i in INSTRUMENTS if str(i) == k_str)
                        fired_at = datetime.fromisoformat(ts_str).replace(tzinfo=ET)
                        if (now_et() - fired_at).total_seconds() < SIGNAL_COOLDOWN_SECONDS:
                            last_fired[instr] = fired_at
                            log(f"Restored cooldown for {instr.name} — {int((now_et()-fired_at).total_seconds())}s elapsed")
                    except Exception:
                        pass
    except Exception:
        pass

    # Wire last_fired into save_state so it can be persisted each cycle
    save_state._last_fired = last_fired

    # Per-scan bias tracker: {symbol: "bullish"|"bearish"|"neutral"} — populated each cycle
    _scan_biases: dict = {}

    if PAPER_TRADE:
        log("*** PAPER TRADE MODE — no real orders will be placed ***")

    # Initial platform config pull at startup
    _poll_platform_config()

    while True:
        et_now = now_et()
        try:
            # Poll platform for updated user risk settings every 5 min
            if last_config_poll is None or (et_now - last_config_poll).total_seconds() >= 300:
                _poll_platform_config()
                last_config_poll = et_now

            # Daily reset
            today = et_now.date()
            if today != last_reset_date:
                for bot in bots.values():
                    bot.reset_daily()
                last_reset_date = today
                last_fired.clear()
                # Snapshot balance at start of new day — required for true daily P&L
                fresh_bal, _ = executor.get_account_balance()
                if fresh_bal > 0:
                    balance_at_day_open = fresh_bal
                    acct_balance = fresh_bal
                log(f"Daily state reset. Day-open balance: ${balance_at_day_open:,.2f}")

            new_balance, _ = executor.get_account_balance()
            if new_balance > 0:
                acct_balance = new_balance
                if balance_at_day_open == 0:
                    # First successful balance read — set day-open baseline
                    balance_at_day_open = acct_balance
                live_pnl = round(acct_balance - balance_at_day_open, 2)
                last_known_pnl = live_pnl
            else:
                # API returned zeros — use last known value so loss guard stays active
                if last_known_pnl is not None:
                    live_pnl = last_known_pnl
            acct_id = executor.account_id or ""

            if not in_active_window() and not FORCE_SCAN:
                log("Outside trading window. Sleeping...")
                save_state(signal_log, trade_log, "sleeping", et_now.strftime("%H:%M:%S"), last_result="Outside kill zone", account_id=acct_id, balance=acct_balance, daily_pnl=live_pnl, balance_at_day_open=balance_at_day_open, open_positions=executor.get_open_positions() if executor else [], symbol_biases=_scan_biases)
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            if last_known_pnl is not None and live_pnl <= -_runtime["daily_loss_limit"]:
                log(f"Daily loss limit hit (${live_pnl:.2f}). Flattening all positions and stopping for today.")
                executor.flatten_all()
                save_state(signal_log, trade_log, "daily_limit_hit", et_now.strftime("%H:%M:%S"), last_result="Daily loss limit reached — positions flattened", account_id=acct_id, balance=acct_balance, daily_pnl=live_pnl, balance_at_day_open=balance_at_day_open, open_positions=executor.get_open_positions() if executor else [], symbol_biases=_scan_biases)
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Scan every instrument and collect all signals
            _scan_biases.clear()
            all_signals = []
            combined_scan_log = []
            mode_label = "PAPER" if PAPER_TRADE else "LIVE"
            combined_scan_log.append(f"━━━ [{mode_label}] CYCLE START {et_now.strftime('%Y-%m-%d %H:%M:%S ET')} ━━━")
            combined_scan_log.append(f"Account: {acct_id} | Balance: ${acct_balance:,.2f} | Daily P&L: ${live_pnl:,.2f}")

            for instr, cfg in INSTRUMENTS.items():
                bot = bots[instr]
                bot.daily_pnl = live_pnl
                combined_scan_log.append(f"=== {cfg['symbol']} ===")
                signals = bot.run_scan()
                scan_time = now_et().strftime("%H:%M:%S")
                for line in bot.last_scan_log:
                    if line.startswith("---") or line.startswith("===") or line.startswith("━") or line.startswith("RESULT"):
                        combined_scan_log.append(line)
                    else:
                        combined_scan_log.append(f"[{scan_time}] {line}")
                    # Capture per-symbol bias for dashboard
                    if line.startswith("Daily Bias:"):
                        _scan_biases[cfg["symbol"]] = line.split(":", 1)[1].strip().lower()
                for sig in signals:
                    all_signals.append((sig, instr, cfg))

            current_scan_log = combined_scan_log
            for line in current_scan_log:
                print(line)

            last_scan = et_now.strftime("%H:%M:%S ET")

            if not all_signals:
                log("No setup detected across all instruments.")
                combined_scan_log.append(f"━━━ RESULT: No setup — next scan in {SCAN_INTERVAL_SECONDS}s ━━━")
                save_state(signal_log, trade_log, "scanning", last_scan, last_result="No setup detected", account_id=acct_id, balance=acct_balance, daily_pnl=live_pnl, scan_log=current_scan_log, balance_at_day_open=balance_at_day_open, open_positions=executor.get_open_positions() if executor else [], symbol_biases=_scan_biases)
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Pick highest-confidence signal across all instruments
            all_signals.sort(key=lambda x: x[0].confidence, reverse=True)
            best, best_instr, best_cfg = all_signals[0]
            best_symbol = best_cfg["symbol"]
            best_qty = best_cfg["quantity"]

            # News blackout guard — ICT rule: never trade into high-impact macro events.
            # OpenBB provides the economic calendar. No-op if openbb is not installed.
            try:
                from openbb_provider import is_news_blackout
                _blackout, _blackout_reason = is_news_blackout()
                if _blackout:
                    log(f"[BLACKOUT] {_blackout_reason}")
                    combined_scan_log.append(f"BLACKOUT: {_blackout_reason}")
                    save_state(
                        signal_log, trade_log, "news_blackout", last_scan,
                        last_result=_blackout_reason,
                        account_id=acct_id, balance=acct_balance,
                        daily_pnl=live_pnl, scan_log=combined_scan_log,
                        balance_at_day_open=balance_at_day_open,
                        open_positions=executor.get_open_positions() if executor else [],
                        symbol_biases=_scan_biases,
                    )
                    time.sleep(SCAN_INTERVAL_SECONDS)
                    continue
            except Exception as _be:
                log(f"[BLACKOUT] Check failed: {_be} — proceeding with trade")

            # Signal cooldown — don't re-fire same instrument within SIGNAL_COOLDOWN_SECONDS
            last_trade_time = last_fired.get(best_instr)
            if last_trade_time:
                seconds_since = (et_now - last_trade_time).total_seconds()
                if seconds_since < SIGNAL_COOLDOWN_SECONDS:
                    log(f"Signal cooldown active for {best_symbol} ({int(seconds_since)}s ago) — skipping")
                    time.sleep(SCAN_INTERVAL_SECONDS)
                    continue

            # Open-position guard — never add to an existing position on restart or double-signal
            try:
                open_pos = executor.get_open_positions()
                already_in = any(best_symbol in str(p.get("contractId", "")) for p in open_pos)
                if already_in:
                    log(f"SKIPPED: Already have an open position on {best_symbol} — will not add contracts")
                    last_fired[best_instr] = et_now  # Set cooldown so it doesn't keep trying
                    save_state._last_fired = last_fired
                    time.sleep(SCAN_INTERVAL_SECONDS)
                    continue
            except Exception as pe:
                log(f"Position check failed: {pe} — proceeding with caution")

            # Platform DB duplicate guard — cross-reference open positions from the SaaS DB
            # This catches cases where the broker check passes but we already have an open
            # platform trade (e.g., broker glitch, manual trade, restart overlap)
            if best_symbol in _runtime["open_symbols"]:
                log(f"SKIPPED: Platform DB shows an open trade already exists for {best_symbol} — will not add contracts")
                last_fired[best_instr] = et_now
                save_state._last_fired = last_fired
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Platform runtime guards — enforced even if env vars not set
            if not _runtime["bot_enabled"]:
                log(f"Bot is disabled in dashboard — skipping trade on {best_symbol}")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            if _runtime["automation_locked"]:
                log(f"Automation is locked for this user — user must resume in dashboard before bot can trade")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            if best.confidence < _runtime["min_confidence"]:
                log(f"Signal found but confidence too low: {best.confidence * 100:.0f}% < {_runtime['min_confidence']*100:.0f}% required ({best_symbol})")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # De-duplicate: only log to signal_log if this isn't a repeat of the last entry
            _last_sig = signal_log[-1] if signal_log else {}
            _is_duplicate = (
                _last_sig.get("instrument") == best_symbol
                and _last_sig.get("setup") == best.setup_type.value
                and _last_sig.get("direction") == best.direction.value
                and _last_sig.get("entry") == best.entry_price
            )
            if not _is_duplicate:
                signal_log.append({
                    "time": last_scan,
                    "instrument": best_symbol,
                    "setup": best.setup_type.value,
                    "direction": best.direction.value,
                    "entry": best.entry_price,
                    "stop_loss": best.stop_loss,
                    "take_profit": best.take_profit_2,
                    "confidence": best.confidence,
                })
            save_state(signal_log, trade_log, "scanning", last_scan, account_id=acct_id, balance=acct_balance, daily_pnl=live_pnl, scan_log=current_scan_log, balance_at_day_open=balance_at_day_open, open_positions=executor.get_open_positions() if executor else [], symbol_biases=_scan_biases)

            print(bots[best_instr].format_signal(best))

            if PAPER_TRADE:
                log(f"[PAPER] Would trade: {('BUY' if best.direction == Bias.BULLISH else 'SELL')} {best_qty}x {best_symbol} | Entry:{best.entry_price} SL:{best.stop_loss} TP:{best.take_profit_2} Conf:{best.confidence*100:.0f}%")
                last_fired[best_instr] = et_now
                bots[best_instr].daily_trades += 1
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            # Max-trades-per-day guard — sum daily_trades across all instrument bots
            _trades_today = sum(b.daily_trades for b in bots.values())
            if _trades_today >= _runtime["max_trades_day"]:
                log(f"Max trades per day ({_runtime['max_trades_day']}) reached — skipping {best_symbol}")
                time.sleep(SCAN_INTERVAL_SECONDS)
                continue

            side = "buy" if best.direction == Bias.BULLISH else "sell"
            executor.last_entry_placed = False
            order_placed = executor.place_order(
                symbol=best_symbol,
                side=side,
                quantity=best_qty,
                stop_loss=best.stop_loss,
                take_profit=best.take_profit_2,
            )

            entry_fired = order_placed or getattr(executor, "last_entry_placed", False)

            if entry_fired:
                # Set cooldown immediately — prevents accumulating contracts if brackets fail
                last_fired[best_instr] = et_now
                notify_discord(best, best_symbol, side)
                bots[best_instr].daily_trades += 1
                bots[best_instr].orb.trade_taken_today = True
                trade_log.append({
                    "time": last_scan,
                    "instrument": best_symbol,
                    "side": side,
                    "entry": best.entry_price,
                    "stop_loss": best.stop_loss,
                    "take_profit": best.take_profit_2,
                    "confidence": best.confidence,
                })
                save_state(signal_log, trade_log, "in_trade", last_scan, last_result=f"Order placed: {side.upper()} {best_symbol}", account_id=acct_id, balance=acct_balance, daily_pnl=live_pnl, scan_log=current_scan_log, balance_at_day_open=balance_at_day_open, open_positions=executor.get_open_positions() if executor else [], symbol_biases=_scan_biases)

                # Write entry to journal immediately with full ICT context
                try:
                    current_trade_id = write_bot_entry(best, best_symbol)
                except Exception as je:
                    log(f"Journal entry write failed: {je}")
                    current_trade_id = None

                # 5-min cooldown — check loss limit and position status every 15s
                balance_at_entry = acct_balance
                position_closed = False
                for _ in range(20):
                    time.sleep(15)
                    _bal, _ = executor.get_account_balance()
                    if _bal > 0:
                        acct_balance = _bal
                        live_pnl = round(acct_balance - balance_at_day_open, 2)
                        last_known_pnl = live_pnl

                    # Detect if position closed (SL or TP hit)
                    if not position_closed:
                        try:
                            open_pos = executor.get_open_positions()
                            still_open = any(best_symbol in str(p.get("contractId", "")) for p in open_pos)
                            if not still_open:
                                position_closed = True
                                trade_pnl = round(acct_balance - balance_at_entry, 2) if _bal > 0 else None
                                exit_reason = "sl_hit" if (trade_pnl or 0) < 0 else "tp_hit"
                                if current_trade_id:
                                    try:
                                        update_trade_exit(current_trade_id, exit_price=None, exit_reason=exit_reason, pnl=trade_pnl)
                                        current_trade_id = None
                                    except Exception as je:
                                        log(f"Journal exit write failed: {je}")
                                log(f"Position closed — {exit_reason} | Estimated P&L: ${trade_pnl:.2f}")
                        except Exception:
                            pass  # Don't let position check crash the loop

                    if last_known_pnl is not None and live_pnl <= -_runtime["daily_loss_limit"]:
                        log(f"Daily loss limit hit during cooldown (${live_pnl:.2f}). Flattening all positions.")
                        if current_trade_id and not position_closed:
                            try:
                                trade_pnl = round(acct_balance - balance_at_entry, 2) if acct_balance > 0 else None
                                update_trade_exit(current_trade_id, exit_price=None, exit_reason="loss_limit_hit", pnl=trade_pnl)
                                current_trade_id = None
                            except Exception as je:
                                log(f"Journal exit write failed: {je}")
                        executor.flatten_all()
                        break

                # After cooldown: cancel any orphaned bracket orders (e.g. TP still open after SL hit)
                try:
                    executor.cancel_open_orders()
                except Exception as ce:
                    log(f"Post-trade order cleanup failed: {ce}")
            else:
                log("Order was rejected — not counting as trade taken.")
                time.sleep(SCAN_INTERVAL_SECONDS)

            # Journal + learning update every 30 min
            try:
                if last_journal_update is None or (et_now - last_journal_update).total_seconds() >= 1800:
                    from journal import run_journal_update
                    from learner import run_learning_update
                    run_journal_update()
                    run_learning_update()
                    last_journal_update = et_now

                    # Push updated weights to platform so client bots can benefit from
                    # Joseph's training. Only fires if platform connection is configured.
                    if PLATFORM_URL and PLATFORM_USER_ID and PLATFORM_API_KEY:
                        try:
                            weights_path = os.path.join(DATA_DIR, "weights.json")
                            if os.path.exists(weights_path):
                                with open(weights_path) as _wf:
                                    weights_data = json.load(_wf)
                                requests.post(
                                    f"{PLATFORM_URL}/api/shared-weights",
                                    headers={"X-Bot-Key": PLATFORM_API_KEY},
                                    json={"user_id": int(PLATFORM_USER_ID), "weights": weights_data},
                                    timeout=5,
                                )
                                log(f"[WEIGHTS SYNC] Pushed {len(weights_data)} patterns to platform")
                        except Exception as we:
                            log(f"[WEIGHTS SYNC] Failed: {we}")
            except Exception as je:
                log(f"Journal/learner update failed: {je}")

        except KeyboardInterrupt:
            log("Bot stopped by user.")
            break
        except Exception as e:
            log(f"Error during scan: {e}")
            save_state(signal_log, trade_log, "error", now_et().strftime("%H:%M:%S"), last_result=f"Error: {e}", account_id=acct_id, balance=acct_balance, daily_pnl=live_pnl, scan_log=current_scan_log, balance_at_day_open=balance_at_day_open, open_positions=executor.get_open_positions() if executor else [], symbol_biases=_scan_biases)
            time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    finally:
        # Release single-instance lock
        try:
            if os.path.exists(_LOCK_FILE):
                os.remove(_LOCK_FILE)
        except Exception:
            pass
