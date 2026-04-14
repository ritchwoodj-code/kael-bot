"""
Trade Journal — Outcome Tracker
Fetches closed trades from TopstepX, matches them to bot signals,
and builds a learning dataset in trade_journal.json.

Run standalone:  python journal.py
Auto-called by:  run_bot.py every 30 minutes
"""

import os
import json
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

ET = ZoneInfo("America/New_York")
DATA_DIR     = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
JOURNAL_FILE = os.path.join(DATA_DIR, "trade_journal.json")
STATE_FILE   = os.path.join(DATA_DIR, "bot_state.json")
BASE_URL     = "https://api.topstepx.com/api"


def load_journal():
    if os.path.exists(JOURNAL_FILE):
        with open(JOURNAL_FILE) as f:
            return json.load(f)
    return {"trades": [], "stats": {}}


def save_journal(j):
    tmp = JOURNAL_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(j, f, indent=2)
    os.replace(tmp, JOURNAL_FILE)


def get_token():
    r = requests.post(f"{BASE_URL}/Auth/loginKey", json={
        "userName": os.getenv("TOPSTEP_USERNAME"),
        "apiKey":   os.getenv("TOPSTEP_API_KEY"),
    })
    r.raise_for_status()
    return r.json()["token"]


def get_account_id(token):
    r = requests.post(f"{BASE_URL}/Account/search",
                      headers={"Authorization": f"Bearer {token}"},
                      json={"onlyActiveAccounts": True})
    r.raise_for_status()
    accounts = r.json()
    if isinstance(accounts, dict):
        accounts = accounts.get("accounts") or accounts.get("data") or []
    prac = next((a for a in accounts if "PRAC" in str(a.get("name","")).upper()), accounts[0])
    return prac["id"]


def fetch_fills(token, account_id):
    r = requests.post(f"{BASE_URL}/Trade/search",
                      headers={"Authorization": f"Bearer {token}"},
                      json={"accountId": account_id})
    r.raise_for_status()
    data = r.json()
    return data["trades"] if isinstance(data, dict) else data


def pair_fills(fills):
    """
    Pair entry and exit fills into completed trades.
    Uses FIFO matching: each BUY is paired with the next SELL (and vice versa).
    Returns list of completed trade dicts.
    """
    from collections import defaultdict
    by_contract = defaultdict(list)
    for f in fills:
        if not f.get("voided"):
            by_contract[f["contractId"]].append(f)

    completed = []
    for contract, contract_fills in by_contract.items():
        # Sort chronologically
        contract_fills.sort(key=lambda x: x["creationTimestamp"])

        # Separate into buy queue and sell queue, pair FIFO
        buy_q  = [f for f in contract_fills if f["side"] == 0]
        sell_q = [f for f in contract_fills if f["side"] == 1]

        # Infer symbol from contract ID (e.g. CON.F.US.MGC.M26 -> MGC)
        parts = contract.split(".")
        symbol = parts[3] if len(parts) >= 5 else contract

        # Match each buy with a sell FIFO
        pairs = list(zip(buy_q, sell_q))
        for buy, sell in pairs:
            entry_price = buy["price"]
            exit_price  = sell["price"]
            pnl_raw     = sell.get("profitAndLoss") or buy.get("profitAndLoss")

            # Compute PnL from price delta if API doesn't provide it
            if pnl_raw is None:
                if "MGC" in contract or "GC" in contract:
                    pnl_raw = (exit_price - entry_price) * 10
                elif "MNQ" in contract or "NQ" in contract:
                    pnl_raw = (exit_price - entry_price) * 2
                elif "MES" in contract or "ES" in contract:
                    pnl_raw = (exit_price - entry_price) * 50
                else:
                    pnl_raw = 0

            entry_ts = datetime.fromisoformat(buy["creationTimestamp"].replace("Z", "+00:00"))
            exit_ts  = datetime.fromisoformat(sell["creationTimestamp"].replace("Z", "+00:00"))
            duration_min = round((exit_ts - entry_ts).total_seconds() / 60, 1)

            completed.append({
                "id":           f"{buy['id']}_{sell['id']}",
                "date":         entry_ts.astimezone(ET).strftime("%Y-%m-%d"),
                "entry_time":   entry_ts.astimezone(ET).strftime("%H:%M"),
                "exit_time":    exit_ts.astimezone(ET).strftime("%H:%M"),
                "duration_min": duration_min,
                "symbol":       symbol,
                "side":         "long",
                "entry_price":  entry_price,
                "exit_price":   exit_price,
                "pnl":          round(pnl_raw, 2),
                "win":          pnl_raw > 0,
                "setup":        None,
                "session":      None,
                "bias":         None,
                "confidence":   None,
                "sl":           None,
                "tp":           None,
                "bot_trade":    False,
                "status":       "closed",
            })

        # Partial close handling — log unpaired fills as open positions instead of silently dropping them
        paired_count = len(pairs)
        for buy in buy_q[paired_count:]:
            entry_ts = datetime.fromisoformat(buy["creationTimestamp"].replace("Z", "+00:00"))
            completed.append({
                "id":           f"{buy['id']}_open",
                "date":         entry_ts.astimezone(ET).strftime("%Y-%m-%d"),
                "entry_time":   entry_ts.astimezone(ET).strftime("%H:%M"),
                "exit_time":    None,
                "duration_min": None,
                "symbol":       symbol,
                "side":         "long",
                "entry_price":  buy["price"],
                "exit_price":   None,
                "pnl":          0,
                "win":          False,
                "setup":        None, "session": None, "bias": None,
                "confidence":   None, "sl": None, "tp": None,
                "bot_trade":    False,
                "status":       "open",
            })
        for sell in sell_q[paired_count:]:
            entry_ts = datetime.fromisoformat(sell["creationTimestamp"].replace("Z", "+00:00"))
            completed.append({
                "id":           f"{sell['id']}_open",
                "date":         entry_ts.astimezone(ET).strftime("%Y-%m-%d"),
                "entry_time":   entry_ts.astimezone(ET).strftime("%H:%M"),
                "exit_time":    None,
                "duration_min": None,
                "symbol":       symbol,
                "side":         "short",
                "entry_price":  sell["price"],
                "exit_price":   None,
                "pnl":          0,
                "win":          False,
                "setup":        None, "session": None, "bias": None,
                "confidence":   None, "sl": None, "tp": None,
                "bot_trade":    False,
                "status":       "open",
            })

    return completed


def match_signals(completed_trades, state_file=STATE_FILE):
    """Match bot signals to completed trades by time + symbol proximity."""
    if not os.path.exists(state_file):
        return completed_trades

    with open(state_file) as f:
        state = json.load(f)

    signals = state.get("signals", [])

    for trade in completed_trades:
        for sig in signals:
            if sig.get("instrument", "").upper() != trade["symbol"].upper():
                continue
            # Match if signal time is within 3 min of entry
            try:
                sig_time = datetime.strptime(
                    f"{trade['date']} {sig['time'].replace(' ET','').strip()}",
                    "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=ET)
                entry_time = datetime.strptime(
                    f"{trade['date']} {trade['entry_time']}", "%Y-%m-%d %H:%M"
                ).replace(tzinfo=ET)
                if abs((sig_time - entry_time).total_seconds()) <= 180:
                    trade["setup"]      = sig.get("setup")
                    trade["session"]    = sig.get("session")
                    trade["bias"]       = sig.get("direction")
                    trade["confidence"] = sig.get("confidence")
                    trade["sl"]         = sig.get("stop_loss")
                    trade["tp"]         = sig.get("take_profit")
                    trade["bot_trade"]  = True
                    break
            except Exception:
                continue

    return completed_trades


def write_bot_entry(signal, symbol: str) -> str:
    """Write a bot trade entry immediately when the entry order is confirmed.
    Captures full ICT context — setup, session, bias, confidence, and all confluences."""
    journal = load_journal()
    now_et = datetime.now(ET)
    trade_id = f"bot_{now_et.strftime('%Y%m%d_%H%M%S')}_{symbol}"
    entry = {
        "id":           trade_id,
        "date":         now_et.strftime("%Y-%m-%d"),
        "entry_time":   now_et.strftime("%H:%M"),
        "exit_time":    None,
        "duration_min": None,
        "symbol":       symbol,
        "side":         "long" if signal.direction.value == "bullish" else "short",
        "entry_price":  signal.entry_price,
        "exit_price":   None,
        "pnl":          None,
        "win":          None,
        "setup":        signal.setup_type.value,
        "session":      signal.session,
        "bias":         signal.direction.value,
        "confidence":   signal.confidence,
        "confluences":  signal.confluences,
        "sl":           signal.stop_loss,
        "tp":           signal.take_profit_2,
        "bot_trade":    True,
        "exit_reason":  None,
        "status":       "open",
    }
    journal["trades"].append(entry)
    closed = [t for t in journal["trades"] if t.get("pnl") is not None]
    journal["stats"] = compute_stats(closed)
    save_journal(journal)
    why = ", ".join(signal.confluences[:3]) if signal.confluences else "n/a"
    print(f"[{now_et.strftime('%H:%M:%S')}] JOURNAL ENTRY: {symbol} {entry['side'].upper()} "
          f"@ {signal.entry_price} | Setup:{signal.setup_type.value} "
          f"Conf:{signal.confidence*100:.0f}% | Why: {why}")
    return trade_id


def update_trade_exit(trade_id: str, exit_price, exit_reason: str, pnl: float = None):
    """Update an open bot trade record with exit details.
    Called when SL/TP hits, flatten_all fires, or position is cancelled."""
    journal = load_journal()
    now_et = datetime.now(ET)
    for trade in journal["trades"]:
        if trade["id"] != trade_id:
            continue
        try:
            entry_dt = datetime.strptime(
                f"{trade['date']} {trade['entry_time']}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=ET)
            trade["duration_min"] = round((now_et - entry_dt).total_seconds() / 60, 1)
        except Exception:
            pass
        trade["exit_time"]   = now_et.strftime("%H:%M")
        trade["exit_price"]  = exit_price
        trade["exit_reason"] = exit_reason
        trade["status"]      = "closed"
        if pnl is not None:
            trade["pnl"] = round(pnl, 2)
            trade["win"] = pnl > 0
        elif exit_price and trade.get("entry_price"):
            sym   = trade["symbol"]
            delta = exit_price - trade["entry_price"]
            if trade["side"] == "short":
                delta = -delta
            if "MGC" in sym or "GC" in sym:
                pnl = round(delta * 10, 2)
            elif "MNQ" in sym or "NQ" in sym:
                pnl = round(delta * 2, 2)
            else:
                pnl = 0
            trade["pnl"] = pnl
            trade["win"] = pnl > 0
        break
    closed = [t for t in journal["trades"] if t.get("pnl") is not None]
    journal["stats"] = compute_stats(closed)
    save_journal(journal)
    outcome = "WIN" if (pnl or 0) > 0 else "LOSS"
    print(f"[{now_et.strftime('%H:%M:%S')}] JOURNAL EXIT: {trade_id} | "
          f"Reason:{exit_reason} | P&L:${pnl:.2f} | {outcome}")


def compute_stats(trades):
    if not trades:
        return {}

    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    total_pnl = sum(t["pnl"] for t in trades)

    # By setup
    setup_stats = {}
    for t in trades:
        setup = t.get("setup") or "unknown"
        if setup not in setup_stats:
            setup_stats[setup] = {"trades": 0, "wins": 0, "pnl": 0.0}
        setup_stats[setup]["trades"] += 1
        if t["win"]:
            setup_stats[setup]["wins"] += 1
        setup_stats[setup]["pnl"] = round(setup_stats[setup]["pnl"] + t["pnl"], 2)

    for s in setup_stats.values():
        s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] else 0

    # By symbol
    sym_stats = {}
    for t in trades:
        sym = t["symbol"]
        if sym not in sym_stats:
            sym_stats[sym] = {"trades": 0, "wins": 0, "pnl": 0.0}
        sym_stats[sym]["trades"] += 1
        if t["win"]:
            sym_stats[sym]["wins"] += 1
        sym_stats[sym]["pnl"] = round(sym_stats[sym]["pnl"] + t["pnl"], 2)

    for s in sym_stats.values():
        s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] else 0

    return {
        "total_trades":  len(trades),
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate":      round(len(wins) / len(trades) * 100, 1),
        "total_pnl":     round(total_pnl, 2),
        "avg_win":       round(sum(t["pnl"] for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss":      round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0,
        "by_setup":      setup_stats,
        "by_symbol":     sym_stats,
        "last_updated":  datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET"),
    }


TICKER_MAP = {"MNQ": "MNQ=F", "MGC": "MGC=F", "NQ": "NQ=F", "GC": "GC=F"}


def check_post_trade_outcomes():
    """
    For every sl_hit bot trade without a post_trade_outcome, pull price data
    from the exit time forward 90 minutes and check if price reached the
    original TP anyway.

    Outcomes:
      valid_early_exit — SL hit but setup was correct, timing was off
      setup_failed     — SL hit and price never reached TP (setup was wrong)
      pending          — not enough time has passed yet (< 90 min since exit)

    This is what teaches Kael the difference between a bad setup and a bad entry.
    """
    journal = load_journal()
    now_et  = datetime.now(ET)
    updated = False

    for trade in journal["trades"]:
        if not trade.get("bot_trade"):
            continue
        if trade.get("exit_reason") != "sl_hit":
            continue
        if trade.get("post_trade_outcome") and trade["post_trade_outcome"] != "pending":
            continue
        if not trade.get("exit_time") or not trade.get("tp"):
            continue

        try:
            exit_dt = datetime.strptime(
                f"{trade['date']} {trade['exit_time']}", "%Y-%m-%d %H:%M"
            ).replace(tzinfo=ET)
        except Exception:
            continue

        minutes_elapsed = (now_et - exit_dt).total_seconds() / 60
        if minutes_elapsed < 90:
            if not trade.get("post_trade_outcome"):
                trade["post_trade_outcome"] = "pending"
                updated = True
            continue

        # Fetch post-exit price data
        try:
            import yfinance as yf
            sym    = trade["symbol"]
            ticker = TICKER_MAP.get(sym, sym + "=F")
            df     = yf.Ticker(ticker).history(interval="1m", period="5d")
            if df is None or len(df) == 0:
                continue
            df.columns = [c.lower() for c in df.columns]
            if df.index.tz is not None:
                df.index = df.index.tz_convert("America/New_York")

            window_end = exit_dt + timedelta(minutes=90)
            post_df    = df[(df.index >= exit_dt) & (df.index <= window_end)]
            if len(post_df) == 0:
                continue

            tp   = float(trade["tp"])
            side = trade.get("side", "long")
            reached_tp = (post_df["low"].min() <= tp) if side == "short" else (post_df["high"].max() >= tp)

            if reached_tp:
                trade["post_trade_outcome"] = "valid_early_exit"
                print(f"[POST-TRADE] {sym} {side.upper()} — stopped out but TP hit within 90min. "
                      f"TIMING ISSUE — setup was right, entry was early.")
            else:
                trade["post_trade_outcome"] = "setup_failed"
                print(f"[POST-TRADE] {sym} {side.upper()} — stopped out, price never reached TP. "
                      f"SETUP FAILED — pattern genuinely didn't work.")
            updated = True

        except Exception as e:
            print(f"[POST-TRADE] Check failed for {trade['id']}: {e}")
            continue

    if updated:
        closed = [t for t in journal["trades"] if t.get("pnl") is not None]
        journal["stats"] = compute_stats(closed)
        save_journal(journal)

    return journal


def run_journal_update():
    print(f"[{datetime.now(ET).strftime('%H:%M:%S')}] Updating trade journal...")
    journal = load_journal()
    existing_ids = {t["id"] for t in journal["trades"]}

    token      = get_token()
    account_id = get_account_id(token)
    fills      = fetch_fills(token, account_id)

    completed = pair_fills(fills)
    completed = match_signals(completed)

    new_trades = [t for t in completed if t["id"] not in existing_ids]
    journal["trades"].extend(new_trades)
    journal["stats"] = compute_stats(journal["trades"])
    save_journal(journal)

    stats = journal["stats"]
    print(f"[{datetime.now(ET).strftime('%H:%M:%S')}] Journal updated — {len(journal['trades'])} total trades | "
          f"Win rate: {stats.get('win_rate', 0)}% | Total PnL: ${stats.get('total_pnl', 0):,.2f}")
    if new_trades:
        print(f"  +{len(new_trades)} new trades recorded")

    # Check post-trade outcomes for any stopped-out trades
    check_post_trade_outcomes()

    return journal


if __name__ == "__main__":
    j = run_journal_update()
    print("\n=== STATS ===")
    print(json.dumps(j["stats"], indent=2))
    print("\n=== RECENT TRADES ===")
    for t in j["trades"][-10:]:
        outcome = "WIN" if t["win"] else "LOSS"
        bot = " [BOT]" if t["bot_trade"] else ""
        print(f"  {t['date']} {t['entry_time']} {t['symbol']} {t['side'].upper()} "
              f"@ {t['entry_price']} -> {t['exit_price']} | ${t['pnl']:+.2f} {outcome}"
              f"{bot} setup:{t['setup']}")
