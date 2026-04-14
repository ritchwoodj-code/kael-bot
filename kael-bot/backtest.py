# -*- coding: utf-8 -*-
"""
KAEL Backtester
Runs ICT strategy logic against historical yfinance data.
Generates performance stats saved to backtest_results.json.

Usage:
    python backtest.py --symbol MNQ --days 90
    python backtest.py --symbol MNQ --days 90 --symbol MGC --days 90
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

# Import ICT logic from existing bot
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ict_bot import ICTBot

ET = ZoneInfo("America/New_York")
RESULTS_FILE = os.path.join(os.path.dirname(__file__), "backtest_results.json")

TICKER_MAP = {
    "MNQ": "MNQ=F",
    "MGC": "MGC=F",
    "MES": "MES=F",
    "NQ":  "NQ=F",
    "GC":  "GC=F",
}

POINT_VALUES = {"MNQ": 2.0, "MGC": 10.0, "MES": 5.0, "NQ": 20.0, "GC": 100.0}

KILL_ZONES_ET = [
    (3,  0,  5,  0,  "London"),
    (8, 30, 12,  0,  "NY AM"),
    (10, 0, 11,  0,  "Silver Bullet"),
    (13,30, 16,  0,  "NY PM"),
]


def in_kill_zone(dt_et):
    h, m = dt_et.hour, dt_et.minute
    cur = h * 60 + m
    for sh, sm, eh, em, name in KILL_ZONES_ET:
        if sh * 60 + sm <= cur < eh * 60 + em:
            return True, name
    return False, None


def fetch_history(symbol, days):
    ticker = TICKER_MAP.get(symbol, symbol + "=F")
    end = datetime.now()
    start = end - timedelta(days=days + 5)  # buffer for weekends
    print(f"  Fetching {ticker} ({days}d history)...")
    data = {}
    for tf, interval in [("1m", "1m"), ("5m", "5m"), ("15m", "15m"), ("4h", "1h"), ("D", "1d")]:
        period = "7d" if tf == "1m" else f"{days + 5}d"
        try:
            df = yf.download(ticker, period=period, interval=interval,
                             progress=False, auto_adjust=True)
            if not df.empty:
                data[tf] = df
        except Exception as e:
            print(f"    Warning: could not fetch {tf}: {e}")
    return data


def simulate_trade(entry, direction, symbol, candles_1m, sl_pts=50, rr=2.0):
    """
    Walk forward through 1m candles after entry to find SL or TP hit.
    Returns: (exit_price, exit_reason, bars_held, pnl)
    """
    point_val = POINT_VALUES.get(symbol, 1.0)
    sl = entry - sl_pts if direction == "buy" else entry + sl_pts
    tp = entry + sl_pts * rr if direction == "buy" else entry - sl_pts * rr
    max_bars = 240  # 4 hours max hold

    for i, (ts, row) in enumerate(candles_1m.iterrows()):
        if i > max_bars:
            close = float(row["Close"].iloc[0] if hasattr(row["Close"], "iloc") else row["Close"])
            pnl = (close - entry if direction == "buy" else entry - close) * point_val
            return close, "time_exit", i, round(pnl, 2)
        try:
            hi = float(row["High"].iloc[0] if hasattr(row["High"], "iloc") else row["High"])
            lo = float(row["Low"].iloc[0]  if hasattr(row["Low"],  "iloc") else row["Low"])
        except Exception:
            continue
        if direction == "buy":
            if lo <= sl:
                pnl = (sl - entry) * point_val
                return sl, "sl_hit", i, round(pnl, 2)
            if hi >= tp:
                pnl = (tp - entry) * point_val
                return tp, "tp_hit", i, round(pnl, 2)
        else:
            if hi >= sl:
                pnl = (entry - sl) * point_val
                return sl, "sl_hit", i, round(pnl, 2)
            if lo <= tp:
                pnl = (entry - tp) * point_val
                return tp, "tp_hit", i, round(pnl, 2)

    return entry, "no_exit", max_bars, 0.0


def run_backtest(symbol, days):
    print(f"\n[KAEL BACKTEST] {symbol} — {days} days")
    data = fetch_history(symbol, days)
    if not data or "D" not in data or "5m" not in data:
        print("  Not enough data. Skipping.")
        return None

    bot = ICTBot(symbol, data)
    trades = []
    equity = 50000.0
    equity_curve = [{"date": "start", "equity": equity}]
    daily_counts = {}
    skipped = 0

    # Walk forward day by day
    daily_df = data["D"]
    dates = [ts.date() for ts in daily_df.index]

    for date in dates:
        day_str = str(date)
        # Skip weekends
        if date.weekday() >= 5:
            continue
        daily_counts[day_str] = 0

        # Get intraday 5m bars for this day
        df_5m = data["5m"]
        day_5m = df_5m[df_5m.index.date == date]
        if day_5m.empty:
            continue

        # Get 1m bars for simulation
        df_1m = data.get("1m", pd.DataFrame())

        for ts, row in day_5m.iterrows():
            if daily_counts[day_str] >= 3:
                break
            dt_et = ts.astimezone(ET) if hasattr(ts, 'astimezone') else ts

            active, zone = in_kill_zone(dt_et)
            if not active:
                continue

            # Feed current slice of data to bot
            try:
                sliced = {}
                for tf, df in data.items():
                    sliced[tf] = df[df.index <= ts]
                bot_slice = ICTBot(symbol, sliced)
                signal = bot_slice.scan_for_setup()
            except Exception:
                skipped += 1
                continue

            if signal is None:
                continue
            if signal.confidence < 0.65:
                skipped += 1
                continue

            entry = signal.entry_price
            direction = "buy" if signal.direction.value == "bullish" else "sell"
            sl_pts = abs(entry - signal.stop_loss)
            rr = abs(signal.take_profit_2 - entry) / sl_pts if sl_pts > 0 else 2.0

            # Simulate on 1m bars after entry
            future_1m = df_1m[df_1m.index > ts].head(240) if not df_1m.empty else pd.DataFrame()

            if future_1m.empty:
                skipped += 1
                continue

            exit_price, exit_reason, bars, pnl = simulate_trade(
                entry, direction, symbol, future_1m, sl_pts=sl_pts, rr=rr
            )

            win = exit_reason == "tp_hit"
            equity += pnl
            daily_counts[day_str] += 1

            trades.append({
                "date":        day_str,
                "time":        dt_et.strftime("%H:%M ET"),
                "symbol":      symbol,
                "direction":   direction,
                "setup":       signal.setup_type.value if hasattr(signal.setup_type, 'value') else str(signal.setup_type),
                "session":     zone,
                "confidence":  round(signal.confidence, 2),
                "entry":       entry,
                "sl":          signal.stop_loss,
                "tp":          signal.take_profit_2,
                "exit_price":  exit_price,
                "exit_reason": exit_reason,
                "bars_held":   bars,
                "pnl":         pnl,
                "win":         win,
                "equity":      round(equity, 2),
            })

            equity_curve.append({"date": day_str, "equity": round(equity, 2)})

    if not trades:
        print(f"  No trades generated. Skipped {skipped} low-confidence signals.")
        return None

    # Calculate stats
    wins   = [t for t in trades if t["win"]]
    losses = [t for t in trades if not t["win"]]
    total  = len(trades)
    wr     = round(len(wins) / total * 100, 1) if total else 0
    total_pnl = round(sum(t["pnl"] for t in trades), 2)
    avg_win   = round(sum(t["pnl"] for t in wins)   / len(wins),   2) if wins else 0
    avg_loss  = round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0
    profit_factor = round(abs(sum(t["pnl"] for t in wins)) / abs(sum(t["pnl"] for t in losses)), 2) if losses and sum(t["pnl"] for t in losses) != 0 else 0
    best  = max(t["pnl"] for t in trades)
    worst = min(t["pnl"] for t in trades)

    # Max drawdown
    peak = 50000.0
    max_dd = 0.0
    for point in equity_curve:
        eq = point["equity"]
        if eq > peak:
            peak = eq
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
    max_dd_pct = round(max_dd / 50000 * 100, 1)

    # Streak
    streak = 0
    streak_type = ""
    for t in reversed(trades):
        if not streak_type:
            streak_type = "W" if t["win"] else "L"
            streak = 1
        elif (streak_type == "W" and t["win"]) or (streak_type == "L" and not t["win"]):
            streak += 1
        else:
            break

    # By setup
    by_setup = {}
    for t in trades:
        s = t["setup"]
        if s not in by_setup:
            by_setup[s] = {"trades": 0, "wins": 0, "pnl": 0}
        by_setup[s]["trades"] += 1
        if t["win"]:
            by_setup[s]["wins"] += 1
        by_setup[s]["pnl"] = round(by_setup[s]["pnl"] + t["pnl"], 2)
    for s in by_setup:
        by_setup[s]["win_rate"] = round(by_setup[s]["wins"] / by_setup[s]["trades"] * 100, 1)

    result = {
        "symbol":         symbol,
        "days":           days,
        "generated":      datetime.now().strftime("%Y-%m-%d %H:%M"),
        "stats": {
            "total_trades":    total,
            "wins":            len(wins),
            "losses":          len(losses),
            "win_rate":        wr,
            "total_pnl":       total_pnl,
            "avg_win":         avg_win,
            "avg_loss":        avg_loss,
            "profit_factor":   profit_factor,
            "best_trade":      round(best, 2),
            "worst_trade":     round(worst, 2),
            "max_drawdown":    round(max_dd, 2),
            "max_drawdown_pct": max_dd_pct,
            "current_streak":  streak,
            "streak_type":     streak_type,
            "skipped_signals": skipped,
        },
        "by_setup":      by_setup,
        "equity_curve":  equity_curve,
        "trades":        trades,
    }

    print(f"  Trades: {total} | Win rate: {wr}% | P&L: ${total_pnl:+,.2f} | PF: {profit_factor}")
    return result


def main():
    parser = argparse.ArgumentParser(description="KAEL Backtester")
    parser.add_argument("--symbol", action="append", default=[], help="Symbol to backtest (e.g. MNQ)")
    parser.add_argument("--days",   type=int, default=90, help="Days of history")
    args = parser.parse_args()

    symbols = args.symbol if args.symbol else ["MNQ"]
    results = {}

    for sym in symbols:
        result = run_backtest(sym.upper(), args.days)
        if result:
            results[sym.upper()] = result

    if results:
        with open(RESULTS_FILE, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\nResults saved to backtest_results.json")
        for sym, r in results.items():
            s = r["stats"]
            print(f"\n{'='*40}")
            print(f"  {sym} ({r['days']}d backtest)")
            print(f"  Trades:         {s['total_trades']}")
            print(f"  Win Rate:       {s['win_rate']}%")
            print(f"  Total P&L:      ${s['total_pnl']:+,.2f}")
            print(f"  Profit Factor:  {s['profit_factor']}")
            print(f"  Avg Win:        ${s['avg_win']:+,.2f}")
            print(f"  Avg Loss:       ${s['avg_loss']:+,.2f}")
            print(f"  Max Drawdown:   ${s['max_drawdown']:,.2f} ({s['max_drawdown_pct']}%)")
    else:
        print("\nNo results generated.")


if __name__ == "__main__":
    main()
