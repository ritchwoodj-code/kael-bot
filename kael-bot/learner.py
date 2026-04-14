"""
Kael Self-Learning Layer
Reads trade_journal.json, computes performance by pattern,
and writes adjusted confidence weights to weights.json.

The bot loads weights.json before every scan and multiplies
its raw confidence score by the learned weight for that pattern.

Pattern key: "{setup}|{session}|{bias}"
  e.g. "silver_bullet|NY AM|bullish"

Weight rules:
  - Minimum 5 trades before any adjustment (avoid overfitting noise)
  - Win rate >= 70% -> boost up to 1.25x
  - Win rate 50-70% -> neutral (1.0x)
  - Win rate 30-50% -> reduce to 0.80x
  - Win rate < 30%  -> reduce to 0.60x (near-block)
  - Win rate == 0% with 10+ trades -> block (weight = 0.0)

Weights decay 10% toward 1.0 each week so old data
doesn't permanently dominate.
"""

import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
DATA_DIR     = os.environ.get("DATA_DIR", os.path.dirname(__file__))
JOURNAL_FILE = os.path.join(DATA_DIR, "trade_journal.json")
WEIGHTS_FILE = os.path.join(DATA_DIR, "weights.json")
MIN_TRADES   = 5   # minimum trades before adjusting weight
DECAY_RATE   = 0.10  # weekly decay toward neutral


def load_journal():
    if not os.path.exists(JOURNAL_FILE):
        return {"trades": []}
    with open(JOURNAL_FILE) as f:
        return json.load(f)


def load_weights():
    if not os.path.exists(WEIGHTS_FILE):
        return {}
    with open(WEIGHTS_FILE) as f:
        return json.load(f)


def save_weights(weights):
    tmp = WEIGHTS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(weights, f, indent=2)
    os.replace(tmp, WEIGHTS_FILE)


def pattern_key(trade):
    setup   = trade.get("setup") or "unknown"
    session = trade.get("session") or "unknown"
    bias    = trade.get("bias") or "unknown"
    return f"{setup}|{session}|{bias}"


def compute_weights(trades):
    """Compute confidence weight multiplier for each pattern.

    Uses post_trade_outcome to distinguish between two types of losses:
      valid_early_exit — setup was correct, entry timing was off (don't penalize setup)
      setup_failed     — setup was genuinely wrong (penalize normally)

    valid_early_exit trades count as 0.5 wins so Kael doesn't block good setups
    just because it entered too early a few times.
    """
    from collections import defaultdict

    pattern_stats = defaultdict(lambda: {"trades": 0, "wins": 0.0, "pnl": 0.0, "timing_issues": 0, "true_fails": 0})

    for t in trades:
        key          = pattern_key(t)
        post_outcome = t.get("post_trade_outcome")
        pattern_stats[key]["trades"] += 1
        pattern_stats[key]["pnl"]     = round(pattern_stats[key]["pnl"] + (t.get("pnl") or 0), 2)

        if t.get("win"):
            pattern_stats[key]["wins"] += 1
        elif post_outcome == "valid_early_exit":
            # Setup was right — timing was wrong. Count as half-win so we don't unfairly
            # destroy confidence on a pattern that actually works.
            pattern_stats[key]["wins"]         += 0.5
            pattern_stats[key]["timing_issues"] += 1
        elif post_outcome == "setup_failed":
            pattern_stats[key]["true_fails"] += 1

    weights = {}
    for key, stats in pattern_stats.items():
        n      = stats["trades"]
        wins   = stats["wins"]
        timing = stats["timing_issues"]
        wr     = wins / n if n > 0 else 0.5

        if n < MIN_TRADES:
            weight = 1.0
            note   = f"insufficient_data ({n} trades)"
        elif wr >= 0.70:
            weight = round(1.0 + (wr - 0.70) / 0.30 * 0.25, 3)
            note   = "boosted"
        elif wr >= 0.50:
            weight = 1.0
            note   = "neutral"
        elif wr >= 0.30:
            weight = 0.80
            note   = "reduced"
        elif n >= 10 and wr == 0.0:
            weight = 0.0
            note   = "BLOCKED — 0% win rate over 10+ trades"
        else:
            weight = 0.60
            note   = "heavily_reduced"

        # Flag timing issues separately — doesn't hurt weight but shows in dashboard
        if timing > 0:
            note += f" | early_entry:{timing}x"

        weights[key] = {
            "weight":        weight,
            "trades":        n,
            "wins":          int(wins),
            "win_rate":      round(wr * 100, 1),
            "timing_issues": timing,
            "true_fails":    stats["true_fails"],
            "pnl":       stats["pnl"],
            "note":      note,
            "updated":   datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET"),
        }

    return weights


def apply_decay(old_weights, new_weights):
    """
    Decay weights that haven't been updated recently toward 1.0.
    This prevents old bad data from blocking good new patterns.
    """
    now = datetime.now(ET)
    for key, data in old_weights.items():
        if key in new_weights:
            continue  # will be recalculated fresh
        try:
            updated = datetime.strptime(data["updated"], "%Y-%m-%d %H:%M:%S ET").replace(tzinfo=ET)
            weeks_old = (now - updated).days / 7
            if weeks_old >= 1:
                old_w = data["weight"]
                decayed = round(old_w + (1.0 - old_w) * DECAY_RATE * weeks_old, 3)
                decayed = max(0.0, min(1.5, decayed))
                new_weights[key] = {**data, "weight": decayed, "note": f"decayed ({weeks_old:.1f}wk)"}
        except Exception:
            pass  # Skip malformed weight entries
    return new_weights


def run_learning_update():
    print(f"[{datetime.now(ET).strftime('%H:%M:%S')}] Running learning update...")
    journal      = load_journal()
    old_weights  = load_weights()
    trades       = journal.get("trades", [])

    # Only learn from bot-identified trades with known setup
    bot_trades = [t for t in trades if t.get("bot_trade") and t.get("setup")]

    if not bot_trades:
        print(f"[{datetime.now(ET).strftime('%H:%M:%S')}] Not enough tagged trades yet — collecting data.")
        return old_weights

    new_weights = compute_weights(bot_trades)
    new_weights = apply_decay(old_weights, new_weights)
    save_weights(new_weights)

    # Print summary
    boosted  = [k for k, v in new_weights.items() if v["note"] == "boosted"]
    reduced  = [k for k, v in new_weights.items() if "reduced" in v["note"]]
    blocked  = [k for k, v in new_weights.items() if v["note"].startswith("BLOCKED")]

    print(f"[{datetime.now(ET).strftime('%H:%M:%S')}] Weights updated — "
          f"{len(new_weights)} patterns | "
          f"{len(boosted)} boosted | {len(reduced)} reduced | {len(blocked)} blocked")
    for k in boosted:
        v = new_weights[k]
        print(f"  BOOST {v['weight']}x  [{k}]  WR:{v['win_rate']}% ({v['trades']} trades)")
    for k in blocked:
        print(f"  BLOCK  [{k}]  WR:0% ({new_weights[k]['trades']} trades)")

    return new_weights


if __name__ == "__main__":
    weights = run_learning_update()
    print("\n=== CURRENT WEIGHTS ===")
    print(json.dumps(weights, indent=2))
