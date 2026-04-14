"""
Paper Executor — Zero-broker simulation mode.
No external accounts, no API keys, no subscriptions needed.

Tracks simulated positions and P&L in memory using yfinance prices for fills.
Set BROKER=paper in .env to activate.
"""

import math
import time
import yfinance as yf
from datetime import datetime
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")

# Simulated starting balance
PAPER_BALANCE = 50_000.00

# Maps base symbol to yfinance ticker for live price lookups
_PRICE_TICKERS = {
    "MNQ": "MNQ=F",
    "NQ":  "NQ=F",
    "MGC": "MGC=F",
    "GC":  "GC=F",
    "MES": "MES=F",
    "ES":  "ES=F",
}

# Tick sizes
_TICK_SIZES = {
    "MGC": 0.10, "MNQ": 0.25, "MES": 0.25,
    "GC":  0.10, "NQ":  0.25, "ES":  0.25,
}

# Point values ($ per 1.0 move)
_POINT_VALUES = {
    "MGC": 10.00, "MNQ": 2.00, "MES": 5.00,
    "GC":  100.00, "NQ": 20.00, "ES":  50.00,
}


def _ts():
    return datetime.now(_ET).strftime("%H:%M:%S")


def _live_price(symbol: str) -> float:
    """Fetch latest price from yfinance. Returns 0.0 on failure."""
    ticker = _PRICE_TICKERS.get(symbol.upper(), symbol)
    try:
        df = yf.Ticker(ticker).history(interval="1m", period="1d")
        if df is not None and not df.empty:
            return float(df["Close"].iloc[-1])
    except Exception:
        pass
    return 0.0


class PaperExecutor:
    """
    Simulated broker executor.
    Tracks open positions in memory. P&L is calculated from live yfinance prices.
    """

    def __init__(self):
        self.account_id = "PAPER-SIM-001"
        self.starting_balance = PAPER_BALANCE
        self._balance = PAPER_BALANCE
        self._realized_pnl = 0.0
        # Open positions: { symbol: {side, qty, entry_price, stop_loss, take_profit} }
        self._positions: dict = {}
        # Closed trade history for P&L tracking
        self._closed_trades: list = []
        print(f"[{_ts()}] Paper executor ready — ${PAPER_BALANCE:,.2f} simulated balance. No broker connection needed.")

    # ── Contracts (no-op — just return the symbol as the "id") ───────────────

    def get_contract_id(self, symbol: str) -> str:
        return symbol.upper()

    # ── Orders ────────────────────────────────────────────────────────────────

    @staticmethod
    def _tick_round(price: float, symbol: str, direction: int = 0) -> float:
        tick = _TICK_SIZES.get(symbol.upper(), 0.01)
        if direction == -1:
            return round(math.floor(price / tick) * tick, 10)
        if direction == 1:
            return round(math.ceil(price / tick) * tick, 10)
        return round(round(price / tick) * tick, 10)

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        stop_loss: float,
        take_profit: float,
    ) -> bool:
        sym = symbol.upper()
        fill_price = _live_price(sym)
        if fill_price == 0.0:
            print(f"[{_ts()}] PAPER: Could not get live price for {sym} — order skipped")
            return False

        self.last_entry_placed = True
        self._positions[sym] = {
            "side": side,
            "qty": quantity,
            "entry_price": fill_price,
            "stop_loss": self._tick_round(stop_loss, sym),
            "take_profit": self._tick_round(take_profit, sym),
        }

        sl = self._tick_round(stop_loss, sym)
        tp = self._tick_round(take_profit, sym, direction=-1 if side == "buy" else 1)
        print(f"[{_ts()}] PAPER ORDER FILLED: {'BUY' if side == 'buy' else 'SELL'} {quantity}x {sym} @ {fill_price:.2f}")
        print(f"[{_ts()}] PAPER SL: {sl} | TP: {tp}")
        return True

    # ── Balance / P&L ─────────────────────────────────────────────────────────

    def get_account_balance(self):
        """
        Compute current balance:
        - Starts at PAPER_BALANCE
        - Adds realized P&L from closed trades
        - Adds unrealized P&L from open positions (mark-to-market)
        """
        unrealized = 0.0
        for sym, pos in list(self._positions.items()):
            current_price = _live_price(sym)
            if current_price == 0.0:
                continue
            point_val = _POINT_VALUES.get(sym, 1.0)
            price_diff = current_price - pos["entry_price"]
            if pos["side"] == "sell":
                price_diff = -price_diff
            unrealized += price_diff * point_val * pos["qty"]

            # Auto-close if SL or TP hit
            if pos["side"] == "buy":
                if current_price <= pos["stop_loss"]:
                    self._close_position(sym, pos["stop_loss"], "sl_hit")
                elif current_price >= pos["take_profit"]:
                    self._close_position(sym, pos["take_profit"], "tp_hit")
            else:
                if current_price >= pos["stop_loss"]:
                    self._close_position(sym, pos["stop_loss"], "sl_hit")
                elif current_price <= pos["take_profit"]:
                    self._close_position(sym, pos["take_profit"], "tp_hit")

        balance = round(self.starting_balance + self._realized_pnl + unrealized, 2)
        daily_pnl = round(balance - self.starting_balance, 2)
        return balance, daily_pnl

    def _close_position(self, sym: str, exit_price: float, reason: str):
        pos = self._positions.pop(sym, None)
        if not pos:
            return
        point_val = _POINT_VALUES.get(sym, 1.0)
        price_diff = exit_price - pos["entry_price"]
        if pos["side"] == "sell":
            price_diff = -price_diff
        trade_pnl = round(price_diff * point_val * pos["qty"], 2)
        self._realized_pnl += trade_pnl
        self._closed_trades.append({
            "symbol": sym, "side": pos["side"], "qty": pos["qty"],
            "entry": pos["entry_price"], "exit": exit_price,
            "pnl": trade_pnl, "reason": reason,
            "time": datetime.now(_ET).strftime("%Y-%m-%d %H:%M:%S ET"),
        })
        result = "WIN" if trade_pnl > 0 else "LOSS"
        print(f"[{_ts()}] PAPER POSITION CLOSED [{result}]: {sym} {reason} | Exit: {exit_price} | P&L: ${trade_pnl:+.2f} | Total realized: ${self._realized_pnl:+.2f}")

    # ── Positions / orders ────────────────────────────────────────────────────

    def get_open_positions(self):
        """Return open positions in a format compatible with run_bot.py checks."""
        result = []
        for sym, pos in self._positions.items():
            current_price = _live_price(sym)
            point_val = _POINT_VALUES.get(sym, 1.0)
            price_diff = current_price - pos["entry_price"] if current_price else 0
            if pos["side"] == "sell":
                price_diff = -price_diff
            unrealized = round(price_diff * point_val * pos["qty"], 2)
            result.append({
                "contractId": sym,
                "netSize": pos["qty"] if pos["side"] == "buy" else -pos["qty"],
                "side": 0 if pos["side"] == "buy" else 1,
                # Rich fields for dashboard display
                "symbol":      sym,
                "direction":   pos["side"],
                "entry_price": pos["entry_price"],
                "stop_loss":   pos["stop_loss"],
                "take_profit": pos["take_profit"],
                "qty":         pos["qty"],
                "current_price": current_price,
                "unrealized_pnl": unrealized,
            })
        return result

    def cancel_open_orders(self, contract_id=None):
        # No bracket orders to cancel in paper mode — they're tracked internally
        pass

    def flatten_all(self):
        """Close all open positions at current market price."""
        if not self._positions:
            print(f"[{_ts()}] PAPER: Account already flat.")
            return True
        for sym in list(self._positions.keys()):
            price = _live_price(sym) or self._positions[sym]["entry_price"]
            self._close_position(sym, price, "flatten")
        print(f"[{_ts()}] PAPER: All positions flattened.")
        return True
