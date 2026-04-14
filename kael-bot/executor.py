"""
TopstepX Execution Layer — ProjectX Gateway API
Handles authentication, account lookup, and order placement.
"""

import os
import math
import time
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

BASE_URL = "https://api.topstepx.com/api"
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")


def _notify_critical(message: str):
    """Send urgent Discord alert for critical trading issues."""
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(DISCORD_WEBHOOK, json={
            "embeds": [{"title": "KAEL CRITICAL ALERT", "description": message, "color": 0xFF0000}]
        }, timeout=5)
    except Exception:
        pass

USERNAME = os.getenv("TOPSTEP_USERNAME")
API_KEY = os.getenv("TOPSTEP_API_KEY")

TOKEN_LIFETIME_MINUTES = 50  # Refresh token before it expires


class TopstepExecutor:

    def __init__(self):
        self.token = None
        self.token_expiry = None
        self.account_id = None
        # Persistent HTTP session — reuses TCP connections across all API calls
        # (avoids TLS handshake overhead on every request)
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Contract ID cache — each place_order previously made a Contract/search round-trip
        self._contract_cache: dict = {}
        self._authenticate()
        self._get_account()

    def _authenticate(self):
        """Login with API key and get JWT token. Retries 3x with 2s backoff."""
        url = f"{BASE_URL}/Auth/loginKey"
        payload = {"userName": USERNAME, "apiKey": API_KEY}
        last_err = None
        for attempt in range(3):
            try:
                res = self.session.post(url, json=payload, timeout=15)
                res.raise_for_status()
                data = res.json()
                self.token = data.get("token")
                if not self.token:
                    raise Exception(f"Auth failed: {data}")
                self.token_expiry = datetime.now() + timedelta(minutes=TOKEN_LIFETIME_MINUTES)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Authenticated with TopstepX")
                return
            except Exception as e:
                last_err = e
                if attempt < 2:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Auth attempt {attempt+1} failed: {e} — retrying in 2s...")
                    time.sleep(2)
        raise Exception(f"Auth failed after 3 attempts: {last_err}")

    def _ensure_token(self):
        """Re-authenticate if token is about to expire."""
        if self.token_expiry and datetime.now() >= self.token_expiry:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Token expiring — refreshing...")
            self._authenticate()

    def _headers(self):
        self._ensure_token()
        return {"Authorization": f"Bearer {self.token}"}

    def _parse_list(self, data):
        """Parse API response that could be list or wrapped object."""
        if isinstance(data, list):
            return data
        for key in ("accounts", "positions", "contracts", "data", "items"):
            if isinstance(data, dict) and isinstance(data.get(key), list):
                return data[key]
        return []

    def _get_account(self):
        """Get the active Express trading account ID."""
        url = f"{BASE_URL}/Account/search"
        res = self.session.post(url, headers=self._headers(), json={"onlyActiveAccounts": True}, timeout=10)
        res.raise_for_status()
        accounts = self._parse_list(res.json())
        if not accounts:
            raise Exception("No active accounts found.")
        # Prefer practice/sim account — never auto-select a live/combine account
        account = next((a for a in accounts if "PRAC" in str(a.get("name", "")).upper()
                        or "SIM" in str(a.get("name", "")).upper()
                        or "PAPER" in str(a.get("name", "")).upper()), accounts[0])
        self.account_id = account["id"]
        self.starting_balance = account.get("balance", 0)
        self._balance_fields_logged = False  # reset debug flag on reconnect
        name = account.get("name", self.account_id)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Account: {name} (id:{self.account_id}) | Balance: ${self.starting_balance:,.2f}")

    def get_contract_id(self, symbol: str) -> str:
        """Search for a contract by exact symbol match (e.g. 'MGC', 'MNQ').
        Result is cached for the session lifetime — contract IDs don't change."""
        symbol_upper = symbol.upper()
        if symbol_upper in self._contract_cache:
            return self._contract_cache[symbol_upper]

        url = f"{BASE_URL}/Contract/search"
        res = self.session.post(url, headers=self._headers(), json={"searchText": symbol, "live": False}, timeout=10)
        res.raise_for_status()
        contracts = self._parse_list(res.json())
        if not contracts:
            raise Exception(f"No contract found for {symbol}")

        # Exact match first — prevents MGC search returning GC, MNQ returning NQ, etc.
        exact = [c for c in contracts if str(c.get("name", "")).upper() == symbol_upper
                 or str(c.get("symbol", "")).upper() == symbol_upper
                 or str(c.get("ticker", "")).upper() == symbol_upper]
        if exact:
            chosen = exact[0]
        else:
            starts = [c for c in contracts if str(c.get("name", "")).upper().startswith(symbol_upper)
                      or str(c.get("symbol", "")).upper().startswith(symbol_upper)]
            chosen = starts[0] if starts else contracts[0]

        name = chosen.get("name") or chosen.get("symbol") or chosen.get("ticker") or chosen["id"]
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Contract resolved: {symbol} → {name} (id:{chosen['id']})")
        self._contract_cache[symbol_upper] = chosen["id"]
        return chosen["id"]

    # Tick sizes per symbol — prices must be multiples of these
    TICK_SIZES = {
        "MGC": 0.10,
        "MNQ": 0.25,
        "MES": 0.25,
        "GC":  0.10,
        "NQ":  0.25,
        "ES":  0.25,
    }

    @staticmethod
    def _tick_round(price: float, symbol: str, direction: int = 0) -> float:
        """Round price to the nearest valid tick. direction: -1=floor, 0=nearest, 1=ceil."""
        tick = TopstepExecutor.TICK_SIZES.get(symbol.upper(), 0.01)
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
    ):
        """
        Place a market order then attach SL (StopLimit) and TP (Limit) brackets.
        side: "buy" for long, "sell" for short
        Returns True if entry was placed (even if brackets had issues).
        Returns False only if entry itself was rejected.
        """
        contract_id = self.get_contract_id(symbol)
        order_side = 0 if side == "buy" else 1
        exit_side = 1 if side == "buy" else 0
        tick = self.TICK_SIZES.get(symbol.upper(), 0.01)

        # ── Step 1: Market entry ──────────────────────────────────────────────
        entry_payload = {
            "accountId": self.account_id,
            "contractId": contract_id,
            "type": 2,          # Market order
            "side": order_side,
            "size": quantity,
        }
        url = f"{BASE_URL}/Order/place"
        res = self.session.post(url, json=entry_payload, headers=self._headers(), timeout=10)
        res.raise_for_status()
        result = res.json()

        if isinstance(result, dict):
            error_msg = result.get("errorMessage") or result.get("error") or result.get("message", "")
            success = result.get("success", True)
            if not success or (error_msg and "error" in str(error_msg).lower()):
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ENTRY REJECTED: {error_msg}")
                return False

        print(f"[{datetime.now().strftime('%H:%M:%S')}] ENTRY PLACED: {side.upper()} {quantity}x {symbol}")
        # Entry is live — set entry_placed so caller can record cooldown even if brackets fail
        self.last_entry_placed = True

        # ── Step 2: Stop loss (StopLimit, type=4) ────────────────────────────
        # type 3 (StopMarket) is rejected by TopstepX. Use StopLimit (type 4) with a
        # 2-tick limit buffer so the fill always executes even with slight slippage.
        sl_placed = False
        sl_price = self._tick_round(stop_loss, symbol)
        # Limit price: 2 ticks past stop (worse direction) to ensure fill
        sl_limit = self._tick_round(sl_price - 2 * tick if side == "buy" else sl_price + 2 * tick, symbol)
        try:
            sl_payload = {
                "accountId": self.account_id,
                "contractId": contract_id,
                "type": 4,          # StopLimit order
                "side": exit_side,
                "size": quantity,
                "stopPrice": sl_price,
                "limitPrice": sl_limit,
            }
            sl_res = self.session.post(url, json=sl_payload, headers=self._headers(), timeout=10)
            sl_res.raise_for_status()
            sl_result = sl_res.json()
            sl_err = sl_result.get("errorMessage") or sl_result.get("error") or "" if isinstance(sl_result, dict) else ""
            sl_placed = not sl_err
            if sl_placed:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] STOP LOSS SET: {sl_price} (limit:{sl_limit})")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] CRITICAL: STOP LOSS FAILED on {symbol} {side.upper()}: {sl_err} — position is UNPROTECTED")
                _notify_critical(f"STOP LOSS FAILED on {symbol} {side.upper()} — position is UNPROTECTED!\nSL error: {sl_err}\nStop price: {sl_price}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] STOP LOSS ERROR: {e}")

        # ── Step 3: Take profit (Limit, type=1) ──────────────────────────────
        tp_placed = False
        # Tick-align the TP price. For a BUY we want limit SELL at or above TP, for SELL limit BUY at or below TP.
        tp_price = self._tick_round(take_profit, symbol, direction=-1 if side == "buy" else 1)
        try:
            tp_payload = {
                "accountId": self.account_id,
                "contractId": contract_id,
                "type": 1,          # Limit order
                "side": exit_side,
                "size": quantity,
                "limitPrice": tp_price,
            }
            tp_res = self.session.post(url, json=tp_payload, headers=self._headers(), timeout=10)
            tp_res.raise_for_status()
            tp_result = tp_res.json()
            tp_err = tp_result.get("errorMessage") or tp_result.get("error") or "" if isinstance(tp_result, dict) else ""
            tp_placed = not tp_err
            if tp_placed:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] TAKE PROFIT SET: {tp_price}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] TAKE PROFIT FAILED: {tp_err}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] TAKE PROFIT ERROR: {e}")

        # ── Step 4: Emergency flatten if BOTH brackets failed ─────────────────
        if not sl_placed and not tp_placed:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] CRITICAL: Both SL and TP failed — flattening position immediately")
            self.flatten_all()
            # Still return True so caller records the cooldown — entry IS live
            return True

        if not sl_placed:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: SL not set — monitor manually")
        if not tp_placed:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: TP not set — monitor manually")

        return True

    def get_account_balance(self):
        """Fetch current balance and compute daily P&L as delta from starting balance."""
        try:
            url = f"{BASE_URL}/Account/search"
            res = self.session.post(url, headers=self._headers(), json={"onlyActiveAccounts": True}, timeout=10)
            res.raise_for_status()
            accounts = self._parse_list(res.json())
            account = next((a for a in accounts if str(a.get("id")) == str(self.account_id)),
                           accounts[0] if accounts else {})
            balance = account.get("balance", 0)
            daily_pnl = round(balance - self.starting_balance, 2)
            return balance, daily_pnl
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Balance check failed: {e}")
            return 0, 0

    def cancel_open_orders(self, contract_id=None):
        """Cancel all open/working orders for this account (optionally filtered by contract)."""
        try:
            search_payload = {"accountId": self.account_id}
            if contract_id:
                search_payload["contractId"] = contract_id
            res = self.session.post(f"{BASE_URL}/Order/search", headers=self._headers(),
                                json=search_payload, timeout=10)
            if not res.ok:
                return
            orders = res.json()
            if isinstance(orders, dict):
                orders = orders.get("orders") or orders.get("data") or []
            # Cancel any order that is still open/working
            open_statuses = {0, 1, 2}  # 0=pending, 1=working, 2=partiallyFilled
            cancelled = 0
            for order in orders:
                status = order.get("status", -1)
                if status in open_statuses:
                    try:
                        cancel_res = self.session.post(f"{BASE_URL}/Order/cancel",
                                                      headers=self._headers(),
                                                      json={"orderId": order["id"], "accountId": self.account_id},
                                                      timeout=10)
                        if cancel_res.ok:
                            cancelled += 1
                    except Exception:
                        pass
            if cancelled:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Cancelled {cancelled} open order(s).")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] cancel_open_orders failed: {e}")

    def get_open_positions(self):
        """Return list of open positions (non-zero size) for this account. Returns [] on any error."""
        try:
            res = self.session.post(
                f"{BASE_URL}/Position/search",
                headers=self._headers(),
                json={"accountId": self.account_id},
                timeout=10,
            )
            if not res.ok:
                return []
            data = res.json()
            positions = self._parse_list(data) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            return [p for p in positions if (p.get("netSize") or p.get("size") or 0) != 0]
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] get_open_positions failed: {e}")
            return []

    def flatten_all(self):
        """Close all open positions and cancel all open bracket orders."""
        # Cancel open orders first (SL/TP brackets orphaned after position close)
        self.cancel_open_orders()
        # Try 1: flattenAll (ProjectX v2)
        flatten_ok = False
        for endpoint in ["/Position/flattenAll", "/Position/closeAll", "/Order/cancelAllAndClose"]:
            try:
                url = f"{BASE_URL}{endpoint}"
                res = self.session.post(url, headers=self._headers(), json={"accountId": self.account_id}, timeout=10)
                if res.status_code == 404:
                    continue
                res.raise_for_status()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] All positions flattened via {endpoint}.")
                flatten_ok = True
                break
            except Exception:
                continue

        # Try 2: Fetch open positions and close each with an opposing market order
        if not flatten_ok:
            try:
                pos_res = self.session.post(f"{BASE_URL}/Position/search", headers=self._headers(),
                                           json={"accountId": self.account_id}, timeout=10)
                positions = pos_res.json() if pos_res.ok else []
                if isinstance(positions, dict):
                    positions = positions.get("positions") or positions.get("data") or []
                closed = 0
                failed = 0
                for pos in positions:
                    pos_size = pos.get("netSize") or pos.get("size") or 0
                    pos_side = pos.get("side", 0)
                    if pos_size == 0:
                        continue
                    close_side = 1 if pos_side == 0 else 0
                    contract_id = pos.get("contractId")
                    try:
                        close_res = self.session.post(f"{BASE_URL}/Order/place", headers=self._headers(), json={
                            "accountId": self.account_id,
                            "contractId": contract_id,
                            "type": 2,
                            "side": close_side,
                            "size": abs(pos_size),
                        }, timeout=10)
                        close_res.raise_for_status()
                        closed += 1
                    except Exception as close_err:
                        failed += 1
                        print(f"[{datetime.now().strftime('%H:%M:%S')}] FLATTEN WARNING: Failed to close {contract_id}: {close_err}")
                if closed:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Flattened {closed} position(s) via individual close orders.")
                if failed:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] FLATTEN INCOMPLETE: {failed} position(s) could NOT be closed.")
                flatten_ok = closed > 0
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] FLATTEN FAILED (all methods): {e}")

        # Verify positions are actually flat after flatten attempt
        try:
            time.sleep(2)  # Brief delay for order processing
            verify_res = self.session.post(f"{BASE_URL}/Position/search", headers=self._headers(),
                                          json={"accountId": self.account_id}, timeout=10)
            if verify_res.ok:
                remaining = verify_res.json()
                if isinstance(remaining, dict):
                    remaining = remaining.get("positions") or remaining.get("data") or []
                open_pos = [p for p in remaining if (p.get("netSize") or p.get("size") or 0) != 0]
                if open_pos:
                    msg = f"MANUAL INTERVENTION REQUIRED — {len(open_pos)} position(s) NOT closed after flatten attempt!"
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
                    _notify_critical(msg)
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Flatten verified — account is flat.")
        except Exception as ve:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Flatten verification failed: {ve}")

        return flatten_ok
