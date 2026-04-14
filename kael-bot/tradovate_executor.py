"""
Tradovate Execution Layer — Demo/Sim Account
Drop-in replacement for executor.py (TopstepX).
Handles auth, account lookup, and order placement against Tradovate's demo API.

Set BROKER=tradovate in .env to activate.
"""

import os
import math
import time
import uuid
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

BASE_URL = "https://demo.tradovateapi.com/v1"
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

USERNAME    = os.getenv("TRADOVATE_USERNAME")
PASSWORD    = os.getenv("TRADOVATE_PASSWORD")
DEVICE_ID   = os.getenv("TRADOVATE_DEVICE_ID", str(uuid.uuid4()))
APP_ID      = os.getenv("TRADOVATE_APP_ID", "Sample App")
APP_VERSION = os.getenv("TRADOVATE_APP_VERSION", "1.0")
CID         = os.getenv("TRADOVATE_CID")     # Required for developer API access — leave blank if not registered
SEC         = os.getenv("TRADOVATE_SEC")     # Required for developer API access — leave blank if not registered

TOKEN_LIFETIME_MINUTES = 70  # Tradovate tokens last ~80min — refresh at 70


def _notify_critical(message: str):
    if not DISCORD_WEBHOOK:
        return
    try:
        requests.post(DISCORD_WEBHOOK, json={
            "embeds": [{"title": "KAEL CRITICAL ALERT", "description": message, "color": 0xFF0000}]
        }, timeout=5)
    except Exception:
        pass


class TradovateExecutor:

    def __init__(self):
        self.token = None
        self.token_expiry = None
        self.account_id = None
        self.account_spec = None   # Tradovate uses accountSpec (username) in order payloads
        self.starting_balance = 0
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._contract_cache: dict = {}  # symbol -> (contract_id, contract_name)
        self._authenticate()
        self._get_account()

    # ── Auth ─────────────────────────────────────────────────────────────────

    def _authenticate(self):
        url = f"{BASE_URL}/auth/accesstokenrequest"
        payload = {
            "name": USERNAME,
            "password": PASSWORD,
            "deviceId": DEVICE_ID,
            "appId": APP_ID,
            "appVersion": APP_VERSION,
        }
        # CID/SEC are required only for developer API registration — omit if absent
        if CID:
            payload["cid"] = int(CID)
        if SEC:
            payload["sec"] = SEC

        last_err = None
        for attempt in range(3):
            try:
                res = self.session.post(url, json=payload, timeout=15)
                res.raise_for_status()
                data = res.json()
                self.token = data.get("accessToken")
                if not self.token:
                    raise Exception(f"Auth failed — no accessToken in response: {data}")
                self.token_expiry = datetime.now() + timedelta(minutes=TOKEN_LIFETIME_MINUTES)
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Authenticated with Tradovate demo")
                return
            except Exception as e:
                last_err = e
                if attempt < 2:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Auth attempt {attempt+1} failed: {e} — retrying in 2s...")
                    time.sleep(2)
        raise Exception(f"Auth failed after 3 attempts: {last_err}")

    def _ensure_token(self):
        if self.token_expiry and datetime.now() >= self.token_expiry:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Token expiring — refreshing...")
            self._authenticate()

    def _headers(self):
        self._ensure_token()
        return {"Authorization": f"Bearer {self.token}"}

    # ── Account ───────────────────────────────────────────────────────────────

    def _get_account(self):
        res = self.session.get(f"{BASE_URL}/account/list", headers=self._headers(), timeout=10)
        res.raise_for_status()
        accounts = res.json()
        if not accounts:
            raise Exception("No accounts found on this Tradovate demo account.")

        # Prefer demo/sim account — never auto-select live
        account = next(
            (a for a in accounts if a.get("accountType", "").lower() in ("demo", "simulation", "customer")
             and not a.get("liveOnly", False)),
            accounts[0]
        )
        self.account_id = account["id"]
        self.account_spec = account.get("name", USERNAME)

        # Get starting balance from cash balance snapshot
        self.starting_balance = self._fetch_balance()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Account: {self.account_spec} (id:{self.account_id}) | Balance: ${self.starting_balance:,.2f}")

    def _fetch_balance(self) -> float:
        """Pull current account net liquidation value from Tradovate."""
        try:
            # Try snapshot endpoint first
            res = self.session.post(
                f"{BASE_URL}/cashBalance/getcashbalanceSnapshot",
                headers=self._headers(),
                json={"accountId": self.account_id},
                timeout=10,
            )
            if res.ok:
                data = res.json()
                # Tradovate returns a list — find our account
                if isinstance(data, list):
                    entry = next((d for d in data if d.get("accountId") == self.account_id), data[0] if data else {})
                elif isinstance(data, dict):
                    entry = data
                else:
                    entry = {}
                # Try common balance field names
                for field in ("totalCashValue", "netLiq", "cashBalance", "balance", "realizedPnL"):
                    val = entry.get(field)
                    if val and float(val) > 0:
                        return float(val)
        except Exception:
            pass

        # Fallback: account list balance field
        try:
            res = self.session.get(f"{BASE_URL}/account/list", headers=self._headers(), timeout=10)
            if res.ok:
                accounts = res.json()
                acct = next((a for a in accounts if a.get("id") == self.account_id), {})
                for field in ("balance", "netLiq", "cashBalance", "initialBalance"):
                    val = acct.get(field)
                    if val and float(val) > 0:
                        return float(val)
        except Exception:
            pass

        return 0.0

    # ── Contracts ─────────────────────────────────────────────────────────────

    # Tick sizes per base symbol
    TICK_SIZES = {
        # Equity Index Micros
        "MNQ": 0.25,    # Micro Nasdaq 100
        "MES": 0.25,    # Micro S&P 500
        "M2K": 0.10,    # Micro Russell 2000
        "MYM": 1.00,    # Micro Dow Jones
        # Full-size reference
        "NQ":  0.25,
        "ES":  0.25,
        # Metals
        "MGC": 0.10,    # Micro Gold
        "GC":  0.10,    # Full Gold
        # Energy
        "MCL": 0.01,    # Micro Crude Oil
        # FX Micros
        "M6E": 0.00005, # Micro EUR/USD (0.5 pip)
        "M6B": 0.0001,  # Micro GBP/USD (1 pip)
    }

    # CME quarterly month codes for futures contract resolution
    _QUARTERLY_SYMBOLS = {"MNQ", "MES", "NQ", "ES", "M2K", "MYM"}
    _MONTHLY_SYMBOLS   = {"MGC", "GC", "MCL", "M6E", "M6B"}
    _MONTH_CODES = {1:"F",2:"G",3:"H",4:"J",5:"K",6:"M",7:"N",8:"Q",9:"U",10:"V",11:"X",12:"Z"}
    _QUARTERLY_MONTHS = [3, 6, 9, 12]  # H, M, U, Z

    def _active_contract_name(self, symbol: str) -> str:
        """
        Build the active front-month contract name for Tradovate.
        e.g. MNQ in April 2026 → MNQM6 (June 2026 is front month after March rolloff)
        """
        now = datetime.now()
        year, month = now.year, now.month
        sym_upper = symbol.upper()

        if sym_upper in self._QUARTERLY_SYMBOLS:
            # Find the next quarterly month that hasn't rolled off yet
            # CME rolls ~3rd Friday of expiry month — approximate: if month > expiry month, advance
            for qm in self._QUARTERLY_MONTHS:
                if qm >= month:
                    contract_month = qm
                    contract_year = year
                    break
            else:
                # Rolled into next year
                contract_month = self._QUARTERLY_MONTHS[0]
                contract_year = year + 1
        else:
            # Monthly contract — next month if we're in the last week of current month
            import calendar
            _, last_day = calendar.monthrange(year, month)
            days_left = last_day - now.day
            if days_left <= 5:
                contract_month = month + 1 if month < 12 else 1
                contract_year = year if month < 12 else year + 1
            else:
                contract_month = month
                contract_year = year

        month_code = self._MONTH_CODES[contract_month]
        year_code = str(contract_year)[-1]  # e.g. 2026 → "6"
        return f"{sym_upper}{month_code}{year_code}"

    def get_contract_id(self, symbol: str) -> int:
        """
        Resolve base symbol (e.g. 'MNQ') to Tradovate contract ID.
        Tries active front month first, then searches API if not found.
        Result is cached for the session.
        """
        sym_upper = symbol.upper()
        if sym_upper in self._contract_cache:
            return self._contract_cache[sym_upper][0]

        # Try to find via direct name lookup (front month)
        contract_name = self._active_contract_name(sym_upper)
        try:
            res = self.session.get(
                f"{BASE_URL}/contract/find",
                headers=self._headers(),
                params={"name": contract_name},
                timeout=10,
            )
            if res.ok:
                data = res.json()
                if isinstance(data, dict) and data.get("id"):
                    self._contract_cache[sym_upper] = (data["id"], contract_name)
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] Contract resolved: {symbol} → {contract_name} (id:{data['id']})")
                    return data["id"]
        except Exception:
            pass

        # Fallback: search API
        res = self.session.post(
            f"{BASE_URL}/contract/search",
            headers=self._headers(),
            json={"searchText": sym_upper},
            timeout=10,
        )
        res.raise_for_status()
        contracts = res.json()
        if not contracts:
            raise Exception(f"No contract found for {symbol}")

        # Prefer exact symbol matches, then startswith
        exact = [c for c in contracts if str(c.get("name", "")).upper().startswith(sym_upper)]
        chosen = exact[0] if exact else contracts[0]
        chosen_name = chosen.get("name", sym_upper)
        self._contract_cache[sym_upper] = (chosen["id"], chosen_name)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Contract resolved (search): {symbol} → {chosen_name} (id:{chosen['id']})")
        return chosen["id"]

    def _get_contract_name(self, symbol: str) -> str:
        """Get the resolved full contract name (e.g. MNQM6) for order placement."""
        sym_upper = symbol.upper()
        if sym_upper in self._contract_cache:
            return self._contract_cache[sym_upper][1]
        self.get_contract_id(sym_upper)  # populates cache
        return self._contract_cache[sym_upper][1]

    @staticmethod
    def _tick_round(price: float, symbol: str, direction: int = 0) -> float:
        tick = TradovateExecutor.TICK_SIZES.get(symbol.upper(), 0.01)
        if direction == -1:
            return round(math.floor(price / tick) * tick, 10)
        if direction == 1:
            return round(math.ceil(price / tick) * tick, 10)
        return round(round(price / tick) * tick, 10)

    # ── Orders ────────────────────────────────────────────────────────────────

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        stop_loss: float,
        take_profit: float,
    ) -> bool:
        """
        Place a market order then attach SL (StopLimit) and TP (Limit) brackets.
        side: "buy" for long, "sell" for short.
        Returns True if entry was placed, False only if entry was rejected.
        """
        contract_name = self._get_contract_name(symbol)
        tick = self.TICK_SIZES.get(symbol.upper(), 0.01)
        action = "Buy" if side == "buy" else "Sell"
        exit_action = "Sell" if side == "buy" else "Buy"

        base_payload = {
            "accountSpec": self.account_spec,
            "accountId": self.account_id,
            "symbol": contract_name,
            "orderQty": quantity,
            "isAutomated": True,
        }

        url = f"{BASE_URL}/order/placeorder"

        # ── Step 1: Market entry ──────────────────────────────────────────────
        entry_payload = {**base_payload, "action": action, "orderType": "Market"}
        res = self.session.post(url, json=entry_payload, headers=self._headers(), timeout=10)
        res.raise_for_status()
        result = res.json()

        if isinstance(result, dict):
            err = result.get("errorMessage") or result.get("error") or result.get("failureReason") or ""
            if err and "error" in str(err).lower():
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ENTRY REJECTED: {err}")
                return False

        print(f"[{datetime.now().strftime('%H:%M:%S')}] ENTRY PLACED: {action.upper()} {quantity}x {contract_name}")
        self.last_entry_placed = True

        # ── Step 2: Stop loss (StopLimit with 2-tick buffer) ──────────────────
        sl_placed = False
        sl_price = self._tick_round(stop_loss, symbol)
        sl_limit = self._tick_round(
            sl_price - 2 * tick if side == "buy" else sl_price + 2 * tick,
            symbol
        )
        try:
            sl_payload = {
                **base_payload,
                "action": exit_action,
                "orderType": "StopLimit",
                "stopPrice": sl_price,
                "price": sl_limit,
            }
            sl_res = self.session.post(url, json=sl_payload, headers=self._headers(), timeout=10)
            sl_res.raise_for_status()
            sl_result = sl_res.json()
            sl_err = (sl_result.get("errorMessage") or sl_result.get("error") or "") if isinstance(sl_result, dict) else ""
            sl_placed = not sl_err
            if sl_placed:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] STOP LOSS SET: {sl_price} (limit:{sl_limit})")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] CRITICAL: STOP LOSS FAILED on {symbol}: {sl_err} — position UNPROTECTED")
                _notify_critical(f"STOP LOSS FAILED on {contract_name} {action} — position is UNPROTECTED!\nError: {sl_err}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] STOP LOSS ERROR: {e}")

        # ── Step 3: Take profit (Limit) ───────────────────────────────────────
        tp_placed = False
        tp_price = self._tick_round(take_profit, symbol, direction=-1 if side == "buy" else 1)
        try:
            tp_payload = {
                **base_payload,
                "action": exit_action,
                "orderType": "Limit",
                "price": tp_price,
            }
            tp_res = self.session.post(url, json=tp_payload, headers=self._headers(), timeout=10)
            tp_res.raise_for_status()
            tp_result = tp_res.json()
            tp_err = (tp_result.get("errorMessage") or tp_result.get("error") or "") if isinstance(tp_result, dict) else ""
            tp_placed = not tp_err
            if tp_placed:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] TAKE PROFIT SET: {tp_price}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] TAKE PROFIT FAILED: {tp_err}")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] TAKE PROFIT ERROR: {e}")

        # ── Step 4: Emergency flatten if both brackets failed ─────────────────
        if not sl_placed and not tp_placed:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] CRITICAL: Both SL and TP failed — flattening immediately")
            self.flatten_all()
            return True  # Entry did go live — return True so caller records cooldown

        if not sl_placed:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: SL not set — monitor manually")
        if not tp_placed:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING: TP not set — monitor manually")

        return True

    # ── Account balance ───────────────────────────────────────────────────────

    def get_account_balance(self):
        """Returns (balance, daily_pnl) where daily_pnl is delta from starting_balance."""
        balance = self._fetch_balance()
        if balance == 0:
            return 0, 0
        daily_pnl = round(balance - self.starting_balance, 2)
        return balance, daily_pnl

    # ── Open orders / positions ───────────────────────────────────────────────

    def cancel_open_orders(self, contract_id=None):
        """Cancel all open orders for this account (optionally filtered by contract)."""
        try:
            res = self.session.get(
                f"{BASE_URL}/order/list",
                headers=self._headers(),
                params={"accountId": self.account_id},
                timeout=10,
            )
            if not res.ok:
                return
            orders = res.json()
            if isinstance(orders, dict):
                orders = orders.get("orders") or orders.get("data") or []
            # Tradovate order statuses: Working=1, Accepted=0 — cancel those
            open_statuses = {"Working", "Accepted", "PendingNew"}
            cancelled = 0
            for order in orders:
                if contract_id and order.get("contractId") != contract_id:
                    continue
                if order.get("ordStatus") in open_statuses or order.get("status") in open_statuses:
                    try:
                        cancel_res = self.session.post(
                            f"{BASE_URL}/order/cancelorder",
                            headers=self._headers(),
                            json={"orderId": order["id"]},
                            timeout=10,
                        )
                        if cancel_res.ok:
                            cancelled += 1
                    except Exception:
                        pass
            if cancelled:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Cancelled {cancelled} open order(s).")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] cancel_open_orders failed: {e}")

    def get_open_positions(self):
        """Return list of open positions (non-zero netPos) for this account."""
        try:
            res = self.session.get(
                f"{BASE_URL}/position/list",
                headers=self._headers(),
                params={"accountId": self.account_id},
                timeout=10,
            )
            if not res.ok:
                return []
            data = res.json()
            positions = data if isinstance(data, list) else (data.get("positions") or data.get("data") or [])
            # Filter to only positions for our account with non-zero size
            return [
                p for p in positions
                if p.get("accountId") == self.account_id
                and (p.get("netPos") or p.get("netSize") or p.get("size") or 0) != 0
            ]
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] get_open_positions failed: {e}")
            return []

    def flatten_all(self):
        """Close all open positions and cancel all open bracket orders."""
        self.cancel_open_orders()

        flatten_ok = False

        # Try: liquidateposition for each open position
        try:
            positions = self.get_open_positions()
            closed = 0
            for pos in positions:
                net_pos = pos.get("netPos") or pos.get("netSize") or pos.get("size") or 0
                if net_pos == 0:
                    continue
                contract_id = pos.get("contractId")
                # Try Tradovate's liquidateposition endpoint
                try:
                    liq_res = self.session.post(
                        f"{BASE_URL}/order/liquidateposition",
                        headers=self._headers(),
                        json={"accountId": self.account_id, "contractId": contract_id, "admin": False},
                        timeout=10,
                    )
                    if liq_res.ok:
                        closed += 1
                        continue
                except Exception:
                    pass
                # Fallback: opposing market order
                close_action = "Sell" if net_pos > 0 else "Buy"
                contract_name = next(
                    (v[1] for k, v in self._contract_cache.items() if v[0] == contract_id),
                    str(contract_id),
                )
                try:
                    close_res = self.session.post(
                        f"{BASE_URL}/order/placeorder",
                        headers=self._headers(),
                        json={
                            "accountSpec": self.account_spec,
                            "accountId": self.account_id,
                            "symbol": contract_name,
                            "action": close_action,
                            "orderQty": abs(net_pos),
                            "orderType": "Market",
                            "isAutomated": True,
                        },
                        timeout=10,
                    )
                    if close_res.ok:
                        closed += 1
                except Exception as ce:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] FLATTEN WARNING: {ce}")

            if closed:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Flattened {closed} position(s).")
            flatten_ok = closed > 0
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] FLATTEN FAILED: {e}")

        # Verify flat
        try:
            time.sleep(2)
            remaining = self.get_open_positions()
            if remaining:
                msg = f"MANUAL INTERVENTION REQUIRED — {len(remaining)} position(s) NOT closed after flatten!"
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
                _notify_critical(msg)
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Flatten verified — account is flat.")
        except Exception:
            pass

        return flatten_ok
