"""
=============================================================================
ICT TRADING BOT - ALGORITHMIC SPECIFICATION
=============================================================================
Author: Built for Joseph Ritchwood / Polaris Digital Studio
Instruments:
  Equity Index Micros : MES, MNQ, M2K, MYM (+ full NQ, GC for reference)
  Metals              : MGC, GC
  Energy              : MCL (Micro Crude Oil)
  FX Micros           : M6E (EUR/USD), M6B (GBP/USD)
Strategies: ICT Silver Bullet, Asian Range Sweep, Opening Range Breakout
=============================================================================
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
_ET = ZoneInfo("America/New_York")
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
import time as _time_module


class Instrument(Enum):
    # Full-size (high capital required — use with caution on prop accounts)
    NQ  = "NQ=F"
    GC  = "GC=F"
    # Equity Index Micros
    MNQ = "MNQ=F"    # Micro Nasdaq 100  — $2/pt
    MES = "MES=F"    # Micro S&P 500     — $5/pt
    M2K = "M2K=F"    # Micro Russell 2000 — $5/pt
    MYM = "MYM=F"    # Micro Dow Jones   — $0.50/pt
    # Metals Micro
    MGC = "MGC=F"    # Micro Gold        — $10/pt
    # Energy Micro
    MCL = "MCL=F"    # Micro Crude Oil   — $100/pt (per $1 barrel move, conservative sizing)
    # FX Micros — ICT was built on forex; London KZ setups are excellent here
    M6E = "M6E=F"    # Micro EUR/USD     — $1.25/pip ($12,500/full pt)
    M6B = "M6B=F"    # Micro GBP/USD     — $0.625/pip ($6,250/full pt)


class SessionType(Enum):
    ASIA = "asia"
    LONDON = "london"
    NY_AM = "ny_am"
    NY_PM = "ny_pm"
    OVERLAP = "overlap"


class Bias(Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class SetupType(Enum):
    SILVER_BULLET = "silver_bullet"
    ASIAN_RANGE_SWEEP = "asian_range_sweep"
    ORB = "opening_range_breakout"


@dataclass
class SessionWindow:
    name: str
    start: time
    end: time
    priority: int


SESSIONS = {
    "asia": SessionWindow("Asia", time(18, 0), time(3, 0), priority=3),
    "london": SessionWindow("London", time(3, 0), time(8, 0), priority=2),
    "london_killzone": SessionWindow("London KZ", time(3, 0), time(5, 0), priority=2),
    "ny_am_killzone": SessionWindow("NY AM KZ", time(8, 30), time(13, 30), priority=1),
    "london_late": SessionWindow("London Late", time(5, 0), time(8, 30), priority=3),
    "silver_bullet_london": SessionWindow("SB London", time(3, 0), time(4, 0), priority=2),
    "silver_bullet_midnight": SessionWindow("SB Midnight", time(0, 0), time(2, 0), priority=3),
    "silver_bullet_ny_am": SessionWindow("SB NY AM", time(10, 0), time(11, 0), priority=1),
    "silver_bullet_ny_pm": SessionWindow("SB NY PM", time(14, 0), time(15, 0), priority=2),
    "ny_pm_killzone": SessionWindow("NY PM KZ", time(13, 30), time(16, 0), priority=2),
    "orb_window": SessionWindow("ORB", time(9, 30), time(9, 45), priority=1),
    "orb_trade_window": SessionWindow("ORB Trade", time(9, 45), time(14, 45), priority=1),
}

RISK_PARAMS = {
    # ── Full-size (not recommended for prop accounts under $50k) ─────────────
    Instrument.NQ: {
        "tick_value": 5.00,
        "point_value": 20.00,
        "max_sl_points": 50,
        "default_sl_points": 15,
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 1000,
        "max_trades_per_day": 2,
    },
    Instrument.GC: {
        "tick_value": 10.00,
        "point_value": 100.00,
        "max_sl_points": 5.0,
        "default_sl_points": 3.0,
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 1000,
        "max_trades_per_day": 2,
    },
    # ── Equity Index Micros — ICT-native, all same kill zone schedule ─────────
    Instrument.MNQ: {
        "tick_value": 0.50,        # 0.25 tick × $2/pt
        "point_value": 2.00,
        "max_sl_points": 50,
        "default_sl_points": 15,
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 500,
        "max_trades_per_day": 3,
    },
    Instrument.MES: {
        "tick_value": 1.25,        # 0.25 tick × $5/pt
        "point_value": 5.00,
        "max_sl_points": 50,       # same point scale as MNQ
        "default_sl_points": 15,
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 500,
        "max_trades_per_day": 3,
    },
    Instrument.M2K: {
        "tick_value": 0.50,        # 0.10 tick × $5/pt
        "point_value": 5.00,
        "max_sl_points": 15,       # Russell moves in smaller point ranges
        "default_sl_points": 8,
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 500,
        "max_trades_per_day": 3,
    },
    Instrument.MYM: {
        "tick_value": 0.50,        # 1 pt tick × $0.50/pt
        "point_value": 0.50,
        "max_sl_points": 200,      # Dow moves in hundreds of points; 200 pts = $100
        "default_sl_points": 80,
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 500,
        "max_trades_per_day": 3,
    },
    # ── Metals Micro ────────────────────────────────────────────────────────────
    Instrument.MGC: {
        "tick_value": 1.00,        # 0.10 tick × $10/pt
        "point_value": 10.00,
        "max_sl_points": 5.0,
        "default_sl_points": 3.0,
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 500,
        "max_trades_per_day": 3,
    },
    # ── Energy Micro — more conservative; fundamentals can gap ──────────────────
    Instrument.MCL: {
        "tick_value": 1.00,        # 0.01 tick × 100 barrels = $1/tick
        "point_value": 100.00,     # $1 price move × 100 barrels = $100/pt
        "max_sl_points": 2.50,     # $2.50 barrel move = $250 max risk
        "default_sl_points": 1.50, # $1.50 barrel move = $150 default risk
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 300,     # Tighter daily cap — energy can gap hard
        "max_trades_per_day": 2,
    },
    # ── FX Micros — sl_points expressed in price units (e.g. 0.0050 = 50 pips)
    # point_value is per full 1.0000 price unit move ─────────────────────────────
    Instrument.M6E: {
        "tick_value": 1.25,        # 12,500 EUR × 0.0001 per pip
        "point_value": 12500.00,   # 12,500 EUR × 1.0000 (per full unit move)
        "max_sl_points": 0.010,    # 100 pip max stop
        "default_sl_points": 0.005,# 50 pip default stop
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 300,
        "max_trades_per_day": 2,
    },
    Instrument.M6B: {
        "tick_value": 0.625,       # 6,250 GBP × 0.0001 per pip
        "point_value": 6250.00,    # 6,250 GBP × 1.0000 (per full unit move)
        "max_sl_points": 0.010,    # 100 pip max stop
        "default_sl_points": 0.005,# 50 pip default stop
        "min_rr_ratio": 2.0,
        "target_rr_ratio": 3.0,
        "max_daily_loss": 300,
        "max_trades_per_day": 2,
    },
}


@dataclass
class SwingPoint:
    timestamp: datetime
    price: float
    is_high: bool
    timeframe: str
    strength: int = 1


@dataclass
class FairValueGap:
    timestamp: datetime
    top: float
    bottom: float
    midpoint: float
    is_bullish: bool
    timeframe: str
    is_filled: bool = False
    is_respected: bool = False


@dataclass
class OrderBlock:
    timestamp: datetime
    high: float
    low: float
    open_price: float
    close: float
    is_bullish: bool
    timeframe: str
    is_breaker: bool = False


@dataclass
class LiquidityLevel:
    price: float
    is_buyside: bool
    source: str
    swept: bool = False
    timestamp: Optional[datetime] = None


@dataclass
class MarketStructureShift:
    timestamp: datetime
    price: float
    direction: Bias
    swing_broken: SwingPoint
    displacement: bool = False


@dataclass
class TradeSignal:
    timestamp: datetime
    instrument: Instrument
    setup_type: SetupType
    direction: Bias
    entry_price: float
    stop_loss: float
    take_profit_1: float
    take_profit_2: float
    risk_reward: float
    confidence: float
    session: str
    confluences: list
    notes: str = ""


def _infer_timeframe(df: pd.DataFrame) -> str:
    if len(df) < 2:
        return "unknown"
    diff = (df.index[1] - df.index[0]).total_seconds()
    if diff <= 60:
        return "1m"
    elif diff <= 300:
        return "5m"
    elif diff <= 900:
        return "15m"
    elif diff <= 3600:
        return "1h"
    elif diff <= 14400:
        return "4h"
    else:
        return "1d"


class MarketStructureAnalyzer:

    @staticmethod
    def find_swing_points(df: pd.DataFrame, lookback: int = 5) -> list:
        n = len(df)
        if n < 2 * lookback + 1:
            return []
        highs = df["high"].to_numpy()
        lows  = df["low"].to_numpy()
        tf    = _infer_timeframe(df)
        swings = []
        try:
            # Fully vectorized path — numpy 1.20+ (Nov 2020)
            from numpy.lib.stride_tricks import sliding_window_view
            w  = 2 * lookback + 1
            hw = sliding_window_view(highs, w)   # shape: (n-w+1, w)
            lw = sliding_window_view(lows,  w)
            c  = lookback                        # center index within each window
            sh = (hw[:, c] > hw[:, :c].max(axis=1)) & (hw[:, c] > hw[:, c+1:].max(axis=1))
            sl = (lw[:, c] < lw[:, :c].min(axis=1)) & (lw[:, c] < lw[:, c+1:].min(axis=1))
            for k in np.where(sh)[0]:
                i = lookback + k
                swings.append(SwingPoint(df.index[i], highs[i], True,  tf, lookback))
            for k in np.where(sl)[0]:
                i = lookback + k
                swings.append(SwingPoint(df.index[i], lows[i],  False, tf, lookback))
        except ImportError:
            # Fallback: numpy slice max/min — still ~10x faster than pandas .iloc per cell
            for i in range(lookback, n - lookback):
                h = highs[i]
                if h > highs[i - lookback:i].max() and h > highs[i + 1:i + lookback + 1].max():
                    swings.append(SwingPoint(df.index[i], h, True,  tf, lookback))
                l = lows[i]
                if l < lows[i - lookback:i].min() and l < lows[i + 1:i + lookback + 1].min():
                    swings.append(SwingPoint(df.index[i], l, False, tf, lookback))
        return sorted(swings, key=lambda s: s.timestamp)

    @staticmethod
    def detect_structure_shift(df, swings, current_idx):
        if len(swings) < 4 or current_idx >= len(df):
            return None

        current_candle = df.iloc[current_idx]
        recent_highs = [s for s in swings if s.is_high][-3:]
        recent_lows = [s for s in swings if not s.is_high][-3:]

        if len(recent_highs) < 2 or len(recent_lows) < 2:
            return None

        making_higher_highs = recent_highs[-1].price > recent_highs[-2].price
        making_higher_lows = recent_lows[-1].price > recent_lows[-2].price
        bullish_structure = making_higher_highs and making_higher_lows

        making_lower_highs = recent_highs[-1].price < recent_highs[-2].price
        making_lower_lows = recent_lows[-1].price < recent_lows[-2].price
        bearish_structure = making_lower_highs and making_lower_lows

        candle_range = current_candle["high"] - current_candle["low"]
        candle_body = abs(current_candle["close"] - current_candle["open"])
        lookback_start = max(0, current_idx - 10)
        avg_range = (df.iloc[lookback_start:current_idx]["high"] - df.iloc[lookback_start:current_idx]["low"]).mean()

        is_displacement = (
            candle_body >= candle_range * 0.60 and
            candle_range >= avg_range * 1.5
        )

        if bullish_structure:
            most_recent_low = recent_lows[-1]
            if current_candle["close"] < most_recent_low.price:
                return MarketStructureShift(
                    timestamp=df.index[current_idx],
                    price=current_candle["close"],
                    direction=Bias.BEARISH,
                    swing_broken=most_recent_low,
                    displacement=is_displacement,
                )

        if bearish_structure:
            most_recent_high = recent_highs[-1]
            if current_candle["close"] > most_recent_high.price:
                return MarketStructureShift(
                    timestamp=df.index[current_idx],
                    price=current_candle["close"],
                    direction=Bias.BULLISH,
                    swing_broken=most_recent_high,
                    displacement=is_displacement,
                )

        return None

    @staticmethod
    def get_daily_bias(df_daily, df_4h):
        if len(df_daily) < 3 or len(df_4h) < 20:
            return Bias.NEUTRAL

        prev_day = df_daily.iloc[-2]
        two_days_ago = df_daily.iloc[-3]

        closed_above_prev_high = prev_day["close"] > two_days_ago["high"]
        closed_below_prev_low = prev_day["close"] < two_days_ago["low"]
        swept_low_closed_above = prev_day["low"] < two_days_ago["low"] and prev_day["close"] > prev_day["open"]
        swept_high_closed_below = prev_day["high"] > two_days_ago["high"] and prev_day["close"] < prev_day["open"]

        swings_4h = MarketStructureAnalyzer.find_swing_points(df_4h.tail(40), lookback=3)
        recent_4h_highs = [s for s in swings_4h if s.is_high][-3:]
        recent_4h_lows = [s for s in swings_4h if not s.is_high][-3:]

        bullish_4h = False
        bearish_4h = False

        if len(recent_4h_highs) >= 2 and len(recent_4h_lows) >= 2:
            bullish_4h = (
                recent_4h_highs[-1].price > recent_4h_highs[-2].price and
                recent_4h_lows[-1].price > recent_4h_lows[-2].price
            )
            bearish_4h = (
                recent_4h_highs[-1].price < recent_4h_highs[-2].price and
                recent_4h_lows[-1].price < recent_4h_lows[-2].price
            )

        range_high = df_4h.tail(20)["high"].max()
        range_low = df_4h.tail(20)["low"].min()
        equilibrium = (range_high + range_low) / 2
        current_price = df_4h.iloc[-1]["close"]

        in_discount = current_price < equilibrium
        in_premium = current_price > equilibrium

        if (closed_above_prev_high or swept_low_closed_above) and bullish_4h:
            return Bias.BULLISH
        elif (closed_below_prev_low or swept_high_closed_below) and bearish_4h:
            return Bias.BEARISH
        else:
            return Bias.NEUTRAL

    @staticmethod
    def get_daily_bias_verbose(df_daily, df_4h):
        reasons = []
        if len(df_daily) < 3 or len(df_4h) < 20:
            reasons.append("Insufficient data for bias calculation")
            return Bias.NEUTRAL, reasons

        prev_day = df_daily.iloc[-2]
        two_days_ago = df_daily.iloc[-3]

        closed_above_prev_high = prev_day["close"] > two_days_ago["high"]
        closed_below_prev_low = prev_day["close"] < two_days_ago["low"]
        swept_low_closed_above = prev_day["low"] < two_days_ago["low"] and prev_day["close"] > prev_day["open"]
        swept_high_closed_below = prev_day["high"] > two_days_ago["high"] and prev_day["close"] < prev_day["open"]

        swings_4h = MarketStructureAnalyzer.find_swing_points(df_4h.tail(40), lookback=3)
        recent_4h_highs = [s for s in swings_4h if s.is_high][-3:]
        recent_4h_lows = [s for s in swings_4h if not s.is_high][-3:]

        bullish_4h = False
        bearish_4h = False
        if len(recent_4h_highs) >= 2 and len(recent_4h_lows) >= 2:
            bullish_4h = (recent_4h_highs[-1].price > recent_4h_highs[-2].price and
                          recent_4h_lows[-1].price > recent_4h_lows[-2].price)
            bearish_4h = (recent_4h_highs[-1].price < recent_4h_highs[-2].price and
                          recent_4h_lows[-1].price < recent_4h_lows[-2].price)

        range_high = df_4h.tail(20)["high"].max()
        range_low = df_4h.tail(20)["low"].min()
        equilibrium = (range_high + range_low) / 2
        current_price = df_4h.iloc[-1]["close"]

        if closed_above_prev_high:
            reasons.append(f"Prev day closed above prior high ({two_days_ago['high']:.2f}) [bullish]")
        elif swept_low_closed_above:
            reasons.append(f"Prev day swept low + bullish close [manipulation + distribution]")
        elif closed_below_prev_low:
            reasons.append(f"Prev day closed below prior low ({two_days_ago['low']:.2f}) [bearish]")
        elif swept_high_closed_below:
            reasons.append(f"Prev day swept high + bearish close [manipulation + distribution]")
        else:
            reasons.append(f"Prev day: indecisive — no directional close vs prior range")

        if bullish_4h:
            reasons.append(f"4H structure: HH+HL (bullish trend)")
        elif bearish_4h:
            reasons.append(f"4H structure: LH+LL (bearish trend)")
        else:
            reasons.append(f"4H structure: mixed / no clear trend")

        reasons.append(f"Price {'at DISCOUNT' if current_price < equilibrium else 'at PREMIUM'} vs 4H EQ {equilibrium:.2f} (now {current_price:.2f})")

        if (closed_above_prev_high or swept_low_closed_above) and bullish_4h:
            return Bias.BULLISH, reasons
        elif (closed_below_prev_low or swept_high_closed_below) and bearish_4h:
            return Bias.BEARISH, reasons
        else:
            if not bullish_4h and not bearish_4h:
                reasons.append("=> NEUTRAL: 4H structure mixed")
            elif bullish_4h:
                reasons.append("=> NEUTRAL: 4H bullish but daily candle not confirming")
            else:
                reasons.append("=> NEUTRAL: 4H bearish but daily candle not confirming")
            return Bias.NEUTRAL, reasons


class FVGDetector:

    @staticmethod
    def scan_for_fvg(df, min_gap_pct=0.001):
        n = len(df)
        if n < 3:
            return []

        # Extract to numpy once — avoids repeated pandas indexing inside a loop
        highs  = df["high"].to_numpy()
        lows   = df["low"].to_numpy()
        opens  = df["open"].to_numpy()
        closes = df["close"].to_numpy()
        vols   = df["volume"].to_numpy()

        # 20-bar rolling average volume (vectorized)
        avg_vol = pd.Series(vols).rolling(20, min_periods=1).mean().to_numpy()

        # Offset views: k ∈ [0, n-3) maps to original i = k+2
        # c1 = row i-2 (k), c2 = row i-1 (k+1), c3 = row i (k+2)
        c1h = highs[:-2];  c1l = lows[:-2]
        c2h = highs[1:-1]; c2l = lows[1:-1]; c2o = opens[1:-1]; c2c = closes[1:-1]
        v2  = vols[1:-1];  avg20 = avg_vol[1:-1]
        c3h = highs[2:];   c3l = lows[2:]

        c2_range = c2h - c2l
        c2_body  = np.abs(c2c - c2o)
        strong   = (c2_range > 0) & (c2_body >= c2_range * 0.60)
        vol_ok   = v2 > avg20 * 0.8

        safe_c2c = np.where(c2c != 0, np.abs(c2c), 1e-9)

        # Bullish FVG: gap between c1.high and c3.low
        bull_gap = c3l - c1h
        bull_mask = (c1h < c3l) & strong & vol_ok & (bull_gap / safe_c2c >= min_gap_pct)

        # Bearish FVG: gap between c1.low and c3.high
        bear_gap = c1l - c3h
        bear_mask = (c1l > c3h) & strong & vol_ok & (bear_gap / safe_c2c >= min_gap_pct)

        tf = _infer_timeframe(df)
        fvgs = []
        for k in np.where(bull_mask)[0]:
            fvgs.append(FairValueGap(
                timestamp=df.index[k + 1],
                top=c3l[k], bottom=c1h[k],
                midpoint=(c3l[k] + c1h[k]) / 2,
                is_bullish=True, timeframe=tf,
            ))
        for k in np.where(bear_mask)[0]:
            fvgs.append(FairValueGap(
                timestamp=df.index[k + 1],
                top=c1l[k], bottom=c3h[k],
                midpoint=(c1l[k] + c3h[k]) / 2,
                is_bullish=False, timeframe=tf,
            ))
        return fvgs

    @staticmethod
    def check_fvg_retest(fvg, candle):
        if fvg.is_filled:
            return False
        if fvg.is_bullish:
            return candle["low"] <= fvg.top and candle["low"] >= fvg.bottom
        else:
            return candle["high"] >= fvg.bottom and candle["high"] <= fvg.top

    @staticmethod
    def update_fvg_status(fvg, candle):
        if fvg.is_bullish:
            if candle["close"] < fvg.bottom:
                fvg.is_filled = True
            elif candle["low"] <= fvg.top and candle["close"] > fvg.top:
                fvg.is_respected = True
        else:
            if candle["close"] > fvg.top:
                fvg.is_filled = True
            elif candle["high"] >= fvg.bottom and candle["close"] < fvg.bottom:
                fvg.is_respected = True


class OrderBlockDetector:

    @staticmethod
    def find_order_blocks(df, lookback=20):
        obs = []
        start_idx = max(0, len(df) - lookback)

        for i in range(start_idx + 1, len(df)):
            current = df.iloc[i]
            c_range = current["high"] - current["low"]
            c_body = abs(current["close"] - current["open"])

            if c_range == 0 or c_body < c_range * 0.60:
                continue

            avg_start = max(0, i - 10)
            avg_range = (df.iloc[avg_start:i]["high"] - df.iloc[avg_start:i]["low"]).mean()

            if c_range < avg_range * 1.3:
                continue

            if current["close"] > current["open"]:
                for j in range(i - 1, max(i - 5, start_idx) - 1, -1):
                    candidate = df.iloc[j]
                    if candidate["close"] < candidate["open"]:
                        obs.append(OrderBlock(
                            timestamp=df.index[j],
                            high=candidate["high"],
                            low=candidate["low"],
                            open_price=candidate["open"],
                            close=candidate["close"],
                            is_bullish=True,
                            timeframe=_infer_timeframe(df),
                        ))
                        break

            elif current["close"] < current["open"]:
                for j in range(i - 1, max(i - 5, start_idx) - 1, -1):
                    candidate = df.iloc[j]
                    if candidate["close"] > candidate["open"]:
                        obs.append(OrderBlock(
                            timestamp=df.index[j],
                            high=candidate["high"],
                            low=candidate["low"],
                            open_price=candidate["open"],
                            close=candidate["close"],
                            is_bullish=False,
                            timeframe=_infer_timeframe(df),
                        ))
                        break

        return obs


class LiquidityMapper:

    @staticmethod
    def map_liquidity(df, swings, session_highs_lows=None):
        levels = []
        tolerance = 0.001

        swing_highs = [s for s in swings if s.is_high]
        for i in range(len(swing_highs)):
            for j in range(i + 1, len(swing_highs)):
                price_diff_pct = abs(swing_highs[i].price - swing_highs[j].price) / swing_highs[i].price
                if price_diff_pct <= tolerance:
                    avg_price = (swing_highs[i].price + swing_highs[j].price) / 2
                    levels.append(LiquidityLevel(price=avg_price, is_buyside=True, source="equal_highs", timestamp=swing_highs[j].timestamp))

        swing_lows = [s for s in swings if not s.is_high]
        for i in range(len(swing_lows)):
            for j in range(i + 1, len(swing_lows)):
                price_diff_pct = abs(swing_lows[i].price - swing_lows[j].price) / swing_lows[i].price
                if price_diff_pct <= tolerance:
                    avg_price = (swing_lows[i].price + swing_lows[j].price) / 2
                    levels.append(LiquidityLevel(price=avg_price, is_buyside=False, source="equal_lows", timestamp=swing_lows[j].timestamp))

        if session_highs_lows:
            for key, price in session_highs_lows.items():
                if price is None:
                    continue
                levels.append(LiquidityLevel(price=price, is_buyside="high" in key, source=key))

        if len(df) > 0:
            daily = df.resample("D").agg({"high": "max", "low": "min"}).dropna()
            if len(daily) >= 2:
                prev_day = daily.iloc[-2]
                levels.append(LiquidityLevel(price=prev_day["high"], is_buyside=True, source="prev_day_high"))
                levels.append(LiquidityLevel(price=prev_day["low"], is_buyside=False, source="prev_day_low"))

        return levels

    @staticmethod
    def check_liquidity_sweep(level, candle):
        if level.swept:
            return False
        if level.is_buyside:
            swept = candle["high"] > level.price and candle["close"] < level.price
        else:
            swept = candle["low"] < level.price and candle["close"] > level.price
        if swept:
            level.swept = True
        return swept


class SilverBulletStrategy:

    def __init__(self, instrument):
        self.instrument = instrument
        self.risk_params = RISK_PARAMS[instrument]

    def scan_for_setup(self, df_1m, df_15m, df_4h, df_daily, current_time, log=None):
        if log is None:
            log = []

        active_window = self._get_active_window(current_time)
        if active_window is None:
            log.append(f"  [SilverBullet] No active window at {current_time.strftime('%H:%M')} ET")
            return None
        log.append(f"  [SilverBullet] Window: {active_window.name} ✓")

        log.append(f"  [SilverBullet] Day: {current_time.strftime('%A')} ✓ (all days active)")

        bias = MarketStructureAnalyzer.get_daily_bias(df_daily, df_4h)
        if bias == Bias.NEUTRAL:
            range_high = df_4h.tail(20)["high"].max()
            range_low = df_4h.tail(20)["low"].min()
            eq = (range_high + range_low) / 2
            current_price = df_4h.iloc[-1]["close"]
            if current_price > eq:
                bias = Bias.BEARISH
                log.append(f"  [SilverBullet] Bias: NEUTRAL → BEARISH (price at premium vs EQ {eq:.2f})")
            else:
                bias = Bias.BULLISH
                log.append(f"  [SilverBullet] Bias: NEUTRAL → BULLISH (price at discount vs EQ {eq:.2f})")
        else:
            log.append(f"  [SilverBullet] Bias: {bias.value} ✓")

        swings_15m = MarketStructureAnalyzer.find_swing_points(df_15m.tail(60), lookback=3)
        liquidity_levels = LiquidityMapper.map_liquidity(df_15m, swings_15m)
        log.append(f"  [SilverBullet] Liquidity: {len(liquidity_levels)} levels (BSL:{sum(1 for l in liquidity_levels if l.is_buyside)} SSL:{sum(1 for l in liquidity_levels if not l.is_buyside)})")

        window_data = self._filter_to_window(df_1m, active_window, current_time)
        if len(window_data) < 5:
            log.append(f"  [SilverBullet] Skip -- only {len(window_data)} bars in window (need 5+)")
            return None

        sweep_found = None
        sweep_candle_idx = None
        for idx in range(len(window_data)):
            candle = window_data.iloc[idx]
            for level in liquidity_levels:
                if LiquidityMapper.check_liquidity_sweep(level, candle):
                    if bias == Bias.BULLISH and not level.is_buyside:
                        sweep_found = level
                        sweep_candle_idx = idx
                    elif bias == Bias.BEARISH and level.is_buyside:
                        sweep_found = level
                        sweep_candle_idx = idx

        if sweep_found is None:
            needed = "sellside" if bias == Bias.BULLISH else "buyside"
            log.append(f"  [SilverBullet] Skip -- no {needed} liquidity swept in window")
            return None
        log.append(f"  [SilverBullet] Sweep: {sweep_found.source} @ {sweep_found.price:.2f} ✓")

        swings_1m = MarketStructureAnalyzer.find_swing_points(df_1m.tail(100), lookback=2)
        mss = None
        post_sweep_data = window_data.iloc[sweep_candle_idx:]
        for idx in range(len(post_sweep_data)):
            abs_idx = len(df_1m) - len(window_data) + sweep_candle_idx + idx
            if abs_idx < len(df_1m):
                mss = MarketStructureAnalyzer.detect_structure_shift(df_1m, swings_1m, abs_idx)
                if mss and mss.direction == bias:
                    break
                else:
                    mss = None

        if mss is None:
            log.append(f"  [SilverBullet] Skip -- no MSS detected after sweep")
            return None
        if not mss.displacement:
            log.append(f"  [SilverBullet] Skip -- MSS @ {mss.price:.2f} found but no displacement candle")
            return None
        log.append(f"  [SilverBullet] MSS: {mss.direction.value} with displacement @ {mss.price:.2f} ✓")

        mss_area_start = max(0, len(df_1m) - 20)
        fvgs = FVGDetector.scan_for_fvg(df_1m.iloc[mss_area_start:])
        valid_fvgs = [f for f in fvgs if f.is_bullish == (bias == Bias.BULLISH) and f.timestamp >= window_data.index[sweep_candle_idx]]

        # Filter out FVGs that have been filled by subsequent price action
        recent_slice = df_1m.iloc[mss_area_start:]
        recent_lows = recent_slice["low"].values
        recent_highs = recent_slice["high"].values
        valid_fvgs = [
            f for f in valid_fvgs
            if (f.is_bullish and recent_lows.min() >= f.bottom)
            or (not f.is_bullish and recent_highs.max() <= f.top)
        ]

        if not valid_fvgs:
            log.append(f"  [SilverBullet] Skip -- no unfilled FVG found after MSS")
            return None
        target_fvg = valid_fvgs[-1]
        log.append(f"  [SilverBullet] FVG: {target_fvg.bottom:.2f}--{target_fvg.top:.2f} ({len(valid_fvgs)} found) ✓")

        current_candle = df_1m.iloc[-1]
        if not FVGDetector.check_fvg_retest(target_fvg, current_candle):
            log.append(f"  [SilverBullet] Skip -- price not retesting FVG (close:{current_candle['close']:.2f} zone:{target_fvg.bottom:.2f}--{target_fvg.top:.2f})")
            return None
        log.append(f"  [SilverBullet] FVG retest confirmed ✓")

        entry_price = target_fvg.midpoint

        if bias == Bias.BULLISH:
            stop_loss = target_fvg.bottom - (target_fvg.top - target_fvg.bottom) * 0.2
            bsl_levels = [l for l in liquidity_levels if l.is_buyside and not l.swept]
            tp_target = min((l.price for l in bsl_levels if l.price > entry_price), default=entry_price + (entry_price - stop_loss) * self.risk_params["target_rr_ratio"])
        else:
            stop_loss = target_fvg.top + (target_fvg.top - target_fvg.bottom) * 0.2
            ssl_levels = [l for l in liquidity_levels if not l.is_buyside and not l.swept]
            tp_target = max((l.price for l in ssl_levels if l.price < entry_price), default=entry_price - (stop_loss - entry_price) * self.risk_params["target_rr_ratio"])

        risk = abs(entry_price - stop_loss)
        reward = abs(tp_target - entry_price)

        if risk == 0 or risk > self.risk_params["max_sl_points"]:
            log.append(f"  [SilverBullet] Skip -- risk {risk:.2f} > max {self.risk_params['max_sl_points']}")
            return None

        rr_ratio = reward / risk
        if rr_ratio < self.risk_params["min_rr_ratio"]:
            log.append(f"  [SilverBullet] Skip -- R:R 1:{rr_ratio:.1f} below min 1:{self.risk_params['min_rr_ratio']}")
            return None

        confluences = [
            f"Daily bias: {bias.value}",
            f"Liquidity swept: {sweep_found.source} at {sweep_found.price:.2f}",
            f"MSS confirmed: {mss.direction.value} with displacement",
            f"FVG retest: {target_fvg.bottom:.2f} to {target_fvg.top:.2f}",
            f"Kill zone: {active_window.name}",
            f"R:R = 1:{rr_ratio:.1f}",
        ]

        confidence = 0.5
        if active_window.name in ("SB NY AM", "NY AM KZ"):
            confidence += 0.15
        elif active_window.name in ("SB London", "London KZ"):
            confidence += 0.10
        elif active_window.name in ("SB NY PM", "NY PM KZ", "SB Midnight"):
            confidence += 0.08
        elif active_window.name in ("Asia", "London Late"):
            confidence += 0.06   # Overnight sessions — lower base but still tradeable
        if mss.displacement:
            confidence += 0.10
        if rr_ratio >= 3.0:
            confidence += 0.10
        if sweep_found.source in ("equal_highs", "equal_lows", "prev_day_high", "prev_day_low"):
            confidence += 0.10
        confidence = min(confidence, 0.95)

        log.append(f"  [SilverBullet] SETUP VALID -- {bias.value.upper()} Entry:{entry_price:.2f} SL:{stop_loss:.2f} TP:{tp_target:.2f} R:R 1:{rr_ratio:.1f} Conf:{confidence*100:.0f}%")

        tp1 = entry_price + (tp_target - entry_price) * 0.5 if bias == Bias.BULLISH else entry_price - (entry_price - tp_target) * 0.5

        return TradeSignal(
            timestamp=current_time,
            instrument=self.instrument,
            setup_type=SetupType.SILVER_BULLET,
            direction=bias,
            entry_price=round(entry_price, 2),
            stop_loss=round(stop_loss, 2),
            take_profit_1=round(tp1, 2),
            take_profit_2=round(tp_target, 2),
            risk_reward=round(rr_ratio, 2),
            confidence=round(confidence, 2),
            session=active_window.name,
            confluences=confluences,
        )

    def _get_active_window(self, current_time):
        ct = current_time.time()
        # Check named Silver Bullet windows first (highest priority/confidence)
        for key in ["silver_bullet_london", "silver_bullet_ny_am", "silver_bullet_ny_pm", "silver_bullet_midnight"]:
            window = SESSIONS[key]
            if window.start <= ct < window.end:
                return window
        # Expand to full kill zones and gap-fills
        for key in ["london_killzone", "london_late", "ny_am_killzone", "ny_pm_killzone"]:
            window = SESSIONS[key]
            if window.start <= ct < window.end:
                return window
        # Asia session — cross-midnight (18:00 ET → 03:00 ET next day), excluding midnight SB above
        asia = SESSIONS["asia"]
        if ct >= asia.start or ct < asia.end:
            return asia
        return None

    def _filter_to_window(self, df, window, current_time):
        today = current_time.date()
        if window.end <= window.start:  # cross-midnight window (e.g. Asia 18:00–03:00)
            yesterday = today - timedelta(days=1)
            mask = (
                ((df.index.date == yesterday) & (df.index.time >= window.start)) |
                ((df.index.date == today) & (df.index.time < window.end))
            )
        else:
            mask = (
                (df.index.date == today) &
                (df.index.time >= window.start) &
                (df.index.time < window.end)
            )
        return df[mask]


class AsianRangeSweepStrategy:

    def __init__(self, instrument=Instrument.GC):
        self.instrument = instrument
        self.risk_params = RISK_PARAMS[instrument]

    def calculate_asian_range(self, df_15m, today):
        # Walk back up to 4 days to find the most recent Asian session with data.
        # Handles Monday (yesterday=Sunday has no prior data) and market holidays.
        asia_data = None
        for days_back in range(1, 5):
            session_day = today - timedelta(days=days_back)
            asia_start = datetime.combine(session_day, time(18, 0)).replace(tzinfo=_ET)
            asia_end = datetime.combine(session_day + timedelta(days=1), time(3, 0)).replace(tzinfo=_ET)
            _slice = df_15m[(df_15m.index >= asia_start) & (df_15m.index < asia_end)]
            if len(_slice) > 0:
                asia_data = _slice
                break

        if asia_data is None or len(asia_data) == 0:
            return None

        asian_high = asia_data["high"].max()
        asian_low = asia_data["low"].min()
        asian_range = asian_high - asian_low

        daily_ranges = []
        for d in range(1, 15):
            day = today - timedelta(days=d)
            day_data = df_15m[df_15m.index.date == day]
            if len(day_data) > 0:
                daily_ranges.append(day_data["high"].max() - day_data["low"].min())
        adr = np.mean(daily_ranges) if daily_ranges else asian_range * 3

        return {
            "asian_high": asian_high,
            "asian_low": asian_low,
            "asian_range": asian_range,
            "is_consolidation": asian_range < adr * 0.5,
            "is_expansion": asian_range > adr * 0.75,
            "adr": adr,
        }

    def calculate_prev_ny_range(self, df_15m, today):
        """Previous NY session high/low (8:30 AM – 4:00 PM ET).
        Used during Asia hours to find PDH/PDL liquidity sweep setups."""
        for days_back in range(1, 5):
            session_day = today - timedelta(days=days_back)
            ny_start = datetime.combine(session_day, time(8, 30)).replace(tzinfo=_ET)
            ny_end   = datetime.combine(session_day, time(16, 0)).replace(tzinfo=_ET)
            _slice = df_15m[(df_15m.index >= ny_start) & (df_15m.index < ny_end)]
            if len(_slice) >= 10:
                h = _slice["high"].max()
                l = _slice["low"].min()
                return {"prev_ny_high": h, "prev_ny_low": l, "prev_ny_range": h - l}
        return None

    def scan_for_setup(self, df_1m, df_5m, df_15m, df_4h, df_daily, current_time, log=None):
        if log is None:
            log = []

        ct = current_time.time()
        in_london = time(3, 0) <= ct < time(8, 0)
        in_ny_am  = time(8, 30) <= ct < time(13, 30)
        in_ny_pm  = time(13, 30) <= ct < time(16, 0)
        in_asia   = ct >= time(18, 0) or ct < time(3, 0)

        if not in_london and not in_ny_am and not in_ny_pm and not in_asia:
            log.append(f"  [AsianSweep] No active session at {current_time.strftime('%H:%M')} ET")
            return None

        if in_asia:
            return self._scan_asia_session(df_1m, df_5m, df_15m, df_4h, df_daily, current_time, log)

        session_name = "London" if in_london else ("NY PM" if in_ny_pm else "NY AM")
        log.append(f"  [AsianSweep] Session: {session_name} ✓")

        log.append(f"  [AsianSweep] Day: {current_time.strftime('%A')} ✓ (all days active)")

        asian = self.calculate_asian_range(df_15m, current_time.date())
        if asian is None:
            log.append(f"  [AsianSweep] Skip -- no Asian session data available")
            return None
        log.append(f"  [AsianSweep] Asian range: {asian['asian_range']:.2f} pts H:{asian['asian_high']:.2f} L:{asian['asian_low']:.2f} | {'Consolidation' if asian['is_consolidation'] else 'Expansion (conf penalty)'}")

        bias = MarketStructureAnalyzer.get_daily_bias(df_daily, df_4h)
        if bias == Bias.NEUTRAL:
            log.append(f"  [AsianSweep] Skip -- daily bias NEUTRAL")
            return None
        log.append(f"  [AsianSweep] Bias: {bias.value} ✓")

        expansion_penalty = asian["is_expansion"]

        london_start = datetime.combine(current_time.date(), time(3, 0)).replace(tzinfo=_ET)
        post_asia_data = df_5m[df_5m.index >= london_start]

        sweep_type = None
        sweep_candle = None

        for idx in range(len(post_asia_data)):
            candle = post_asia_data.iloc[idx]
            if candle["low"] < asian["asian_low"] and candle["close"] > asian["asian_low"] and bias == Bias.BULLISH:
                sweep_type = "asian_low"
                sweep_candle = candle
                break
            if candle["high"] > asian["asian_high"] and candle["close"] < asian["asian_high"] and bias == Bias.BEARISH:
                sweep_type = "asian_high"
                sweep_candle = candle
                break

        if sweep_type is None:
            if bias == Bias.BULLISH:
                log.append(f"  [AsianSweep] Skip -- Asian low ({asian['asian_low']:.2f}) not yet swept")
            else:
                log.append(f"  [AsianSweep] Skip -- Asian high ({asian['asian_high']:.2f}) not yet swept")
            return None
        log.append(f"  [AsianSweep] Sweep: {sweep_type} ✓")

        post_sweep = df_1m[df_1m.index >= sweep_candle.name]
        if len(post_sweep) < 10:
            log.append(f"  [AsianSweep] Skip -- only {len(post_sweep)} bars post-sweep (need 10+)")
            return None

        swings_1m = MarketStructureAnalyzer.find_swing_points(post_sweep, lookback=2)
        mss = None
        for idx in range(5, len(post_sweep)):
            mss = MarketStructureAnalyzer.detect_structure_shift(post_sweep, swings_1m, idx)
            if mss and mss.direction == bias:
                break
            else:
                mss = None

        if mss is None:
            log.append(f"  [AsianSweep] Skip -- no MSS after sweep")
            return None
        log.append(f"  [AsianSweep] MSS: {mss.direction.value} {'with' if mss.displacement else 'without'} displacement ✓")

        mss_area = post_sweep[post_sweep.index >= mss.timestamp]
        fvgs = FVGDetector.scan_for_fvg(mss_area)
        valid_fvgs = [f for f in fvgs if f.is_bullish == (bias == Bias.BULLISH)]

        if not valid_fvgs:
            obs = OrderBlockDetector.find_order_blocks(post_sweep)
            valid_obs = [o for o in obs if o.is_bullish == (bias == Bias.BULLISH)]
            if not valid_obs:
                log.append(f"  [AsianSweep] Skip -- no FVG or OB found after MSS")
                return None
            ob = valid_obs[-1]
            entry_price = (ob.high + ob.low) / 2
            entry_zone_top = ob.high
            entry_zone_bottom = ob.low
            log.append(f"  [AsianSweep] Using OB entry: {ob.low:.2f}--{ob.high:.2f}")
        else:
            target_fvg = valid_fvgs[-1]
            entry_price = target_fvg.midpoint
            entry_zone_top = target_fvg.top
            entry_zone_bottom = target_fvg.bottom
            log.append(f"  [AsianSweep] FVG entry: {target_fvg.bottom:.2f}--{target_fvg.top:.2f} ({len(valid_fvgs)} found) ✓")

        current = df_1m.iloc[-1]
        at_entry = False
        if bias == Bias.BULLISH:
            at_entry = current["low"] <= entry_zone_top and current["close"] > entry_zone_bottom
        else:
            at_entry = current["high"] >= entry_zone_bottom and current["close"] < entry_zone_top

        if not at_entry:
            log.append(f"  [AsianSweep] Skip -- price not at entry zone (close:{current['close']:.2f} zone:{entry_zone_bottom:.2f}--{entry_zone_top:.2f})")
            return None
        log.append(f"  [AsianSweep] Price at entry zone ✓")

        if bias == Bias.BULLISH:
            stop_loss = sweep_candle["low"] - asian["asian_range"] * 0.1 if sweep_type == "asian_low" else entry_zone_bottom - asian["asian_range"] * 0.1
            tp1 = asian["asian_high"]
            tp2 = asian["asian_high"] + asian["asian_range"]
        else:
            stop_loss = sweep_candle["high"] + asian["asian_range"] * 0.1 if sweep_type == "asian_high" else entry_zone_top + asian["asian_range"] * 0.1
            tp1 = asian["asian_low"]
            tp2 = asian["asian_low"] - asian["asian_range"]

        risk = abs(entry_price - stop_loss)
        if risk == 0 or risk > self.risk_params["max_sl_points"]:
            log.append(f"  [AsianSweep] Skip -- risk {risk:.2f} > max {self.risk_params['max_sl_points']}")
            return None

        rr_ratio = abs(tp1 - entry_price) / risk
        if rr_ratio < self.risk_params["min_rr_ratio"]:
            log.append(f"  [AsianSweep] Skip -- R:R 1:{rr_ratio:.1f} below min 1:{self.risk_params['min_rr_ratio']}")
            return None

        confluences = [
            f"Daily bias: {bias.value}",
            f"Asian range: {asian['asian_range']:.2f} ({'consolidation' if asian['is_consolidation'] else 'expansion'})",
            f"Sweep: {sweep_type}",
            f"MSS: {mss.direction.value} {'with' if mss.displacement else 'without'} displacement",
            f"Session: {'London' if in_london else ('NY PM' if in_ny_pm else 'NY AM')}",
            f"R:R = 1:{rr_ratio:.1f}",
        ]

        confidence = 0.55
        if asian["is_consolidation"]:
            confidence += 0.15
        if mss.displacement:
            confidence += 0.10
        if in_ny_am:
            confidence += 0.05
        if expansion_penalty:
            confidence -= 0.15
        if current_time.weekday() in [0, 1]:
            confidence += 0.05
        confidence = max(0.3, min(confidence, 0.95))

        log.append(f"  [AsianSweep] SETUP VALID -- {bias.value.upper()} Entry:{entry_price:.2f} SL:{stop_loss:.2f} TP:{tp2:.2f} R:R 1:{rr_ratio:.1f} Conf:{confidence*100:.0f}%")

        return TradeSignal(
            timestamp=current_time,
            instrument=self.instrument,
            setup_type=SetupType.ASIAN_RANGE_SWEEP,
            direction=bias,
            entry_price=round(entry_price, 2),
            stop_loss=round(stop_loss, 2),
            take_profit_1=round(tp1, 2),
            take_profit_2=round(tp2, 2),
            risk_reward=round(rr_ratio, 2),
            confidence=round(confidence, 2),
            session="London" if in_london else ("NY PM" if in_ny_pm else "NY AM"),
            confluences=confluences,
        )

    def _scan_asia_session(self, df_1m, df_5m, df_15m, df_4h, df_daily, current_time, log):
        """Asia session (18:00–03:00 ET): look for sweeps of the previous NY session's PDH/PDL.
        ICT concept: Asia consolidates after NY close, then raids NY liquidity levels before
        London opens. These setups are lower confidence but valid for data collection."""
        log.append(f"  [AsianSweep] Session: Asia (PDH/PDL sweep mode) ✓")

        prev_ny = self.calculate_prev_ny_range(df_15m, current_time.date())
        if prev_ny is None:
            log.append(f"  [AsianSweep] Skip -- no previous NY session data found")
            return None
        log.append(f"  [AsianSweep] Prev NY range: H:{prev_ny['prev_ny_high']:.2f} L:{prev_ny['prev_ny_low']:.2f}")

        bias = MarketStructureAnalyzer.get_daily_bias(df_daily, df_4h)
        if bias == Bias.NEUTRAL:
            log.append(f"  [AsianSweep] Skip -- daily bias NEUTRAL")
            return None
        log.append(f"  [AsianSweep] Bias: {bias.value} ✓")

        # Find sweep of PDH (bearish) or PDL (bullish) in the current Asia session
        asia_start_today = datetime.combine(current_time.date(), time(18, 0)).replace(tzinfo=_ET)
        # Handle cross-midnight: if it's before 03:00, the session started yesterday evening
        if current_time.time() < time(3, 0):
            asia_start_today = datetime.combine(
                current_time.date() - timedelta(days=1), time(18, 0)
            ).replace(tzinfo=_ET)
        asia_data = df_5m[df_5m.index >= asia_start_today]

        if len(asia_data) < 5:
            log.append(f"  [AsianSweep] Skip -- not enough Asia session bars yet ({len(asia_data)})")
            return None

        sweep_type = None
        sweep_candle = None
        for idx in range(len(asia_data)):
            candle = asia_data.iloc[idx]
            # Bullish: sweep PDL (wick below, close above) → expect bounce
            if candle["low"] < prev_ny["prev_ny_low"] and candle["close"] > prev_ny["prev_ny_low"] and bias == Bias.BULLISH:
                sweep_type = "prev_ny_low"
                sweep_candle = candle
                break
            # Bearish: sweep PDH (wick above, close below) → expect drop
            if candle["high"] > prev_ny["prev_ny_high"] and candle["close"] < prev_ny["prev_ny_high"] and bias == Bias.BEARISH:
                sweep_type = "prev_ny_high"
                sweep_candle = candle
                break

        if sweep_type is None:
            target = f"PDL ({prev_ny['prev_ny_low']:.2f})" if bias == Bias.BULLISH else f"PDH ({prev_ny['prev_ny_high']:.2f})"
            log.append(f"  [AsianSweep] Skip -- {target} not yet swept in Asia session")
            return None
        log.append(f"  [AsianSweep] Sweep: {sweep_type} ✓")

        # Require MSS after sweep
        post_sweep = df_1m[df_1m.index >= sweep_candle.name]
        if len(post_sweep) < 8:
            log.append(f"  [AsianSweep] Skip -- only {len(post_sweep)} bars post-sweep (need 8+)")
            return None

        swings_1m = MarketStructureAnalyzer.find_swing_points(post_sweep, lookback=2)
        mss = None
        for idx in range(5, len(post_sweep)):
            mss = MarketStructureAnalyzer.detect_structure_shift(post_sweep, swings_1m, idx)
            if mss and mss.direction == bias:
                break
            else:
                mss = None

        if mss is None:
            log.append(f"  [AsianSweep] Skip -- no MSS after Asia sweep")
            return None
        log.append(f"  [AsianSweep] MSS: {mss.direction.value} ✓")

        # FVG entry zone after MSS
        mss_area = post_sweep[post_sweep.index >= mss.timestamp]
        fvgs = FVGDetector.scan_for_fvg(mss_area)
        valid_fvgs = [f for f in fvgs if f.is_bullish == (bias == Bias.BULLISH)]

        if not valid_fvgs:
            log.append(f"  [AsianSweep] Skip -- no FVG found after MSS in Asia session")
            return None
        target_fvg = valid_fvgs[-1]
        entry_price = target_fvg.midpoint
        entry_zone_top = target_fvg.top
        entry_zone_bottom = target_fvg.bottom
        log.append(f"  [AsianSweep] FVG entry: {entry_zone_bottom:.2f}--{entry_zone_top:.2f} ✓")

        current = df_1m.iloc[-1]
        if bias == Bias.BULLISH:
            at_entry = current["low"] <= entry_zone_top and current["close"] > entry_zone_bottom
        else:
            at_entry = current["high"] >= entry_zone_bottom and current["close"] < entry_zone_top
        if not at_entry:
            log.append(f"  [AsianSweep] Skip -- price not at FVG (close:{current['close']:.2f} zone:{entry_zone_bottom:.2f}--{entry_zone_top:.2f})")
            return None
        log.append(f"  [AsianSweep] Price at entry zone ✓")

        ny_range = prev_ny["prev_ny_range"]
        if bias == Bias.BULLISH:
            stop_loss = sweep_candle["low"] - ny_range * 0.05
            tp1 = prev_ny["prev_ny_high"]
            tp2 = prev_ny["prev_ny_high"] + ny_range * 0.5
        else:
            stop_loss = sweep_candle["high"] + ny_range * 0.05
            tp1 = prev_ny["prev_ny_low"]
            tp2 = prev_ny["prev_ny_low"] - ny_range * 0.5

        risk = abs(entry_price - stop_loss)
        if risk == 0 or risk > self.risk_params["max_sl_points"]:
            log.append(f"  [AsianSweep] Skip -- risk {risk:.2f} > max {self.risk_params['max_sl_points']}")
            return None

        rr_ratio = abs(tp1 - entry_price) / risk
        if rr_ratio < self.risk_params["min_rr_ratio"]:
            log.append(f"  [AsianSweep] Skip -- R:R 1:{rr_ratio:.1f} below min")
            return None

        confidence = 0.50
        if mss.displacement:
            confidence += 0.08
        if rr_ratio >= 3.0:
            confidence += 0.08
        confidence = min(confidence, 0.80)  # Asia setups capped lower — less liquid session

        confluences = [
            f"Daily bias: {bias.value}",
            f"Asia sweep: {sweep_type}",
            f"MSS: {mss.direction.value}",
            f"FVG entry: {entry_zone_bottom:.2f}–{entry_zone_top:.2f}",
            f"Session: Asia",
            f"R:R = 1:{rr_ratio:.1f}",
        ]
        log.append(f"  [AsianSweep] ASIA SETUP VALID -- {bias.value.upper()} Entry:{entry_price:.2f} SL:{stop_loss:.2f} TP:{tp2:.2f} Conf:{confidence*100:.0f}%")

        return TradeSignal(
            timestamp=current_time,
            instrument=self.instrument,
            setup_type=SetupType.ASIAN_RANGE_SWEEP,
            direction=bias,
            entry_price=round(entry_price, 2),
            stop_loss=round(stop_loss, 2),
            take_profit_1=round(tp1, 2),
            take_profit_2=round(tp2, 2),
            risk_reward=round(rr_ratio, 2),
            confidence=round(confidence, 2),
            session="Asia",
            confluences=confluences,
        )


class ORBStrategy:

    def __init__(self, instrument=Instrument.NQ):
        self.instrument = instrument
        self.risk_params = RISK_PARAMS[instrument]
        self.orb_high = None
        self.orb_low = None
        self.orb_range = None
        self.trade_taken_today = False

    def calculate_opening_range(self, df_5m, today):
        orb_start = datetime.combine(today, time(9, 30)).replace(tzinfo=_ET)
        orb_end = datetime.combine(today, time(9, 45)).replace(tzinfo=_ET)
        orb_data = df_5m[(df_5m.index >= orb_start) & (df_5m.index < orb_end)]

        if len(orb_data) < 2:
            return None

        self.orb_high = orb_data["high"].max()
        self.orb_low = orb_data["low"].min()
        self.orb_range = self.orb_high - self.orb_low

        daily_ranges = []
        for d in range(1, 15):
            day = today - timedelta(days=d)
            day_data = df_5m[df_5m.index.date == day]
            if len(day_data) > 0:
                daily_ranges.append(day_data["high"].max() - day_data["low"].min())
        adr = np.mean(daily_ranges) if daily_ranges else self.orb_range * 4
        orb_pct_of_adr = (self.orb_range / adr * 100) if adr > 0 else 50

        return {
            "orb_high": self.orb_high,
            "orb_low": self.orb_low,
            "orb_range": self.orb_range,
            "adr": adr,
            "orb_pct_of_adr": orb_pct_of_adr,
            "is_narrow": orb_pct_of_adr < 30,
            "is_wide": orb_pct_of_adr > 60,
        }

    def scan_for_setup(self, df_5m, current_time):
        ct = current_time.time()
        if not (time(9, 45) <= ct < time(14, 45)):
            return None
        if self.trade_taken_today:
            return None

        # Calculate ORB once and cache; reuse cached values if already set
        orb_info = self.calculate_opening_range(df_5m, current_time.date())
        if orb_info is None:
            return None

        current_candle = df_5m.iloc[-1]

        if current_candle["close"] > self.orb_high:
            direction = Bias.BULLISH
            entry_price = current_candle["close"]
            stop_loss = self.orb_low
            raw_sl = entry_price - stop_loss
            if raw_sl > self.risk_params["max_sl_points"]:
                stop_loss = entry_price - self.risk_params["max_sl_points"]
            target = entry_price + (self.orb_range * 2.0)

        elif current_candle["close"] < self.orb_low:
            direction = Bias.BEARISH
            entry_price = current_candle["close"]
            stop_loss = self.orb_high
            raw_sl = stop_loss - entry_price
            if raw_sl > self.risk_params["max_sl_points"]:
                stop_loss = entry_price + self.risk_params["max_sl_points"]
            target = entry_price - (self.orb_range * 2.0)

        else:
            return None

        risk = abs(entry_price - stop_loss)
        if risk == 0:
            return None
        reward = abs(target - entry_price)
        rr_ratio = reward / risk
        if rr_ratio < self.risk_params["min_rr_ratio"]:
            return None

        confidence = 0.65
        confluences = [
            f"ORB range: {self.orb_range:.2f} points",
            f"Breakout: {direction.value}",
            f"Day: {current_time.strftime('%A')}",
            f"R:R = 1:{rr_ratio:.1f}",
        ]
        if orb_info["is_narrow"]:
            confidence += 0.10
            confluences.append("Narrow ORB")
        if current_time.weekday() == 0:
            confidence += 0.05
        confidence = min(confidence, 0.85)
        return TradeSignal(
            timestamp=current_time,
            instrument=self.instrument,
            setup_type=SetupType.ORB,
            direction=direction,
            entry_price=round(entry_price, 2),
            stop_loss=round(stop_loss, 2),
            take_profit_1=round(target, 2),
            take_profit_2=round(target, 2),
            risk_reward=round(rr_ratio, 2),
            confidence=round(confidence, 2),
            session="ORB",
            confluences=confluences,
            notes="Exit at 4:00 PM ET if target/SL not hit. No re-entry.",
        )

    def reset_daily(self):
        self.orb_high = None
        self.orb_low = None
        self.orb_range = None
        self.trade_taken_today = False


# ---------------------------------------------------------------------------
# DATA CACHE — prevents yfinance rate limiting
# Without this: 10 calls/scan × 4 scans/min = 40 calls/min = IP ban within hours
# With this: re-fetches only when the bar interval has elapsed (~2 calls/min)
# "4h" key stores the df_4h derived from df_1h so resample() isn't repeated every scan.
# ---------------------------------------------------------------------------
_DATA_CACHE: dict = {}  # (instrument_value, interval) -> (fetch_timestamp, dataframe)
_CACHE_TTL = {"1m": 58, "5m": 295, "15m": 890, "1h": 3590, "4h": 3590, "1d": 14300}

# ---------------------------------------------------------------------------
# WEIGHTS CACHE — weights.json is read every scan (every 15s) which is wasteful.
# Cache in memory with a 5-minute TTL — weights only update after closed trades.
# ---------------------------------------------------------------------------
_WEIGHTS_CACHE: dict = {"data": {}, "loaded_at": 0.0}
_WEIGHTS_CACHE_TTL = 300  # 5 minutes


def fetch_data_yfinance(instrument, interval="1m", period="5d"):
    """Cached yfinance fetch — only re-fetches when the bar interval has elapsed."""
    key = (instrument.value, interval)
    ttl = _CACHE_TTL.get(interval, 58)
    cached = _DATA_CACHE.get(key)
    if cached is not None:
        cached_at, df = cached
        age = _time_module.time() - cached_at
        if age < ttl:
            return df
    # Cache miss or TTL expired — fetch fresh
    try:
        import yfinance as yf
    except ImportError:
        raise ImportError("Install yfinance: pip install yfinance")
    # Wrap in BaseException (not just Exception) — yfinance's curl_cffi backend can raise
    # MemoryError inside a C callback which bypasses normal try/except Exception blocks.
    # On any error: return stale cache if available, otherwise re-raise so caller skips scan.
    try:
        ticker = yf.Ticker(instrument.value)
        df = ticker.history(interval=interval, period=period)
        df.columns = [c.lower() for c in df.columns]
        cols_to_keep = ["open", "high", "low", "close", "volume"]
        df = df[[c for c in cols_to_keep if c in df.columns]]
        if df.index.tz is not None:
            df.index = df.index.tz_convert("America/New_York")
        _DATA_CACHE[key] = (_time_module.time(), df)
    except BaseException as e:
        print(f"[FETCH ERROR] {instrument.value} {interval}: {type(e).__name__}: {e}")
        if cached is not None:
            print(f"[FETCH ERROR] Returning stale cache ({interval}) to avoid crash")
            return cached[1]
        raise
    return df


class TradingBot:

    def __init__(self, instrument):
        self.instrument = instrument
        self.silver_bullet = SilverBulletStrategy(instrument)
        self.orb = ORBStrategy(instrument)
        self.asian_sweep = AsianRangeSweepStrategy(instrument)
        self.daily_trades = 0
        self.daily_pnl = 0.0
        self.signals_today = []
        self.last_scan_log = []

    def run_scan(self):
        scan_log = []
        now = datetime.now(_ET)
        scan_log.append(f"--- SCAN {now.strftime('%H:%M:%S ET')} ---")

        risk = RISK_PARAMS[self.instrument]
        if self.daily_trades >= risk["max_trades_per_day"]:
            scan_log.append(f"SKIP: Daily trade limit reached ({self.daily_trades}/{risk['max_trades_per_day']})")
            self.last_scan_log = scan_log
            return []
        if self.daily_pnl <= -risk["max_daily_loss"]:
            scan_log.append(f"SKIP: Daily loss limit hit (${self.daily_pnl:.2f})")
            self.last_scan_log = scan_log
            return []

        scan_log.append(f"Trades today: {self.daily_trades}/{risk['max_trades_per_day']} | Daily P&L: ${self.daily_pnl:.2f}")

        try:
            df_1m = fetch_data_yfinance(self.instrument, "1m", "5d")
            df_5m = fetch_data_yfinance(self.instrument, "5m", "1mo")
            df_15m = fetch_data_yfinance(self.instrument, "15m", "1mo")
            df_1h = fetch_data_yfinance(self.instrument, "1h", "6mo")
            df_daily = fetch_data_yfinance(self.instrument, "1d", "1y")
        except Exception as e:
            scan_log.append(f"SKIP: Market data fetch failed -- {e}")
            print(f"[{now.strftime('%H:%M:%S')}] Data fetch error: {e}")
            self.last_scan_log = scan_log
            return []

        if len(df_1m) < 10 or len(df_15m) < 10:
            scan_log.append(f"SKIP: Insufficient market data (1m:{len(df_1m)} bars, 15m:{len(df_15m)} bars)")
            print(f"[{now.strftime('%H:%M:%S')}] Insufficient market data, skipping scan.")
            self.last_scan_log = scan_log
            return []

        # Data freshness check — skip if 1m feed is stale (yfinance outage or feed issue)
        # Threshold 60min: catches real outages without false-positives outside market hours
        try:
            last_ts = df_1m.index[-1]
            if hasattr(last_ts, "to_pydatetime"):
                last_ts = last_ts.to_pydatetime()
            if last_ts.tzinfo is not None:
                bar_age_min = (now - last_ts).total_seconds() / 60
            else:
                bar_age_min = (now.replace(tzinfo=None) - last_ts).total_seconds() / 60
            if bar_age_min > 60:
                scan_log.append(f"SKIP: Stale 1m data — last bar {bar_age_min:.0f}m ago (yfinance feed issue?)")
                print(f"[{now.strftime('%H:%M:%S')}] Stale 1m data for {self.instrument.name} — skipping scan")
                self.last_scan_log = scan_log
                return []
        except Exception:
            pass  # Don't block scan on timestamp comparison failures

        _4h_key = (self.instrument.value, "4h")
        _4h_cached = _DATA_CACHE.get(_4h_key)
        if _4h_cached and (_time_module.time() - _4h_cached[0]) < _CACHE_TTL["4h"]:
            df_4h = _4h_cached[1]
        else:
            df_4h = df_1h.resample("4h").agg({
                "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
            }).dropna()
            _DATA_CACHE[_4h_key] = (_time_module.time(), df_4h)

        scan_log.append(f"Data: 1m={len(df_1m)} 5m={len(df_5m)} 15m={len(df_15m)} 4h={len(df_4h)} D={len(df_daily)}")

        # Session
        ct = now.time()
        active_windows = []
        if time(3, 0) <= ct < time(4, 0): active_windows.append("SB London + London KZ")
        elif time(4, 0) <= ct < time(5, 0): active_windows.append("London KZ")
        if time(5, 0) <= ct < time(8, 30): active_windows.append("London Late")
        if time(8, 30) <= ct < time(13, 30): active_windows.append("NY AM KZ")
        if time(10, 0) <= ct < time(11, 0): active_windows.append("SB NY AM")
        if time(13, 30) <= ct < time(16, 0): active_windows.append("NY PM KZ")
        if time(14, 0) <= ct < time(15, 0): active_windows.append("SB NY PM")
        if ct >= time(18, 0) or ct < time(3, 0): active_windows.append("Asia")
        session_str = " | ".join(active_windows) if active_windows else "Post-Market (16:00-18:00)"
        scan_log.append(f"Session: {session_str} | Day: {now.strftime('%A')}")
        scan_log.append(f"Price: {df_1m.iloc[-1]['close']:.2f}")

        # Daily bias with full reasoning
        bias, bias_reasons = MarketStructureAnalyzer.get_daily_bias_verbose(df_daily, df_4h)
        scan_log.append(f"Daily Bias: {bias.value.upper()}")
        for r in bias_reasons:
            scan_log.append(f"  {r}")

        # Liquidity map
        swings_15m = MarketStructureAnalyzer.find_swing_points(df_15m.tail(60), lookback=3)
        liq_levels = LiquidityMapper.map_liquidity(df_15m, swings_15m)
        bsl = [l for l in liq_levels if l.is_buyside]
        ssl = [l for l in liq_levels if not l.is_buyside]
        scan_log.append(f"Liquidity: {len(liq_levels)} levels | BSL:{len(bsl)} SSL:{len(ssl)}")
        for lv in sorted(liq_levels, key=lambda x: x.price, reverse=True)[:6]:
            scan_log.append(f"  {'BSL' if lv.is_buyside else 'SSL'} @ {lv.price:.2f} [{lv.source}]")

        # News sentiment — OpenBB (15-min cached, silent no-op if not installed)
        _news_score = 0.0
        try:
            from openbb_provider import get_news_sentiment
            _news_score, _news_headlines = get_news_sentiment(self.instrument.name)
            _sentiment_label = (
                "BULLISH" if _news_score > 0.2 else
                "BEARISH" if _news_score < -0.2 else
                "neutral"
            )
            _preview = (_news_headlines[0][:65] + "...") if _news_headlines else "no recent headlines"
            scan_log.append(f"News Sentiment: {_sentiment_label} ({_news_score:+.2f})")
            scan_log.append(f"  > {_preview}")
        except Exception:
            scan_log.append("News Sentiment: unavailable (install openbb for this feature)")

        # Strategy checks
        scan_log.append("Strategies:")
        signals = []

        sb_signal = self.silver_bullet.scan_for_setup(df_1m, df_15m, df_4h, df_daily, now, log=scan_log)
        if sb_signal:
            signals.append(sb_signal)

        # Asian Range Sweep — metals and FX both range well overnight
        _ars_instruments = (Instrument.GC, Instrument.MGC, Instrument.M6E, Instrument.M6B)
        if self.instrument in _ars_instruments:
            ars_signal = self.asian_sweep.scan_for_setup(df_1m, df_5m, df_15m, df_4h, df_daily, now, log=scan_log)
            if ars_signal:
                signals.append(ars_signal)

        # Opening Range Breakout — equity indexes and crude oil
        _orb_instruments = (Instrument.NQ, Instrument.MNQ, Instrument.MES, Instrument.M2K, Instrument.MYM, Instrument.MCL)
        if self.instrument in _orb_instruments:
            orb_signal = self.orb.scan_for_setup(df_5m, now)
            if orb_signal:
                signals.append(orb_signal)

        # Apply news sentiment boost/penalty to signal confidence
        # +5% when sentiment aligns with trade direction, -5% when it opposes
        if signals and _news_score != 0.0:
            for sig in signals:
                aligned = (
                    (sig.direction == Bias.BULLISH and _news_score > 0.2) or
                    (sig.direction == Bias.BEARISH and _news_score < -0.2)
                )
                opposed = (
                    (sig.direction == Bias.BULLISH and _news_score < -0.3) or
                    (sig.direction == Bias.BEARISH and _news_score > 0.3)
                )
                if aligned:
                    sig.confidence = min(1.0, sig.confidence + 0.05)
                    sig.confluences.append(f"News sentiment aligned ({_news_score:+.2f})")
                    scan_log.append(f"  News: +5% confidence (sentiment aligned — {sig.setup_type.value})")
                elif opposed:
                    sig.confidence = max(0.0, sig.confidence - 0.05)
                    scan_log.append(f"  News: -5% confidence (sentiment opposes — {sig.setup_type.value})")

        # Apply learned weights from journal
        signals = self._apply_weights(signals, scan_log)

        if signals:
            best = max(signals, key=lambda s: s.confidence)
            scan_log.append(f"RESULT: {len(signals)} setup(s) -- best: {best.setup_type.value} {best.direction.value.upper()} @ {best.entry_price:.2f} conf:{best.confidence*100:.0f}%")
        else:
            scan_log.append(f"RESULT: No setup -- all strategies rejected")

        self.last_scan_log = scan_log
        signals.sort(key=lambda s: s.confidence, reverse=True)
        return signals

    def _apply_weights(self, signals, scan_log):
        """Multiply confidence by learned weight for each signal's pattern.
        Weights are loaded from disk at most once every 5 minutes — the file
        only changes after closed trades, so per-scan reads are wasteful."""
        global _WEIGHTS_CACHE
        now_t = _time_module.time()
        if now_t - _WEIGHTS_CACHE["loaded_at"] > _WEIGHTS_CACHE_TTL:
            weights_file = os.path.join(
                os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__))),
                "weights.json",
            )
            try:
                with open(weights_file) as f:
                    _WEIGHTS_CACHE["data"] = json.load(f)
                _WEIGHTS_CACHE["loaded_at"] = now_t
            except Exception:
                pass  # Keep stale cache rather than crash
        weights = _WEIGHTS_CACHE["data"]
        if not weights:
            return signals

        adjusted = []
        for sig in signals:
            key = f"{sig.setup_type.value}|{sig.session}|{sig.direction.value}"
            if key in weights:
                w = weights[key]["weight"]
                if w == 0.0:
                    scan_log.append(f"  [LEARNER] BLOCKED pattern: {key} — 0% historical win rate")
                    continue  # skip this signal entirely
                new_conf = round(min(sig.confidence * w, 0.99), 3)
                if w != 1.0:
                    scan_log.append(f"  [LEARNER] {key} weight:{w}x conf:{sig.confidence*100:.0f}%->{ new_conf*100:.0f}%")
                sig = sig.__class__(
                    **{**sig.__dict__, "confidence": new_conf}
                )
            adjusted.append(sig)
        return adjusted

    def format_signal(self, signal):
        direction = "LONG" if signal.direction == Bias.BULLISH else "SHORT"
        lines = [
            f"\n{'='*60}",
            f"  TRADE SIGNAL: {direction} {signal.instrument.name}",
            f"  Setup: {signal.setup_type.value}",
            f"  Time: {signal.timestamp.strftime('%Y-%m-%d %H:%M:%S')} ET",
            f"  Session: {signal.session}",
            f"{'='*60}",
            f"  Entry:    {signal.entry_price}",
            f"  Stop:     {signal.stop_loss}",
            f"  TP1:      {signal.take_profit_1}",
            f"  TP2:      {signal.take_profit_2}",
            f"  R:R:      1:{signal.risk_reward}",
            f"  Confidence: {signal.confidence * 100:.0f}%",
            f"  {'_'*58}",
            f"  Confluences:",
        ]
        for c in signal.confluences:
            lines.append(f"    + {c}")
        if signal.notes:
            lines.append(f"  Notes: {signal.notes}")
        lines.append(f"{'='*60}\n")
        return "\n".join(lines)

    def reset_daily(self):
        self.daily_trades = 0
        self.daily_pnl = 0.0
        self.signals_today = []
        self.orb.reset_daily()


# ---------------------------------------------------------------------------
# MULTI-INSTRUMENT LAUNCHER — runs GC, NQ, MGC, MNQ simultaneously
# ---------------------------------------------------------------------------
import threading

_SIGNAL_COOLDOWN_MINUTES = 30  # Don't re-alert same setup+direction within this window


def _run_bot_loop(bot: TradingBot, scan_interval_seconds: int = 60):
    """Runs one instrument's scan loop in its own thread."""
    import time as _time
    name = bot.instrument.name
    print(f"[{name}] Scanner started — all days enabled, scanning every {scan_interval_seconds}s")
    last_reset_date = None
    # Tracks last fire time per (setup_type, direction) to prevent duplicate alerts
    last_fired: dict = {}

    while True:
        now = datetime.now(_ET)

        # Daily reset at midnight ET
        if last_reset_date != now.date():
            bot.reset_daily()
            last_reset_date = now.date()
            last_fired.clear()
            print(f"[{name}] Daily counters reset for {now.strftime('%A %Y-%m-%d')}")

        signals = bot.run_scan()

        for line in bot.last_scan_log:
            print(f"[{name}] {line}")

        for sig in signals:
            if sig.confidence < 0.70:
                continue
            dedup_key = (sig.setup_type, sig.direction)
            last_time = last_fired.get(dedup_key)
            if last_time and (now - last_time).total_seconds() < _SIGNAL_COOLDOWN_MINUTES * 60:
                print(f"[{name}] Signal suppressed (cooldown — fired {int((now - last_time).total_seconds() / 60)}m ago): {sig.setup_type.value} {sig.direction.value}")
                continue
            last_fired[dedup_key] = now
            print(bot.format_signal(sig))

        _time.sleep(scan_interval_seconds)


def run_all_instruments(scan_interval_seconds: int = 60):
    """Launch all 4 instruments simultaneously in parallel threads."""
    instruments = [
        Instrument.MNQ, Instrument.MES, Instrument.M2K, Instrument.MYM,  # equity micros
        Instrument.MGC,                                                    # metals micro
        Instrument.MCL,                                                    # energy micro
        Instrument.M6E, Instrument.M6B,                                   # FX micros
    ]
    bots = {inst: TradingBot(inst) for inst in instruments}

    threads = []
    for inst, bot in bots.items():
        t = threading.Thread(
            target=_run_bot_loop,
            args=(bot, scan_interval_seconds),
            name=f"Kael-{inst.name}",
            daemon=True,
        )
        threads.append(t)
        t.start()

    print(f"\nKael ICT — all 4 instruments scanning simultaneously (GC | NQ | MGC | MNQ)")
    print(f"Every day is a trading day. Scan interval: {scan_interval_seconds}s\n")

    import time as _time
    try:
        while all(t.is_alive() for t in threads):
            _time.sleep(1)
    except KeyboardInterrupt:
        print("\nKael ICT scanner stopped.")


if __name__ == "__main__":
    run_all_instruments(scan_interval_seconds=60)
