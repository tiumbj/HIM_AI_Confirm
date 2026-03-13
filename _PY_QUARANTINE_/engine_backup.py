# engine.py
# =============================================================================
# Hybrid Intelligence Mentor (HIM) - Engine
# Version: 2.12.4
# Changelog:
# - Added context.metrics visibility layer for:
#   - Volatility expansion (BB width / ATR) score, threshold, margin
#   - Supertrend conflict diagnostics + distance-to-supertrend (points, ATR)
#   - BOS break distance, retest distance, proximity score/threshold/margin
#   - RR visibility with rr_reason when not computed
# Evidence note:
# - Required to diagnose blocker distribution on M1 where samples showed:
#   - no_vol_expansion frequently and supertrend_conflict often.
# - Metrics must reflect actual values used by gates (score/threshold/margin).
# =============================================================================

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, List

import numpy as np

try:
    import MetaTrader5 as mt5
except Exception:  # pragma: no cover
    mt5 = None


# -----------------------------
# Utilities
# -----------------------------
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def _now_ts() -> float:
    return time.time()


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _sma(arr: np.ndarray, period: int) -> np.ndarray:
    if period <= 0:
        raise ValueError("SMA period must be > 0")
    if len(arr) < period:
        return np.full_like(arr, np.nan, dtype=float)
    c = np.cumsum(arr, dtype=float)
    c[period:] = c[period:] - c[:-period]
    out = c / period
    out[: period - 1] = np.nan
    return out


def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    if period <= 0:
        raise ValueError("EMA period must be > 0")
    out = np.full_like(arr, np.nan, dtype=float)
    if len(arr) < period:
        return out
    alpha = 2.0 / (period + 1.0)
    out[period - 1] = np.nanmean(arr[:period])
    for i in range(period, len(arr)):
        prev = out[i - 1]
        out[i] = alpha * arr[i] + (1 - alpha) * prev
    return out


def _true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    return tr


def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    tr = _true_range(high, low, close)
    return _ema(tr, period)


def bollinger_width(close: np.ndarray, period: int, stddev: float) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    ma = _sma(close, period)
    out_std = np.full_like(close, np.nan, dtype=float)
    if len(close) >= period:
        for i in range(period - 1, len(close)):
            window = close[i - period + 1 : i + 1]
            out_std[i] = np.nanstd(window, ddof=0)
    upper = ma + stddev * out_std
    lower = ma - stddev * out_std
    width = upper - lower
    return ma, upper, lower, width


def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Classic Wilder ADX implementation (simplified, vectorized-ish)
    if len(close) < period + 2:
        n = len(close)
        nan = np.full(n, np.nan, dtype=float)
        return nan, nan, nan

    up_move = high - np.roll(high, 1)
    down_move = np.roll(low, 1) - low
    up_move[0] = 0.0
    down_move[0] = 0.0

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr = _true_range(high, low, close)

    # Wilder smoothing via EMA with alpha = 1/period (approx)
    def _wilder_smooth(x: np.ndarray, p: int) -> np.ndarray:
        out = np.full_like(x, np.nan, dtype=float)
        if len(x) < p:
            return out
        out[p - 1] = np.nansum(x[:p])
        for i in range(p, len(x)):
            out[i] = out[i - 1] - (out[i - 1] / p) + x[i]
        return out

    tr_s = _wilder_smooth(tr, period)
    plus_s = _wilder_smooth(plus_dm, period)
    minus_s = _wilder_smooth(minus_dm, period)

    plus_di = 100.0 * (plus_s / np.where(tr_s == 0, np.nan, tr_s))
    minus_di = 100.0 * (minus_s / np.where(tr_s == 0, np.nan, tr_s))
    dx = 100.0 * np.abs(plus_di - minus_di) / np.where((plus_di + minus_di) == 0, np.nan, (plus_di + minus_di))

    adx_out = np.full_like(dx, np.nan, dtype=float)
    # ADX is Wilder smoothing of DX
    if len(dx) >= 2 * period:
        adx_out[2 * period - 1] = np.nanmean(dx[period : 2 * period])
        for i in range(2 * period, len(dx)):
            adx_out[i] = ((adx_out[i - 1] * (period - 1)) + dx[i]) / period

    return adx_out, plus_di, minus_di


def supertrend(high: np.ndarray, low: np.ndarray, close: np.ndarray, atr_period: int, multiplier: float) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns:
      st_dir: +1 bullish, -1 bearish (nan where not available)
      st_value: supertrend line value (nan where not available)
    """
    n = len(close)
    st_dir = np.full(n, np.nan, dtype=float)
    st = np.full(n, np.nan, dtype=float)

    a = atr(high, low, close, atr_period)
    hl2 = (high + low) / 2.0
    upperband = hl2 + multiplier * a
    lowerband = hl2 - multiplier * a

    final_upper = np.full(n, np.nan, dtype=float)
    final_lower = np.full(n, np.nan, dtype=float)

    for i in range(n):
        if i == 0:
            final_upper[i] = upperband[i]
            final_lower[i] = lowerband[i]
            continue
        # Final upper
        if np.isnan(final_upper[i - 1]) or np.isnan(upperband[i]):
            final_upper[i] = upperband[i]
        else:
            final_upper[i] = upperband[i] if (upperband[i] < final_upper[i - 1] or close[i - 1] > final_upper[i - 1]) else final_upper[i - 1]
        # Final lower
        if np.isnan(final_lower[i - 1]) or np.isnan(lowerband[i]):
            final_lower[i] = lowerband[i]
        else:
            final_lower[i] = lowerband[i] if (lowerband[i] > final_lower[i - 1] or close[i - 1] < final_lower[i - 1]) else final_lower[i - 1]

    for i in range(n):
        if i == 0:
            continue
        if np.isnan(a[i]) or np.isnan(final_upper[i]) or np.isnan(final_lower[i]):
            continue

        if np.isnan(st[i - 1]):
            # initialize
            if close[i] <= final_upper[i]:
                st[i] = final_upper[i]
                st_dir[i] = -1.0
            else:
                st[i] = final_lower[i]
                st_dir[i] = 1.0
            continue

        # trend switch logic
        if st[i - 1] == final_upper[i - 1]:
            if close[i] <= final_upper[i]:
                st[i] = final_upper[i]
                st_dir[i] = -1.0
            else:
                st[i] = final_lower[i]
                st_dir[i] = 1.0
        elif st[i - 1] == final_lower[i - 1]:
            if close[i] >= final_lower[i]:
                st[i] = final_lower[i]
                st_dir[i] = 1.0
            else:
                st[i] = final_upper[i]
                st_dir[i] = -1.0
        else:
            # fallback
            st[i] = st[i - 1]
            st_dir[i] = st_dir[i - 1]

    return st_dir, st


# -----------------------------
# Config
# -----------------------------
@dataclass
class EngineConfig:
    symbol: str = "GOLD"
    htf: str = "H1"
    mtf: str = "M15"
    ltf: str = "M5"

    # core indicator params
    atr_period: int = 14
    atr_sl_mult: float = 2.0

    st_atr_period: int = 10
    st_mult: float = 3.0

    bb_period: int = 20
    bb_stddev: float = 2.0
    bb_width_atr_min: float = 1.0  # volatility expansion threshold (tunable)

    adx_period: int = 14

    min_rr: float = 1.5

    # market structure
    bos_lookback: int = 20
    bos_break_atr_min: float = 0.05  # distance beyond ref high/low, normalized by ATR

    # retest / proximity
    retest_max_bars: int = 15
    retest_distance_atr_max: float = 0.60
    proximity_threshold: float = 0.55  # score threshold

    # event context (for metrics only)
    event_timeframe: Optional[str] = None


def _mt5_tf(tf: str) -> int:
    # minimal mapping
    mp = {
        "M1": mt5.TIMEFRAME_M1,
        "M2": mt5.TIMEFRAME_M2,
        "M3": mt5.TIMEFRAME_M3,
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M15,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
    }
    return mp.get(tf, mt5.TIMEFRAME_M1)


def _rates_to_np(rates) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    # rates is numpy structured array from mt5.copy_rates_from_pos
    time_arr = rates["time"].astype(np.int64)
    open_ = rates["open"].astype(float)
    high = rates["high"].astype(float)
    low = rates["low"].astype(float)
    close = rates["close"].astype(float)
    return time_arr, open_, high, low, close


# -----------------------------
# TradingEngine
# -----------------------------
class TradingEngine:
    engine_version = "2.12.4"

    def __init__(self, config_path: str = "config.json") -> None:
        self.config_path = config_path
        self.raw_cfg = _load_json(config_path)
        self.cfg = self._parse_cfg(self.raw_cfg)

        self._mt5_ready = False
        self._init_mt5()

    def _parse_cfg(self, raw: Dict[str, Any]) -> EngineConfig:
        symbol = raw.get("symbol") or raw.get("Symbol") or "GOLD"
        tfs = raw.get("timeframes") or {}
        commissioning = raw.get("commissioning") or {}
        event_tfs = commissioning.get("event_timeframes")
        event_tf = None
        if isinstance(event_tfs, list) and len(event_tfs) == 1:
            event_tf = str(event_tfs[0])

        ec = EngineConfig(
            symbol=str(symbol),
            htf=str(tfs.get("htf", "H1")),
            mtf=str(tfs.get("mtf", "M15")),
            ltf=str(tfs.get("ltf", "M5")),
            atr_period=int(raw.get("atr_period", raw.get("strategy", {}).get("atr_period", 14))),
            atr_sl_mult=float(raw.get("atr_sl_mult", raw.get("strategy", {}).get("atr_sl_mult", 2.0))),
            st_atr_period=int(raw.get("st_atr_period", raw.get("strategy", {}).get("st_atr_period", 10))),
            st_mult=float(raw.get("st_mult", raw.get("strategy", {}).get("st_mult", 3.0))),
            bb_period=int(raw.get("bb_period", raw.get("strategy", {}).get("bb_period", 20))),
            bb_stddev=float(raw.get("bb_stddev", raw.get("strategy", {}).get("bb_stddev", 2.0))),
            bb_width_atr_min=float(raw.get("bb_width_atr_min", raw.get("strategy", {}).get("bb_width_atr_min", 1.0))),
            adx_period=int(raw.get("adx_period", raw.get("strategy", {}).get("adx_period", 14))),
            min_rr=float(raw.get("min_rr", raw.get("strategy", {}).get("min_rr", 1.5))),
            bos_lookback=int(raw.get("bos_lookback", raw.get("strategy", {}).get("bos_lookback", 20))),
            bos_break_atr_min=float(raw.get("bos_break_atr_min", raw.get("strategy", {}).get("bos_break_atr_min", 0.05))),
            retest_max_bars=int(raw.get("retest_max_bars", raw.get("strategy", {}).get("retest_max_bars", 15))),
            retest_distance_atr_max=float(raw.get("retest_distance_atr_max", raw.get("strategy", {}).get("retest_distance_atr_max", 0.60))),
            proximity_threshold=float(raw.get("proximity_threshold", raw.get("strategy", {}).get("proximity_threshold", 0.55))),
            event_timeframe=event_tf,
        )
        return ec

    def _init_mt5(self) -> None:
        if mt5 is None:
            self._mt5_ready = False
            return
        if mt5.initialize():
            self._mt5_ready = True
            return
        self._mt5_ready = False

    def _get_rates(self, tf: str, bars: int = 300):
        if not self._mt5_ready:
            raise RuntimeError("MT5 not initialized")
        timeframe = _mt5_tf(tf)
        rates = mt5.copy_rates_from_pos(self.cfg.symbol, timeframe, 0, bars)
        if rates is None or len(rates) < 60:
            raise RuntimeError(f"Not enough rates for {self.cfg.symbol} {tf}. got={0 if rates is None else len(rates)}")
        return rates

    def _trend_label(self, close: np.ndarray) -> str:
        # simple trend label by SMA slope
        if len(close) < 60:
            return "ranging"
        s1 = _sma(close, 20)
        s2 = _sma(close, 50)
        i = len(close) - 1
        if np.isnan(s1[i]) or np.isnan(s2[i]):
            return "ranging"
        if s1[i] > s2[i] and s1[i] > s1[i - 5]:
            return "bullish"
        if s1[i] < s2[i] and s1[i] < s1[i - 5]:
            return "bearish"
        return "ranging"

    def _bias_from_htf(self, htf_trend: str) -> str:
        if htf_trend == "bullish":
            return "BUY"
        if htf_trend == "bearish":
            return "SELL"
        return "NONE"

    def _bos_reference(self, high: np.ndarray, low: np.ndarray, lookback: int) -> Tuple[Optional[float], Optional[float]]:
        if len(high) < lookback + 2:
            return None, None
        ref_high = float(np.nanmax(high[-(lookback + 1) : -1]))
        ref_low = float(np.nanmin(low[-(lookback + 1) : -1]))
        return ref_high, ref_low

    def _bos_break_ok(
        self,
        direction: str,
        close_last: float,
        ref_high: Optional[float],
        ref_low: Optional[float],
        atr_last: Optional[float],
        bos_break_atr_min: float,
    ) -> Tuple[Optional[bool], Optional[float], Optional[float]]:
        # returns (ok, dist_points, dist_atr)
        if direction not in ("BUY", "SELL"):
            return None, None, None
        if atr_last is None or atr_last <= 0:
            return None, None, None
        if ref_high is None or ref_low is None:
            return None, None, None

        if direction == "BUY":
            dist = close_last - ref_high
        else:
            dist = ref_low - close_last

        dist_atr = dist / atr_last
        ok = dist_atr >= bos_break_atr_min
        return bool(ok), float(dist), float(dist_atr)

    def _retest_ok(
        self,
        direction: str,
        close: np.ndarray,
        ref_high: Optional[float],
        ref_low: Optional[float],
        atr_last: Optional[float],
        max_bars: int,
        dist_atr_max: float,
    ) -> Tuple[Optional[bool], Optional[float], Optional[float], Optional[float]]:
        # very simple retest definition: within last N bars, price came back near ref level within dist_atr_max
        # returns (ok, dist_points, dist_atr, level_price)
        if direction not in ("BUY", "SELL"):
            return None, None, None, None
        if atr_last is None or atr_last <= 0:
            return None, None, None, None
        if ref_high is None or ref_low is None:
            return None, None, None, None
        if len(close) < max_bars + 2:
            return None, None, None, None

        level = ref_high if direction == "BUY" else ref_low
        window = close[-(max_bars + 1) : -1]
        # distance to level (absolute)
        d = np.abs(window - level)
        min_d = float(np.nanmin(d))
        dist_atr = min_d / atr_last
        ok = dist_atr <= dist_atr_max
        return bool(ok), float(min_d), float(dist_atr), float(level)

    def _proximity_score(
        self,
        retest_dist_atr: Optional[float],
        threshold: float,
    ) -> Tuple[Optional[bool], Optional[float], Optional[float], Optional[float]]:
        # Convert retest distance to a score in [0,1] where closer = higher
        # score = max(0, 1 - dist_atr) (simple)
        if retest_dist_atr is None:
            return None, None, None, None
        score = max(0.0, 1.0 - float(retest_dist_atr))
        ok = score >= threshold
        margin = score - threshold
        return bool(ok), float(score), float(threshold), float(margin)

    def _rr_compute(
        self,
        direction: str,
        entry: Optional[float],
        sl: Optional[float],
        tp: Optional[float],
        min_rr: float,
    ) -> Tuple[Optional[float], Optional[bool], str]:
        if direction not in ("BUY", "SELL"):
            return None, None, "no_direction"
        if entry is None or sl is None or tp is None:
            return None, None, "missing_entry_or_sl_or_tp"
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk <= 0:
            return None, None, "risk_zero"
        rr = reward / risk
        ok = rr >= min_rr
        return float(rr), bool(ok), "ok"

    def _vol_expansion_ok(
        self,
        bb_width_points_last: Optional[float],
        atr_last: Optional[float],
        bb_width_atr_min: float,
    ) -> Tuple[Optional[bool], Optional[float], Optional[float], Optional[float], str]:
        # score = bb_width_atr
        if bb_width_points_last is None:
            return None, None, None, None, "bb_width_missing"
        if atr_last is None or atr_last <= 0:
            return None, None, None, None, "atr_missing_or_zero"
        score = float(bb_width_points_last / atr_last)
        threshold = float(bb_width_atr_min)
        margin = score - threshold
        ok = score >= threshold
        return bool(ok), float(score), float(threshold), float(margin), "ok"

    def generate_signal_package(self) -> Dict[str, Any]:
        """
        Main entrypoint.
        Returns dict:
          symbol, direction, entry_candidate, stop_candidate, tp_candidate, rr, confidence_py, score,
          bos, supertrend_ok, context{blocked_by, HTF_trend, MTF_trend, LTF_trend, atr, gates, metrics, ...}
        """
        ts = _now_ts()
        pkg: Dict[str, Any] = {
            "symbol": self.cfg.symbol,
            "direction": "NONE",
            "entry_candidate": None,
            "stop_candidate": None,
            "tp_candidate": None,
            "rr": None,
            "score": None,
            "confidence_py": None,
            "bos": None,
            "supertrend_ok": None,
            "context": {},
        }

        context: Dict[str, Any] = {
            "engine_version": self.engine_version,
            "timestamp": ts,
            "blocked_by": "",
            "HTF_trend": None,
            "MTF_trend": None,
            "LTF_trend": None,
            "bias_source": "HTF",
            "direction_bias": "NONE",
            "atr": None,
            "gates": {},
            "metrics": {},
        }

        blocked: List[str] = []

        try:
            rates_ltf = self._get_rates(self.cfg.ltf, bars=400)
            rates_htf = self._get_rates(self.cfg.htf, bars=250)
            rates_mtf = self._get_rates(self.cfg.mtf, bars=300)

            t_l, o_l, h_l, lo_l, c_l = _rates_to_np(rates_ltf)
            t_h, o_h, h_h, lo_h, c_h = _rates_to_np(rates_htf)
            t_m, o_m, h_m, lo_m, c_m = _rates_to_np(rates_mtf)

            htf_trend = self._trend_label(c_h)
            mtf_trend = self._trend_label(c_m)
            ltf_trend = self._trend_label(c_l)
            context["HTF_trend"] = htf_trend
            context["MTF_trend"] = mtf_trend
            context["LTF_trend"] = ltf_trend

            direction_bias = self._bias_from_htf(htf_trend)
            context["direction_bias"] = direction_bias
            pkg["direction"] = direction_bias  # “intent direction” even if later blocked

            # ATR on LTF
            atr_arr = atr(h_l, lo_l, c_l, self.cfg.atr_period)
            atr_last = _safe_float(atr_arr[-1])
            context["atr"] = atr_last

            # Supertrend on LTF
            st_dir_arr, st_val_arr = supertrend(h_l, lo_l, c_l, self.cfg.st_atr_period, self.cfg.st_mult)
            st_dir_last = _safe_float(st_dir_arr[-1])
            st_val_last = _safe_float(st_val_arr[-1])

            # Bollinger width on LTF
            bb_ma, bb_up, bb_lo, bb_w = bollinger_width(c_l, self.cfg.bb_period, self.cfg.bb_stddev)
            bb_w_last = _safe_float(bb_w[-1])

            # ADX (optional metric visibility)
            adx_arr, plus_di, minus_di = adx(h_l, lo_l, c_l, self.cfg.adx_period)
            adx_last = _safe_float(adx_arr[-1])
            plus_di_last = _safe_float(plus_di[-1])
            minus_di_last = _safe_float(minus_di[-1])

            # BOS refs on LTF
            ref_high, ref_low = self._bos_reference(h_l, lo_l, self.cfg.bos_lookback)
            context["bos_ref_high"] = ref_high
            context["bos_ref_low"] = ref_low

            close_last = float(c_l[-1])

            # Gates: bias_ok (always true if BUY/SELL, but keep as explicit)
            bias_ok = direction_bias in ("BUY", "SELL")
            context["gates"]["bias_ok"] = bias_ok
            if not bias_ok:
                blocked.append("no_clear_bias")

            # Supertrend conflict: define supertrend direction sign
            # +1 bullish, -1 bearish. conflict if BUY with -1 or SELL with +1.
            supertrend_conflict = None
            supertrend_ok = None
            if st_dir_last is None:
                supertrend_ok = None
                supertrend_conflict = None
            else:
                st_sign = 1 if st_dir_last > 0 else -1
                if direction_bias == "BUY":
                    supertrend_conflict = (st_sign < 0)
                elif direction_bias == "SELL":
                    supertrend_conflict = (st_sign > 0)
                else:
                    supertrend_conflict = None
                supertrend_ok = (False if supertrend_conflict is True else True) if supertrend_conflict is not None else None
            context["gates"]["supertrend_ok"] = supertrend_ok
            if supertrend_conflict:
                blocked.append("supertrend_conflict")

            # Vol expansion gate via bb_width_atr
            vol_ok, vol_score, vol_thr, vol_margin, vol_reason = self._vol_expansion_ok(
                bb_w_last, atr_last, self.cfg.bb_width_atr_min
            )
            context["gates"]["vol_expansion_ok"] = vol_ok
            if vol_ok is False:
                blocked.append("no_vol_expansion")

            # BOS ok (structure availability): here we consider refs exist
            bos_ok = (ref_high is not None and ref_low is not None)
            context["gates"]["bos_ok"] = bos_ok

            # BOS break gate (separate from bos_ok)
            bos_break_ok, bos_break_dist_pts, bos_break_dist_atr = self._bos_break_ok(
                direction_bias, close_last, ref_high, ref_low, atr_last, self.cfg.bos_break_atr_min
            )
            context["gates"]["bos_break_ok"] = bos_break_ok
            if bos_break_ok is False:
                blocked.append("no_bos_break")

            # Retest
            ret_ok, ret_dist_pts, ret_dist_atr, ret_level = self._retest_ok(
                direction_bias, c_l, ref_high, ref_low, atr_last, self.cfg.retest_max_bars, self.cfg.retest_distance_atr_max
            )
            context["gates"]["retest_ok"] = ret_ok
            if ret_ok is False:
                blocked.append("no_retest")

            # Proximity (depends on retest distance)
            prox_ok, prox_score, prox_thr, prox_margin = self._proximity_score(ret_dist_atr, self.cfg.proximity_threshold)
            context["gates"]["proximity_ok"] = prox_ok
            if prox_ok is False:
                blocked.append("low_proximity_score")

            # Candidates (simple baseline)
            entry = close_last
            sl = None
            tp = None
            if atr_last is not None and direction_bias in ("BUY", "SELL"):
                if direction_bias == "BUY":
                    sl = entry - self.cfg.atr_sl_mult * atr_last
                    tp = entry + (self.cfg.atr_sl_mult * atr_last * self.cfg.min_rr)
                else:
                    sl = entry + self.cfg.atr_sl_mult * atr_last
                    tp = entry - (self.cfg.atr_sl_mult * atr_last * self.cfg.min_rr)

            rr, rr_ok, rr_reason = self._rr_compute(direction_bias, entry, sl, tp, self.cfg.min_rr)
            context["gates"]["rr_ok"] = rr_ok
            if rr_ok is False:
                blocked.append("rr_too_low")

            # Decide: only when all critical gates pass
            critical = [
                context["gates"].get("bias_ok") is True,
                context["gates"].get("supertrend_ok") is not False,  # allow None as unknown
                context["gates"].get("vol_expansion_ok") is True,
                context["gates"].get("bos_break_ok") is True,
                context["gates"].get("retest_ok") is True,
                context["gates"].get("proximity_ok") is True,
            ]
            all_ok = all(critical)

            if all_ok and rr_ok is not False:
                # tradable intent
                pkg["entry_candidate"] = entry
                pkg["stop_candidate"] = sl
                pkg["tp_candidate"] = tp
                pkg["rr"] = rr
                pkg["score"] = 1.0
                pkg["confidence_py"] = 0.70  # placeholder baseline
            else:
                pkg["entry_candidate"] = entry
                pkg["stop_candidate"] = sl
                pkg["tp_candidate"] = tp
                pkg["rr"] = rr
                pkg["score"] = 0.0
                pkg["confidence_py"] = 0.0

            pkg["supertrend_ok"] = context["gates"].get("supertrend_ok")
            pkg["bos"] = bool(bos_break_ok) if bos_break_ok is not None else None

            # -------------------------
            # Metrics visibility layer
            # -------------------------
            metrics = context["metrics"]
            metrics["event_timeframe"] = self.cfg.event_timeframe  # "M1" expected from config
            metrics["price_last"] = close_last

            # ATR
            metrics["atr"] = atr_last
            metrics["atr_period"] = self.cfg.atr_period

            # BB
            metrics["bb_period"] = self.cfg.bb_period
            metrics["bb_stddev"] = self.cfg.bb_stddev
            metrics["bb_width_points"] = bb_w_last
            metrics["bb_width_atr"] = (bb_w_last / atr_last) if (bb_w_last is not None and atr_last not in (None, 0.0)) else None
            metrics["bb_width_atr_min"] = self.cfg.bb_width_atr_min

            # Vol expansion
            metrics["vol_expansion_score"] = vol_score
            metrics["vol_expansion_threshold"] = vol_thr
            metrics["vol_expansion_margin"] = vol_margin
            metrics["vol_expansion_reason"] = vol_reason

            # ADX (telemetry)
            metrics["adx_period"] = self.cfg.adx_period
            metrics["adx_value"] = adx_last
            metrics["plus_di"] = plus_di_last
            metrics["minus_di"] = minus_di_last

            # Supertrend
            metrics["supertrend_dir"] = st_dir_last
            metrics["supertrend_value"] = st_val_last
            metrics["direction_bias"] = direction_bias
            metrics["supertrend_conflict"] = supertrend_conflict
            if st_val_last is not None:
                dist_pts = abs(close_last - st_val_last)
                metrics["supertrend_distance_points"] = dist_pts
                metrics["supertrend_distance_atr"] = (dist_pts / atr_last) if (atr_last not in (None, 0.0)) else None
            else:
                metrics["supertrend_distance_points"] = None
                metrics["supertrend_distance_atr"] = None

            # BOS / Break
            metrics["bos_ref_high"] = ref_high
            metrics["bos_ref_low"] = ref_low
            metrics["bos_break_distance_points"] = bos_break_dist_pts
            metrics["bos_break_distance_atr"] = bos_break_dist_atr
            metrics["bos_break_atr_min"] = self.cfg.bos_break_atr_min

            # Retest / Proximity
            metrics["retest_level_price"] = ret_level
            metrics["retest_distance_points"] = ret_dist_pts
            metrics["retest_distance_atr"] = ret_dist_atr
            metrics["retest_distance_atr_max"] = self.cfg.retest_distance_atr_max
            metrics["retest_max_bars"] = self.cfg.retest_max_bars

            metrics["proximity_score"] = prox_score
            metrics["proximity_threshold"] = prox_thr
            metrics["proximity_margin"] = prox_margin

            # RR
            metrics["rr"] = rr
            metrics["min_rr"] = self.cfg.min_rr
            metrics["rr_reason"] = rr_reason

            # Block reasons
            # keep as comma-separated string for backward compatibility; executor will normalize
            context["blocked_by"] = ",".join(blocked)

        except Exception as e:
            context["blocked_by"] = "engine_exception"
            context["error"] = f"{type(e).__name__}: {e}"

        pkg["context"] = context
        return pkg