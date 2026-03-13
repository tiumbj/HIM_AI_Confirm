"""
Hybrid Intelligence Mentor (HIM) Trading Engine
Version: 2.12.3

Changelog:
- 2.12.3 (2026-03-01):
  - ADD: Audit fields for breakout gates
      - retest_checked, retest_bypass_reason
      - proximity_checked, proximity_bypass_reason
      - gates: structured pass/fail summary (BOS/Retest/Proximity/VolExp/Supertrend/RR)
  - ADD: Commissioning counters (in-memory runtime)
      - counts for breakout evals, signals, bypass events, blocked_by reasons
  - ADD: Optional JSONL logging (commissioning.enabled + commissioning.log_path)
  - KEEP: Dynamic Retest Bypass using MTF ADX (breakout only) from v2.12.2
  - KEEP: Adaptive Proximity Bypass using MTF ADX from v2.12.0+
  - KEEP: Config load diagnostics (fail-closed but observable)

Notes:
- AI confirm-only (outside this module)
- Validator/risk-guard remain external layers
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import numpy as np
import MetaTrader5 as mt5

from config_resolver import resolve_effective_config


ENGINE_VERSION = "2.12.3"


class TradingEngine:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.last_config_error: Optional[str] = None
        self.config_path_used: Optional[str] = None
        self.cfg = self.load_config()

        # Commissioning counters (runtime, in-memory)
        self.commissioning: Dict[str, Any] = {
            "engine_version": ENGINE_VERSION,
            "since_ts": time.time(),
            "counts": {
                "eval_total": 0,
                "breakout_eval": 0,
                "sideway_eval": 0,
                "signals_buy": 0,
                "signals_sell": 0,
                "signals_none": 0,
                "retest_bypass": 0,
                "proximity_bypass": 0,
            },
            "blocked_by": {},  # reason -> count
        }

    # -----------------------------
    # Safe helpers
    # -----------------------------
    @staticmethod
    def safe_float(v: Any, default: float = 0.0) -> float:
        if v is None:
            return float(default)
        try:
            return float(v)
        except Exception:
            return float(default)

    @staticmethod
    def safe_int(v: Any, default: int = 0) -> int:
        if v is None:
            return int(default)
        try:
            return int(v)
        except Exception:
            return int(default)

    @staticmethod
    def clamp(x: float, lo: float, hi: float) -> float:
        return float(max(lo, min(hi, x)))

    # -----------------------------
    # Config load + diagnostics
    # -----------------------------
    def _resolve_config_path(self) -> str:
        if os.path.isabs(self.config_path):
            return self.config_path
        return os.path.abspath(self.config_path)

    def load_config(self) -> Dict[str, Any]:
        """
        Fail-closed (returns {} on error) BUT keeps diagnostics in:
        - self.last_config_error
        - self.config_path_used
        """
        self.last_config_error = None
        self.config_path_used = self._resolve_config_path()

        try:
            with open(self.config_path_used, "r", encoding="utf-8") as f:
                raw = json.load(f)
            effective = resolve_effective_config(raw)
            if not isinstance(effective, dict):
                self.last_config_error = "effective_config_not_dict"
                return {}
            return effective
        except FileNotFoundError:
            self.last_config_error = f"config_not_found: {self.config_path_used}"
            return {}
        except json.JSONDecodeError as e:
            self.last_config_error = f"config_json_decode_error: {e}"
            return {}
        except Exception as e:
            self.last_config_error = f"config_load_error: {type(e).__name__}: {e}"
            return {}

    def reload_config(self) -> Dict[str, Any]:
        self.cfg = self.load_config()
        return self.cfg

    # -----------------------------
    # MT5
    # -----------------------------
    @staticmethod
    def ensure_mt5() -> None:
        if mt5.initialize():
            return
        time.sleep(0.2)
        if not mt5.initialize():
            raise RuntimeError("MT5 initialize failed")

    @staticmethod
    def tf(tf_str: str) -> int:
        t = (tf_str or "").strip().upper()
        mapping = {
            "M1": mt5.TIMEFRAME_M1,
            "M2": mt5.TIMEFRAME_M2,
            "M3": mt5.TIMEFRAME_M3,
            "M4": mt5.TIMEFRAME_M4,
            "M5": mt5.TIMEFRAME_M5,
            "M6": mt5.TIMEFRAME_M6,
            "M10": mt5.TIMEFRAME_M10,
            "M12": mt5.TIMEFRAME_M12,
            "M15": mt5.TIMEFRAME_M15,
            "M20": mt5.TIMEFRAME_M20,
            "M30": mt5.TIMEFRAME_M30,
            "H1": mt5.TIMEFRAME_H1,
            "H2": mt5.TIMEFRAME_H2,
            "H3": mt5.TIMEFRAME_H3,
            "H4": mt5.TIMEFRAME_H4,
            "H6": mt5.TIMEFRAME_H6,
            "H8": mt5.TIMEFRAME_H8,
            "H12": mt5.TIMEFRAME_H12,
            "D1": mt5.TIMEFRAME_D1,
            "W1": mt5.TIMEFRAME_W1,
            "MN1": mt5.TIMEFRAME_MN1,
        }
        if t not in mapping:
            raise ValueError(f"unknown timeframe: {tf_str}")
        return mapping[t]

    def get_data(self, symbol: str, timeframe: int, n: int) -> Dict[str, np.ndarray]:
        TradingEngine.ensure_mt5()
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, int(n))
        if rates is None or len(rates) < 50:
            raise RuntimeError(f"no rates (symbol={symbol}, timeframe={timeframe}, n={n})")

        return {
            "time": np.array([r["time"] for r in rates], dtype=np.int64),
            "open": np.array([r["open"] for r in rates], dtype=float),
            "high": np.array([r["high"] for r in rates], dtype=float),
            "low": np.array([r["low"] for r in rates], dtype=float),
            "close": np.array([r["close"] for r in rates], dtype=float),
            "volume": np.array([r["tick_volume"] for r in rates], dtype=float),
        }

    # -----------------------------
    # Indicators
    # -----------------------------
    @staticmethod
    def atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
        high = np.asarray(high, dtype=float)
        low = np.asarray(low, dtype=float)
        close = np.asarray(close, dtype=float)

        tr = np.zeros_like(close, dtype=float)
        tr[0] = high[0] - low[0]
        for i in range(1, len(close)):
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))

        out = np.full_like(close, np.nan, dtype=float)
        if len(close) < period + 1:
            return out

        out[period] = np.nanmean(tr[1 : period + 1])
        for i in range(period + 1, len(close)):
            out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
        return out

    @staticmethod
    def rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
        close = np.asarray(close, dtype=float)
        delta = np.diff(close, prepend=close[0])

        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)

        out = np.full_like(close, np.nan, dtype=float)
        if len(close) < period + 1:
            return out

        avg_gain = np.nanmean(gain[1 : period + 1])
        avg_loss = np.nanmean(loss[1 : period + 1])

        out[period] = 100.0 if avg_loss == 0 else (100.0 - (100.0 / (1.0 + (avg_gain / avg_loss))))
        for i in range(period + 1, len(close)):
            avg_gain = (avg_gain * (period - 1) + gain[i]) / period
            avg_loss = (avg_loss * (period - 1) + loss[i]) / period
            out[i] = 100.0 if avg_loss == 0 else (100.0 - (100.0 / (1.0 + (avg_gain / avg_loss))))
        return out

    @staticmethod
    def bollinger(close: np.ndarray, period: int = 20, std_mult: float = 2.0) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        close = np.asarray(close, dtype=float)
        upper = np.full_like(close, np.nan, dtype=float)
        mid = np.full_like(close, np.nan, dtype=float)
        lower = np.full_like(close, np.nan, dtype=float)

        if len(close) < period:
            return upper, mid, lower

        for i in range(period - 1, len(close)):
            window = close[i - period + 1 : i + 1]
            m = float(np.nanmean(window))
            s = float(np.nanstd(window))
            mid[i] = m
            upper[i] = m + std_mult * s
            lower[i] = m - std_mult * s

        return upper, mid, lower

    @staticmethod
    def adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
        high = np.asarray(high, dtype=float)
        low = np.asarray(low, dtype=float)
        close = np.asarray(close, dtype=float)

        out = np.full_like(close, np.nan, dtype=float)
        if len(close) < period + 2:
            return out

        up_move = high[1:] - high[:-1]
        down_move = low[:-1] - low[1:]

        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        tr = np.zeros(len(close) - 1, dtype=float)
        for i in range(1, len(close)):
            tr[i - 1] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))

        atr = np.full_like(tr, np.nan, dtype=float)
        atr[period - 1] = np.nanmean(tr[:period])
        for i in range(period, len(tr)):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

        plus_dm_sm = np.full_like(plus_dm, np.nan, dtype=float)
        minus_dm_sm = np.full_like(minus_dm, np.nan, dtype=float)
        plus_dm_sm[period - 1] = np.nanmean(plus_dm[:period])
        minus_dm_sm[period - 1] = np.nanmean(minus_dm[:period])
        for i in range(period, len(plus_dm)):
            plus_dm_sm[i] = (plus_dm_sm[i - 1] * (period - 1) + plus_dm[i]) / period
            minus_dm_sm[i] = (minus_dm_sm[i - 1] * (period - 1) + minus_dm[i]) / period

        plus_di = np.full_like(tr, np.nan, dtype=float)
        minus_di = np.full_like(tr, np.nan, dtype=float)
        for i in range(period - 1, len(tr)):
            if atr[i] and atr[i] > 0:
                plus_di[i] = 100.0 * (plus_dm_sm[i] / atr[i])
                minus_di[i] = 100.0 * (minus_dm_sm[i] / atr[i])

        dx = np.full_like(tr, np.nan, dtype=float)
        for i in range(period - 1, len(tr)):
            p = plus_di[i]
            m = minus_di[i]
            denom = p + m
            if denom and denom > 0:
                dx[i] = 100.0 * (abs(p - m) / denom)

        adx_tr = np.full_like(tr, np.nan, dtype=float)
        start = (period - 1) + (period - 1)
        if start < len(tr):
            adx_tr[start] = np.nanmean(dx[period - 1 : start + 1])
            for i in range(start + 1, len(tr)):
                adx_tr[i] = (adx_tr[i - 1] * (period - 1) + dx[i]) / period

        out[1:] = adx_tr
        return out

    @staticmethod
    def supertrend(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 10, mult: float = 3.0) -> Tuple[np.ndarray, np.ndarray]:
        high = np.asarray(high, dtype=float)
        low = np.asarray(low, dtype=float)
        close = np.asarray(close, dtype=float)

        atr = TradingEngine.atr(high, low, close, period)
        hl2 = (high + low) / 2.0

        upperband = hl2 + mult * atr
        lowerband = hl2 - mult * atr

        st = np.full_like(close, np.nan, dtype=float)
        direction = np.full_like(close, 1, dtype=int)  # 1 bullish, -1 bearish

        for i in range(1, len(close)):
            if not np.isfinite(atr[i]):
                continue

            if np.isfinite(st[i - 1]):
                upperband[i] = min(upperband[i], upperband[i - 1]) if close[i - 1] <= upperband[i - 1] else upperband[i]
                lowerband[i] = max(lowerband[i], lowerband[i - 1]) if close[i - 1] >= lowerband[i - 1] else lowerband[i]

            if np.isfinite(st[i - 1]):
                if st[i - 1] == upperband[i - 1]:
                    if close[i] <= upperband[i]:
                        st[i] = upperband[i]
                        direction[i] = -1
                    else:
                        st[i] = lowerband[i]
                        direction[i] = 1
                else:
                    if close[i] >= lowerband[i]:
                        st[i] = lowerband[i]
                        direction[i] = 1
                    else:
                        st[i] = upperband[i]
                        direction[i] = -1
            else:
                st[i] = lowerband[i] if close[i] >= hl2[i] else upperband[i]
                direction[i] = 1 if close[i] >= hl2[i] else -1

        dir01 = np.where(direction == 1, 1, 0).astype(int)
        return st, dir01

    # -----------------------------
    # Structure
    # -----------------------------
    @staticmethod
    def structure(data: Dict[str, Any], sensitivity: int = 3) -> Tuple[str, float, float]:
        high = np.asarray(data["high"], dtype=float)
        low = np.asarray(data["low"], dtype=float)
        close = np.asarray(data["close"], dtype=float)

        if len(close) < (sensitivity * 2 + 5):
            return "ranging", float("nan"), float("nan")

        piv_hi = []
        piv_lo = []
        for i in range(sensitivity, len(close) - sensitivity):
            window_hi = high[i - sensitivity : i + sensitivity + 1]
            window_lo = low[i - sensitivity : i + sensitivity + 1]
            if high[i] == np.nanmax(window_hi):
                piv_hi.append((i, float(high[i])))
            if low[i] == np.nanmin(window_lo):
                piv_lo.append((i, float(low[i])))

        bos_hi = piv_hi[-1][1] if len(piv_hi) >= 1 else float("nan")
        bos_lo = piv_lo[-1][1] if len(piv_lo) >= 1 else float("nan")

        trend = "ranging"
        if len(piv_hi) >= 2 and len(piv_lo) >= 2:
            last_hi = piv_hi[-1][1]
            prev_hi = piv_hi[-2][1]
            last_lo = piv_lo[-1][1]
            prev_lo = piv_lo[-2][1]
            if last_hi > prev_hi and last_lo > prev_lo:
                trend = "bullish"
            elif last_hi < prev_hi and last_lo < prev_lo:
                trend = "bearish"
            else:
                trend = "ranging"

        return trend, float(bos_hi), float(bos_lo)

    # -----------------------------
    # Mode knobs
    # -----------------------------
    def _get_sideway_knobs(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        c = cfg.get("sideway_scalp", {}) or {}
        adx_period = self.safe_int(c.get("adx_period", 14), 14)
        bb_period = self.safe_int(c.get("bb_period", 20), 20)
        rsi_period = self.safe_int(c.get("rsi_period", 14), 14)

        return {
            "enabled": bool(c.get("enabled", True)),
            "adx_period": max(adx_period, 2),
            "adx_max": max(self.safe_float(c.get("adx_max", 22.0), 22.0), 0.0),
            "bb_period": max(bb_period, 5),
            "bb_std": max(self.safe_float(c.get("bb_std", 2.0), 2.0), 0.1),
            "bb_width_atr_max": max(self.safe_float(c.get("bb_width_atr_max", 6.0), 6.0), 0.1),
            "rsi_period": max(rsi_period, 2),
            "rsi_overbought": self.clamp(self.safe_float(c.get("rsi_overbought", 70.0), 70.0), 50.0, 95.0),
            "rsi_oversold": self.clamp(self.safe_float(c.get("rsi_oversold", 30.0), 30.0), 5.0, 50.0),
            "require_confirmation": bool(c.get("require_confirmation", True)),
            "touch_buffer_atr": self.clamp(self.safe_float(c.get("touch_buffer_atr", 0.10), 0.10), 0.0, 0.50),
            "near_trigger_atr": self.clamp(self.safe_float(c.get("near_trigger_atr", 0.50), 0.50), 0.0, 2.0),
            "allow_soft_trigger": bool(c.get("allow_soft_trigger", True)),
            "rsi_soft_band": self.clamp(self.safe_float(c.get("rsi_soft_band", 10.0), 10.0), 0.0, 25.0),
            "proximity_window_atr": self.clamp(self.safe_float(c.get("proximity_window_atr", 1.00), 1.00), 0.10, 5.0),
            "proximity_score_min": self.clamp(self.safe_float(c.get("proximity_score_min", 0.70), 0.70), 0.0, 1.0),
        }

    def _get_breakout_knobs(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        b = cfg.get("breakout", {}) or {}

        confirm_buffer_atr = self.clamp(self.safe_float(b.get("confirm_buffer_atr", 0.05), 0.05), 0.0, 1.0)
        retest_required = bool(b.get("retest_required", True))
        retest_band_atr = self.clamp(self.safe_float(b.get("retest_band_atr", 0.30), 0.30), 0.0, 3.0)
        retest_lookback_bars = max(self.safe_int(b.get("retest_lookback_bars", 5), 5), 2)

        sl_buffer_atr = self.clamp(
            self.safe_float(b.get("sl_buffer_atr", max(confirm_buffer_atr, retest_band_atr, 0.10)),
                            max(confirm_buffer_atr, retest_band_atr, 0.10)),
            0.0,
            5.0,
        )

        bb_width_atr_min = self.clamp(self.safe_float(b.get("bb_width_atr_min", 5.0), 5.0), 0.1, 50.0)

        proximity_threshold_atr = self.clamp(self.safe_float(b.get("proximity_threshold_atr", 1.50), 1.50), 0.1, 10.0)
        proximity_min_score = self.clamp(self.safe_float(b.get("proximity_min_score", 0.0), 0.0), 0.0, 100.0)

        # Adaptive proximity bypass (existing)
        adx_proximity_override = self.clamp(self.safe_float(b.get("adx_proximity_override", 30.0), 30.0), 0.0, 100.0)

        # Dynamic retest bypass (v2.12.2+)
        dynamic_retest_bypass_adx = self.clamp(self.safe_float(b.get("dynamic_retest_bypass_adx", 35.0), 35.0), 0.0, 100.0)

        return {
            "confirm_buffer_atr": confirm_buffer_atr,
            "retest_required": retest_required,
            "retest_band_atr": retest_band_atr,
            "retest_lookback_bars": retest_lookback_bars,
            "sl_buffer_atr": sl_buffer_atr,
            "bb_width_atr_min": bb_width_atr_min,
            "proximity_threshold_atr": proximity_threshold_atr,
            "proximity_min_score": proximity_min_score,
            "adx_proximity_override": adx_proximity_override,
            "dynamic_retest_bypass_adx": dynamic_retest_bypass_adx,
        }

    # -----------------------------
    # Commissioning utilities
    # -----------------------------
    def _count_blocked(self, blocked_by: Optional[str]) -> None:
        if not blocked_by:
            return
        d = self.commissioning.get("blocked_by", {})
        d[blocked_by] = int(d.get(blocked_by, 0)) + 1
        self.commissioning["blocked_by"] = d

    def _maybe_log_jsonl(self, cfg: Dict[str, Any], row: Dict[str, Any]) -> None:
        c = cfg.get("commissioning", {}) or {}
        if not bool(c.get("enabled", False)):
            return
        path = str(c.get("log_path", "logs/commissioning.jsonl"))
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception:
            # fail-safe: never break engine
            return

    # -----------------------------
    # Main
    # -----------------------------
    def generate_signal_package(self) -> Dict[str, Any]:
        cfg = self.reload_config()
        self.commissioning["counts"]["eval_total"] = int(self.commissioning["counts"]["eval_total"]) + 1

        config_load_ok = bool(cfg) and (self.last_config_error is None)

        symbol = str(cfg.get("symbol", "GOLD"))
        mode = str(cfg.get("mode", "balanced")).strip().lower()
        tf_cfg = cfg.get("timeframes", {}) or {}

        risk_cfg = cfg.get("risk", {}) or {}
        if not risk_cfg:
            legacy_atr = cfg.get("atr", {}) or {}
            risk_cfg = {"atr_period": legacy_atr.get("period", 14), "atr_sl_mult": legacy_atr.get("sl_mult", 1.8)}

        atr_period = self.safe_int(risk_cfg.get("atr_period", 14), 14)
        atr_sl_mult = self.safe_float(risk_cfg.get("atr_sl_mult", 1.8), 1.8)
        min_rr = self.safe_float(cfg.get("min_rr", 2.0), 2.0)

        st_cfg = cfg.get("supertrend", {}) or {}
        st_period = self.safe_int(st_cfg.get("period", 10), 10)
        st_mult = self.safe_float(st_cfg.get("multiplier", st_cfg.get("mult", 3.0)), 3.0)

        sens_cfg = cfg.get("structure_sensitivity", {}) or {}
        sens_htf = self.safe_int(sens_cfg.get("htf", 5), 5)
        sens_mtf = self.safe_int(sens_cfg.get("mtf", 4), 4)
        sens_ltf = self.safe_int(sens_cfg.get("ltf", 3), 3)

        htf = str(tf_cfg.get("htf", "H1"))
        mtf = str(tf_cfg.get("mtf", "M15"))
        ltf = str(tf_cfg.get("ltf", "M5"))

        blocked_reasons = []
        watch_state = "NONE"
        debug: Dict[str, Any] = {}

        # Gate audit structure
        gates = {
            "bias_ok": None,
            "supertrend_ok": None,
            "vol_expansion_ok": None,
            "bos_ok": None,
            "bos_break_ok": None,
            "retest_checked": False,
            "retest_ok": None,
            "retest_bypass_reason": None,
            "proximity_checked": False,
            "proximity_ok": None,
            "proximity_bypass_reason": None,
            "rr_ok": None,
        }

        try:
            htf_data = self.get_data(symbol, self.tf(htf), 600)
            mtf_data = self.get_data(symbol, self.tf(mtf), 800)
            ltf_data = self.get_data(symbol, self.tf(ltf), 1200)
        except Exception as e:
            self.commissioning["counts"]["signals_none"] = int(self.commissioning["counts"]["signals_none"]) + 1
            self._count_blocked("no_data")
            return {
                "symbol": symbol,
                "direction": "NONE",
                "entry_candidate": None,
                "stop_candidate": None,
                "tp_candidate": None,
                "rr": 0.0,
                "score": 0.0,
                "confidence_py": 0,
                "bos": False,
                "supertrend_ok": False,
                "context": {
                    "blocked_by": "no_data",
                    "error": str(e),
                    "engine_version": ENGINE_VERSION,
                    "mode": mode,
                    "config_load_ok": config_load_ok,
                    "config_path_used": self.config_path_used,
                    "config_error": self.last_config_error,
                    "gates": gates,
                },
            }

        htf_trend, _, _ = self.structure(htf_data, sens_htf)
        mtf_trend, _, _ = self.structure(mtf_data, sens_mtf)
        ltf_trend, bos_hi, bos_lo = self.structure(ltf_data, sens_ltf)

        close = np.asarray(ltf_data["close"], dtype=float)
        high = np.asarray(ltf_data["high"], dtype=float)
        low = np.asarray(ltf_data["low"], dtype=float)
        open_ = np.asarray(ltf_data["open"], dtype=float)

        mtf_close = np.asarray(mtf_data["close"], dtype=float)
        mtf_high = np.asarray(mtf_data["high"], dtype=float)
        mtf_low = np.asarray(mtf_data["low"], dtype=float)

        ltf_close = float(close[-1])

        atr_arr = self.atr(high, low, close, atr_period)
        atr_val = float(atr_arr[-1]) if len(atr_arr) and np.isfinite(atr_arr[-1]) else 0.0

        st_line, st_dir_arr = self.supertrend(high, low, close, st_period, st_mult)
        st_dir = "bullish" if int(st_dir_arr[-1]) == 1 else "bearish"
        st_value = float(st_line[-1]) if len(st_line) else float("nan")

        # Bias (HTF first, then MTF fallback)
        if mode == "sideway_scalp":
            direction_bias = "NEUTRAL"
            bias_source = "SIDEWAY_MODE"
        else:
            direction_bias = "NONE"
            bias_source = "NO_CLEAR_BIAS"
            if htf_trend == "bullish":
                direction_bias = "BUY"
                bias_source = "HTF"
            elif htf_trend == "bearish":
                direction_bias = "SELL"
                bias_source = "HTF"
            elif mtf_trend == "bullish":
                direction_bias = "BUY"
                bias_source = "MTF_FALLBACK"
            elif mtf_trend == "bearish":
                direction_bias = "SELL"
                bias_source = "MTF_FALLBACK"
            else:
                blocked_reasons.append("no_clear_bias")

        gates["bias_ok"] = bool(direction_bias in ("BUY", "SELL") or mode == "sideway_scalp")

        # Supertrend filter (for non-sideway)
        supertrend_ok = True if mode == "sideway_scalp" else False
        if mode != "sideway_scalp":
            if direction_bias == "BUY" and st_dir == "bullish":
                supertrend_ok = True
            elif direction_bias == "SELL" and st_dir == "bearish":
                supertrend_ok = True
            else:
                if direction_bias in ("BUY", "SELL"):
                    blocked_reasons.append("supertrend_conflict")
        gates["supertrend_ok"] = bool(supertrend_ok)

        direction_out = "NONE"
        entry_candidate = None
        stop_candidate = None
        tp_candidate = None
        rr = 0.0

        ctx: Dict[str, Any] = {}

        proximity_score_side = 0.0
        proximity_score_bk = 0.0
        proximity_bypassed_due_to_high_adx = False

        retest_bypassed_due_to_high_adx = False
        dynamic_retest_bypass_adx_used: Optional[float] = None
        adx_proximity_override_used: Optional[float] = None

        score_out = 0.0

        if mode == "sideway_scalp":
            self.commissioning["counts"]["sideway_eval"] = int(self.commissioning["counts"]["sideway_eval"]) + 1
            k = self._get_sideway_knobs(cfg)

            adx_arr = self.adx(mtf_high, mtf_low, mtf_close, k["adx_period"])
            adx_val = float(adx_arr[-1]) if (len(adx_arr) and np.isfinite(adx_arr[-1])) else float("nan")

            bb_u, _, bb_l = self.bollinger(close, k["bb_period"], k["bb_std"])
            bb_upper = float(bb_u[-1]) if (len(bb_u) and np.isfinite(bb_u[-1])) else float("nan")
            bb_lower = float(bb_l[-1]) if (len(bb_l) and np.isfinite(bb_l[-1])) else float("nan")

            bb_width = float(bb_upper - bb_lower) if (np.isfinite(bb_upper) and np.isfinite(bb_lower)) else float("nan")
            bb_width_atr = float(bb_width / atr_val) if (atr_val > 0 and np.isfinite(bb_width)) else float("nan")

            rsi_arr = self.rsi(close, k["rsi_period"])
            rsi_val = float(rsi_arr[-1]) if (len(rsi_arr) and np.isfinite(rsi_arr[-1])) else float("nan")

            last_open = float(open_[-1])
            last_close = float(close[-1])
            prev_open = float(open_[-2]) if len(open_) >= 2 else last_open
            prev_close = float(close[-2]) if len(close) >= 2 else last_close

            bullish_reversal = (last_close > last_open) and (prev_close < prev_open)
            bearish_reversal = (last_close < last_open) and (prev_close > prev_open)

            if not (np.isfinite(adx_val) and adx_val <= k["adx_max"]):
                blocked_reasons.append("not_sideway_adx")
            if not (np.isfinite(bb_width_atr) and bb_width_atr <= k["bb_width_atr_max"]):
                blocked_reasons.append("not_sideway_bbwidth")

            touch_buffer_points = float(k["touch_buffer_atr"] * atr_val) if atr_val > 0 else 0.0
            near_points = float(k["near_trigger_atr"] * atr_val) if atr_val > 0 else 0.0

            buy_trigger_level = (bb_lower + touch_buffer_points) if np.isfinite(bb_lower) else float("nan")
            sell_trigger_level = (bb_upper - touch_buffer_points) if np.isfinite(bb_upper) else float("nan")

            dist_buy = float(ltf_close - buy_trigger_level) if np.isfinite(buy_trigger_level) else float("nan")
            dist_sell = float(sell_trigger_level - ltf_close) if np.isfinite(sell_trigger_level) else float("nan")

            d_buy_abs = abs(dist_buy) if np.isfinite(dist_buy) else float("inf")
            d_sell_abs = abs(dist_sell) if np.isfinite(dist_sell) else float("inf")
            best_dist = float(min(d_buy_abs, d_sell_abs))

            window = float(max(k["proximity_window_atr"] * atr_val, 1e-9))
            proximity_score_side = float(max(0.0, 1.0 - (best_dist / window))) if (np.isfinite(best_dist) and window > 0) else 0.0

            if len(blocked_reasons) == 0 and k["enabled"]:
                buy_touch = np.isfinite(buy_trigger_level) and (ltf_close <= buy_trigger_level)
                sell_touch = np.isfinite(sell_trigger_level) and (ltf_close >= sell_trigger_level)

                buy_soft = np.isfinite(buy_trigger_level) and (ltf_close <= (buy_trigger_level + near_points))
                sell_soft = np.isfinite(sell_trigger_level) and (ltf_close >= (sell_trigger_level - near_points))

                buy_confirm_hard = (np.isfinite(rsi_val) and rsi_val <= k["rsi_oversold"]) or bullish_reversal
                sell_confirm_hard = (np.isfinite(rsi_val) and rsi_val >= k["rsi_overbought"]) or bearish_reversal

                buy_soft_rsi = (np.isfinite(rsi_val) and rsi_val <= (k["rsi_oversold"] + k["rsi_soft_band"]))
                sell_soft_rsi = (np.isfinite(rsi_val) and rsi_val >= (k["rsi_overbought"] - k["rsi_soft_band"]))
                buy_confirm_soft = bullish_reversal or buy_soft_rsi
                sell_confirm_soft = bearish_reversal or sell_soft_rsi

                buy_allowed = bool(buy_touch and buy_confirm_hard)
                sell_allowed = bool(sell_touch and sell_confirm_hard)

                if not buy_allowed and k["allow_soft_trigger"]:
                    if buy_soft and buy_confirm_soft and (proximity_score_side >= float(k["proximity_score_min"])):
                        buy_allowed = True

                if not sell_allowed and k["allow_soft_trigger"]:
                    if sell_soft and sell_confirm_soft and (proximity_score_side >= float(k["proximity_score_min"])):
                        sell_allowed = True

                if buy_allowed and not sell_allowed:
                    direction_out = "BUY"
                elif sell_allowed and not buy_allowed:
                    direction_out = "SELL"

                if direction_out in ("BUY", "SELL") and atr_val > 0:
                    entry_candidate = float(ltf_close)
                    stop_candidate = float(entry_candidate - (atr_sl_mult * atr_val)) if direction_out == "BUY" else float(entry_candidate + (atr_sl_mult * atr_val))

                    rr_eps = 1e-6
                    target_rr = float(min_rr) + rr_eps

                    if direction_out == "BUY":
                        risk = float(entry_candidate - stop_candidate)
                        if risk > 0:
                            tp_candidate = float(entry_candidate + (risk * target_rr))
                            rr = float((tp_candidate - entry_candidate) / risk)
                    else:
                        risk = float(stop_candidate - entry_candidate)
                        if risk > 0:
                            tp_candidate = float(entry_candidate - (risk * target_rr))
                            rr = float((entry_candidate - tp_candidate) / risk)

                    if rr < (float(min_rr) - 1e-9):
                        blocked_reasons.append("rr_below_floor")

            ctx = {
                "sideway": True,
                "adx_val": adx_val,
                "bb_width_atr": bb_width_atr,
                "rsi_val": rsi_val,
                "proximity_score": proximity_score_side,
            }

            score_out = float(proximity_score_side)

        elif mode == "breakout":
            self.commissioning["counts"]["breakout_eval"] = int(self.commissioning["counts"]["breakout_eval"]) + 1
            kb = self._get_breakout_knobs(cfg)

            # MTF ADX
            adx_period_mtf = self.safe_int((cfg.get("sideway_scalp", {}) or {}).get("adx_period", 14), 14)
            adx_arr = self.adx(mtf_high, mtf_low, mtf_close, adx_period_mtf)
            adx_val = float(adx_arr[-1]) if (len(adx_arr) and np.isfinite(adx_arr[-1])) else float("nan")

            # Vol-expansion using BB
            sw = cfg.get("sideway_scalp", {}) or {}
            bb_period = self.safe_int(sw.get("bb_period", 20), 20)
            bb_std = self.safe_float(sw.get("bb_std", 2.0), 2.0)

            bb_u, _, bb_l = self.bollinger(close, bb_period, bb_std)
            bb_upper = float(bb_u[-1]) if (len(bb_u) and np.isfinite(bb_u[-1])) else float("nan")
            bb_lower = float(bb_l[-1]) if (len(bb_l) and np.isfinite(bb_l[-1])) else float("nan")
            bb_width = float(bb_upper - bb_lower) if (np.isfinite(bb_upper) and np.isfinite(bb_lower)) else float("nan")
            bb_width_atr = float(bb_width / atr_val) if (atr_val > 0 and np.isfinite(bb_width)) else float("nan")

            # Gate: vol expansion
            vol_ok = bool(np.isfinite(bb_width_atr) and bb_width_atr >= float(kb["bb_width_atr_min"]))
            gates["vol_expansion_ok"] = vol_ok
            if not vol_ok:
                blocked_reasons.append("no_vol_expansion")

            # Gate: bias + supertrend already set above
            if direction_bias not in ("BUY", "SELL"):
                blocked_reasons.append("no_clear_bias")
            if direction_bias in ("BUY", "SELL") and not supertrend_ok:
                blocked_reasons.append("supertrend_conflict")

            bos_ref_high = float(bos_hi) if np.isfinite(bos_hi) else float("nan")
            bos_ref_low = float(bos_lo) if np.isfinite(bos_lo) else float("nan")
            bos_ok = bool(np.isfinite(bos_ref_high) and np.isfinite(bos_ref_low))
            gates["bos_ok"] = bos_ok

            confirm_buf = float(kb["confirm_buffer_atr"] * atr_val) if atr_val > 0 else 0.0
            retest_band = float(kb["retest_band_atr"] * atr_val) if atr_val > 0 else 0.0
            sl_buf = float(kb["sl_buffer_atr"] * atr_val) if atr_val > 0 else 0.0

            lookback = int(kb["retest_lookback_bars"])
            lookback = max(2, min(lookback, len(close)))

            adx_proximity_override_used = float(kb["adx_proximity_override"])
            dynamic_retest_bypass_adx_used = float(kb["dynamic_retest_bypass_adx"])

            def _maybe_bypass_proximity() -> bool:
                return bool(np.isfinite(adx_val) and (adx_val > adx_proximity_override_used))

            def _maybe_bypass_retest() -> bool:
                return bool(np.isfinite(adx_val) and (adx_val > dynamic_retest_bypass_adx_used))

            # Evaluate breakout direction
            if len(blocked_reasons) == 0:
                if not bos_ok:
                    blocked_reasons.append("no_bos_refs")
                else:
                    if direction_bias == "BUY":
                        level = bos_ref_high
                        # Gate: BOS break
                        bos_break_ok = bool(ltf_close > (level + confirm_buf))
                        gates["bos_break_ok"] = bos_break_ok
                        if not bos_break_ok:
                            blocked_reasons.append("no_bos_break")
                        else:
                            # Gate: Retest (checked only when required)
                            if bool(kb["retest_required"]):
                                gates["retest_checked"] = True
                                if _maybe_bypass_retest():
                                    retest_bypassed_due_to_high_adx = True
                                    gates["retest_ok"] = True
                                    gates["retest_bypass_reason"] = "adx_high"
                                    self.commissioning["counts"]["retest_bypass"] = int(self.commissioning["counts"]["retest_bypass"]) + 1
                                else:
                                    recent_low = float(np.nanmin(low[-lookback:]))
                                    retest_ok = bool(np.isfinite(recent_low) and recent_low <= (level + retest_band) and ltf_close > level)
                                    gates["retest_ok"] = retest_ok
                                    if not retest_ok:
                                        blocked_reasons.append("no_retest")
                            else:
                                gates["retest_checked"] = False
                                gates["retest_ok"] = True

                            # Gate: Proximity (always checked)
                            gates["proximity_checked"] = True
                            if atr_val > 0 and float(kb["proximity_threshold_atr"]) > 0:
                                dist_atr = float(abs(ltf_close - level) / atr_val)
                                th = float(kb["proximity_threshold_atr"])
                                proximity_score_bk = float(max(0.0, (th - dist_atr) / th) * 100.0)
                            else:
                                proximity_score_bk = 0.0

                            prox_ok = bool(proximity_score_bk >= float(kb["proximity_min_score"]))
                            if prox_ok:
                                gates["proximity_ok"] = True
                            else:
                                if _maybe_bypass_proximity():
                                    proximity_bypassed_due_to_high_adx = True
                                    gates["proximity_ok"] = True
                                    gates["proximity_bypass_reason"] = "adx_high"
                                    self.commissioning["counts"]["proximity_bypass"] = int(self.commissioning["counts"]["proximity_bypass"]) + 1
                                else:
                                    gates["proximity_ok"] = False
                                    blocked_reasons.append("low_proximity_score")

                            # Build trade if all ok
                            if len(blocked_reasons) == 0:
                                direction_out = "BUY"
                                entry_candidate = float(ltf_close)

                                sl_base = bos_ref_low
                                stop_candidate = float(sl_base - sl_buf)

                                risk = float(entry_candidate - stop_candidate)
                                if risk <= 0:
                                    blocked_reasons.append("bad_risk")
                                    gates["rr_ok"] = False
                                else:
                                    tp_candidate = float(entry_candidate + (risk * (float(min_rr) + 1e-6)))
                                    rr = float((tp_candidate - entry_candidate) / risk)
                                    gates["rr_ok"] = bool(rr >= float(min_rr))

                                    if not gates["rr_ok"]:
                                        blocked_reasons.append("rr_below_floor")

                    elif direction_bias == "SELL":
                        level = bos_ref_low
                        bos_break_ok = bool(ltf_close < (level - confirm_buf))
                        gates["bos_break_ok"] = bos_break_ok
                        if not bos_break_ok:
                            blocked_reasons.append("no_bos_break")
                        else:
                            if bool(kb["retest_required"]):
                                gates["retest_checked"] = True
                                if _maybe_bypass_retest():
                                    retest_bypassed_due_to_high_adx = True
                                    gates["retest_ok"] = True
                                    gates["retest_bypass_reason"] = "adx_high"
                                    self.commissioning["counts"]["retest_bypass"] = int(self.commissioning["counts"]["retest_bypass"]) + 1
                                else:
                                    recent_high = float(np.nanmax(high[-lookback:]))
                                    retest_ok = bool(np.isfinite(recent_high) and recent_high >= (level - retest_band) and ltf_close < level)
                                    gates["retest_ok"] = retest_ok
                                    if not retest_ok:
                                        blocked_reasons.append("no_retest")
                            else:
                                gates["retest_checked"] = False
                                gates["retest_ok"] = True

                            gates["proximity_checked"] = True
                            if atr_val > 0 and float(kb["proximity_threshold_atr"]) > 0:
                                dist_atr = float(abs(ltf_close - level) / atr_val)
                                th = float(kb["proximity_threshold_atr"])
                                proximity_score_bk = float(max(0.0, (th - dist_atr) / th) * 100.0)
                            else:
                                proximity_score_bk = 0.0

                            prox_ok = bool(proximity_score_bk >= float(kb["proximity_min_score"]))
                            if prox_ok:
                                gates["proximity_ok"] = True
                            else:
                                if _maybe_bypass_proximity():
                                    proximity_bypassed_due_to_high_adx = True
                                    gates["proximity_ok"] = True
                                    gates["proximity_bypass_reason"] = "adx_high"
                                    self.commissioning["counts"]["proximity_bypass"] = int(self.commissioning["counts"]["proximity_bypass"]) + 1
                                else:
                                    gates["proximity_ok"] = False
                                    blocked_reasons.append("low_proximity_score")

                            if len(blocked_reasons) == 0:
                                direction_out = "SELL"
                                entry_candidate = float(ltf_close)

                                sl_base = bos_ref_high
                                stop_candidate = float(sl_base + sl_buf)

                                risk = float(stop_candidate - entry_candidate)
                                if risk <= 0:
                                    blocked_reasons.append("bad_risk")
                                    gates["rr_ok"] = False
                                else:
                                    tp_candidate = float(entry_candidate - (risk * (float(min_rr) + 1e-6)))
                                    rr = float((entry_candidate - tp_candidate) / risk)
                                    gates["rr_ok"] = bool(rr >= float(min_rr))
                                    if not gates["rr_ok"]:
                                        blocked_reasons.append("rr_below_floor")

            ctx = {
                "breakout": True,
                "adx_val": adx_val,
                "adx_period_mtf": int(adx_period_mtf),
                "adx_proximity_override": float(adx_proximity_override_used),
                "dynamic_retest_bypass_adx": float(dynamic_retest_bypass_adx_used),
                "proximity_bypassed_due_to_high_adx": bool(proximity_bypassed_due_to_high_adx),
                "retest_bypassed_due_to_high_adx": bool(retest_bypassed_due_to_high_adx),
                "bb_width_atr": bb_width_atr,
                "bb_width_atr_min": float(kb["bb_width_atr_min"]),
                "proximity_score": float(proximity_score_bk),
                "proximity_min_score": float(kb["proximity_min_score"]),
                "proximity_threshold_atr": float(kb["proximity_threshold_atr"]),
                "retest_required": bool(kb["retest_required"]),
                "retest_band_atr": float(kb["retest_band_atr"]),
                "retest_lookback_bars": int(kb["retest_lookback_bars"]),
            }

            score_out = float(proximity_score_bk)

        # Finalize + counters + logging
        blocked_by = ",".join(blocked_reasons) if blocked_reasons else None
        if debug:
            ctx["debug"] = debug

        if direction_out == "BUY":
            self.commissioning["counts"]["signals_buy"] = int(self.commissioning["counts"]["signals_buy"]) + 1
        elif direction_out == "SELL":
            self.commissioning["counts"]["signals_sell"] = int(self.commissioning["counts"]["signals_sell"]) + 1
        else:
            self.commissioning["counts"]["signals_none"] = int(self.commissioning["counts"]["signals_none"]) + 1

        self._count_blocked(blocked_by)

        out = {
            "symbol": symbol,
            "direction": direction_out,
            "entry_candidate": entry_candidate,
            "stop_candidate": stop_candidate,
            "tp_candidate": tp_candidate,
            "rr": float(rr),
            "score": float(score_out),
            "confidence_py": 0,
            "bos": bool(direction_out in ("BUY", "SELL")),
            "supertrend_ok": bool(supertrend_ok),
            "context": {
                "blocked_by": blocked_by,
                "engine_version": ENGINE_VERSION,
                "mode": mode,
                "config_load_ok": config_load_ok,
                "config_path_used": self.config_path_used,
                "config_error": self.last_config_error,
                "HTF_trend": htf_trend,
                "MTF_trend": mtf_trend,
                "LTF_trend": ltf_trend,
                "bias_source": bias_source,
                "direction_bias": direction_bias,
                "watch_state": watch_state,
                "supertrend_dir": st_dir,
                "supertrend_value": st_value,
                "bos_ref_high": float(bos_hi) if np.isfinite(bos_hi) else None,
                "bos_ref_low": float(bos_lo) if np.isfinite(bos_lo) else None,
                "atr": float(atr_val),
                "atr_period": int(atr_period),
                "atr_sl_mult": float(atr_sl_mult),
                "min_rr": float(min_rr),
                "gates": gates,
                # snapshot of commissioning counters (lightweight)
                "commissioning_counts": self.commissioning["counts"],
                **ctx,
            },
        }

        # Optional: JSONL logging row
        log_row = {
            "ts": time.time(),
            "engine_version": ENGINE_VERSION,
            "symbol": symbol,
            "mode": mode,
            "direction": direction_out,
            "blocked_by": blocked_by,
            "adx_val": ctx.get("adx_val"),
            "proximity_score": ctx.get("proximity_score"),
            "proximity_bypassed": ctx.get("proximity_bypassed_due_to_high_adx"),
            "retest_bypassed": ctx.get("retest_bypassed_due_to_high_adx"),
            "gates": gates,
            "entry": entry_candidate,
            "sl": stop_candidate,
            "tp": tp_candidate,
            "rr": rr,
        }
        self._maybe_log_jsonl(cfg, log_row)

        return out