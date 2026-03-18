# engine.py
# ============================================================
# ชื่อโค้ด: HIM Trading Engine (engine.py)
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\engine.py
# คำสั่งรัน: python engine.py
# เวอร์ชัน: v2.13.0
# ============================================================
# =============================================================================
# Hybrid Intelligence Mentor (HIM) - Trading Engine
#
# Version: v2.13.0
# File: c:\Data\Bot\HIM_AI_Confirm\engine.py
#
# PRODUCTION MODEL (DO NOT CHANGE STRUCTURE)
#
# CHANGELOG (v2.13.0):
# - FIX: Use CLOSED candles only (skip still-forming current bar from MT5 copy_rates_from_pos)
#
# CHANGELOG (v2.12.9):
# - ADD: Regime classification (RISK_OFF/RANGE/TREND/EXPANSION) + bounded adaptive thresholds
# - FIX: Output schema for production chain: request_id + decision(BUY/SELL/HOLD) + plan(entry/sl/tp)
# - FIX: Robust last-finite sampling for ATR/BB width/Supertrend to avoid NaN->null permanent blocks
#
# CHANGELOG (v2.12.8):
# - FIX: RR gate mismatch with config.min_rr
#   Previous: RR placeholder hard-coded to ~1.3 (TP=1.3 ATR, SL=1.0 ATR)
#            causing rr_ok=false when min_rr > 1.3 (commissioning hard block)
#   Now: RR placeholder derives TP from min_rr:
#        sl_atr = 1.0
#        tp_atr = max(base_tp_atr, min_rr * sl_atr)
#        rr = tp_atr / sl_atr  (>= min_rr)
#
# - Keep: v2.12.7 backward-compatible constructor + call signatures
# - Keep: v2.12.6 M1 Supertrend relaxation by ATR distance
#
# EVIDENCE NOTE:
# - Commissioning showed rr_too_low = 100% and rr_ok=false = 100% while plan.rr ~1.3
# - Config min_rr = 1.6 -> impossible to pass with fixed RR=1.3
#
# SAFETY:
# - Confirm-only: engine MUST NOT place orders.
# =============================================================================

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

try:
    import MetaTrader5 as mt5
except Exception:  # pragma: no cover
    mt5 = None


# -----------------------------
# MT5 timeframe map
# -----------------------------
MT5_TF_MAP = {
    "M1":  mt5.TIMEFRAME_M1 if mt5 else None,
    "M5":  mt5.TIMEFRAME_M5 if mt5 else None,
    "M10": getattr(mt5, "TIMEFRAME_M10", None) if mt5 else None,
    "M15": mt5.TIMEFRAME_M15 if mt5 else None,
    "M30": mt5.TIMEFRAME_M30 if mt5 else None,
    "H1":  mt5.TIMEFRAME_H1 if mt5 else None,
    "H4":  mt5.TIMEFRAME_H4 if mt5 else None,
    "D1":  mt5.TIMEFRAME_D1 if mt5 else None,
}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _tf_to_mt5(tf: str) -> int:
    if tf not in MT5_TF_MAP or MT5_TF_MAP[tf] is None:
        raise ValueError(f"Unsupported or unavailable timeframe: {tf}")
    return MT5_TF_MAP[tf]


def _rates_to_df(rates: Any) -> pd.DataFrame:
    df = pd.DataFrame(rates)
    if df.empty:
        return df
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    return df


# -----------------------------
# Indicators
# -----------------------------
def atr_wilder(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.ewm(alpha=1.0 / period, adjust=False).mean()


def bollinger_width_atr(
    df: pd.DataFrame, bb_period: int, bb_std: float, atr: pd.Series
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    close = df["close"].astype(float)
    ma = close.rolling(bb_period).mean()
    sd = close.rolling(bb_period).std(ddof=0)
    upper = ma + bb_std * sd
    lower = ma - bb_std * sd
    width = (upper - lower).abs()
    width_atr = width / atr.replace(0.0, np.nan)
    return upper, lower, width_atr


def supertrend(
    df: pd.DataFrame, atr: pd.Series, period: int, multiplier: float
) -> Tuple[pd.Series, pd.Series]:
    """
    Returns:
      st_line: Supertrend line value
      st_dir:  +1 bullish, -1 bearish
    """
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    hl2 = (high + low) / 2.0
    basic_ub = hl2 + multiplier * atr
    basic_lb = hl2 - multiplier * atr

    final_ub = basic_ub.copy()
    final_lb = basic_lb.copy()

    for i in range(1, len(df)):
        if np.isnan(final_ub.iloc[i - 1]):
            final_ub.iloc[i] = basic_ub.iloc[i]
        else:
            if (basic_ub.iloc[i] < final_ub.iloc[i - 1]) or (close.iloc[i - 1] > final_ub.iloc[i - 1]):
                final_ub.iloc[i] = basic_ub.iloc[i]
            else:
                final_ub.iloc[i] = final_ub.iloc[i - 1]

        if np.isnan(final_lb.iloc[i - 1]):
            final_lb.iloc[i] = basic_lb.iloc[i]
        else:
            if (basic_lb.iloc[i] > final_lb.iloc[i - 1]) or (close.iloc[i - 1] < final_lb.iloc[i - 1]):
                final_lb.iloc[i] = basic_lb.iloc[i]
            else:
                final_lb.iloc[i] = final_lb.iloc[i - 1]

    st_line = pd.Series(index=df.index, dtype=float)
    st_dir = pd.Series(index=df.index, dtype=float)

    st_dir.iloc[0] = 1.0
    st_line.iloc[0] = final_lb.iloc[0] if close.iloc[0] >= final_lb.iloc[0] else final_ub.iloc[0]
    if close.iloc[0] < st_line.iloc[0]:
        st_dir.iloc[0] = -1.0

    for i in range(1, len(df)):
        prev_line = st_line.iloc[i - 1]
        prev_dir = st_dir.iloc[i - 1]

        if prev_dir > 0:
            if close.iloc[i] <= final_ub.iloc[i]:
                st_dir.iloc[i] = -1.0
                st_line.iloc[i] = final_ub.iloc[i]
            else:
                st_dir.iloc[i] = 1.0
                st_line.iloc[i] = max(final_lb.iloc[i], prev_line) if not np.isnan(prev_line) else final_lb.iloc[i]
        else:
            if close.iloc[i] >= final_lb.iloc[i]:
                st_dir.iloc[i] = 1.0
                st_line.iloc[i] = final_lb.iloc[i]
            else:
                st_dir.iloc[i] = -1.0
                st_line.iloc[i] = min(final_ub.iloc[i], prev_line) if not np.isnan(prev_line) else final_ub.iloc[i]

    st_dir = st_dir.ffill()
    st_line = st_line.ffill()
    return st_line, st_dir


# -----------------------------
# Strategy config
# -----------------------------
@dataclass
class EngineConfig:
    symbol: str = "GOLD"

    htf: str = "H1"
    mtf: str = "M15"
    ltf: str = "M5"

    rates_lookback: int = 600

    atr_period: int = 14
    bb_period: int = 20
    bb_std: float = 2.0

    st_atr_period: int = 10
    st_mult: float = 3.0

    bb_width_atr_min: float = 0.90
    bb_width_atr_min_m1: float = 0.65

    bos_lookback: int = 40
    bos_break_atr_min: float = 0.40

    min_rr: float = 1.20

    # M1 supertrend relaxation
    st_relax_dist_atr_m1: float = 0.30

    # NEW (v2.12.8): base RR components (still production-safe placeholders)
    rr_sl_atr: float = 1.0
    rr_base_tp_atr: float = 1.3

    trend_entry_enabled: bool = False
    trend_entry_min_align: int = 2
    trend_entry_max_st_distance_atr: float = 1.2


class TradingEngine:
    """
    Backward-compatible ctor:
      TradingEngine("config.json")  OR  TradingEngine(cfg_dict)
    """

    def __init__(self, config: Union[str, Dict[str, Any]] = "config.json"):
        self.config_source = config
        self.raw_cfg = self._normalize_config(config)
        self.cfg = self._build_cfg(self.raw_cfg)
        self._regime_state: Dict[str, Dict[str, Any]] = {}

    def _normalize_config(self, config: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        if isinstance(config, dict):
            return config
        if isinstance(config, str):
            return _load_json(config)
        raise TypeError(f"Unsupported config type: {type(config)}")

    def _build_cfg(self, raw: Dict[str, Any]) -> EngineConfig:
        tfs = raw.get("timeframes", {}) if isinstance(raw, dict) else {}
        symbol = raw.get("symbol", "GOLD")
        te = raw.get("trend_entry", {}) if isinstance(raw, dict) else {}
        te = te if isinstance(te, dict) else {}

        return EngineConfig(
            symbol=str(symbol),
            htf=str(tfs.get("htf", raw.get("htf", "H1"))),
            mtf=str(tfs.get("mtf", raw.get("mtf", "M15"))),
            ltf=str(tfs.get("ltf", raw.get("ltf", "M5"))),

            rates_lookback=int(raw.get("rates_lookback", 600)),

            atr_period=int(raw.get("atr_period", 14)),
            bb_period=int(raw.get("bb_period", 20)),
            bb_std=_safe_float(raw.get("bb_std", 2.0), 2.0),

            st_atr_period=int(raw.get("st_atr_period", raw.get("st_period", 10))),
            st_mult=_safe_float(raw.get("st_mult", raw.get("st_multiplier", 3.0)), 3.0),

            bb_width_atr_min=_safe_float(raw.get("bb_width_atr_min", 0.90), 0.90),
            bb_width_atr_min_m1=_safe_float(raw.get("bb_width_atr_min_m1", 0.65), 0.65),

            bos_lookback=int(raw.get("bos_lookback", 40)),
            bos_break_atr_min=_safe_float(raw.get("bos_break_atr_min", 0.40), 0.40),

            min_rr=_safe_float(raw.get("min_rr", 1.20), 1.20),

            st_relax_dist_atr_m1=_safe_float(raw.get("st_relax_dist_atr_m1", 0.30), 0.30),

            rr_sl_atr=_safe_float(raw.get("rr_sl_atr", 1.0), 1.0),
            rr_base_tp_atr=_safe_float(raw.get("rr_base_tp_atr", 1.3), 1.3),

            trend_entry_enabled=bool(te.get("enabled", False)),
            trend_entry_min_align=int(te.get("min_align", 2)),
            trend_entry_max_st_distance_atr=_safe_float(te.get("max_supertrend_distance_atr", 1.2), 1.2),
        )

    def _ensure_mt5(self) -> None:
        if mt5 is None:
            raise RuntimeError("MetaTrader5 package not available.")
        # Try to initialize if not connected or if last error was Authorization failed
        if not mt5.initialize():
            err = mt5.last_error()
            if err and err[0] == -6:
                print("[WARNING] MT5 Authorization Failed! Terminal needs login.")
            # We don't raise error here, we let it try to fetch and fail gracefully, returning empty df.

    def _fetch_rates(self, symbol: str, timeframe: str, n: int) -> pd.DataFrame:
        self._ensure_mt5()
        tf = _tf_to_mt5(timeframe)
        rates = mt5.copy_rates_from_pos(symbol, tf, 1, n)
        return _rates_to_df(rates)

    def _compute_tf_bundle(self, symbol: str, tf: str) -> Dict[str, Any]:
        df = self._fetch_rates(symbol, tf, self.cfg.rates_lookback)
        need = max(self.cfg.atr_period, self.cfg.bb_period, self.cfg.st_atr_period) + 5
        if df.empty or len(df) < need:
            return {"tf": tf, "ok": False, "reason": "not_enough_rates", "df_len": int(len(df))}

        atr = atr_wilder(df, self.cfg.atr_period)
        upper, lower, bb_width_atr = bollinger_width_atr(df, self.cfg.bb_period, self.cfg.bb_std, atr)
        st_line, st_dir = supertrend(df, atr, self.cfg.st_atr_period, self.cfg.st_mult)

        i = df.index[-1]
        close = float(df.loc[i, "close"])

        def _last_finite(s: pd.Series, default: float = np.nan, lookback: int = 6) -> float:
            tail = s.tail(lookback)
            for v in reversed(list(tail.values)):
                try:
                    fv = float(v)
                    if np.isfinite(fv):
                        return fv
                except Exception:
                    continue
            return float(default)

        a = _last_finite(atr, np.nan)
        stv = _last_finite(st_line, np.nan)
        stdir_val = _last_finite(st_dir, 1.0)
        stdir = int(1 if stdir_val > 0 else -1)

        bbwa = _last_finite(bb_width_atr, np.nan)
        bbu = _last_finite(upper, np.nan)
        bbl = _last_finite(lower, np.nan)

        signed_dist = (close - stv) if np.isfinite(stv) else np.nan
        abs_dist = abs(signed_dist) if np.isfinite(signed_dist) else np.nan

        dist_atr = (abs_dist / a) if (np.isfinite(a) and a != 0.0 and np.isfinite(abs_dist)) else np.nan
        signed_atr = (signed_dist / a) if (np.isfinite(a) and a != 0.0 and np.isfinite(signed_dist)) else np.nan

        return {
            "tf": tf,
            "ok": True,
            "df": df,
            "close": close,
            "atr": float(a) if np.isfinite(a) else np.nan,
            "bb_width_atr": float(bbwa) if np.isfinite(bbwa) else np.nan,
            "st_line": float(stv) if np.isfinite(stv) else np.nan,
            "st_dir": stdir,
            "st_distance_atr": float(dist_atr) if np.isfinite(dist_atr) else np.nan,
            "st_distance_atr_signed": float(signed_atr) if np.isfinite(signed_atr) else np.nan,
            "bb_upper": float(bbu) if np.isfinite(bbu) else np.nan,
            "bb_lower": float(bbl) if np.isfinite(bbl) else np.nan,
        }

    def _derive_htf_bias(self, htf_bundle: Dict[str, Any], mtf_bundle: Dict[str, Any] = None) -> str:
        # Use HTF bias primarily, but if we want to catch faster M15 trends,
        # we can align with MTF if it's strong.
        # For profitability and fast adaptation, we use MTF (M15) as the main bias if available.
        if mtf_bundle and mtf_bundle.get("ok"):
            return "bullish" if int(mtf_bundle["st_dir"]) > 0 else "bearish"
        
        if not htf_bundle.get("ok"):
            return "unknown"
        return "bullish" if int(htf_bundle["st_dir"]) > 0 else "bearish"

    def _bos_gate(self, ev_bundle: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        df = ev_bundle.get("df")
        if df is None or df.empty or not ev_bundle.get("ok"):
            return False, {"bos_break_up_atr": np.nan, "bos_break_dn_atr": np.nan}, "no_bos_break"

        lookback = int(self.cfg.bos_lookback)
        if len(df) < lookback + 5:
            return False, {"bos_break_up_atr": np.nan, "bos_break_dn_atr": np.nan}, "no_bos_break"

        close = float(ev_bundle["close"])
        atr = float(ev_bundle["atr"]) if ev_bundle.get("atr") else np.nan
        if atr == 0.0 or np.isnan(atr):
            return False, {"bos_break_up_atr": np.nan, "bos_break_dn_atr": np.nan}, "no_bos_break"

        recent = df.iloc[-(lookback + 1):-1]
        swing_high = float(recent["high"].max())
        swing_low = float(recent["low"].min())

        break_up = (close - swing_high) / atr
        break_dn = (swing_low - close) / atr

        ok = (break_up >= self.cfg.bos_break_atr_min) or (break_dn >= self.cfg.bos_break_atr_min)
        return ok, {"bos_break_up_atr": float(break_up), "bos_break_dn_atr": float(break_dn)}, (None if ok else "no_bos_break")

    def _rr_gate(self, ev_bundle: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], Optional[str]]:
        """
        v2.12.8 FIX:
        RR must be compatible with config.min_rr to avoid permanent commissioning blocks.

        - sl_atr = cfg.rr_sl_atr (default 1.0)
        - tp_atr = max(cfg.rr_base_tp_atr, cfg.min_rr * sl_atr)
        - rr = tp_atr / sl_atr  (>= min_rr)
        """
        atr = float(ev_bundle.get("atr", np.nan))
        if atr == 0.0 or np.isnan(atr):
            return False, {"rr": np.nan, "sl_atr": self.cfg.rr_sl_atr, "tp_atr": np.nan}, "rr_too_low"

        sl_atr = float(self.cfg.rr_sl_atr) if self.cfg.rr_sl_atr > 0 else 1.0
        base_tp_atr = float(self.cfg.rr_base_tp_atr) if self.cfg.rr_base_tp_atr > 0 else 1.3

        # derive tp from min_rr (core fix)
        tp_atr = max(base_tp_atr, float(self.cfg.min_rr) * sl_atr)
        rr = (tp_atr / sl_atr) if sl_atr != 0 else np.nan

        ok = (not np.isnan(rr)) and (rr >= float(self.cfg.min_rr))
        return ok, {"rr": float(rr), "sl_atr": float(sl_atr), "tp_atr": float(tp_atr)}, (None if ok else "rr_too_low")

    # -------------------------------------------------------------------------
    # Backward-compatible public API
    # -------------------------------------------------------------------------
    def generate_signal_package(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        """
        Supports:
          - generate_signal_package(event_timeframe="M1")                 (new)
          - generate_signal_package("GOLD")                               (legacy symbol only)
          - generate_signal_package("GOLD", "M1")                         (legacy symbol,timeframe)
          - generate_signal_package(symbol="GOLD", event_timeframe="M1")  (explicit)
        """
        # defaults
        symbol = kwargs.pop("symbol", self.cfg.symbol)
        event_timeframe = kwargs.pop("event_timeframe", "M1")

        # legacy positional parsing
        if len(args) == 1:
            a0 = str(args[0])
            if a0 in MT5_TF_MAP:
                event_timeframe = a0
            else:
                symbol = a0
        elif len(args) >= 2:
            symbol = str(args[0])
            event_timeframe = str(args[1])

        t0 = _now_ms()

        htf = self._compute_tf_bundle(symbol, self.cfg.htf)
        mtf = self._compute_tf_bundle(symbol, self.cfg.mtf)
        ltf = self._compute_tf_bundle(symbol, self.cfg.ltf)
        ev = self._compute_tf_bundle(symbol, event_timeframe)

        # Fix bias conflict: use MTF bias to catch 400-500 point moves earlier
        bias = self._derive_htf_bias(htf, mtf)

        gates: Dict[str, bool] = {}
        metrics: Dict[str, Any] = {}
        blocked_by: List[str] = []

        data_ok = all(b.get("ok") for b in [htf, mtf, ltf, ev])
        gates["data_ok"] = bool(data_ok)
        if not data_ok:
            blocked_by.append("data_not_ready")

        # regime features (alignment)
        align = 0
        if htf.get("ok") and int(htf.get("st_dir", 0)) == int(ev.get("st_dir", 0)):
            align += 1
        if mtf.get("ok") and int(mtf.get("st_dir", 0)) == int(ev.get("st_dir", 0)):
            align += 1
        if ltf.get("ok") and int(ltf.get("st_dir", 0)) == int(ev.get("st_dir", 0)):
            align += 1
        metrics["alignment_score"] = int(align)

        # volatility gate (adaptive, bounded)
        bb_width_atr = _safe_float(ev.get("bb_width_atr"), np.nan)
        vol_thr_base = self.cfg.bb_width_atr_min_m1 if event_timeframe == "M1" else self.cfg.bb_width_atr_min
        vol_thr_used = float(vol_thr_base) * (0.80 if align >= 2 else 1.00)
        vol_thr_used = max(0.20, min(vol_thr_used, float(vol_thr_base)))

        vol_ok = (not np.isnan(bb_width_atr)) and (bb_width_atr >= vol_thr_used)
        gates["vol_expansion_ok"] = bool(vol_ok)
        metrics["bb_width_atr"] = bb_width_atr
        metrics["bb_width_atr_min_base"] = float(vol_thr_base)
        metrics["bb_width_atr_min_used"] = float(vol_thr_used)
        if data_ok and not vol_ok:
            blocked_by.append("no_vol_expansion")

        # supertrend conflict gate + M1 relaxation
        st_dir_ev = int(ev.get("st_dir", 0)) if ev.get("ok") else 0
        st_dist_atr = _safe_float(ev.get("st_distance_atr"), np.nan)
        metrics["supertrend_dir_event"] = st_dir_ev
        metrics["supertrend_distance_atr"] = st_dist_atr
        metrics["st_relax_dist_atr_m1"] = float(self.cfg.st_relax_dist_atr_m1)

        conflict = False
        if bias == "bullish" and st_dir_ev < 0:
            conflict = True
        if bias == "bearish" and st_dir_ev > 0:
            conflict = True

        relaxed = False
        if event_timeframe in ("M1", "M5", "M15") and conflict:
            if (not np.isnan(st_dist_atr)) and (st_dist_atr <= float(self.cfg.st_relax_dist_atr_m1)):
                relaxed = True

        supertrend_ok = (not conflict) or relaxed
        gates["supertrend_conflict"] = bool(conflict)
        gates["supertrend_relaxed_m1"] = bool(relaxed)
        gates["supertrend_ok"] = bool(supertrend_ok)
        if data_ok and not supertrend_ok:
            blocked_by.append("supertrend_conflict")

        # RR (FIXED in v2.12.8)
        rr_ok, rr_metrics, rr_block = self._rr_gate(ev)
        gates["rr_ok"] = bool(rr_ok)
        metrics.update(rr_metrics)
        metrics["min_rr_used"] = float(self.cfg.min_rr)
        if data_ok and rr_block:
            blocked_by.append(rr_block)

        candidate_regime = "RISK_OFF" if not data_ok else (
            "EXPANSION" if gates.get("vol_expansion_ok") and int(metrics.get("alignment_score", 0)) >= 2 else (
                "TREND" if int(metrics.get("alignment_score", 0)) >= 2 else "RANGE"
            )
        )

        tf_sec = {"M1": 60, "M5": 300, "M15": 900, "H1": 3600}.get(event_timeframe, 60)
        lock_ms = int(tf_sec * 10 * 1000)
        key = f"{symbol}:{event_timeframe}"
        st = self._regime_state.get(key) or {"regime": candidate_regime, "pending": None, "pending_n": 0, "last_change_ms": 0}

        regime = str(st.get("regime") or candidate_regime)
        last_change_ms = int(st.get("last_change_ms") or 0)

        if candidate_regime == "RISK_OFF":
            regime = "RISK_OFF"
            st["pending"] = None
            st["pending_n"] = 0
            st["last_change_ms"] = _now_ms()
        else:
            if candidate_regime != regime:
                if st.get("pending") != candidate_regime:
                    st["pending"] = candidate_regime
                    st["pending_n"] = 1
                else:
                    st["pending_n"] = int(st.get("pending_n") or 0) + 1

                if (_now_ms() - last_change_ms) >= lock_ms and int(st.get("pending_n") or 0) >= 3:
                    regime = candidate_regime
                    st["regime"] = regime
                    st["pending"] = None
                    st["pending_n"] = 0
                    st["last_change_ms"] = _now_ms()
            else:
                st["pending"] = None
                st["pending_n"] = 0

        self._regime_state[key] = st
        metrics["regime"] = regime
        metrics["regime_candidate"] = candidate_regime

        # BOS (direction-aware vs HTF bias)
        bos_ok_raw, bos_metrics, _ = self._bos_gate(ev)
        metrics.update(bos_metrics)

        break_up = _safe_float(bos_metrics.get("bos_break_up_atr"), np.nan)
        break_dn = _safe_float(bos_metrics.get("bos_break_dn_atr"), np.nan)

        bos_ok = bool(bos_ok_raw)
        if bias == "bullish":
            bos_ok = (not np.isnan(break_up)) and (break_up >= float(self.cfg.bos_break_atr_min))
        elif bias == "bearish":
            bos_ok = (not np.isnan(break_dn)) and (break_dn >= float(self.cfg.bos_break_atr_min))

        gates["bos_break_ok"] = bool(bos_ok)
        align_ok = int(metrics.get("alignment_score", 0)) >= int(self.cfg.trend_entry_min_align)
        st_dist_ok = (not np.isnan(st_dist_atr)) and (st_dist_atr <= float(self.cfg.trend_entry_max_st_distance_atr))
        trend_entry_ok = (
            bool(self.cfg.trend_entry_enabled)
            and data_ok
            and bool(rr_ok)
            and bool(vol_ok)
            and (not conflict)
            and align_ok
            and st_dist_ok
            and regime in ("TREND", "EXPANSION")
        )
        gates["trend_entry_ok"] = bool(trend_entry_ok)

        if data_ok and not bos_ok and not trend_entry_ok:
            blocked_by.append("no_bos_break")

        status = "BLOCKED" if blocked_by else "PASS"

        action = "HOLD"
        plan = {"entry": 0.0, "sl": 0.0, "tp": 0.0}

        break_up = _safe_float(metrics.get("bos_break_up_atr"), np.nan)
        break_dn = _safe_float(metrics.get("bos_break_dn_atr"), np.nan)

        if data_ok and not blocked_by and regime != "RISK_OFF":
            if bias == "bullish" and (bool(bos_ok) or bool(trend_entry_ok)):
                action = "BUY"
            elif bias == "bearish" and (bool(bos_ok) or bool(trend_entry_ok)):
                action = "SELL"

        close_px = _safe_float(ev.get("close"), 0.0)
        atr_px = _safe_float(ev.get("atr"), 0.0)
        sl_atr = float(metrics.get("sl_atr", float(self.cfg.rr_sl_atr)))
        tp_atr = float(metrics.get("tp_atr", float(self.cfg.rr_base_tp_atr)))

        if action in ("BUY", "SELL") and atr_px > 0:
            entry = float(close_px)
            if action == "BUY":
                sl = entry - (atr_px * sl_atr)
                tp = entry + (atr_px * tp_atr)
            else:
                sl = entry + (atr_px * sl_atr)
                tp = entry - (atr_px * tp_atr)
            plan = {"entry": float(entry), "sl": float(sl), "tp": float(tp)}

        request_id = f"REQ-{symbol}-{_now_ms()}"

        return {
            "engine_version": "v2.13.0",
            "request_id": request_id,
            "ts_ms": _now_ms(),
            "symbol": symbol,
            "event_timeframe": event_timeframe,
            "timeframes": {"htf": self.cfg.htf, "mtf": self.cfg.mtf, "ltf": self.cfg.ltf},
            "bias": bias,
            "status": status,
            "decision": action,
            "plan": plan,
            "price": {
                "close": _safe_float(ev.get("close"), np.nan),
                "atr": _safe_float(ev.get("atr"), np.nan),
            },
            "gates": gates,
            "blocked_by": blocked_by,
            "metrics": metrics,
            "debug": {
                "htf_ok": bool(htf.get("ok")),
                "mtf_ok": bool(mtf.get("ok")),
                "ltf_ok": bool(ltf.get("ok")),
                "event_ok": bool(ev.get("ok")),
            },
            "latency_ms": int(_now_ms() - t0),
        }

    # aliases for executor discovery
    def evaluate(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.generate_signal_package(*args, **kwargs)

    def eval_signal(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return self.generate_signal_package(*args, **kwargs)


if __name__ == "__main__":
    e = TradingEngine("config.json")
    out = e.generate_signal_package(event_timeframe="M1")
    print(json.dumps(out, ensure_ascii=False))
