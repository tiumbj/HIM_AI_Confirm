"""
File: regime_switch_executor.py
Path: C:\\Hybrid_Intelligence_Mentor\\regime_switch_executor.py

HIM - Regime Switch Executor
Version: 1.0.0

Changelog:
- v1.0.0: Add deterministic regime switch (Sideway vs Trend) using ADX + BBWidth/ATR
          Writes effective config to .state/effective_config.regime.json
          Then calls engine.TradingEngine(effective_config).generate_signal_package()

Design Notes (ไทย):
- Regime (ภาวะตลาด) = Sideway หรือ Trend
- ใช้ ADX (ความแรงเทรนด์) + BBWidth/ATR (ความกว้าง band เทียบความผันผวน)
- ถ้า ADX > adx_max => ถือว่า "Trend" และจะ override gate ฝั่ง sideway เพื่อไม่บล็อกตั้งแต่ต้น
- ไม่แก้ engine.py เพื่อลด regression; ใช้ effective config ชั่วคราวแทน

Requirements:
- MetaTrader5 package
- numpy
- engine.py must expose TradingEngine(config_path)
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np

try:
    import MetaTrader5 as mt5
except Exception as e:
    print("ERROR: MetaTrader5 import failed:", e)
    sys.exit(1)


# ---------------------------
# Utilities
# ---------------------------

def _ensure_state_dir() -> str:
    state_dir = os.path.join(os.getcwd(), ".state")
    os.makedirs(state_dir, exist_ok=True)
    return state_dir


def _load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _tf_from_minutes(minutes: int) -> int:
    """
    MT5 TIMEFRAME constants typically match minutes for M1/M5/M15 etc,
    but engine/replay may shim this. For live MT5, we use official constants.
    """
    mapping = {
        1: mt5.TIMEFRAME_M1,
        2: getattr(mt5, "TIMEFRAME_M2", mt5.TIMEFRAME_M1),
        3: mt5.TIMEFRAME_M3,
        4: mt5.TIMEFRAME_M4,
        5: mt5.TIMEFRAME_M5,
        10: mt5.TIMEFRAME_M10,
        12: mt5.TIMEFRAME_M12,
        15: mt5.TIMEFRAME_M15,
        20: getattr(mt5, "TIMEFRAME_M20", mt5.TIMEFRAME_M15),
        30: mt5.TIMEFRAME_M30,
        60: mt5.TIMEFRAME_H1,
        240: mt5.TIMEFRAME_H4,
        1440: mt5.TIMEFRAME_D1,
    }
    return mapping.get(minutes, mt5.TIMEFRAME_M15)


def _copy_rates(symbol: str, timeframe, count: int) -> Optional[np.ndarray]:
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
    if rates is None:
        return None
    return rates


def _ema(arr: np.ndarray, period: int) -> np.ndarray:
    """
    EMA แบบง่าย ใช้ smoothing alpha=2/(p+1)
    """
    if len(arr) == 0:
        return arr
    alpha = 2.0 / (period + 1.0)
    out = np.empty_like(arr, dtype=float)
    out[0] = float(arr[0])
    for i in range(1, len(arr)):
        out[i] = alpha * float(arr[i]) + (1 - alpha) * out[i - 1]
    return out


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """
    ATR (Wilder-like via EMA approximation) สำหรับ regime check
    """
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    # Wilder smoothing ~ EMA alpha=1/period
    alpha = 1.0 / float(period)
    out = np.empty_like(tr, dtype=float)
    out[0] = float(tr[0])
    for i in range(1, len(tr)):
        out[i] = alpha * float(tr[i]) + (1 - alpha) * out[i - 1]
    return out


def _adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    """
    ADX (Wilder) แบบย่อสำหรับ regime check
    คืนเป็น array; ใช้ค่าท้ายสุดเป็น current ADX
    """
    up_move = high - np.roll(high, 1)
    down_move = np.roll(low, 1) - low
    up_move[0] = 0.0
    down_move[0] = 0.0

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr = _atr(high, low, close, period=period)
    # Wilder smoothing for DM
    alpha = 1.0 / float(period)
    plus_dm_sm = np.empty_like(plus_dm, dtype=float)
    minus_dm_sm = np.empty_like(minus_dm, dtype=float)
    plus_dm_sm[0] = float(plus_dm[0])
    minus_dm_sm[0] = float(minus_dm[0])
    for i in range(1, len(plus_dm)):
        plus_dm_sm[i] = alpha * float(plus_dm[i]) + (1 - alpha) * plus_dm_sm[i - 1]
        minus_dm_sm[i] = alpha * float(minus_dm[i]) + (1 - alpha) * minus_dm_sm[i - 1]

    # DI
    eps = 1e-12
    plus_di = 100.0 * (plus_dm_sm / np.maximum(atr, eps))
    minus_di = 100.0 * (minus_dm_sm / np.maximum(atr, eps))
    dx = 100.0 * (np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, eps))

    # ADX = Wilder smoothing of DX
    adx = np.empty_like(dx, dtype=float)
    adx[0] = float(dx[0])
    for i in range(1, len(dx)):
        adx[i] = alpha * float(dx[i]) + (1 - alpha) * adx[i - 1]
    return adx


def _bb_width_atr(close: np.ndarray, atr: np.ndarray, period: int = 20, stdev_mult: float = 2.0) -> np.ndarray:
    """
    BB width / ATR (normalized)
    width = (upper-lower)
    bb_width_atr = width / ATR
    """
    if len(close) < period:
        return np.zeros_like(close, dtype=float)
    # SMA
    sma = np.convolve(close, np.ones(period) / period, mode="valid")
    # Rolling std (simple; O(n*period) avoided using convolution approach is complex; keep simple for regime check)
    std = np.zeros_like(sma, dtype=float)
    for i in range(len(sma)):
        w = close[i:i + period]
        std[i] = float(np.std(w, ddof=0))
    upper = sma + stdev_mult * std
    lower = sma - stdev_mult * std
    width = upper - lower

    # align lengths
    pad = np.full((period - 1,), np.nan, dtype=float)
    width_full = np.concatenate([pad, width])

    eps = 1e-12
    out = width_full / np.maximum(atr, eps)
    return out


@dataclass
class RegimeDecision:
    regime: str  # "SIDEWAY" or "TREND"
    adx: float
    adx_max: float
    bb_width_atr: float
    bb_width_atr_max: float
    timeframe_minutes: int


def decide_regime(cfg: Dict[str, Any], symbol: str) -> Tuple[Optional[RegimeDecision], Optional[str]]:
    """
    ใช้ MTF (ค่า default M15) สำหรับ regime decision
    """
    mode = cfg.get("mode", "sideway_scalp")
    profiles = cfg.get("profiles", {})
    sideway = profiles.get(mode, {}) if isinstance(profiles, dict) else {}

    # อ่าน threshold จาก config (fallback)
    sideway_block = sideway.get("sideway_scalp", cfg.get("sideway_scalp", {}))
    adx_max = float(sideway_block.get("adx_max", 35.0))
    bb_width_atr_max = float(sideway_block.get("bb_width_atr_max", 6.0))

    # เลือก TF สำหรับ regime check (default M15)
    tf_min = 15
    tfs = sideway.get("timeframes") or cfg.get("timeframes") or {}
    if isinstance(tfs, dict):
        # prefer MTF minutes if present
        mtf = tfs.get("MTF") or tfs.get("mtf") or tfs.get("mid") or None
        if isinstance(mtf, int):
            tf_min = mtf

    tf = _tf_from_minutes(int(tf_min))

    rates = _copy_rates(symbol, tf, 250)
    if rates is None or len(rates) < 60:
        return None, "no_rates"

    high = rates["high"].astype(float)
    low = rates["low"].astype(float)
    close = rates["close"].astype(float)

    adx_arr = _adx(high, low, close, period=14)
    atr_arr = _atr(high, low, close, period=14)
    bbw_atr_arr = _bb_width_atr(close, atr_arr, period=20, stdev_mult=2.0)

    # last valid
    adx_val = float(adx_arr[-1])
    bbw_atr_val = float(bbw_atr_arr[-1]) if not np.isnan(bbw_atr_arr[-1]) else float(np.nan)

    # policy:
    # - TREND if ADX > adx_max
    # - SIDEWAY otherwise, but still require BBWidth/ATR <= max (if bbw_atr not nan)
    if adx_val > adx_max:
        regime = "TREND"
    else:
        if np.isnan(bbw_atr_val):
            regime = "SIDEWAY"
        else:
            regime = "SIDEWAY" if bbw_atr_val <= bb_width_atr_max else "TREND"

    return RegimeDecision(
        regime=regime,
        adx=adx_val,
        adx_max=adx_max,
        bb_width_atr=bbw_atr_val,
        bb_width_atr_max=bb_width_atr_max,
        timeframe_minutes=int(tf_min),
    ), None


def build_effective_config(cfg: Dict[str, Any], decision: RegimeDecision) -> Dict[str, Any]:
    """
    สร้าง effective config ชั่วคราว:
    - ถ้า TREND: override sideway gates ให้ไม่ block ตั้งแต่ต้น
      (ไม่ใช่การเปลี่ยน validator; เป็นการทำให้ engine เข้า phase คำนวณ entry/sl/tp ได้)
    """
    eff = json.loads(json.dumps(cfg))  # deep copy

    # mark decision in config for trace/debug
    eff.setdefault("runtime", {})
    eff["runtime"]["regime"] = {
        "regime": decision.regime,
        "adx": decision.adx,
        "adx_max": decision.adx_max,
        "bb_width_atr": decision.bb_width_atr,
        "bb_width_atr_max": decision.bb_width_atr_max,
        "tf_min": decision.timeframe_minutes,
        "ts": int(time.time()),
    }

    if decision.regime == "TREND":
        # override only regime gates (ไม่แตะ RR/SL bounds)
        # เพื่อให้ engine ผ่านไปถึงการสร้าง candidate แล้วค่อยให้ validator/guardrails ตัดสิน
        sideway_block = eff.get("sideway_scalp", {})
        sideway_block["adx_max"] = 999.0
        sideway_block["bb_width_atr_max"] = 999.0
        eff["sideway_scalp"] = sideway_block

    return eff


def main() -> None:
    config_path = os.path.join(os.getcwd(), "config.json")
    if not os.path.isfile(config_path):
        print("ERROR: config.json not found in current directory")
        sys.exit(1)

    cfg = _load_json(config_path)
    symbol = cfg.get("symbol", "GOLD")

    # MT5 init
    if not mt5.initialize():
        print("ERROR: MT5 initialize failed:", mt5.last_error())
        sys.exit(1)

    # ensure symbol
    info = mt5.symbol_info(symbol)
    if info is None:
        print(f"ERROR: symbol not found in MT5: {symbol}")
        mt5.shutdown()
        sys.exit(1)
    if not info.visible:
        mt5.symbol_select(symbol, True)

    decision, err = decide_regime(cfg, symbol)
    if err:
        print("REGIME_ERROR:", err)
        mt5.shutdown()
        sys.exit(1)

    print(f"REGIME_DECISION | regime={decision.regime} | tf=M{decision.timeframe_minutes} | "
          f"adx={decision.adx:.2f} (max={decision.adx_max}) | "
          f"bb_width_atr={decision.bb_width_atr:.3f} (max={decision.bb_width_atr_max})")

    eff = build_effective_config(cfg, decision)

    state_dir = _ensure_state_dir()
    eff_path = os.path.join(state_dir, "effective_config.regime.json")
    _save_json(eff_path, eff)
    print("WROTE_EFFECTIVE_CONFIG:", eff_path)

    # Call engine
    try:
        from engine import TradingEngine  # type: ignore
    except Exception as e:
        print("ERROR: cannot import engine.TradingEngine:", e)
        mt5.shutdown()
        sys.exit(1)

    try:
        eng = TradingEngine(eff_path)
        pkg = eng.generate_signal_package()
    except Exception as e:
        print("ERROR: engine call failed:", e)
        mt5.shutdown()
        sys.exit(1)

    ctx = pkg.get("context", {}) if isinstance(pkg, dict) else {}
    print("ENGINE_OUT | direction=", pkg.get("direction"))
    print("ENGINE_OUT | blocked_by=", ctx.get("blocked_by"))
    print("ENGINE_OUT | rr=", pkg.get("rr"), "min_rr=", ctx.get("min_rr"))
    print("ENGINE_OUT | entry=", pkg.get("entry_candidate"), "sl=", pkg.get("stop_candidate"), "tp=", pkg.get("tp_candidate"))

    mt5.shutdown()


if __name__ == "__main__":
    main()