"""
File: regime_switch.py
Path: C:\\Hybrid_Intelligence_Mentor\\regime_switch.py
Version: 1.0.0

Changelog:
- v1.0.0: Reusable regime switch module (ADX + BBWidth/ATR) + effective config builder.

Purpose (TH):
- ตัดสิน Regime: SIDEWAY หรือ TREND ด้วย ADX และ BBWidth/ATR
- สร้าง effective config ชั่วคราวเพื่อ "ไม่ block ตั้งแต่ sideway gate" เมื่อเป็น TREND
- ไม่แตะ validator (validator ต้อง strict เหมือนเดิม)
"""

from __future__ import annotations

import copy
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import numpy as np

try:
    import MetaTrader5 as mt5
except Exception as e:
    mt5 = None  # type: ignore


@dataclass
class RegimeDecision:
    regime: str  # "SIDEWAY" or "TREND"
    adx: float
    adx_max: float
    bb_width_atr: float
    bb_width_atr_max: float
    timeframe_minutes: int


def _tf_from_minutes(minutes: int):
    if mt5 is None:
        raise RuntimeError("MetaTrader5 not available")
    mapping = {
        1: mt5.TIMEFRAME_M1,
        5: mt5.TIMEFRAME_M5,
        15: mt5.TIMEFRAME_M15,
        30: mt5.TIMEFRAME_M30,
        60: mt5.TIMEFRAME_H1,
        240: mt5.TIMEFRAME_H4,
        1440: mt5.TIMEFRAME_D1,
    }
    return mapping.get(minutes, mt5.TIMEFRAME_M15)


def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    alpha = 1.0 / float(period)
    out = np.empty_like(tr, dtype=float)
    out[0] = float(tr[0])
    for i in range(1, len(tr)):
        out[i] = alpha * float(tr[i]) + (1 - alpha) * out[i - 1]
    return out


def _adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
    up_move = high - np.roll(high, 1)
    down_move = np.roll(low, 1) - low
    up_move[0] = 0.0
    down_move[0] = 0.0

    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr = _atr(high, low, close, period=period)
    alpha = 1.0 / float(period)

    plus_dm_sm = np.empty_like(plus_dm, dtype=float)
    minus_dm_sm = np.empty_like(minus_dm, dtype=float)
    plus_dm_sm[0] = float(plus_dm[0])
    minus_dm_sm[0] = float(minus_dm[0])
    for i in range(1, len(plus_dm)):
        plus_dm_sm[i] = alpha * float(plus_dm[i]) + (1 - alpha) * plus_dm_sm[i - 1]
        minus_dm_sm[i] = alpha * float(minus_dm[i]) + (1 - alpha) * minus_dm_sm[i - 1]

    eps = 1e-12
    plus_di = 100.0 * (plus_dm_sm / np.maximum(atr, eps))
    minus_di = 100.0 * (minus_dm_sm / np.maximum(atr, eps))
    dx = 100.0 * (np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, eps))

    adx = np.empty_like(dx, dtype=float)
    adx[0] = float(dx[0])
    for i in range(1, len(dx)):
        adx[i] = alpha * float(dx[i]) + (1 - alpha) * adx[i - 1]
    return adx


def _bb_width_atr(close: np.ndarray, atr: np.ndarray, period: int = 20, stdev_mult: float = 2.0) -> np.ndarray:
    if len(close) < period:
        return np.zeros_like(close, dtype=float)

    sma = np.convolve(close, np.ones(period) / period, mode="valid")
    std = np.zeros_like(sma, dtype=float)
    for i in range(len(sma)):
        w = close[i : i + period]
        std[i] = float(np.std(w, ddof=0))

    upper = sma + stdev_mult * std
    lower = sma - stdev_mult * std
    width = upper - lower

    pad = np.full((period - 1,), np.nan, dtype=float)
    width_full = np.concatenate([pad, width])

    eps = 1e-12
    out = width_full / np.maximum(atr, eps)
    return out


def decide_regime(cfg: Dict[str, Any], symbol: str) -> Tuple[Optional[RegimeDecision], Optional[str]]:
    """
    ใช้ TF = MTF (default M15) เพื่อประเมิน regime
    - TREND: ADX > adx_max
    - SIDEWAY: ADX <= adx_max และ BBWidth/ATR <= bb_width_atr_max (ถ้ามีค่า)
    """
    if mt5 is None:
        return None, "mt5_not_available"

    mode = cfg.get("mode", "sideway_scalp")
    profiles = cfg.get("profiles", {}) if isinstance(cfg.get("profiles", {}), dict) else {}

    # thresholds: อ่านจาก top-level sideway_scalp เป็นหลัก (ตามที่คุณยืนยันใน context)
    sideway_block = cfg.get("sideway_scalp", {}) if isinstance(cfg.get("sideway_scalp", {}), dict) else {}
    adx_max = float(sideway_block.get("adx_max", 35.0))
    bb_width_atr_max = float(sideway_block.get("bb_width_atr_max", 6.0))

    # timeframe: prefer profiles[mode].timeframes.MTF (minutes) else default 15
    tf_min = 15
    tfs = None
    if mode in profiles and isinstance(profiles[mode], dict):
        tfs = profiles[mode].get("timeframes")
    if tfs is None:
        tfs = cfg.get("timeframes")
    if isinstance(tfs, dict):
        mtf = tfs.get("MTF") or tfs.get("mtf")
        if isinstance(mtf, int):
            tf_min = mtf

    tf = _tf_from_minutes(int(tf_min))
    rates = mt5.copy_rates_from_pos(symbol, tf, 0, 250)
    if rates is None or len(rates) < 60:
        return None, "no_rates"

    high = rates["high"].astype(float)
    low = rates["low"].astype(float)
    close = rates["close"].astype(float)

    adx_arr = _adx(high, low, close, period=14)
    atr_arr = _atr(high, low, close, period=14)
    bbw_atr_arr = _bb_width_atr(close, atr_arr, period=20, stdev_mult=2.0)

    adx_val = float(adx_arr[-1])
    bbw_atr_val = float(bbw_atr_arr[-1]) if not np.isnan(bbw_atr_arr[-1]) else float(np.nan)

    if adx_val > adx_max:
        regime = "TREND"
    else:
        if np.isnan(bbw_atr_val):
            regime = "SIDEWAY"
        else:
            regime = "SIDEWAY" if bbw_atr_val <= bb_width_atr_max else "TREND"

    return (
        RegimeDecision(
            regime=regime,
            adx=adx_val,
            adx_max=adx_max,
            bb_width_atr=bbw_atr_val,
            bb_width_atr_max=bb_width_atr_max,
            timeframe_minutes=int(tf_min),
        ),
        None,
    )


def build_effective_config(cfg: Dict[str, Any], decision: RegimeDecision) -> Dict[str, Any]:
    """
    หาก TREND: override sideway gates เพื่อไม่ block ตั้งแต่ต้น
    หมายเหตุ: ไม่ใช่การเปลี่ยนกลยุทธ์ให้ sideway เทรดเทรนด์
    แต่เป็นการเปิดทางให้ engine คำนวณ candidate แล้วให้ validator/guardrails ตัดสิน
    """
    eff = copy.deepcopy(cfg)
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
        sideway_block = eff.get("sideway_scalp", {})
        if not isinstance(sideway_block, dict):
            sideway_block = {}
        sideway_block["adx_max"] = 999.0
        sideway_block["bb_width_atr_max"] = 999.0
        eff["sideway_scalp"] = sideway_block

    return eff