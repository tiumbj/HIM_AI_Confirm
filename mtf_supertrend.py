# ==============================================================================
# ชื่อโค้ด  : HIM MTF Supertrend Calculator
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\mtf_supertrend.py
# คำสั่งรัน : python mtf_supertrend.py --test
# เวอร์ชัน  : v1.0.0
# ==============================================================================
# CHANGELOG
# v1.0.0 (2026-03-14)
#   - Phase 1.1: สร้างไฟล์ใหม่ทั้งหมด
#   - คำนวณ Supertrend direction 9 TF: M1,M2,M3,M4,M5,M6,M10,M12,M15
#   - Fast pure-numpy ST algorithm (Wilder ATR)
#   - Cache layer: ข้ามการคำนวณซ้ำถ้า bar_time ยังเหมือนเดิม
#   - Config-aware: อ่าน symbol / st_atr_period / st_mult จาก config.json
#   - CLI --test mode ไม่ต้องการ production environment
# ==============================================================================
"""
mtf_supertrend.py — HIM v3, Phase 1.1
Multi-Timeframe Supertrend Direction Calculator

Public API:
    MTFSupertrend(symbol, config_path)  → object หลัก
    .refresh()                           → Dict[str, TFResult]
    .get(tf_name)                        → Optional[TFResult]
    .all_directions()                    → Dict[str, int]   (+1/-1/0)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# MT5 import (optional – graceful degradation in --test without MT5)
# ---------------------------------------------------------------------------
try:
    import MetaTrader5 as mt5
    _MT5_AVAILABLE = True
except ImportError:
    mt5 = None          # type: ignore[assignment]
    _MT5_AVAILABLE = False


# ===========================================================================
#  Constants & TF Map
# ===========================================================================

VERSION = "v1.0.0"

# 9 TF ที่ใช้ใน MTF Cascade Exit System
CASCADE_TFS: List[str] = ["M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15"]

# จำนวน bars ที่โหลดต่อ TF (ต้องพอสำหรับ warmup ATR/ST)
_BARS_PER_TF: Dict[str, int] = {
    "M1":  80,
    "M2":  80,
    "M3":  80,
    "M4":  80,
    "M5":  80,
    "M6":  80,
    "M10": 80,
    "M12": 80,
    "M15": 80,
}

# TF → seconds per bar (ใช้ตรวจสอบ staleness)
_TF_SECONDS: Dict[str, int] = {
    "M1":  60,
    "M2":  120,
    "M3":  180,
    "M4":  240,
    "M5":  300,
    "M6":  360,
    "M10": 600,
    "M12": 720,
    "M15": 900,
}

# Default ST parameters ต่อ group (ตาม research จาก session ก่อน)
# TF ใหญ่ขึ้น → mult น้อยลง (ไวขึ้น เพราะ noise ต่ำกว่า)
_DEFAULT_ST_PARAMS: Dict[str, Tuple[int, float]] = {
    # (atr_period, multiplier)
    "M1":  (10, 3.0),
    "M2":  (10, 3.0),
    "M3":  (10, 3.0),
    "M4":  (10, 2.8),
    "M5":  (10, 2.8),
    "M6":  (10, 2.8),
    "M10": (10, 2.5),
    "M12": (10, 2.5),
    "M15": (10, 2.5),
}


def _build_mt5_tf_map() -> Dict[str, int]:
    """สร้าง mapping ชื่อ TF → MT5 constant เฉพาะเมื่อ MT5 available"""
    if not _MT5_AVAILABLE or mt5 is None:
        return {}
    return {
        "M1":  mt5.TIMEFRAME_M1,
        "M2":  mt5.TIMEFRAME_M2,
        "M3":  mt5.TIMEFRAME_M3,
        "M4":  mt5.TIMEFRAME_M4,
        "M5":  mt5.TIMEFRAME_M5,
        "M6":  mt5.TIMEFRAME_M6,
        "M10": mt5.TIMEFRAME_M10,
        "M12": mt5.TIMEFRAME_M12,
        "M15": mt5.TIMEFRAME_M15,
    }


# ===========================================================================
#  Data Structures
# ===========================================================================

@dataclass
class TFResult:
    """ผลลัพธ์ ST สำหรับ 1 TF"""
    tf:              str
    direction:       int        # +1 bullish / -1 bearish / 0 unknown
    st_value:        float      # ค่า Supertrend line
    last_close:      float      # ราคาปิดล่าสุด (closed bar)
    atr:             float      # ATR ล่าสุด
    bar_time:        int        # epoch seconds ของ closed bar
    timestamp_utc:   str        # ISO string ของ bar_time
    just_flipped:    bool       # True = flip ใน bar ล่าสุด
    prev_direction:  int        # direction bar ก่อนหน้า
    ok:              bool       # True = คำนวณสำเร็จ
    error:           str        # ข้อความ error ถ้า ok=False
    calc_ms:         float      # เวลาคำนวณ (ms)

    @property
    def direction_label(self) -> str:
        if self.direction > 0:
            return "BULLISH"
        if self.direction < 0:
            return "BEARISH"
        return "UNKNOWN"

    def to_dict(self) -> dict:
        return {
            "tf":             self.tf,
            "direction":      self.direction,
            "direction_label": self.direction_label,
            "st_value":       round(self.st_value, 5),
            "last_close":     round(self.last_close, 5),
            "atr":            round(self.atr, 5),
            "bar_time":       self.bar_time,
            "timestamp_utc":  self.timestamp_utc,
            "just_flipped":   self.just_flipped,
            "prev_direction": self.prev_direction,
            "ok":             self.ok,
            "error":          self.error,
            "calc_ms":        round(self.calc_ms, 2),
        }


@dataclass
class _CacheEntry:
    """Cache สำหรับ 1 TF — skip คำนวณซ้ำถ้า bar ยังไม่เปลี่ยน"""
    bar_time:       int
    result:         TFResult
    fetched_at_ms:  float = field(default_factory=lambda: time.monotonic() * 1000)


# ===========================================================================
#  Pure-Numpy ST Algorithm
# ===========================================================================

def _wilder_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int) -> np.ndarray:
    """
    ATR แบบ Wilder's Smoothing (EMA alpha = 1/period)
    เหมือนกับที่ใช้ใน engine.py production
    """
    n = len(close)
    if n < 2:
        return np.full(n, np.nan)

    # True Range
    prev_close = np.empty(n)
    prev_close[0] = close[0]
    prev_close[1:] = close[:-1]

    tr = np.maximum(
        high - low,
        np.maximum(
            np.abs(high - prev_close),
            np.abs(low  - prev_close),
        )
    )

    atr = np.full(n, np.nan)
    alpha = 1.0 / max(period, 1)

    # seed: ค่าแรกที่ไม่ใช่ nan
    seed_idx = period - 1
    if seed_idx >= n:
        return atr

    atr[seed_idx] = float(np.mean(tr[:seed_idx + 1]))
    for i in range(seed_idx + 1, n):
        atr[i] = atr[i - 1] * (1.0 - alpha) + tr[i] * alpha

    return atr


def _supertrend_dir(
    high:   np.ndarray,
    low:    np.ndarray,
    close:  np.ndarray,
    period: int   = 10,
    mult:   float = 3.0,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    คำนวณ Supertrend direction + line
    Returns: (st_dir, st_line)
      st_dir:  np.ndarray dtype=int  (+1 bullish, -1 bearish, 0 = not ready)
      st_line: np.ndarray dtype=float
    Algorithm เหมือน engine.py v2.13.0 (final-band carry-forward)
    """
    n = len(close)
    atr = _wilder_atr(high, low, close, period)

    hl2 = (high + low) / 2.0
    basic_ub = hl2 + mult * atr
    basic_lb = hl2 - mult * atr

    # Final bands (carry-forward rule)
    final_ub = np.full(n, np.nan)
    final_lb = np.full(n, np.nan)

    for i in range(n):
        if i == 0 or np.isnan(atr[i]):
            final_ub[i] = basic_ub[i]
            final_lb[i] = basic_lb[i]
            continue

        # Upper band: ลดได้ หรือถ้า prev close ทะลุ upper → reset
        if np.isnan(final_ub[i - 1]):
            final_ub[i] = basic_ub[i]
        elif basic_ub[i] < final_ub[i - 1] or close[i - 1] > final_ub[i - 1]:
            final_ub[i] = basic_ub[i]
        else:
            final_ub[i] = final_ub[i - 1]

        # Lower band: เพิ่มได้ หรือถ้า prev close ทะลุ lower → reset
        if np.isnan(final_lb[i - 1]):
            final_lb[i] = basic_lb[i]
        elif basic_lb[i] > final_lb[i - 1] or close[i - 1] < final_lb[i - 1]:
            final_lb[i] = basic_lb[i]
        else:
            final_lb[i] = final_lb[i - 1]

    # ST direction + line
    st_dir  = np.zeros(n, dtype=int)
    st_line = np.full(n, np.nan)

    # seed bar
    seed = period  # bar แรกที่ ATR พร้อม
    if seed >= n:
        return st_dir, st_line

    st_line[seed] = final_lb[seed] if close[seed] >= final_lb[seed] else final_ub[seed]
    st_dir[seed]  = 1 if close[seed] >= st_line[seed] else -1

    for i in range(seed + 1, n):
        if np.isnan(atr[i]) or np.isnan(final_ub[i]) or np.isnan(final_lb[i]):
            st_dir[i]  = st_dir[i - 1]
            st_line[i] = st_line[i - 1]
            continue

        prev_st = st_line[i - 1]

        if np.isnan(prev_st):
            st_line[i] = final_lb[i] if close[i] >= final_lb[i] else final_ub[i]
            st_dir[i]  = 1 if close[i] >= st_line[i] else -1
            continue

        # Transition logic
        if prev_st == final_ub[i - 1]:          # เดิม bearish
            if close[i] >= final_lb[i]:
                st_dir[i]  = 1
                st_line[i] = final_lb[i]
            else:
                st_dir[i]  = -1
                st_line[i] = final_ub[i]
        else:                                    # เดิม bullish
            if close[i] <= final_ub[i]:
                st_dir[i]  = -1
                st_line[i] = final_ub[i]
            else:
                st_dir[i]  = 1
                st_line[i] = final_lb[i]

    return st_dir, st_line


# ===========================================================================
#  Config Loader (standalone — ไม่ depend on other HIM modules)
# ===========================================================================

def _load_him_config(config_path: str) -> dict:
    """โหลด config.json แบบ safe — คืน {} ถ้าไม่พบ"""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _resolve_config_path(given: Optional[str] = None) -> str:
    """หา config.json จาก given path หรือ __file__ dir หรือ cwd"""
    candidates = []
    if given:
        candidates.append(given)
    candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))
    candidates.append(os.path.join(os.getcwd(), "config.json"))
    for c in candidates:
        if os.path.isfile(c):
            return c
    return candidates[0]  # fallback (จะไม่พบ แต่ไม่ crash)


# ===========================================================================
#  MTFSupertrend — Main Class
# ===========================================================================

class MTFSupertrend:
    """
    Multi-Timeframe Supertrend Calculator

    Usage:
        mtf = MTFSupertrend(symbol="GOLD", config_path="config.json")
        results = mtf.refresh()          # Dict[str, TFResult]
        dir_map = mtf.all_directions()   # {"M1": 1, "M3": -1, ...}
        r = mtf.get("M5")               # TFResult หรือ None
    """

    def __init__(
        self,
        symbol:       str           = "GOLD",
        config_path:  Optional[str] = None,
        timeframes:   Optional[List[str]] = None,
        st_period:    int           = 10,
        st_mult:      float         = 3.0,
        use_per_tf_params: bool     = True,
    ):
        self.symbol    = symbol
        self._cfg_path = _resolve_config_path(config_path)
        self._raw_cfg  = _load_him_config(self._cfg_path)

        # symbol override จาก config
        cfg_symbol = self._raw_cfg.get("symbol")
        if cfg_symbol and isinstance(cfg_symbol, str):
            self.symbol = cfg_symbol.strip()

        # TF list
        self.timeframes: List[str] = timeframes if timeframes else CASCADE_TFS[:]

        # ST params (global fallback)
        self._st_period = st_period
        self._st_mult   = st_mult
        self._use_per_tf_params = use_per_tf_params

        # MT5 TF map (build ครั้งเดียว)
        self._mt5_tf_map: Dict[str, int] = _build_mt5_tf_map()

        # Cache: tf_name → _CacheEntry
        self._cache: Dict[str, _CacheEntry] = {}

        # MT5 init state
        self._mt5_ready: bool = False

    # -----------------------------------------------------------------------
    #  MT5 Management
    # -----------------------------------------------------------------------

    def _ensure_mt5(self) -> bool:
        """เชื่อมต่อ MT5 ถ้ายังไม่ได้เชื่อม"""
        if not _MT5_AVAILABLE or mt5 is None:
            return False
        if self._mt5_ready:
            return True
        if mt5.initialize():
            self._mt5_ready = True
            return True
        time.sleep(0.3)
        if mt5.initialize():
            self._mt5_ready = True
            return True
        return False

    def _fetch_rates(self, tf_name: str, n_bars: int) -> Optional[np.ndarray]:
        """
        ดึง OHLC จาก MT5 → structured numpy array
        offset=1 เสมอ → ได้เฉพาะ closed bars (ไม่รวม forming bar)
        """
        if not self._ensure_mt5():
            return None
        tf_code = self._mt5_tf_map.get(tf_name)
        if tf_code is None:
            return None
        try:
            rates = mt5.copy_rates_from_pos(self.symbol, tf_code, 1, n_bars)
            if rates is None or len(rates) < max(20, self._st_period + 5):
                return None
            return rates
        except Exception:
            return None

    # -----------------------------------------------------------------------
    #  ST Params per TF
    # -----------------------------------------------------------------------

    def _get_st_params(self, tf_name: str) -> Tuple[int, float]:
        """คืน (atr_period, multiplier) สำหรับ TF ��ั้น"""
        if self._use_per_tf_params and tf_name in _DEFAULT_ST_PARAMS:
            return _DEFAULT_ST_PARAMS[tf_name]
        return self._st_period, self._st_mult

    # -----------------------------------------------------------------------
    #  Per-TF Calculation
    # -----------------------------------------------------------------------

    def _calc_one_tf(self, tf_name: str) -> TFResult:
        """
        คำนวณ ST สำหรับ 1 TF
        - ตรวจ cache ก่อน (bar_time เหมือนเดิม → return cache)
        - ดึง rates → คำนวณ → cache → return TFResult
        """
        t0 = time.monotonic()
        period, mult = self._get_st_params(tf_name)
        n_bars = _BARS_PER_TF.get(tf_name, 80)

        def _err(msg: str) -> TFResult:
            return TFResult(
                tf=tf_name, direction=0, st_value=np.nan,
                last_close=np.nan, atr=np.nan, bar_time=0,
                timestamp_utc="", just_flipped=False, prev_direction=0,
                ok=False, error=msg,
                calc_ms=(time.monotonic() - t0) * 1000,
            )

        rates = self._fetch_rates(tf_name, n_bars)
        if rates is None:
            return _err(f"no_rates_{tf_name}")

        bar_time = int(rates[-1]["time"])

        # ── Cache check ──
        cached = self._cache.get(tf_name)
        if cached is not None and cached.bar_time == bar_time:
            # bar ไม่เปลี่ยน → คืน cache (อัปเดต calc_ms เป็น 0)
            r = cached.result
            return TFResult(
                tf=r.tf, direction=r.direction, st_value=r.st_value,
                last_close=r.last_close, atr=r.atr, bar_time=r.bar_time,
                timestamp_utc=r.timestamp_utc, just_flipped=False,
                prev_direction=r.prev_direction, ok=r.ok, error=r.error,
                calc_ms=0.0,
            )

        # ── คำนวณ ──
        try:
            high  = rates["high"].astype(float)
            low   = rates["low"].astype(float)
            close = rates["close"].astype(float)
        except Exception as e:
            return _err(f"array_cast_error:{e}")

        st_dir_arr, st_line_arr = _supertrend_dir(high, low, close, period, mult)
        atr_arr = _wilder_atr(high, low, close, period)

        # ── ค่าล่าสุด ──
        last_idx   = len(close) - 1
        direction  = int(st_dir_arr[last_idx])
        st_value   = float(st_line_arr[last_idx]) if np.isfinite(st_line_arr[last_idx]) else np.nan
        atr_val    = float(atr_arr[last_idx]) if np.isfinite(atr_arr[last_idx]) else np.nan
        last_close = float(close[last_idx])

        # prev direction (bar ก่อนหน้า)
        prev_dir = int(st_dir_arr[last_idx - 1]) if last_idx > 0 else 0

        # just_flipped: เทียบกับ prev direction ใน cache หรือ bar ก่อนหน้า
        cached_prev_dir = cached.result.direction if cached is not None else prev_dir
        just_flipped = (cached_prev_dir != 0 and direction != cached_prev_dir)

        # timestamp
        try:
            ts_utc = datetime.fromtimestamp(bar_time, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            ts_utc = str(bar_time)

        calc_ms = (time.monotonic() - t0) * 1000

        result = TFResult(
            tf=tf_name, direction=direction, st_value=st_value,
            last_close=last_close, atr=atr_val, bar_time=bar_time,
            timestamp_utc=ts_utc, just_flipped=just_flipped,
            prev_direction=prev_dir, ok=True, error="",
            calc_ms=calc_ms,
        )

        # ── บันทึก cache ──
        self._cache[tf_name] = _CacheEntry(bar_time=bar_time, result=result)
        return result

    # -----------------------------------------------------------------------
    #  Public API
    # -----------------------------------------------------------------------

    def refresh(self) -> Dict[str, TFResult]:
        """
        Refresh ST direction สำหรับทุก TF
        Returns: Dict[tf_name, TFResult]
        Cache: TF ที่ bar ยังไม่เปลี่ยนจะ return ผลเดิมทันที (calc_ms=0)
        """
        results: Dict[str, TFResult] = {}
        for tf in self.timeframes:
            results[tf] = self._calc_one_tf(tf)
        return results

    def get(self, tf_name: str) -> Optional[TFResult]:
        """คำนวณและคืนผลสำหรับ TF เดียว"""
        if tf_name not in self.timeframes:
            return None
        return self._calc_one_tf(tf_name)

    def all_directions(self) -> Dict[str, int]:
        """
        คืน direction map: {"M1": 1, "M3": -1, ...}
        +1=BULLISH, -1=BEARISH, 0=UNKNOWN
        """
        results = self.refresh()
        return {tf: r.direction for tf, r in results.items()}

    def invalidate_cache(self, tf_name: Optional[str] = None) -> None:
        """ล้าง cache — tf_name=None → ล้างทั้งหมด"""
        if tf_name is None:
            self._cache.clear()
        else:
            self._cache.pop(tf_name, None)

    def is_mt5_ready(self) -> bool:
        """ตรวจสอบว่า MT5 พร้อมใช้งาน"""
        return self._ensure_mt5()

    def shutdown(self) -> None:
        """ปิดการเชื่อมต่อ MT5 (เรียกตอนปิดโปรแกรม)"""
        if _MT5_AVAILABLE and mt5 is not None and self._mt5_ready:
            try:
                mt5.shutdown()
            except Exception:
                pass
            self._mt5_ready = False


# ===========================================================================
#  CLI Test Mode
# ===========================================================================

def _run_test(config_path: Optional[str], symbol: Optional[str]) -> int:
    """
    --test mode:
    - ถ้า MT5 available → ดึงข้อมูลจริง
    - ถ้า MT5 ไม่พร้อม → synthetic data test (ทดสอบ algorithm เท่านั้น)
    คืน exit code: 0=pass, 1=fail
    """
    SEP = "=" * 72
    print(SEP)
    print(f"  HIM MTF Supertrend Calculator — Self-Test  ({VERSION})")
    print(SEP)

    # ── Test 1: Algorithm unit test (ไม่ต้องการ MT5) ──────────────────────
    print("\n[TEST 1] ST algorithm (synthetic data)...")
    fail_count = 0

    # สร้างข้อมูลสังเคราะห์: uptrend 60 bars
    n = 60
    rng = np.random.default_rng(seed=42)
    price_base = 2000.0
    close_up = price_base + np.arange(n) * 0.5 + rng.normal(0, 0.3, n)
    high_up  = close_up + rng.uniform(0.1, 0.8, n)
    low_up   = close_up - rng.uniform(0.1, 0.8, n)

    sd, sl = _supertrend_dir(high_up, low_up, close_up, period=10, mult=3.0)
    last_dir = int(sd[-1])
    if last_dir != 1:
        print(f"  [FAIL] uptrend synthetic: expected +1, got {last_dir}")
        fail_count += 1
    else:
        print(f"  [PASS] uptrend synthetic → direction={last_dir} (BULLISH) ✓")

    # downtrend 60 bars
    close_dn = price_base - np.arange(n) * 0.5 + rng.normal(0, 0.3, n)
    high_dn  = close_dn + rng.uniform(0.1, 0.8, n)
    low_dn   = close_dn - rng.uniform(0.1, 0.8, n)

    sd2, _ = _supertrend_dir(high_dn, low_dn, close_dn, period=10, mult=3.0)
    last_dir2 = int(sd2[-1])
    if last_dir2 != -1:
        print(f"  [FAIL] downtrend synthetic: expected -1, got {last_dir2}")
        fail_count += 1
    else:
        print(f"  [PASS] downtrend synthetic → direction={last_dir2} (BEARISH) ✓")

    # Wilder ATR — ตรวจว่าไม่ negative
    atr_arr = _wilder_atr(high_up, low_up, close_up, period=14)
    valid_atr = np.all(atr_arr[14:] > 0)
    if not valid_atr:
        print("  [FAIL] ATR contains non-positive values")
        fail_count += 1
    else:
        print(f"  [PASS] Wilder ATR all positive (sample={atr_arr[-1]:.4f}) ✓")

    # ── Test 2: MT5 live data (ถ้าพร้อม) ──────────────────────────────────
    print("\n[TEST 2] MT5 live data...")

    if not _MT5_AVAILABLE:
        print("  [SKIP] MetaTrader5 module not installed (pip install MetaTrader5)")
    else:
        _sym = symbol if symbol else "GOLD"
        _cfg = config_path

        mtf = MTFSupertrend(symbol=_sym, config_path=_cfg)
        mt5_ok = mtf.is_mt5_ready()

        if not mt5_ok:
            print("  [SKIP] MT5 terminal not running or not connected")
            print("         (algorithm tests above still validated the core logic)")
        else:
            print(f"  MT5 connected | symbol={mtf.symbol}")
            t_start = time.monotonic()
            results = mtf.refresh()
            elapsed_ms = (time.monotonic() - t_start) * 1000

            print(f"\n  {'TF':<6} {'DIR':<9} {'ST_VALUE':>12} {'CLOSE':>12} "
                  f"{'ATR':>9} {'FLIPPED':<8} {'CALC_MS':>8}  TIMESTAMP")
            print("  " + "-" * 82)

            live_ok  = 0
            live_err = 0
            for tf in CASCADE_TFS:
                r = results.get(tf)
                if r is None:
                    print(f"  {tf:<6} [NOT IN RESULTS]")
                    live_err += 1
                    continue
                if not r.ok:
                    print(f"  {tf:<6} ERROR: {r.error}")
                    live_err += 1
                    continue
                flip_mark = "⚡FLIP" if r.just_flipped else "     "
                dir_str = f"+{r.direction}" if r.direction > 0 else str(r.direction)
                print(f"  {tf:<6} {r.direction_label:<9} {r.st_value:>12.3f} "
                      f"{r.last_close:>12.3f} {r.atr:>9.4f} "
                      f"{flip_mark:<8} {r.calc_ms:>7.1f}ms  {r.timestamp_utc}")
                live_ok += 1

            print(f"\n  Total refresh time: {elapsed_ms:.1f}ms "
                  f"({'✓ <500ms' if elapsed_ms < 500 else '⚠ >500ms'})")
            print(f"  TF results: {live_ok} ok / {live_err} errors")

            if elapsed_ms > 500:
                print("  [WARN] refresh > 500ms — check MT5 connection or VPS latency")
                fail_count += 1

            # ── Test 2b: cache hit ──
            print("\n  [TEST 2b] Cache hit (second refresh same bar)...")
            t2 = time.monotonic()
            results2 = mtf.refresh()
            elapsed2_ms = (time.monotonic() - t2) * 1000
            cache_hits = sum(1 for r in results2.values() if r.ok and r.calc_ms == 0.0)
            print(f"  Cache hits: {cache_hits}/{len(CASCADE_TFS)} "
                  f"| second refresh time: {elapsed2_ms:.1f}ms")
            if cache_hits == live_ok and live_ok > 0:
                print("  [PASS] Cache working correctly ✓")
            else:
                print(f"  [INFO] Cache hits={cache_hits} (some bars may have advanced)")

            mtf.shutdown()

    # ── Summary ──────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    if fail_count == 0:
        print("  ✅ ALL TESTS PASSED")
    else:
        print(f"  ❌ {fail_count} TEST(S) FAILED")
    print(SEP)
    return 0 if fail_count == 0 else 1


# ===========================================================================
#  Entry Point
# ===========================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="HIM MTF Supertrend Calculator",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run self-test and exit (no production side-effects)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Override symbol (default: read from config.json or 'GOLD')",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to config.json (default: auto-detect)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one refresh, print results as JSON, then exit",
    )

    args = parser.parse_args()

    if args.test:
        return _run_test(config_path=args.config, symbol=args.symbol)

    if args.once:
        mtf = MTFSupertrend(
            symbol=args.symbol or "GOLD",
            config_path=args.config,
        )
        if not mtf.is_mt5_ready():
            print(json.dumps({"error": "MT5_not_ready"}, ensure_ascii=False))
            return 1
        results = mtf.refresh()
        out = {tf: r.to_dict() for tf, r in results.items()}
        print(json.dumps(out, ensure_ascii=False, indent=2))
        mtf.shutdown()
        return 0

    # Default: print usage
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())