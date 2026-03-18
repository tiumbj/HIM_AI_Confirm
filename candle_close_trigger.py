# ==============================================================================
# ชื่อโค้ด  : HIM Candle Close Trigger
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\candle_close_trigger.py
# คำสั่งรัน : python candle_close_trigger.py --test
# เวอร์ชัน  : v1.0.0
# ==============================================================================
# CHANGELOG
# v1.0.0 (2026-03-14)
#   - Phase 1.3: สร้างไฟล์ใหม่ทั้งหมด
#   - ตรวจจับ M1 bar ปิด → invoke callback ครั้งเดียวต่อ bar
#   - Latency target < 200ms จาก bar close → callback
#   - CPU idle ต่ำ: adaptive sleep หลีกเลี่ยง busy-wait
#   - Standalone — ไม่แก้ไฟล์อื่นใดทั้งสิ้น
#   - รองรับ multi-TF (M1–M15) สำหรับ future Phase 2 integration
#   - --test mode แสดง log ทุก bar close + latency measurement
# ==============================================================================
"""
candle_close_trigger.py — HIM v3, Phase 1.3

Candle-Close Event Trigger

วัตถุประสงค์:
  ตรวจจับการปิดของ M1 bar (หรือ TF ที่กำหนด) แล้วเรียก callback
  เพื่อใช้แทน time.sleep(poll_interval) ใน mentor_executor.loop() ใน Phase 2

Public API:
  CandleCloseTrigger(symbol, timeframe, on_new_candle, ...)
    .run()              → blocking loop (รัน until stop())
    .run_async()        → เริ่ม background daemon thread
    .stop()             → หยุด loop อย่างปลอดภัย
    .last_bar_time      → epoch int ของ bar ที่ปิดล่าสุด
    .callback_count     → จำนวนครั้งที่ callback ถูกเรียก

  NewBarEvent (dataclass) → ข้อมูลที่ส่งไปกับ callback:
    .timeframe, .bar_time, .bar_time_utc,
    .open, .high, .low, .close, .volume,
    .latency_ms         → เวลา (ms) จาก bar close ถึง callback

Design:
  - ใช้ adaptive sleep เพื่อลด CPU:
      ช่วงไกลจาก bar close → sleep นาน (0.5–2.0s)
      ช่วงใกล้ bar close   → sleep สั้น (0.05–0.1s)
  - ดึงข้อมูล closed bar เท่านั้น (copy_rates_from_pos offset=1)
  - ตรวจ bar_time เปลี่ยน → callback exactly once per bar
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

# ---------------------------------------------------------------------------
# MT5 import (optional — graceful degradation ใน --test ไม่มี MT5)
# ---------------------------------------------------------------------------
try:
    import MetaTrader5 as mt5
    _MT5_AVAILABLE = True
except ImportError:
    mt5 = None          # type: ignore[assignment]
    _MT5_AVAILABLE = False


# ===========================================================================
#  Version & Constants
# ===========================================================================

VERSION = "v1.0.0"

# TF ที่ MT5 รองรับ (ใช้สำหรับ validate input)
_MT5_TF_SECONDS: Dict[str, int] = {
    "M1":  60,
    "M2":  120,
    "M3":  180,
    "M4":  240,
    "M5":  300,
    "M6":  360,
    "M10": 600,
    "M12": 720,
    "M15": 900,
    "M20": 1200,
    "M30": 1800,
    "H1":  3600,
    "H4":  14400,
    "D1":  86400,
}

# Timing constants
_POLL_NEAR_SEC:   float = 0.02   # polling interval เมื่อใกล้ bar close (< 2s)
_POLL_NORMAL_SEC: float = 0.10   # polling interval ปกติ (< 10s)
_POLL_FAR_SEC:    float = 0.5    # polling interval เมื่อห่างจาก bar close (> 10s)
_NEAR_THRESHOLD:  float = 2.0    # วินาทีที่ถือว่า "ใกล้" bar close
_FAR_THRESHOLD:   float = 10.0   # วินาทีที่ถือว่า "ไกล" bar close

# MT5 init retry
_MT5_INIT_RETRIES: int   = 3
_MT5_INIT_DELAY:   float = 0.5


# ===========================================================================
#  Logger
# ===========================================================================

def _get_logger(name: str = "candle_close_trigger") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(fmt)
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger


_logger = _get_logger()


# ===========================================================================
#  Data Structures
# ===========================================================================

@dataclass
class NewBarEvent:
    """
    ข้อมูลที่ส่งไปพร้อม callback ทุกครั้งที่ bar ใหม่ปิด
    latency_ms = เวลา (ms) จาก bar close time (theo) ถึง callback invoke
    """
    timeframe:    str
    bar_time:     int           # epoch seconds (UTC) ของ bar ที่เพิ่งปิด
    bar_time_utc: str           # ISO 8601 string
    open:         float
    high:         float
    low:          float
    close:        float
    volume:       int
    latency_ms:   float         # ms จาก theoretical close ถึง callback
    sequence:     int           # ลำดับ bar ที่ trigger นับจาก start

    def to_dict(self) -> dict:
        return {
            "timeframe":    self.timeframe,
            "bar_time":     self.bar_time,
            "bar_time_utc": self.bar_time_utc,
            "open":         round(self.open,  5),
            "high":         round(self.high,  5),
            "low":          round(self.low,   5),
            "close":        round(self.close, 5),
            "volume":       self.volume,
            "latency_ms":   round(self.latency_ms, 2),
            "sequence":     self.sequence,
        }


# Callback type: รับ NewBarEvent → คืน None (หรือ Any)
OnNewCandleCallback = Callable[[NewBarEvent], None]


# ===========================================================================
#  MT5 TF Map Builder
# ===========================================================================

def _build_mt5_tf_map() -> Dict[str, int]:
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
        "M20": mt5.TIMEFRAME_M20,
        "M30": mt5.TIMEFRAME_M30,
        "H1":  mt5.TIMEFRAME_H1,
        "H4":  mt5.TIMEFRAME_H4,
        "D1":  mt5.TIMEFRAME_D1,
    }


# ===========================================================================
#  MT5 Server Time Helper
# ===========================================================================

def _server_time_epoch() -> int:
    """
    คืน MT5 server time (epoch seconds)
    Fallback → local time ถ้า MT5 ไม่พร้อม
    """
    if not _MT5_AVAILABLE or mt5 is None:
        return int(time.time())
    try:
        fn = getattr(mt5, "time_current", None)
        if callable(fn):
            t = fn()
            if t and t > 0:
                return int(t)
    except Exception:
        pass
    return int(time.time())


# ===========================================================================
#  CandleCloseTrigger — Main Class
# ===========================================================================

class CandleCloseTrigger:
    """
    ตรวจจับ bar ใหม่ปิดและเรียก callback ครั้งเดียวต่อ bar

    Parameters
    ----------
    symbol        : str  — ชื่อ symbol (เช่น "GOLD", "XAUUSD")
    timeframe     : str  — TF ที่ต้องการตรวจ (default "M1")
    on_new_candle : callback ที่รับ NewBarEvent
    check_interval_ms : int  — override polling interval (ms), 0 = adaptive
    max_errors    : int  — หยุด loop ถ้า error ติดกันเกินค่านี้ (0 = ไม่หยุด)
    error_callback: callback สำหรับ error events (optional)

    ตัวอย่าง:
        def my_callback(event: NewBarEvent):
            print(f"New M1 bar closed at {event.bar_time_utc} | close={event.close}")

        trigger = CandleCloseTrigger("GOLD", "M1", on_new_candle=my_callback)
        trigger.run()               # blocking
        # หรือ
        t = trigger.run_async()     # non-blocking (daemon thread)
        ...
        trigger.stop()
    """

    def __init__(
        self,
        symbol:            str,
        timeframe:         str                            = "M1",
        on_new_candle:     Optional[OnNewCandleCallback] = None,
        check_interval_ms: int                           = 0,
        max_errors:        int                           = 0,
        error_callback:    Optional[Callable[[Exception], None]] = None,
    ) -> None:
        self.symbol    = symbol.strip()
        self.timeframe = timeframe.strip().upper()

        if self.timeframe not in _MT5_TF_SECONDS:
            raise ValueError(
                f"Unsupported timeframe '{timeframe}'. "
                f"Supported: {', '.join(sorted(_MT5_TF_SECONDS.keys()))}"
            )

        self._on_new_candle = on_new_candle
        self._check_interval_ms = check_interval_ms
        self._max_errors        = max_errors
        self._error_callback    = error_callback

        # MT5
        self._mt5_tf_map: Dict[str, int] = _build_mt5_tf_map()
        self._mt5_ready = False

        # State
        self._last_bar_time: int   = 0
        self._sequence:      int   = 0
        self._running:       bool  = False
        self._thread: Optional[threading.Thread] = None

        # Stats
        self._callback_count:   int   = 0
        self._error_count_seq:  int   = 0    # consecutive errors
        self._total_errors:     int   = 0
        self._latency_samples:  List[float] = []

        _logger.info(
            "CandleCloseTrigger init | symbol=%s tf=%s version=%s",
            self.symbol, self.timeframe, VERSION,
        )

    # -----------------------------------------------------------------------
    #  Properties
    # -----------------------------------------------------------------------

    @property
    def last_bar_time(self) -> int:
        return self._last_bar_time

    @property
    def callback_count(self) -> int:
        return self._callback_count

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def avg_latency_ms(self) -> float:
        if not self._latency_samples:
            return 0.0
        return sum(self._latency_samples) / len(self._latency_samples)

    @property
    def tf_seconds(self) -> int:
        return _MT5_TF_SECONDS.get(self.timeframe, 60)

    # -----------------------------------------------------------------------
    #  MT5 Management
    # -----------------------------------------------------------------------

    def _ensure_mt5(self) -> bool:
        if not _MT5_AVAILABLE or mt5 is None:
            return False
        if self._mt5_ready:
            return True
        for attempt in range(_MT5_INIT_RETRIES):
            try:
                if mt5.initialize():
                    self._mt5_ready = True
                    return True
            except Exception:
                pass
            if attempt < _MT5_INIT_RETRIES - 1:
                time.sleep(_MT5_INIT_DELAY)
        return False

    def _fetch_last_closed_bar(self) -> Optional[dict]:
        """
        ดึง bar ที่ปิดล่าสุด (offset=1 เสมอ — ไม่รวม forming bar)
        Returns dict: {time, open, high, low, close, tick_volume} หรือ None
        """
        if not self._ensure_mt5():
            return None
        tf_code = self._mt5_tf_map.get(self.timeframe)
        if tf_code is None:
            return None
        try:
            rates = mt5.copy_rates_from_pos(self.symbol, tf_code, 1, 1)
            if rates is None or len(rates) == 0:
                return None
            r = rates[0]
            return {
                "time":        int(r["time"]),
                "open":        float(r["open"]),
                "high":        float(r["high"]),
                "low":         float(r["low"]),
                "close":       float(r["close"]),
                "tick_volume": int(r.get("tick_volume", r.get("volume", 0))),
            }
        except Exception as e:
            _logger.debug("fetch_bar_error: %s", e)
            return None

    # -----------------------------------------------------------------------
    #  Adaptive Sleep Calculation
    # -----------------------------------------------------------------------

    def _adaptive_sleep(self, server_time: int) -> float:
        """
        คำนวณเวลา sleep ที่เหมาะสมตามตำแหน่งใน bar cycle

        Logic:
          seconds_into_bar = server_time % tf_seconds
          seconds_to_close = tf_seconds - seconds_into_bar

          ถ้า seconds_to_close ≤ NEAR_THRESHOLD  → poll เร็ว (POLL_NEAR_SEC)
          ถ้า seconds_to_close ≤ FAR_THRESHOLD   → poll ปานกลาง (POLL_NORMAL_SEC)
          ถ้า seconds_to_close > FAR_THRESHOLD   → poll ช้า (POLL_FAR_SEC)

        Override: ถ้า _check_interval_ms > 0 → ใช้ค่านั้นคงที่
        """
        if self._check_interval_ms > 0:
            return self._check_interval_ms / 1000.0

        tf_sec = self.tf_seconds
        sec_into_bar  = server_time % tf_sec
        sec_to_close  = tf_sec - sec_into_bar

        if sec_to_close <= _NEAR_THRESHOLD:
            return _POLL_NEAR_SEC
        if sec_to_close <= _FAR_THRESHOLD:
            return _POLL_NORMAL_SEC
        return _POLL_FAR_SEC

    # -----------------------------------------------------------------------
    #  Latency Calculation
    # -----------------------------------------------------------------------

    def _calc_latency_ms(self, bar_time: int) -> float:
        """
        คำนวณ latency จาก theoretical bar close time ถึงปัจจุบัน
        bar close เกิดที่ bar_time + tf_seconds (เวลาที่ bar นั้นควรจะปิดสมบูรณ์)
        """
        theoretical_close = bar_time + self.tf_seconds
        now_ms = time.time() * 1000.0
        close_ms = theoretical_close * 1000.0
        return max(0.0, now_ms - close_ms)

    # -----------------------------------------------------------------------
    #  Callback Invocation
    # -----------------------------------------------------------------------

    def _invoke_callback(self, bar_data: dict) -> None:
        """
        สร้าง NewBarEvent แล้วเรียก on_new_candle callback
        wrap ด้วย try/except ป้องกัน exception ใน callback ทำให้ loop หยุด
        """
        bar_time   = bar_data["time"]
        latency_ms = self._calc_latency_ms(bar_time)
        self._sequence += 1

        try:
            ts_utc = datetime.fromtimestamp(bar_time, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        except Exception:
            ts_utc = str(bar_time)

        event = NewBarEvent(
            timeframe=self.timeframe,
            bar_time=bar_time,
            bar_time_utc=ts_utc,
            open=bar_data["open"],
            high=bar_data["high"],
            low=bar_data["low"],
            close=bar_data["close"],
            volume=bar_data["tick_volume"],
            latency_ms=latency_ms,
            sequence=self._sequence,
        )

        self._callback_count += 1
        self._latency_samples.append(latency_ms)
        if len(self._latency_samples) > 100:
            self._latency_samples.pop(0)

        _logger.info(
            "NEW_BAR | tf=%s seq=%d bar_utc=%s close=%.3f latency=%.1fms",
            self.timeframe, self._sequence, ts_utc, event.close, latency_ms,
        )

        if latency_ms > 500:
            _logger.warning(
                "HIGH_LATENCY | seq=%d latency=%.1fms (target<200ms) "
                "— check MT5 connection / VPS latency",
                self._sequence, latency_ms,
            )

        if self._on_new_candle is not None:
            try:
                self._on_new_candle(event)
            except Exception as e:
                _logger.error(
                    "CALLBACK_ERROR | seq=%d | %s: %s",
                    self._sequence, type(e).__name__, e,
                )

    # -----------------------------------------------------------------------
    #  Main Loop
    # -----------------------------------------------------------------------

    def run(self) -> None:
        """
        Blocking loop — ทำงานจนกว่า stop() จะถูกเรียก
        เรียก on_new_candle callback ครั้งเดียวต่อ bar ที่ปิด
        """
        self._running = True
        _logger.info(
            "CandleCloseTrigger started | symbol=%s tf=%s tf_sec=%ds",
            self.symbol, self.timeframe, self.tf_seconds,
        )

        while self._running:
            try:
                server_time = _server_time_epoch()
                bar = self._fetch_last_closed_bar()

                if bar is None:
                    # MT5 ไม่พร้อม → sleep แล้ว retry
                    _logger.debug("bar_fetch_none | sleeping 1.0s")
                    time.sleep(1.0)
                    self._error_count_seq += 1
                    self._total_errors    += 1
                    if (
                        self._max_errors > 0
                        and self._error_count_seq >= self._max_errors
                    ):
                        _logger.error(
                            "MAX_ERRORS_REACHED | consecutive=%d — stopping",
                            self._error_count_seq,
                        )
                        self._running = False
                    continue

                self._error_count_seq = 0
                new_bar_time = bar["time"]

                # ─── ตรวจว่า bar_time เปลี่ยนหรือไม่ ───
                if new_bar_time != self._last_bar_time:
                    # บันทึก state ก่อน invoke (ป้องกัน double-fire ถ้า callback ช้า)
                    self._last_bar_time = new_bar_time
                    self._invoke_callback(bar)

                # ─── Adaptive sleep ───
                sleep_sec = self._adaptive_sleep(server_time)
                time.sleep(sleep_sec)

            except Exception as e:
                self._error_count_seq += 1
                self._total_errors    += 1
                _logger.error(
                    "LOOP_ERROR | consecutive=%d | %s: %s",
                    self._error_count_seq, type(e).__name__, e,
                )
                if self._error_callback is not None:
                    try:
                        self._error_callback(e)
                    except Exception:
                        pass
                if (
                    self._max_errors > 0
                    and self._error_count_seq >= self._max_errors
                ):
                    _logger.error("MAX_ERRORS_REACHED — stopping loop")
                    self._running = False
                else:
                    time.sleep(1.0)

        _logger.info(
            "CandleCloseTrigger stopped | total_callbacks=%d avg_latency=%.1fms errors=%d",
            self._callback_count, self.avg_latency_ms, self._total_errors,
        )

    def run_async(self) -> threading.Thread:
        """
        เริ่ม run() เป็น daemon thread
        Returns thread handle
        """
        if self._running:
            raise RuntimeError("CandleCloseTrigger already running")
        t = threading.Thread(
            target=self.run,
            name=f"candle_trigger_{self.symbol}_{self.timeframe}",
            daemon=True,
        )
        self._thread = t
        t.start()
        return t

    def stop(self) -> None:
        """หยุด loop อย่างปลอดภัย (non-blocking)"""
        self._running = False
        _logger.info("CandleCloseTrigger stop requested")

    def wait(self, timeout: float = 5.0) -> None:
        """รอให้ thread หยุด (ใช้หลัง stop() ถ้าต้องการ)"""
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def stats(self) -> dict:
        """คืน stats ปัจจุบัน"""
        return {
            "symbol":           self.symbol,
            "timeframe":        self.timeframe,
            "is_running":       self._running,
            "last_bar_time":    self._last_bar_time,
            "callback_count":   self._callback_count,
            "avg_latency_ms":   round(self.avg_latency_ms, 2),
            "total_errors":     self._total_errors,
            "consecutive_errors": self._error_count_seq,
            "mt5_ready":        self._mt5_ready,
        }


# ===========================================================================
#  Synthetic Bar Generator (ใช้ใน --test โดยไม่ต้องการ MT5)
# ===========================================================================

class _SyntheticBarSource:
    """
    จำลอง MT5 copy_rates_from_pos สำหรับ --test mode
    สร้าง bar สังเคราะห์ทุก tf_seconds วินาที (wall-clock จริง)
    ใช้ M1_FAST_SEC เพื่อเร่งเวลาในการทดสอบ
    """

    def __init__(self, tf_seconds: int, fast_sec: float = 5.0) -> None:
        self._tf_sec  = tf_seconds
        self._fast_sec = fast_sec    # M1 จำลองปิดทุก fast_sec วินาที
        self._last_time: int = 0
        self._base_price: float = 2320.0
        self._t_start   = time.monotonic()
        import random as _random
        self._rng = _random

    def _current_bar_time(self) -> int:
        """คืน bar_time ของ bar ปัจจุบันที่ปิดแล้ว (ตาม fast clock)"""
        elapsed  = time.monotonic() - self._t_start
        bar_idx  = int(elapsed / self._fast_sec)
        # bar_idx * fast_sec = เวลาที่ bar นั้นเริ่ม → bar_time = epoch_start + bar_idx * fast
        epoch_base = int(time.time()) - int(elapsed)
        return epoch_base + int(bar_idx * self._fast_sec)

    def fetch(self) -> Optional[dict]:
        bar_time = self._current_bar_time()
        drift   = self._rng.uniform(-0.5, 0.5)
        _open   = self._base_price + drift
        _close  = _open + self._rng.uniform(-1.0, 1.0)
        _high   = max(_open, _close) + abs(self._rng.gauss(0, 0.3))
        _low    = min(_open, _close) - abs(self._rng.gauss(0, 0.3))
        self._base_price = _close
        return {
            "time":        bar_time,
            "open":        round(_open,  3),
            "high":        round(_high,  3),
            "low":         round(_low,   3),
            "close":       round(_close, 3),
            "tick_volume": self._rng.randint(80, 300),
        }


class _SyntheticCandleCloseTrigger(CandleCloseTrigger):
    """
    Subclass ของ CandleCloseTrigger ที่ override _fetch_last_closed_bar
    สำหรับ --test mode เมื่อ MT5 ไม่พร้อม
    """

    def __init__(self, *args, fast_sec: float = 5.0, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._synthetic = _SyntheticBarSource(
            tf_seconds=self.tf_seconds, fast_sec=fast_sec
        )
        _logger.info(
            "SyntheticMode | bar interval=%.1fs (M1 fast-mode)", fast_sec
        )

    def _fetch_last_closed_bar(self) -> Optional[dict]:
        return self._synthetic.fetch()

    def _adaptive_sleep(self, server_time: int) -> float:
        # ใน synthetic mode → poll เร็วขึ้น
        return 0.1

    def _calc_latency_ms(self, bar_time: int) -> float:
        # latency สังเคราะห์ = เวลา callback - bar_time สร้าง (ms)
        return (time.time() - bar_time) * 1000.0 if bar_time < time.time() else 0.0


# ===========================================================================
#  CLI Test Mode
# ===========================================================================

def _run_test(symbol: str, timeframe: str, duration_sec: int, fast_sec: float) -> int:
    SEP = "=" * 72
    print(SEP)
    print(f"  HIM Candle Close Trigger — Self-Test  ({VERSION})")
    print(f"  symbol={symbol}  tf={timeframe}  duration={duration_sec}s")
    print(SEP)

    fail_count = 0

    # ─── Unit Test 1: Constructor validation ────────────────────────────
    print("\n[TEST 1] Constructor validation...")

    try:
        CandleCloseTrigger("GOLD", "INVALID_TF")
        print("  [FAIL] Should raise ValueError for invalid TF")
        fail_count += 1
    except ValueError as e:
        print(f"  [PASS] Invalid TF rejected: {e} ✓")

    try:
        t = CandleCloseTrigger("GOLD", "M1")
        assert t.timeframe == "M1"
        assert t.tf_seconds == 60
        assert t.callback_count == 0
        assert t.last_bar_time == 0
        print("  [PASS] Valid constructor ✓")
    except Exception as e:
        print(f"  [FAIL] Constructor error: {e}")
        fail_count += 1

    # ─── Unit Test 2: Adaptive sleep calculation ─────────────────────────
    print("\n[TEST 2] Adaptive sleep calculation...")

    trig = CandleCloseTrigger("GOLD", "M1")

    # M1 = 60s bar. server_time % 60 = 55 → sec_to_close = 5 → NORMAL range
    server_near = (int(time.time()) // 60) * 60 + 58    # sec_into_bar=58, to_close=2 → NEAR
    server_mid  = (int(time.time()) // 60) * 60 + 50    # sec_into_bar=50, to_close=10 → NORMAL
    server_far  = (int(time.time()) // 60) * 60 + 5     # sec_into_bar=5,  to_close=55 → FAR

    sleep_near   = trig._adaptive_sleep(server_near)
    sleep_mid    = trig._adaptive_sleep(server_mid)
    sleep_far    = trig._adaptive_sleep(server_far)

    tests_ok = True
    if sleep_near > _NEAR_THRESHOLD:
        print(f"  [FAIL] Near sleep too long: {sleep_near:.3f}s")
        fail_count += 1
        tests_ok = False
    if not (_POLL_NEAR_SEC <= sleep_mid <= _POLL_FAR_SEC):
        print(f"  [FAIL] Mid sleep out of range: {sleep_mid:.3f}s")
        fail_count += 1
        tests_ok = False
    if sleep_far < _POLL_NORMAL_SEC:
        print(f"  [FAIL] Far sleep too short: {sleep_far:.3f}s")
        fail_count += 1
        tests_ok = False

    if tests_ok:
        print(
            f"  [PASS] near={sleep_near:.3f}s  mid={sleep_mid:.3f}s  far={sleep_far:.3f}s ✓"
        )

    # override test
    trig_fixed = CandleCloseTrigger("GOLD", "M1", check_interval_ms=150)
    fixed_sleep = trig_fixed._adaptive_sleep(server_far)
    if abs(fixed_sleep - 0.150) > 0.001:
        print(f"  [FAIL] Fixed interval: expected 0.150s, got {fixed_sleep:.3f}s")
        fail_count += 1
    else:
        print(f"  [PASS] Fixed interval override: {fixed_sleep:.3f}s ✓")

    # ─── Unit Test 3: NewBarEvent construction ───────────────────────────
    print("\n[TEST 3] NewBarEvent construction...")

    received_events: List[NewBarEvent] = []

    def _test_callback(event: NewBarEvent) -> None:
        received_events.append(event)

    def _bad_callback(event: NewBarEvent) -> None:
        raise RuntimeError("intentional callback error")

    # สร้าง trigger พร้อม bad callback → ต้องไม่ crash loop
    trig_bad = CandleCloseTrigger("GOLD", "M1", on_new_candle=_bad_callback)
    # inject callback โดยตรงเพื่อทดสอบ _invoke_callback
    bar_data = {
        "time":        int(time.time()) - 120,
        "open":        2320.0,
        "high":        2322.5,
        "low":         2319.0,
        "close":       2321.8,
        "tick_volume": 150,
    }
    trig_bad._invoke_callback(bar_data)   # ไม่ควร raise
    print("  [PASS] Bad callback does not crash trigger ✓")

    # test callback ปกติ
    trig_ok = CandleCloseTrigger("GOLD", "M1", on_new_candle=_test_callback)
    trig_ok._invoke_callback(bar_data)
    if len(received_events) != 1:
        print(f"  [FAIL] Expected 1 event, got {len(received_events)}")
        fail_count += 1
    else:
        e0 = received_events[0]
        assert e0.timeframe == "M1"
        assert e0.close == 2321.8
        assert e0.sequence == 1
        print(f"  [PASS] NewBarEvent: tf={e0.timeframe} close={e0.close} seq={e0.sequence} ✓")

    # ─── Test 4: Live MT5 or Synthetic loop ──────────────────────────────
    print(f"\n[TEST 4] Candle-close detection loop ({duration_sec}s)...")

    mt5_ok = False
    if _MT5_AVAILABLE and mt5 is not None:
        try:
            mt5_ok = mt5.initialize()
        except Exception:
            mt5_ok = False

    use_synthetic = not mt5_ok
    if use_synthetic:
        print(f"  Mode: SYNTHETIC (MT5 not available, fast_sec={fast_sec:.1f}s per 'M1')")
    else:
        print(f"  Mode: LIVE MT5 | symbol={symbol} tf={timeframe}")

    live_events:    List[NewBarEvent] = []
    callback_times: List[float]       = []

    def _live_callback(event: NewBarEvent) -> None:
        now = time.monotonic()
        callback_times.append(now)
        live_events.append(event)
        print(
            f"  ✅ BAR CLOSED | seq={event.sequence:>3} "
            f"tf={event.timeframe} bar_utc={event.bar_time_utc} "
            f"close={event.close:.3f} latency={event.latency_ms:.1f}ms"
        )

    if use_synthetic:
        trigger = _SyntheticCandleCloseTrigger(
            symbol=symbol,
            timeframe=timeframe,
            on_new_candle=_live_callback,
            fast_sec=fast_sec,
        )
    else:
        trigger = CandleCloseTrigger(
            symbol=symbol,
            timeframe=timeframe,
            on_new_candle=_live_callback,
        )

    _ = trigger.run_async()
    t_deadline = time.monotonic() + duration_sec

    # แสดง heartbeat ทุก 5 วินาที
    last_hb = time.monotonic()
    while time.monotonic() < t_deadline:
        if (time.monotonic() - last_hb) >= 5.0:
            s = trigger.stats()
            print(
                f"  [HB] running={s['is_running']} "
                f"callbacks={s['callback_count']} "
                f"avg_latency={s['avg_latency_ms']}ms "
                f"errors={s['total_errors']}"
            )
            last_hb = time.monotonic()
        time.sleep(0.5)

    trigger.stop()
    trigger.wait(timeout=3.0)

    final_stats = trigger.stats()
    print(f"\n  Final stats: {json.dumps(final_stats, indent=4)}")

    # ─── Validate results ─────────────────────────────────────────────────
    if use_synthetic:
        # ใน synthetic mode ควรได้อย่างน้อย 1 bar ต่อ fast_sec วินาที
        expected_min = max(1, int(duration_sec / fast_sec) - 1)
        if len(live_events) < expected_min:
            print(
                f"  [FAIL] Expected ≥{expected_min} callbacks, got {len(live_events)}"
            )
            fail_count += 1
        else:
            print(
                f"  [PASS] Received {len(live_events)} callbacks "
                f"(expected ≥{expected_min}) ✓"
            )

        # ตรวจ exactly-once: ไม่ควรมี bar_time ซ้ำ
        bar_times = [e.bar_time for e in live_events]
        if len(bar_times) != len(set(bar_times)):
            print("  [FAIL] Duplicate bar_time detected (callback fired >1x per bar)")
            fail_count += 1
        else:
            print("  [PASS] No duplicate bar_time — exactly-once guarantee ✓")

        # ตรวจ sequence monotonic
        seqs = [e.sequence for e in live_events]
        if seqs != sorted(seqs):
            print(f"  [FAIL] sequence not monotonic: {seqs}")
            fail_count += 1
        else:
            print(f"  [PASS] Sequence monotonic: {seqs} ✓")

    else:
        # Live mode — ถ้า M1 อาจไม่มี bar ใหม่ใน duration_sec
        # (ถ้า < duration_sec < 60s อาจได้ 0 bar)
        print(
            f"  [INFO] Live mode: {len(live_events)} new M1 bars detected "
            f"in {duration_sec}s (expected 0–1 for M1)"
        )
        if len(live_events) > 0:
            avg_lat = sum(e.latency_ms for e in live_events) / len(live_events)
            print(f"  [INFO] Average latency: {avg_lat:.1f}ms")
            if avg_lat < 200:
                print("  [PASS] Latency < 200ms target ✓")
            else:
                print(f"  [WARN] Latency {avg_lat:.1f}ms > 200ms target (check connection)")

    if trigger._total_errors > 0:
        print(f"  [WARN] total_errors={trigger._total_errors} (non-fatal)")

    # ─── Summary ─────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    if fail_count == 0:
        print("  ✅ ALL TESTS PASSED — Phase 1.3 READY")
    else:
        print(f"  ❌ {fail_count} TEST(S) FAILED")
    print(SEP)
    return 0 if fail_count == 0 else 1


# ===========================================================================
#  Entry Point
# ===========================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="HIM Candle Close Trigger",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run self-test and exit. Exit 0=pass, 1=fail.",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="GOLD",
        help="Symbol (default: GOLD)",
    )
    parser.add_argument(
        "--tf",
        type=str,
        default="M1",
        help="Timeframe (default: M1)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Test duration in seconds (default: 30)",
    )
    parser.add_argument(
        "--fast-sec",
        type=float,
        default=5.0,
        help="Synthetic M1 bar interval in seconds for --test (default: 5.0)",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Run live trigger (blocking) — prints JSON on each bar close",
    )

    args = parser.parse_args()

    if args.test:
        return _run_test(
            symbol=args.symbol,
            timeframe=args.tf,
            duration_sec=args.duration,
            fast_sec=args.fast_sec,
        )

    if args.run:
        def _print_callback(event: NewBarEvent) -> None:
            print(json.dumps(event.to_dict(), ensure_ascii=False))

        trigger = CandleCloseTrigger(
            symbol=args.symbol,
            timeframe=args.tf,
            on_new_candle=_print_callback,
        )
        trigger.run()
        return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())