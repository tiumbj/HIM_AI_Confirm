# ==============================================================================
# ชื่อโค้ด  : HIM MTF Cascade Exit System
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\mtf_cascade_exit.py
# คำสั่งรัน : python mtf_cascade_exit.py --dry-test
# เวอร์ชัน  : v1.0.1
# ==============================================================================
# CHANGELOG
# v1.0.1 (2026-03-14)  — Phase 1.2 FIX
#   FIX-1: warn_thr ปรับจาก 0.40 → 0.15 ให้สอดคล้องกับน้ำหนักจริงของ micro group
#           micro group weight (M1+M2+M3) = 2.1 / total 13.1 = 0.160
#           ต้องการ: Case B (micro flip only) → EARLY_WARN ไม่ใช่ HOLD
#   FIX-2: dry-test loop ไม่ re-register position ทันทีหลัง EXIT
#           ใช้ flag _dry_pos_alive + cooldown ควบคุม lifecycle อย่างชัดเจน
#   FIX-3: เพิ่ม suppress_warn threshold ปรับตามสัดส่วน warn_thr ที่เปลี่ยน
#
# v1.0.0 (2026-03-14)
#   - Phase 1.2: สร้างไฟล์ใหม่ทั้งหมด
#   - CascadeConsensusEngine, AdaptiveThreshold, MTFCascadeExitSystem
#   - register/unregister, dry_run loop
#   - get_cascade_system() singleton
# ==============================================================================
"""
mtf_cascade_exit.py — HIM v3, Phase 1.2

Multi-Timeframe Cascade Exit System

ความสัมพันธ์กับ Phase อื่น:
  Phase 1.1 (mtf_supertrend.py) → ให้ TFResult + direction map
  Phase 1.2 (ไฟล์นี้)          → ประมวลผล consensus + ตัดสิน exit
  Phase 3   (mt5_executor.py)   → เรียก register() หลัง order fill
  Phase 3   (cascade_runner.py) → รัน loop() เป็น standalone process

Public API:
    PositionCtx                  — dataclass ข้อมูล position ที่เปิดอยู่
    CascadeResult                — dataclass ผล consensus ต่อ 1 cycle
    CascadeConsensusEngine       — คำนวณ weighted consensus
    AdaptiveThreshold            — ปรับ threshold ตาม profit zone
    MTFCascadeExitSystem         — main orchestrator
    get_cascade_system(...)      — singleton factory
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np

# ---------------------------------------------------------------------------
# MT5 import (optional — graceful degradation ใน dry-test / unit test)
# ---------------------------------------------------------------------------
try:
    import MetaTrader5 as mt5
    _MT5_AVAILABLE = True
except ImportError:
    mt5 = None          # type: ignore[assignment]
    _MT5_AVAILABLE = False


# ===========================================================================
#  Version & Logging
# ===========================================================================

VERSION = "v1.0.1"

_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def _get_logger(name: str = "mtf_cascade_exit") -> logging.Logger:
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
#  Constants — TF Groups & Weights
# ===========================================================================

# 9 TF ทั้งหมดของ Cascade System
CASCADE_TFS: List[str] = ["M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15"]

# 3 Groups ตาม noise / confirmation profile
TF_GROUP: Dict[str, str] = {
    "M1":  "micro",
    "M2":  "micro",
    "M3":  "micro",
    "M4":  "confirm",
    "M5":  "confirm",
    "M6":  "confirm",
    "M10": "anchor",
    "M12": "anchor",
    "M15": "anchor",
}

# Weights — ยิ่ง TF ใหญ่ น่าเชื่อถือกว่า
TF_WEIGHT: Dict[str, float] = {
    "M1":  0.5,
    "M2":  0.7,
    "M3":  0.9,
    "M4":  1.2,
    "M5":  1.5,
    "M6":  1.6,
    "M10": 2.0,
    "M12": 2.2,
    "M15": 2.5,
}

# รวม weight ทั้งหมด = 13.1
_TOTAL_WEIGHT_MAX: float = sum(TF_WEIGHT.values())

# รวม weight ต่อ group
_GROUP_WEIGHT_MAX: Dict[str, float] = {
    g: sum(TF_WEIGHT[tf] for tf in CASCADE_TFS if TF_GROUP[tf] == g)
    for g in ("micro", "confirm", "anchor")
}
# micro = 2.1, confirm = 4.3, anchor = 6.7

# ---------------------------------------------------------------------------
# Threshold defaults (คำนวณจากน้ำหนักจริง ไม่ใช่ arbitrary)
#
#   micro group  = 2.1 / 13.1 = 0.160   → warn_thr ต้องต่ำกว่า 0.160
#   confirm group= 4.3 / 13.1 = 0.328
#   anchor group = 6.7 / 13.1 = 0.511
#
#   ออกแบบ:
#     WARN_THR    = 0.14  : micro เริ่ม flip ≥ 1 TF → EARLY_WARN
#     EXIT_THR    = 0.50  : confirm + micro flip เป็นส่วนใหญ่
#     ANCHOR_THR  = 0.50  : anchor group flip ≥ 50% → EXIT_ANCHOR (override)
#
#   หมายเหตุ: EXIT_THR ตรวจร่วมกับ confirm_ratio ≥ 0.50 ด้วย (double gate)
#             ป้องกัน false exit จาก micro noise อย่างเดียว
# ---------------------------------------------------------------------------
_DEFAULT_WARN_THR:   float = 0.14
_DEFAULT_EXIT_THR:   float = 0.50
_DEFAULT_ANCHOR_THR: float = 0.50


# ===========================================================================
#  Data Structures
# ===========================================================================

@dataclass
class PositionCtx:
    """
    ข้อมูล position ที่เพิ่ง execute — ส่งมาจาก mt5_executor (Phase 3)
    """
    ticket:       int
    direction:    str         # "BUY" หรือ "SELL"
    entry_price:  float
    atr_at_entry: float       # ATR ณ ตอน entry
    volume:       float       # lot size
    sl:           float = 0.0
    tp:           float = 0.0
    open_time:    float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        self.direction = str(self.direction).upper().strip()
        if self.direction not in ("BUY", "SELL"):
            raise ValueError(
                f"PositionCtx.direction must be 'BUY' or 'SELL', got '{self.direction}'"
            )
        if self.atr_at_entry <= 0:
            self.atr_at_entry = 1.0     # safe fallback


@dataclass
class GroupScore:
    """Weighted score สำหรับ 1 group"""
    group:          str
    weight_against: float
    weight_total:   float
    count_against:  int
    count_total:    int

    @property
    def ratio(self) -> float:
        return self.weight_against / self.weight_total if self.weight_total > 0 else 0.0


@dataclass
class CascadeResult:
    """ผลลัพธ์การประเมิน consensus ต่อ 1 cycle ต่อ 1 position"""

    total_weight_against:  float
    total_weight_possible: float
    consensus_ratio:       float        # 0.0–1.0

    groups:     Dict[str, GroupScore]
    action:     str        # "HOLD" / "EARLY_WARN" / "EXIT_CONFIRMED" / "EXIT_ANCHOR"
    reason:     str
    confidence: float      # 0.0–1.0
    profit_atr: float
    ts_utc:     str
    tf_details: Dict[str, dict]

    @property
    def should_exit(self) -> bool:
        return self.action in ("EXIT_CONFIRMED", "EXIT_ANCHOR")

    def to_dict(self) -> dict:
        return {
            "action":                self.action,
            "reason":                self.reason,
            "confidence":            round(self.confidence, 3),
            "consensus_ratio":       round(self.consensus_ratio, 3),
            "total_weight_against":  round(self.total_weight_against, 2),
            "total_weight_possible": round(self.total_weight_possible, 2),
            "profit_atr":            round(self.profit_atr, 4),
            "ts_utc":                self.ts_utc,
            "groups": {
                g: {
                    "ratio":         round(s.ratio, 3),
                    "against":       round(s.weight_against, 2),
                    "total":         round(s.weight_total, 2),
                    "count_against": s.count_against,
                    "count_total":   s.count_total,
                }
                for g, s in self.groups.items()
            },
            "tf_details": self.tf_details,
        }


# ===========================================================================
#  CascadeConsensusEngine
# ===========================================================================

class CascadeConsensusEngine:
    """
    ประมวลผล ST direction map → Weighted Cascade consensus → CascadeResult

    Threshold defaults (v1.0.1 — คำนวณจากน้ำหนักจริง):
      warn_thr    = 0.14  : micro เริ่ม flip ≥ 1 TF → EARLY_WARN
                             (micro total = 2.1/13.1 = 0.160, ดังนั้น flip แค่ M1 = 0.5/13.1 = 0.038
                              flip M1+M2 = 1.2/13.1 = 0.092, flip M1+M2+M3 = 2.1/13.1 = 0.160)
                             warn ที่ 0.14 = micro flip ≥ 2 TF (M1+M2 = 0.092 ไม่พอ, M1+M3 = 0.107 ไม่พอ,
                             M2+M3 = 0.122 ไม่พอ, M1+M2+M3 = 0.160 พอ)
                             → ตั้ง 0.14 เพื่อให้ micro flip ทั้ง 3 → warn, flip 1-2 → hold
      exit_thr    = 0.50  : ต้องการ confirm+micro หรือ anchor+micro
      anchor_thr  = 0.50  : anchor group flip ≥ 50%

    Cascade Priority (invariant):
      1. Anchor Override  → EXIT_ANCHOR   (anchor flip ≥ anchor_thr)
      2. Confirmed Exit   → EXIT_CONFIRMED (ratio ≥ exit_thr AND confirm ≥ 50%)
      3. Early Warning    → EARLY_WARN    (ratio ≥ warn_thr)
      4. Hold             → HOLD
    """

    def __init__(
        self,
        warn_thr:   float = _DEFAULT_WARN_THR,
        exit_thr:   float = _DEFAULT_EXIT_THR,
        anchor_thr: float = _DEFAULT_ANCHOR_THR,
        tf_weights: Optional[Dict[str, float]] = None,
        tf_groups:  Optional[Dict[str, str]] = None,
    ) -> None:
        self.warn_thr   = warn_thr
        self.exit_thr   = exit_thr
        self.anchor_thr = anchor_thr
        self._weights   = tf_weights if tf_weights is not None else TF_WEIGHT
        self._groups    = tf_groups  if tf_groups  is not None else TF_GROUP

    def evaluate(
        self,
        direction_map:   Dict[str, int],
        trade_direction: str,
        profit_atr:      float = 0.0,
    ) -> CascadeResult:
        """
        Parameters
        ----------
        direction_map   : {"M1": +1, "M5": -1, ...}  จาก MTFSupertrend.all_directions()
        trade_direction : "BUY" หรือ "SELL"
        profit_atr      : กำไร/ขาดทุน ณ ปัจจุบัน ใน ATR unit
        """
        ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        d  = str(trade_direction).upper().strip()

        group_against: Dict[str, float] = {"micro": 0.0, "confirm": 0.0, "anchor": 0.0}
        group_total:   Dict[str, float] = {"micro": 0.0, "confirm": 0.0, "anchor": 0.0}
        group_cnt_ag:  Dict[str, int]   = {"micro": 0,   "confirm": 0,   "anchor": 0}
        group_cnt_tot: Dict[str, int]   = {"micro": 0,   "confirm": 0,   "anchor": 0}

        total_against  = 0.0
        total_possible = 0.0
        tf_details: Dict[str, dict] = {}

        for tf in CASCADE_TFS:
            st_dir = direction_map.get(tf, 0)
            if st_dir == 0:
                tf_details[tf] = {"st_dir": 0, "is_against": False, "weight": 0.0, "skip": True}
                continue

            w = self._weights.get(tf, 1.0)
            g = self._groups.get(tf, "confirm")

            is_against = (
                (d == "BUY"  and st_dir < 0) or
                (d == "SELL" and st_dir > 0)
            )

            total_possible      += w
            group_total[g]      += w
            group_cnt_tot[g]    += 1

            if is_against:
                total_against    += w
                group_against[g] += w
                group_cnt_ag[g]  += 1

            tf_details[tf] = {
                "st_dir":     st_dir,
                "is_against": is_against,
                "weight":     w,
                "group":      g,
                "skip":       False,
            }

        # ─── Ratios ───
        consensus_ratio = total_against / total_possible if total_possible > 0 else 0.0
        anchor_ratio    = (
            group_against["anchor"] / group_total["anchor"]
            if group_total["anchor"] > 0 else 0.0
        )
        confirm_ratio   = (
            group_against["confirm"] / group_total["confirm"]
            if group_total["confirm"] > 0 else 0.0
        )

        # ─── GroupScore objects ───
        groups = {
            g: GroupScore(
                group=g,
                weight_against=group_against[g],
                weight_total=group_total[g],
                count_against=group_cnt_ag[g],
                count_total=group_cnt_tot[g],
            )
            for g in ("micro", "confirm", "anchor")
        }

        # ─── Cascade Decision (Priority 1→4) ───

        # Priority 1: Anchor Override
        if anchor_ratio >= self.anchor_thr and group_total["anchor"] > 0:
            return CascadeResult(
                total_weight_against=total_against,
                total_weight_possible=total_possible,
                consensus_ratio=consensus_ratio,
                groups=groups,
                action="EXIT_ANCHOR",
                reason=f"anchor_flip:{anchor_ratio:.3f}_thr:{self.anchor_thr}",
                confidence=min(1.0, anchor_ratio * 1.3),
                profit_atr=profit_atr,
                ts_utc=ts,
                tf_details=tf_details,
            )

        # Priority 2: Confirmed Exit
        if consensus_ratio >= self.exit_thr and confirm_ratio >= 0.50:
            return CascadeResult(
                total_weight_against=total_against,
                total_weight_possible=total_possible,
                consensus_ratio=consensus_ratio,
                groups=groups,
                action="EXIT_CONFIRMED",
                reason=f"consensus:{consensus_ratio:.3f}_confirm:{confirm_ratio:.3f}",
                confidence=min(1.0, consensus_ratio * 1.2),
                profit_atr=profit_atr,
                ts_utc=ts,
                tf_details=tf_details,
            )

        # Priority 3: Early Warning
        if consensus_ratio >= self.warn_thr:
            return CascadeResult(
                total_weight_against=total_against,
                total_weight_possible=total_possible,
                consensus_ratio=consensus_ratio,
                groups=groups,
                action="EARLY_WARN",
                reason=f"micro_flipping:{consensus_ratio:.3f}_warn_thr:{self.warn_thr}",
                confidence=min(1.0, consensus_ratio * 0.8),
                profit_atr=profit_atr,
                ts_utc=ts,
                tf_details=tf_details,
            )

        # Priority 4: Hold
        return CascadeResult(
            total_weight_against=total_against,
            total_weight_possible=total_possible,
            consensus_ratio=consensus_ratio,
            groups=groups,
            action="HOLD",
            reason=f"consensus_low:{consensus_ratio:.3f}",
            confidence=0.0,
            profit_atr=profit_atr,
            ts_utc=ts,
            tf_details=tf_details,
        )


# ===========================================================================
#  AdaptiveThreshold
# ===========================================================================

class AdaptiveThreshold:
    """
    ปรับ exit decision ตาม profit zone

    Rule 1: profit_atr ≥ high_profit_atr  → ลด exit threshold (ปกป้องกำไร)
    Rule 2: profit_atr ≤ loss_atr         → ออกเร็วขึ้น (cut loss)
    Rule 3: 0 ≤ profit_atr < min_profit   → suppress EARLY_WARN → HOLD
    """

    def __init__(
        self,
        high_profit_atr:      float = 1.5,
        high_profit_exit_thr: float = 0.35,
        loss_atr:             float = -0.30,
        loss_exit_thr:        float = 0.30,
        min_profit_for_warn:  float = 0.20,
    ) -> None:
        self.high_profit_atr      = high_profit_atr
        self.high_profit_exit_thr = high_profit_exit_thr
        self.loss_atr             = loss_atr
        self.loss_exit_thr        = loss_exit_thr
        self.min_profit_for_warn  = min_profit_for_warn

    def adjust(self, base: CascadeResult) -> CascadeResult:
        """
        ปรับ action ของ CascadeResult ตาม profit zone
        คืน CascadeResult ใหม่ (immutable — ไม่ mutate เดิม)
        """
        action = base.action
        reason = base.reason
        ratio  = base.consensus_ratio
        profit = base.profit_atr

        # Rule 1: กำไรสูง → ลด exit threshold
        if profit >= self.high_profit_atr:
            if action in ("HOLD", "EARLY_WARN") and ratio >= self.high_profit_exit_thr:
                action = "EXIT_CONFIRMED"
                reason = f"profit_protect:{profit:.2f}ATR_ratio:{ratio:.3f}"

        # Rule 2: ขาดทุน → ออกเร็วขึ้น
        elif profit <= self.loss_atr:
            if action in ("HOLD", "EARLY_WARN") and ratio >= self.loss_exit_thr:
                action = "EXIT_CONFIRMED"
                reason = f"cut_loss:{profit:.2f}ATR_ratio:{ratio:.3f}"

        # Rule 3: กำไรน้อยเกินไป → suppress EARLY_WARN
        elif 0.0 <= profit < self.min_profit_for_warn:
            if action == "EARLY_WARN":
                action = "HOLD"
                reason = f"suppress_warn:profit_too_low:{profit:.3f}ATR"

        if action == base.action:
            return base

        return CascadeResult(
            total_weight_against=base.total_weight_against,
            total_weight_possible=base.total_weight_possible,
            consensus_ratio=base.consensus_ratio,
            groups=base.groups,
            action=action,
            reason=reason,
            confidence=base.confidence,
            profit_atr=base.profit_atr,
            ts_utc=base.ts_utc,
            tf_details=base.tf_details,
        )


# ===========================================================================
#  MTFCascadeExitSystem
# ===========================================================================

class MTFCascadeExitSystem:
    """
    Main Orchestrator — รัน background loop ทุก loop_interval วินาที

    Lifecycle:
      1. register(pos)      → เพิ่ม position เข้า watch list
      2. loop()             → evaluate consensus ทุก 200ms → close ถ้า EXIT_*
      3. unregister(ticket) → ลบ position ออก (auto หลัง close สำเร็จ)
      4. start_background() → daemon thread
      5. stop()             → หยุด loop

    DRY_RUN mode: ไม่ส่ง order จริง — log เท่านั้น
    """

    LOOP_INTERVAL:    float = 0.2
    WARN_TO_EXIT_CNT: int   = 3

    def __init__(
        self,
        symbol:         str   = "GOLD",
        magic:          int   = 0,
        dry_run:        bool  = False,
        engine:         Optional[CascadeConsensusEngine] = None,
        threshold:      Optional[AdaptiveThreshold]      = None,
        mtf_supertrend: Optional[object]                 = None,
        loop_interval:  float = LOOP_INTERVAL,
        warn_to_exit:   int   = WARN_TO_EXIT_CNT,
        log_dir:        str   = _LOG_DIR,
    ) -> None:
        self.symbol     = symbol
        self.magic      = magic
        self.dry_run    = dry_run or (
            os.environ.get("DRY_RUN", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
        )
        self._engine    = engine    or CascadeConsensusEngine()
        self._threshold = threshold or AdaptiveThreshold()
        self._mtf       = mtf_supertrend
        self.loop_interval = loop_interval
        self.warn_to_exit  = warn_to_exit
        self._log_dir      = log_dir

        self._positions:  Dict[int, PositionCtx] = {}
        self._warn_count: Dict[int, int]          = {}
        self._lock    = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

        os.makedirs(self._log_dir, exist_ok=True)
        self._log_path = os.path.join(self._log_dir, "cascade_exit.jsonl")

        _logger.info(
            "MTFCascadeExitSystem init | symbol=%s magic=%d dry_run=%s version=%s",
            self.symbol, self.magic, self.dry_run, VERSION,
        )

    # -----------------------------------------------------------------------
    #  Position Registry
    # -----------------------------------------------------------------------

    def register(self, pos: PositionCtx) -> None:
        with self._lock:
            self._positions[pos.ticket]  = pos
            self._warn_count[pos.ticket] = 0
        _logger.info(
            "CASCADE_REGISTERED | ticket=%d dir=%s entry=%.3f atr=%.4f vol=%.2f",
            pos.ticket, pos.direction, pos.entry_price, pos.atr_at_entry, pos.volume,
        )

    def unregister(self, ticket: int) -> None:
        with self._lock:
            removed = self._positions.pop(ticket, None)
            self._warn_count.pop(ticket, None)
        if removed:
            _logger.info("CASCADE_UNREGISTERED | ticket=%d", ticket)

    def position_count(self) -> int:
        with self._lock:
            return len(self._positions)

    def registered_tickets(self) -> List[int]:
        with self._lock:
            return list(self._positions.keys())

    # -----------------------------------------------------------------------
    #  Profit Calculation
    # -----------------------------------------------------------------------

    def _profit_atr(self, pos: PositionCtx, mid_price: float) -> float:
        if pos.atr_at_entry <= 0:
            return 0.0
        delta = (
            (mid_price - pos.entry_price) if pos.direction == "BUY"
            else (pos.entry_price - mid_price)
        )
        return delta / pos.atr_at_entry

    # -----------------------------------------------------------------------
    #  MT5 Price Fetch
    # -----------------------------------------------------------------------

    def _get_mid_price(self) -> Optional[float]:
        if not _MT5_AVAILABLE or mt5 is None:
            return None
        try:
            tick = mt5.symbol_info_tick(self.symbol)
            if tick is None:
                return None
            return (float(tick.bid) + float(tick.ask)) / 2.0
        except Exception:
            return None

    def _get_exit_price(self, direction: str) -> Optional[float]:
        if not _MT5_AVAILABLE or mt5 is None:
            return None
        try:
            tick = mt5.symbol_info_tick(self.symbol)
            if tick is None:
                return None
            return float(tick.bid) if direction == "BUY" else float(tick.ask)
        except Exception:
            return None

    # -----------------------------------------------------------------------
    #  MT5 Close Order
    # -----------------------------------------------------------------------

    def _close_position(self, pos: PositionCtx, exit_price: float, reason: str) -> bool:
        if self.dry_run:
            _logger.info(
                "CASCADE_DRY_CLOSE | ticket=%d dir=%s price=%.3f reason=%s",
                pos.ticket, pos.direction, exit_price, reason,
            )
            return True

        if not _MT5_AVAILABLE or mt5 is None:
            _logger.warning("CASCADE_CLOSE_FAIL | ticket=%d | MT5 not available", pos.ticket)
            return False

        try:
            order_type = mt5.ORDER_TYPE_SELL if pos.direction == "BUY" else mt5.ORDER_TYPE_BUY
            req = {
                "action":       mt5.TRADE_ACTION_DEAL,
                "position":     pos.ticket,
                "symbol":       self.symbol,
                "volume":       float(pos.volume),
                "type":         order_type,
                "price":        float(exit_price),
                "deviation":    50,
                "magic":        int(self.magic),
                "comment":      f"HIM_CASCADE_{reason[:20]}",
                "type_time":    mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            result = mt5.order_send(req)
            if result is None:
                _logger.warning(
                    "CASCADE_CLOSE_FAIL | ticket=%d | order_send=None", pos.ticket
                )
                return False
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                _logger.warning(
                    "CASCADE_CLOSE_FAIL | ticket=%d retcode=%d comment=%s",
                    pos.ticket, result.retcode, getattr(result, "comment", ""),
                )
                return False
            _logger.info(
                "CASCADE_CLOSE_OK | ticket=%d dir=%s price=%.3f reason=%s",
                pos.ticket, pos.direction, exit_price, reason,
            )
            return True
        except Exception as e:
            _logger.error(
                "CASCADE_CLOSE_EXCEPTION | ticket=%d | %s: %s",
                pos.ticket, type(e).__name__, e,
            )
            return False

    # -----------------------------------------------------------------------
    #  JSONL Logging
    # -----------------------------------------------------------------------

    def _log_event(self, event: dict) -> None:
        try:
            with open(self._log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False, default=str) + "\n")
        except Exception:
            pass

    # -----------------------------------------------------------------------
    #  Direction Map Source
    # -----------------------------------------------------------------------

    def _get_direction_map(self) -> Dict[str, int]:
        if self._mtf is None:
            return {}
        try:
            return self._mtf.all_directions()  # type: ignore[union-attr]
        except Exception as e:
            _logger.warning("direction_map_error: %s", e)
            return {}

    # -----------------------------------------------------------------------
    #  run_once — evaluate + act สำหรับทุก registered position
    # -----------------------------------------------------------------------

    def run_once(self, direction_map: Optional[Dict[str, int]] = None) -> List[dict]:
        """
        เรียกทุก loop_interval
        direction_map: override (ใช้ใน dry-test / unit test)
        Returns: list ของ action events ที่เกิดขึ้น
        """
        if direction_map is None:
            direction_map = self._get_direction_map()

        mid_price = self._get_mid_price()
        if mid_price is None and not self.dry_run:
            return []

        actions: List[dict] = []

        with self._lock:
            tickets = list(self._positions.keys())

        for ticket in tickets:
            with self._lock:
                pos = self._positions.get(ticket)
            if pos is None:
                continue

            p_atr = self._profit_atr(pos, mid_price) if mid_price is not None else 0.0

            base_result  = self._engine.evaluate(direction_map, pos.direction, p_atr)
            final_result = self._threshold.adjust(base_result)

            event_base = {
                "ticket":     ticket,
                "direction":  pos.direction,
                "profit_atr": round(p_atr, 4),
                "action":     final_result.action,
                "reason":     final_result.reason,
                "consensus":  round(final_result.consensus_ratio, 3),
                "confidence": round(final_result.confidence, 3),
                "ts_utc":     final_result.ts_utc,
                "groups": {
                    g: round(s.ratio, 3)
                    for g, s in final_result.groups.items()
                },
                "dry_run": self.dry_run,
            }

            # EXIT_CONFIRMED / EXIT_ANCHOR
            if final_result.should_exit:
                exit_price = (
                    self._get_exit_price(pos.direction)
                    if mid_price is not None
                    else pos.entry_price
                )
                if exit_price is None:
                    exit_price = pos.entry_price

                ok = self._close_position(pos, exit_price, final_result.reason)
                evt = {
                    "event":      "CASCADE_EXIT",
                    "exit_price": round(exit_price, 5),
                    "close_ok":   ok,
                    **event_base,
                }
                if ok:
                    self.unregister(ticket)
                self._log_event(evt)
                actions.append(evt)
                _logger.info(
                    "CASCADE_EXIT | ticket=%d dir=%s pnl=%.4fATR action=%s close_ok=%s",
                    ticket, pos.direction, p_atr, final_result.action, ok,
                )

            # EARLY_WARN
            elif final_result.action == "EARLY_WARN":
                with self._lock:
                    self._warn_count[ticket] = self._warn_count.get(ticket, 0) + 1
                    cnt = self._warn_count[ticket]

                evt = {"event": "CASCADE_WARN", "warn_count": cnt, **event_base}
                self._log_event(evt)
                actions.append(evt)
                _logger.info(
                    "CASCADE_WARN | ticket=%d pnl=%.4fATR warn=%d consensus=%.3f",
                    ticket, p_atr, cnt, final_result.consensus_ratio,
                )

                # warn ติดกัน N ครั้ง → force EXIT
                if cnt >= self.warn_to_exit:
                    exit_price = self._get_exit_price(pos.direction) or pos.entry_price
                    ok = self._close_position(pos, exit_price, "warn_cascade_forced")
                    force_evt = {
                        "event":      "CASCADE_EXIT",
                        "exit_price": round(exit_price, 5),
                        "close_ok":   ok,
                        "reason":     "warn_cascade_forced",
                        **{k: v for k, v in event_base.items() if k != "reason"},
                    }
                    if ok:
                        self.unregister(ticket)
                    self._log_event(force_evt)
                    actions.append(force_evt)
                    _logger.info(
                        "CASCADE_EXIT_FORCED | ticket=%d warn_count=%d", ticket, cnt
                    )

            # HOLD
            else:
                with self._lock:
                    self._warn_count[ticket] = 0

        return actions

    # -----------------------------------------------------------------------
    #  Loop + Thread Management
    # -----------------------------------------------------------------------

    def loop(self) -> None:
        self._running = True
        _logger.info(
            "MTFCascadeExitSystem loop started | symbol=%s dry_run=%s interval=%.1fs",
            self.symbol, self.dry_run, self.loop_interval,
        )
        while self._running:
            try:
                self.run_once()
            except Exception as e:
                _logger.error("CASCADE_LOOP_ERROR: %s: %s", type(e).__name__, e)
            time.sleep(self.loop_interval)
        _logger.info("MTFCascadeExitSystem loop stopped")

    def start_background(
        self, mtf_supertrend_instance: Optional[object] = None
    ) -> threading.Thread:
        if mtf_supertrend_instance is not None:
            self._mtf = mtf_supertrend_instance
        if self._running:
            raise RuntimeError("MTFCascadeExitSystem already running")
        t = threading.Thread(target=self.loop, name="cascade_exit_loop", daemon=True)
        self._thread = t
        t.start()
        return t

    def stop(self) -> None:
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3.0)
        self._thread = None
        _logger.info("MTFCascadeExitSystem stopped")


# ===========================================================================
#  Singleton Factory
# ===========================================================================

_cascade_instances: Dict[str, MTFCascadeExitSystem] = {}
_cascade_lock = threading.Lock()


def get_cascade_system(
    symbol:      str,
    magic:       int,
    dry_run:     bool = False,
    config_path: Optional[str] = None,
) -> MTFCascadeExitSystem:
    """
    Singleton factory — คืน instance เดิมถ้ามีอยู่แล้ว
    Phase 3: mt5_executor เรียก get_cascade_system(symbol, magic).register(pos)
    """
    key = f"{symbol}:{magic}"
    with _cascade_lock:
        if key not in _cascade_instances:
            _cascade_instances[key] = MTFCascadeExitSystem(
                symbol=symbol, magic=magic, dry_run=dry_run,
            )
        return _cascade_instances[key]


def clear_cascade_instances() -> None:
    """ล้าง singleton cache (test teardown เท่านั้น)"""
    with _cascade_lock:
        _cascade_instances.clear()


# ===========================================================================
#  Dry-Test Helpers
# ===========================================================================

def _make_synthetic_dir_map(cycle: int, direction: str = "BUY") -> Dict[str, int]:
    """
    สร้าง direction map สังเคราะห์สำหรับ dry-test loop
    phase ขึ้นอยู่กับ cycle เพื่อจำลอง 3 สถานะ:
      phase 0  : ทุก TF เห็นด้วย → HOLD
      phase 1  : micro flip (M1+M2+M3) → EARLY_WARN territory
      phase 2  : ทุก TF flip → EXIT territory
    """
    agree_dir   =  1 if direction == "BUY" else -1
    against_dir = -1 if direction == "BUY" else  1

    phase = (cycle // 8) % 3
    if phase == 0:
        return {tf: agree_dir for tf in CASCADE_TFS}
    elif phase == 1:
        return {
            "M1":  against_dir, "M2":  against_dir, "M3":  against_dir,
            "M4":  agree_dir,   "M5":  agree_dir,   "M6":  agree_dir,
            "M10": agree_dir,   "M12": agree_dir,   "M15": agree_dir,
        }
    else:
        return {tf: against_dir for tf in CASCADE_TFS}


# ===========================================================================
#  CLI Dry-Test Mode
# ===========================================================================

def _run_dry_test(duration_sec: int = 60) -> int:
    SEP = "=" * 72
    print(SEP)
    print(f"  HIM MTF Cascade Exit System — Dry-Test  ({VERSION})")
    print(f"  Duration: {duration_sec}s | DRY_RUN=True | No real orders")
    print(SEP)

    fail_count = 0

    # ─── Unit Test 1: CascadeConsensusEngine ─────────────────────────────
    print("\n[TEST 1] CascadeConsensusEngine — synthetic direction maps...")
    engine    = CascadeConsensusEngine()
    threshold = AdaptiveThreshold()

    # Case A: ทุก TF เห็นด้วย → HOLD
    dir_hold = {tf: 1 for tf in CASCADE_TFS}
    r = engine.evaluate(dir_hold, "BUY", profit_atr=0.5)
    if r.action != "HOLD":
        print(f"  [FAIL] Case A (all agree BUY): expected HOLD, got {r.action}")
        fail_count += 1
    else:
        print(f"  [PASS] Case A: all agree → {r.action} (ratio={r.consensus_ratio:.3f}) ✓")

    # Case B: micro flip ทั้ง 3 TF (M1+M2+M3) → ต้องเป็น WARN หรือ EXIT
    # micro weight = 2.1, ratio = 2.1/13.1 = 0.160
    # warn_thr = 0.14 → 0.160 > 0.14 → ต้องเป็น EARLY_WARN
    dir_warn = {**dir_hold, "M1": -1, "M2": -1, "M3": -1}
    r_b = engine.evaluate(dir_warn, "BUY", profit_atr=0.3)
    if r_b.action not in ("EARLY_WARN", "EXIT_CONFIRMED", "EXIT_ANCHOR"):
        print(
            f"  [FAIL] Case B (micro M1+M2+M3 flip, ratio={r_b.consensus_ratio:.3f}): "
            f"expected WARN/EXIT, got {r_b.action} | warn_thr={engine.warn_thr}"
        )
        fail_count += 1
    else:
        print(
            f"  [PASS] Case B: micro flip → {r_b.action} "
            f"(ratio={r_b.consensus_ratio:.3f} vs warn_thr={engine.warn_thr}) ✓"
        )

    # Case C: ทุก TF flip → EXIT_CONFIRMED หรือ EXIT_ANCHOR
    dir_exit = {tf: -1 for tf in CASCADE_TFS}
    r_c = engine.evaluate(dir_exit, "BUY", profit_atr=0.8)
    if not r_c.should_exit:
        print(f"  [FAIL] Case C (all flip): expected EXIT, got {r_c.action}")
        fail_count += 1
    else:
        print(f"  [PASS] Case C: all flip → {r_c.action} (ratio={r_c.consensus_ratio:.3f}) ✓")

    # Case D: Anchor only flip → EXIT_ANCHOR
    dir_anchor = {tf: (1 if TF_GROUP[tf] != "anchor" else -1) for tf in CASCADE_TFS}
    r_d = engine.evaluate(dir_anchor, "BUY", profit_atr=1.0)
    if r_d.action != "EXIT_ANCHOR":
        print(f"  [FAIL] Case D (anchor flip): expected EXIT_ANCHOR, got {r_d.action}")
        fail_count += 1
    else:
        print(f"  [PASS] Case D: anchor flip → {r_d.action} ✓")

    # ─── Unit Test 2: AdaptiveThreshold ──────────────────────────────────
    print("\n[TEST 2] AdaptiveThreshold adjustments...")

    # High profit + HOLD + ratio ≥ high_profit_exit_thr → EXIT_CONFIRMED
    r_high = CascadeResult(
        total_weight_against=5.5,
        total_weight_possible=13.1,
        consensus_ratio=0.42,
        groups={
            g: GroupScore(g, 0.0, _GROUP_WEIGHT_MAX[g], 0, 0)
            for g in ("micro", "confirm", "anchor")
        },
        action="HOLD", reason="test", confidence=0.0,
        profit_atr=2.0, ts_utc="", tf_details={},
    )
    r_adj = threshold.adjust(r_high)
    if r_adj.action != "EXIT_CONFIRMED":
        print(f"  [FAIL] High profit HOLD→EXIT_CONFIRMED, got {r_adj.action}")
        fail_count += 1
    else:
        print(f"  [PASS] High profit: HOLD → {r_adj.action} ✓")

    # Loss + EARLY_WARN + ratio ≥ loss_exit_thr → EXIT_CONFIRMED
    r_loss = CascadeResult(
        total_weight_against=4.5,
        total_weight_possible=13.1,
        consensus_ratio=0.34,
        groups={
            g: GroupScore(g, 0.0, _GROUP_WEIGHT_MAX[g], 0, 0)
            for g in ("micro", "confirm", "anchor")
        },
        action="EARLY_WARN", reason="test", confidence=0.3,
        profit_atr=-0.5, ts_utc="", tf_details={},
    )
    r_adj2 = threshold.adjust(r_loss)
    if r_adj2.action != "EXIT_CONFIRMED":
        print(f"  [FAIL] Loss WARN→EXIT_CONFIRMED, got {r_adj2.action}")
        fail_count += 1
    else:
        print(f"  [PASS] Loss: EARLY_WARN → {r_adj2.action} ✓")

    # ─── Unit Test 3: register / unregister ──────────────────────────────
    print("\n[TEST 3] PositionCtx register / unregister...")
    sys_test = MTFCascadeExitSystem(symbol="GOLD", magic=202603, dry_run=True)

    pos1 = PositionCtx(
        ticket=10001, direction="BUY", entry_price=2320.0, atr_at_entry=3.5, volume=0.01
    )
    pos2 = PositionCtx(
        ticket=10002, direction="SELL", entry_price=2310.0, atr_at_entry=3.2, volume=0.01
    )

    sys_test.register(pos1)
    sys_test.register(pos2)
    if sys_test.position_count() != 2:
        print(f"  [FAIL] register: expected 2, got {sys_test.position_count()}")
        fail_count += 1
    else:
        print(f"  [PASS] register: 2 positions ✓")

    sys_test.unregister(10001)
    if sys_test.position_count() != 1:
        print(f"  [FAIL] unregister: expected 1, got {sys_test.position_count()}")
        fail_count += 1
    else:
        print(f"  [PASS] unregister: 1 remains ✓")

    # ─── Test 4: run_once() dry loop (duration_sec) ───────────────────────
    # FIX-2: ควบคุม lifecycle ด้วย _dry_pos_alive flag
    #   - register เมื่อเริ่มต้น หรือหลัง cooldown เท่านั้น
    #   - เมื่อ CASCADE_EXIT → ตั้ง flag=False + บันทึก re-open time
    #   - re-register เฉพาะเมื่อ cooldown ผ่านแล้ว (1 วินาที)
    #   - ป้องกัน re-register ทันทีใน cycle เดียวกัน
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n[TEST 4] run_once() dry loop for {duration_sec}s...")
    print("  (synthetic direction map — no real orders)\n")

    _DRY_TICKET       = 99001
    _DRY_DIRECTION    = "BUY"
    _DRY_ENTRY        = 2320.0
    _DRY_ATR          = 3.0
    _DRY_REOPEN_DELAY = 1.0   # วินาทีที่รอก่อน re-register หลัง EXIT

    live_sys = MTFCascadeExitSystem(symbol="GOLD", magic=202603, dry_run=True)

    def _make_dry_pos() -> PositionCtx:
        return PositionCtx(
            ticket=_DRY_TICKET,
            direction=_DRY_DIRECTION,
            entry_price=_DRY_ENTRY,
            atr_at_entry=_DRY_ATR,
            volume=0.01,
        )

    # register ครั้งแรก
    live_sys.register(_make_dry_pos())

    _dry_pos_alive      = True
    _dry_exit_time: Optional[float] = None   # เวลาที่ EXIT ล่าสุด

    t_start    = time.monotonic()
    cycle      = 0
    exit_count  = 0
    warn_count  = 0
    hold_count  = 0
    loop_errors = 0
    last_print  = t_start

    print(f"  {'CYCLE':>6} {'STATE':<10} {'ACTION':<18} {'CONSENSUS':>10} "
          f"{'PROFIT_ATR':>11}  REASON")
    print("  " + "-" * 78)

    while (time.monotonic() - t_start) < duration_sec:
        try:
            dir_map   = _make_synthetic_dir_map(cycle=cycle, direction=_DRY_DIRECTION)
            mock_drift = np.sin(cycle * 0.12) * 1.5
            mock_mid   = _DRY_ENTRY + mock_drift
            p_atr      = (mock_mid - _DRY_ENTRY) / _DRY_ATR

            now = time.monotonic()

            # ─ Re-register หลัง cooldown (FIX-2) ─
            if not _dry_pos_alive and _dry_exit_time is not None:
                if (now - _dry_exit_time) >= _DRY_REOPEN_DELAY:
                    live_sys.register(_make_dry_pos())
                    _dry_pos_alive = True
                    _dry_exit_time = None

            # ─ run_once ���
            events = live_sys.run_once(direction_map=dir_map)

            for ev in events:
                if ev.get("event") == "CASCADE_EXIT" and ev.get("close_ok"):
                    exit_count     += 1
                    _dry_pos_alive  = False
                    _dry_exit_time  = time.monotonic()
                elif ev.get("event") == "CASCADE_WARN":
                    warn_count += 1

            # คำนวณ action สำหรับ display (ไม่ต้องรัน engine ซ้ำ)
            base_r  = live_sys._engine.evaluate(dir_map, _DRY_DIRECTION, p_atr)
            final_r = live_sys._threshold.adjust(base_r)
            if final_r.action == "HOLD":
                hold_count += 1

            state_str = "ALIVE" if _dry_pos_alive else f"WAIT({_DRY_REOPEN_DELAY:.0f}s)"

            if (now - last_print) >= 1.0:
                print(
                    f"  {cycle:>6} {state_str:<10} {final_r.action:<18} "
                    f"{final_r.consensus_ratio:>10.3f} "
                    f"{p_atr:>11.4f}  {final_r.reason[:40]}"
                )
                last_print = now

        except Exception as e:
            loop_errors += 1
            _logger.error("DRY_TEST_LOOP_ERROR cycle=%d: %s", cycle, e)

        cycle += 1
        time.sleep(live_sys.loop_interval)

    total_elapsed = time.monotonic() - t_start
    print(
        f"\n  Loop finished | cycles={cycle} elapsed={total_elapsed:.1f}s | "
        f"HOLD={hold_count} WARN={warn_count} EXIT={exit_count} ERRORS={loop_errors}"
    )

    if loop_errors > 0:
        print(f"  [FAIL] {loop_errors} loop errors")
        fail_count += 1
    else:
        print("  [PASS] Dry loop completed without errors ✓")

    # Sanity: ต้องมี exit events อย่างน้อย 1 ครั้งใน duration
    min_expected_exits = 1
    if exit_count < min_expected_exits:
        print(
            f"  [FAIL] Expected ≥{min_expected_exits} exit events, got {exit_count}"
        )
        fail_count += 1
    else:
        print(f"  [PASS] Exit events: {exit_count} ✓")

    # ─── Unit Test 5: Singleton ──────────────────────────────────────────
    print("\n[TEST 5] get_cascade_system() singleton...")
    clear_cascade_instances()
    s1 = get_cascade_system("GOLD", 202603)
    s2 = get_cascade_system("GOLD", 202603)
    s3 = get_cascade_system("GOLD", 999999)
    if s1 is not s2:
        print("  [FAIL] Same key should return same instance")
        fail_count += 1
    elif s1 is s3:
        print("  [FAIL] Different key should return different instance")
        fail_count += 1
    else:
        print("  [PASS] Singleton: same→same, diff→diff ✓")
    clear_cascade_instances()

    # ─── Summary ─────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    if fail_count == 0:
        print("  ✅ ALL TESTS PASSED — Phase 1.2 READY")
    else:
        print(f"  ❌ {fail_count} TEST(S) FAILED")
    print(SEP)
    return 0 if fail_count == 0 else 1


# ===========================================================================
#  Entry Point
# ===========================================================================

def main() -> int:
    parser = argparse.ArgumentParser(
        description="HIM MTF Cascade Exit System",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--dry-test",
        action="store_true",
        help="Run dry test (no real orders). Exit 0=pass, 1=fail.",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Dry-test loop duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default="GOLD",
        help="Symbol override (default: GOLD)",
    )

    args = parser.parse_args()

    if args.dry_test:
        return _run_dry_test(duration_sec=args.duration)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())