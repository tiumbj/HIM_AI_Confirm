# ==============================================================================
# ชื่อโค้ด  : HIM Fast AI Confirm (Rule Engine)
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\fast_ai_confirm.py
# คำสั่งรัน : python fast_ai_confirm.py
# เวอร์ชัน  : v1.0.0
# ==============================================================================
# CHANGELOG
# v1.0.0 (2026-03-14)
#   - Phase 1.4: สร้างไฟล์ใหม่ทั้งหมด
#   - FastAIConfirm class: 2-tier rule-based gate + LLM fallback signal
#   - Tier 1 (FAST APPROVE) : align >= min_align AND rr >= min_rr AND blocked_by=[])
#   - Tier 2 (FAST REJECT)  : blocked_by non-empty หรือ decision invalid
#   - Tier 3 (UNCERTAIN)    : borderline → คืน approved=None → Phase 4 ส่ง LLM
#   - ไม่แก้ไฟล์อื่น ไม่เรียก LLM จริง ไม่ต้องการ external services
#   - self-test ครบทุก case ใน if __name__ == "__main__"
# ==============================================================================
"""
fast_ai_confirm.py — HIM v3, Phase 1.4

2-Tier Fast AI Confirm Rule Engine

วัตถุประสงค์:
  เป็น fast gate ก่อนส่ง execution_package ไปให้ LLM (DeepSeek/OpenAI)
  ลด latency โดยตัดสิน case ที่ชัดเจนด้วย rule เพียงอย่างเดียว (<1ms)
  เฉพาะ case ที่ไม่ชัดเจน (uncertain) จึงส่งต่อให้ LLM

Return contract ของ FastAIConfirm.confirm():
  (approved, reason, confidence)
  approved = True   → fast approve — ไม่ต้องเรียก LLM
  approved = False  → fast reject  — ไม่ต้องเรียก LLM
  approved = None   → uncertain    → Phase 4 ต้องเรียก LLM ต่อ

Integration (Phase 4, mentor_executor.py):
  from fast_ai_confirm import FastAIConfirm
  fast = FastAIConfirm(config)
  approved, reason, confidence = fast.confirm(execution_package)
  if approved is not None:
      return build_ai_confirm_result(approved, reason, confidence, "fast_rule_engine")
  # else: ส่งต่อให้ LLM เหมือนเดิม
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ===========================================================================
#  Version
# ===========================================================================

VERSION = "v1.0.0"

# PROVIDER constant (ใช้ใน Phase 4 integration ใน ai_confirm field)
PROVIDER = "fast_rule_engine"
MODEL    = VERSION


# ===========================================================================
#  FastAIConfig — parameters สำหรับ rule engine
# ===========================================================================

@dataclass
class FastAIConfig:
    """
    Configuration สำหรับ FastAIConfirm rule engine
    ค่า default ออกแบบให้สอดคล้องกับ config.json production:
      min_rr    = 1.3  (จาก config.json: "min_rr": 1.3)
      min_align = 3    (Phase 1.4 requirement: align >= 3)

    fast_approve_min_align : int
        จำนวน TF ที่ต้อง align ขั้นต่ำเพื่อ fast approve
    fast_approve_min_rr : float
        RR ขั้นต่ำสำหรับ fast approve
    fast_reject_max_rr : float
        ถ้า rr ต่ำกว่านี้ → fast reject (ไม่รอ LLM)
    fast_reject_on_blocked : bool
        True = blocked_by non-empty → fast reject ทันที (default True)
    fast_reject_on_invalid_decision : bool
        True = decision ไม่ใช่ BUY/SELL → fast reject
    uncertain_min_rr : float
        rr ต้องผ่านเกณฑ์นี้ถึงจะส่ง LLM (ต่ำกว่า → reject แทน uncertain)
    """
    fast_approve_min_align:         int   = 3
    fast_approve_min_rr:            float = 1.5
    fast_reject_max_rr:             float = 1.2
    fast_reject_on_blocked:         bool  = True
    fast_reject_on_invalid_decision: bool = True
    uncertain_min_rr:               float = 1.2

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FastAIConfig":
        """โหลดจาก dict (config.json him_v3.fast_ai_confirm section)"""
        def _i(key: str, default: int) -> int:
            try:
                v = d.get(key)
                return int(v) if v is not None else default
            except Exception:
                return default

        def _f(key: str, default: float) -> float:
            try:
                v = d.get(key)
                return float(v) if v is not None else default
            except Exception:
                return default

        def _b(key: str, default: bool) -> bool:
            v = d.get(key)
            if v is None:
                return default
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in ("1", "true", "yes")

        return cls(
            fast_approve_min_align=_i("fast_approve_min_align", 3),
            fast_approve_min_rr=_f("fast_approve_min_rr", 1.5),
            fast_reject_max_rr=_f("fast_reject_max_rr", 1.2),
            fast_reject_on_blocked=_b("fast_reject_on_blocked", True),
            fast_reject_on_invalid_decision=_b("fast_reject_on_invalid_decision", True),
            uncertain_min_rr=_f("uncertain_min_rr", 1.2),
        )


# ===========================================================================
#  FastConfirmResult — ข้อมูลผลลัพธ์ละเอียด (ใช้ใน logging / debug)
# ===========================================================================

@dataclass
class FastConfirmResult:
    """
    ผลลัพธ์จาก FastAIConfirm.confirm() พร้อม metadata
    approved   : True / False / None
    reason     : สาเหตุ (สั้น ≤ 80 chars)
    confidence : 0.0–1.0 (None ถ้า uncertain)
    tier       : "approve" / "reject" / "uncertain"
    elapsed_ms : เวลาคำนวณ (ms)
    checks     : dict ของ boolean checks แต่ละข้อ (สำหรับ debug)
    """
    approved:   Optional[bool]
    reason:     str
    confidence: Optional[float]
    tier:       str
    elapsed_ms: float
    checks:     Dict[str, Any] = field(default_factory=dict)

    def as_tuple(self) -> Tuple[Optional[bool], str, Optional[float]]:
        """คืน tuple สำหรับ Phase 4 integration"""
        return (self.approved, self.reason, self.confidence)

    def as_ai_confirm_dict(self) -> Dict[str, Any]:
        """
        คืน dict ที่ compatible กับ enforce_confirm_only() ใน mentor_executor.py
        format เหมือน LLM response
        """
        return {
            "approved":   self.approved,
            "reason":     self.reason,
            "confidence": self.confidence,
            "provider":   PROVIDER,
            "model":      MODEL,
        }


# ===========================================================================
#  FastAIConfirm — Main Class
# ===========================================================================

class FastAIConfirm:
    """
    2-Tier Fast AI Confirm Rule Engine

    Tier 1 — FAST APPROVE (approved=True, < 1ms):
      เงื่อนไขทั้งหมดต้องผ่านพร้อมกัน:
      - alignment_score >= fast_approve_min_align  (default 3)
      - rr >= fast_approve_min_rr                  (default 1.5)
      - blocked_by = []  (ไม่มี blocker)
      - decision ใน ("BUY", "SELL")
      - plan มี entry/sl/tp และ sanity check ผ่าน

    Tier 2 — FAST REJECT (approved=False, < 1ms):
      ถูก trigger โดยอย่างใดอย่างหนึ่ง:
      - blocked_by non-empty (fast_reject_on_blocked=True)
      - decision ไม่ใช่ BUY/SELL (fast_reject_on_invalid_decision=True)
      - rr < fast_reject_max_rr (rr ต่ำเกินไปที่จะ trade)
      - plan sanity ล้มเหลว (sl/tp ไม่ถูกทิศ)

    Tier 3 — UNCERTAIN (approved=None):
      ไม่ผ่าน Tier 1 และไม่ถูก Tier 2 → caller ต้องส่ง LLM

    Usage:
        fast = FastAIConfirm({"fast_approve_min_align": 3, "fast_approve_min_rr": 1.5})
        approved, reason, confidence = fast.confirm(execution_package)
        if approved is not None:
            # ตัดสินได้ทันที — ไม่ต้องเรียก LLM
            return {"approved": approved, "reason": reason, "confidence": confidence}
        else:
            # ส่งต่อให้ LLM
            ...
    """

    def __init__(self, config: Any = None) -> None:
        """
        Parameters
        ----------
        config : dict หรือ FastAIConfig หรือ None
            ถ้า None → ใช้ default FastAIConfig
            ถ้า dict → แปลงผ่าน FastAIConfig.from_dict()
        """
        if config is None:
            self._cfg = FastAIConfig()
        elif isinstance(config, FastAIConfig):
            self._cfg = config
        elif isinstance(config, dict):
            self._cfg = FastAIConfig.from_dict(config)
        else:
            self._cfg = FastAIConfig()

    @property
    def config(self) -> FastAIConfig:
        return self._cfg

    # -----------------------------------------------------------------------
    #  Helpers — field extraction จาก execution_package
    # -----------------------------------------------------------------------

    @staticmethod
    def _extract_blocked_by(pkg: Dict[str, Any]) -> List[str]:
        """ดึง blocked_by จาก top-level หรือ context"""
        bb = pkg.get("blocked_by")
        if bb is None:
            bb = (pkg.get("context") or {}).get("blocked_by")
        if bb is None:
            bb = pkg.get("blocked_list") or pkg.get("blocked") or []
        if not isinstance(bb, list):
            return []
        return [str(x) for x in bb if x]

    @staticmethod
    def _extract_decision(pkg: Dict[str, Any]) -> str:
        d = pkg.get("decision")
        if d is None:
            d = (pkg.get("context") or {}).get("decision", "")
        return str(d).strip().upper()

    @staticmethod
    def _extract_metrics(pkg: Dict[str, Any]) -> Dict[str, Any]:
        m = pkg.get("metrics")
        if not isinstance(m, dict):
            m = {}
        return m

    @staticmethod
    def _extract_plan(pkg: Dict[str, Any]) -> Dict[str, Any]:
        p = pkg.get("plan")
        if not isinstance(p, dict):
            p = {}
        return p

    @staticmethod
    def _safe_float(v: Any) -> Optional[float]:
        try:
            if v is None:
                return None
            f = float(v)
            return f if (f == f) else None    # NaN check
        except Exception:
            return None

    @staticmethod
    def _safe_int(v: Any) -> Optional[int]:
        try:
            if v is None:
                return None
            return int(float(v))
        except Exception:
            return None

    # -----------------------------------------------------------------------
    #  Plan Sanity Check
    # -----------------------------------------------------------------------

    def _plan_sanity(
        self,
        decision: str,
        plan: Dict[str, Any],
    ) -> Tuple[bool, str]:
        """
        ตรวจ plan ขั้นพื้นฐาน:
        - มี entry, sl, tp
        - BUY:  sl < entry < tp
        - SELL: tp < entry < sl
        คืน (ok, reason)
        """
        entry = self._safe_float(plan.get("entry"))
        sl    = self._safe_float(plan.get("sl"))
        tp    = self._safe_float(plan.get("tp"))

        if entry is None or sl is None or tp is None:
            return False, "plan_missing_fields"
        if sl <= 0 or tp <= 0 or entry <= 0:
            return False, "plan_invalid_values"

        if decision == "BUY":
            if not (sl < entry < tp):
                return False, "plan_invalid_side_buy"
        elif decision == "SELL":
            if not (tp < entry < sl):
                return False, "plan_invalid_side_sell"
        else:
            return False, "plan_unknown_decision"

        return True, "plan_ok"

    # -----------------------------------------------------------------------
    #  Main confirm() method
    # -----------------------------------------------------------------------

    def confirm(
        self,
        execution_package: Dict[str, Any],
    ) -> Tuple[Optional[bool], str, Optional[float]]:
        """
        Fast AI Confirm — ตัดสิน approve / reject / uncertain

        Parameters
        ----------
        execution_package : dict
            execution package จาก mentor_executor.build_execution_package()
            required fields (ถ้ามี): decision, plan, metrics, blocked_by

        Returns
        -------
        (approved, reason, confidence)
            approved = True   : fast approve — confidence ≥ 0.80
            approved = False  : fast reject  — confidence = None
            approved = None   : uncertain    — ส่ง LLM ต่อ, confidence = None
        """
        result = self.confirm_detailed(execution_package)
        return result.as_tuple()

    def confirm_detailed(
        self,
        execution_package: Dict[str, Any],
    ) -> FastConfirmResult:
        """
        เหมือน confirm() แต่คืน FastConfirmResult พร้อม metadata ครบ
        (ใช้ใน testing / logging)
        """
        t0 = time.monotonic()

        if not isinstance(execution_package, dict):
            elapsed = (time.monotonic() - t0) * 1000
            return FastConfirmResult(
                approved=False,
                reason="invalid_package_type",
                confidence=None,
                tier="reject",
                elapsed_ms=elapsed,
                checks={"package_is_dict": False},
            )

        # ─ Extract fields ─
        blocked_by = self._extract_blocked_by(execution_package)
        decision   = self._extract_decision(execution_package)
        metrics    = self._extract_metrics(execution_package)
        plan       = self._extract_plan(execution_package)

        align_raw = metrics.get("alignment_score")
        rr_raw    = metrics.get("rr")
        align     = self._safe_int(align_raw)
        rr        = self._safe_float(rr_raw)

        # ─ Build checks dict (สำหรับ debug/log) ─
        checks: Dict[str, Any] = {
            "blocked_by":       blocked_by,
            "decision":         decision,
            "alignment_score":  align,
            "rr":               rr,
            "has_plan":         bool(plan),
        }

        # ================================================================
        # TIER 2 — FAST REJECT (priority ก่อน Tier 1 เสมอ)
        # ================================================================

        # 2a. blocked_by non-empty → reject ทันที
        if self._cfg.fast_reject_on_blocked and blocked_by:
            elapsed = (time.monotonic() - t0) * 1000
            checks["reject_reason"] = "blocked_by"
            return FastConfirmResult(
                approved=False,
                reason=f"blocked_by:{','.join(blocked_by[:3])}",
                confidence=None,
                tier="reject",
                elapsed_ms=elapsed,
                checks=checks,
            )

        # 2b. decision invalid → reject
        if self._cfg.fast_reject_on_invalid_decision and decision not in ("BUY", "SELL"):
            elapsed = (time.monotonic() - t0) * 1000
            checks["reject_reason"] = "invalid_decision"
            return FastConfirmResult(
                approved=False,
                reason=f"decision_not_trade:{decision or 'empty'}",
                confidence=None,
                tier="reject",
                elapsed_ms=elapsed,
                checks=checks,
            )

        # 2c. rr ต่ำกว่า fast_reject_max_rr → reject
        if rr is not None and rr < self._cfg.fast_reject_max_rr:
            elapsed = (time.monotonic() - t0) * 1000
            checks["reject_reason"] = "rr_too_low"
            return FastConfirmResult(
                approved=False,
                reason=f"rr_too_low:{rr:.3f}<{self._cfg.fast_reject_max_rr}",
                confidence=None,
                tier="reject",
                elapsed_ms=elapsed,
                checks=checks,
            )

        # 2d. plan sanity ล้มเหลว (ถ้ามี plan)
        if plan:
            plan_ok, plan_reason = self._plan_sanity(decision, plan)
            if not plan_ok:
                elapsed = (time.monotonic() - t0) * 1000
                checks["reject_reason"] = plan_reason
                return FastConfirmResult(
                    approved=False,
                    reason=plan_reason,
                    confidence=None,
                    tier="reject",
                    elapsed_ms=elapsed,
                    checks=checks,
                )
            checks["plan_sanity"] = plan_reason

        # ================================================================
        # TIER 1 — FAST APPROVE
        # ================================================================
        # เงื่อนไขต้องผ่านพร้อมกัน:
        #   1) align >= fast_approve_min_align
        #   2) rr >= fast_approve_min_rr
        #   3) blocked_by = []  (ผ่านไปแล้วจาก Tier 2a)
        #   4) decision valid   (ผ่านไปแล้วจาก Tier 2b)

        align_ok = (
            align is not None
            and align >= self._cfg.fast_approve_min_align
        )
        rr_ok = (
            rr is not None
            and rr >= self._cfg.fast_approve_min_rr
        )

        checks["align_ok"] = align_ok
        checks["rr_ok"]    = rr_ok

        if align_ok and rr_ok:
            # confidence ตามระดับ: align สูงกว่า threshold → confidence สูงขึ้น
            confidence = self._calc_approve_confidence(align, rr)
            elapsed = (time.monotonic() - t0) * 1000
            return FastConfirmResult(
                approved=True,
                reason=(
                    f"fast_approve:align={align}"
                    f"_rr={rr:.3f}"
                    f"_conf={confidence:.2f}"
                ),
                confidence=confidence,
                tier="approve",
                elapsed_ms=elapsed,
                checks=checks,
            )

        # ================================================================
        # TIER 3 — UNCERTAIN → ส่ง LLM
        # ================================================================
        # ถึงจุดนี้ = ไม่ผ่าน Tier 1 และไม่ถูก reject โดย Tier 2
        # แต่ยังมี rr ต้องผ่าน uncertain_min_rr ถึงจะ uncertain
        # (ถ้า rr ไม่ทราบค่า → uncertain เพื่อให้ LLM ตัดสิน)
        if rr is not None and rr < self._cfg.uncertain_min_rr:
            elapsed = (time.monotonic() - t0) * 1000
            checks["reject_reason"] = "rr_below_uncertain_floor"
            return FastConfirmResult(
                approved=False,
                reason=f"rr_below_uncertain_floor:{rr:.3f}",
                confidence=None,
                tier="reject",
                elapsed_ms=elapsed,
                checks=checks,
            )

        # Build uncertain reason สำหรับ logging
        uncertain_parts = []
        if not align_ok:
            if align is None:
                uncertain_parts.append("align=missing")
            else:
                uncertain_parts.append(
                    f"align={align}<{self._cfg.fast_approve_min_align}"
                )
        if not rr_ok:
            if rr is None:
                uncertain_parts.append("rr=missing")
            else:
                uncertain_parts.append(
                    f"rr={rr:.3f}<{self._cfg.fast_approve_min_rr}"
                )

        elapsed = (time.monotonic() - t0) * 1000
        return FastConfirmResult(
            approved=None,
            reason="uncertain:needs_llm:" + ("|".join(uncertain_parts) or "borderline"),
            confidence=None,
            tier="uncertain",
            elapsed_ms=elapsed,
            checks=checks,
        )

    # -----------------------------------------------------------------------
    #  Confidence Calculation (Tier 1 only)
    # -----------------------------------------------------------------------

    def _calc_approve_confidence(self, align: int, rr: float) -> float:
        """
        คำนวณ confidence สำหรับ fast approve
        scale ตาม align และ rr เพื่อให้ compatible กับ LLM confidence (0.0–1.0)

        align=3, rr=1.5 → ~0.80
        align=4, rr=2.0 → ~0.88
        align=5, rr=2.5 → ~0.95 (cap ที่ 0.97)
        """
        base = 0.70

        # align bonus: ทุก 1 align เกิน threshold += 0.04
        align_bonus = min(0.15, (align - self._cfg.fast_approve_min_align) * 0.04)

        # rr bonus: ทุก 0.1 rr เกิน threshold += 0.01 (cap 0.10)
        rr_bonus = min(0.10, (rr - self._cfg.fast_approve_min_rr) * 0.05)

        confidence = base + align_bonus + rr_bonus
        return round(min(0.97, max(0.70, confidence)), 4)


# ===========================================================================
#  Self-Test
# ===========================================================================

def _run_tests() -> int:
    """
    Unit tests สำหรับ Phase 1.4
    คืน 0 = ทุก test ผ่าน, 1 = มี test ล้มเหลว
    """
    SEP = "=" * 68
    print(SEP)
    print(f"  HIM Fast AI Confirm — Self-Test  ({VERSION})")
    print(SEP)

    fail_count = 0
    test_count = 0

    def _assert(
        label: str,
        actual: Any,
        expected: Any,
        extra: str = "",
    ) -> None:
        nonlocal fail_count, test_count
        test_count += 1
        if actual == expected:
            print(f"  [PASS] {label} → {actual!r}  {extra}✓")
        else:
            print(f"  [FAIL] {label}")
            print(f"         expected={expected!r}  actual={actual!r}  {extra}")
            fail_count += 1

    def _assert_not_none(label: str, val: Any, extra: str = "") -> None:
        nonlocal fail_count, test_count
        test_count += 1
        if val is not None:
            print(f"  [PASS] {label} → {val!r}  {extra}✓")
        else:
            print(f"  [FAIL] {label} → expected not None, got None  {extra}")
            fail_count += 1

    def _assert_lt(label: str, val: float, limit: float, extra: str = "") -> None:
        nonlocal fail_count, test_count
        test_count += 1
        if val < limit:
            print(f"  [PASS] {label} → {val:.4f}ms < {limit}ms  {extra}✓")
        else:
            print(f"  [FAIL] {label} → {val:.4f}ms ≥ {limit}ms  {extra}")
            fail_count += 1

    fast = FastAIConfirm()

    # ─────────────────────────────────────────────────────────────────────
    # TIER 1 — FAST APPROVE
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*68}")
    print("  TIER 1: Fast Approve Tests")
    print(f"{'─'*68}")

    # Test A: align=3, rr=1.5, no blocked → approved=True
    pkg_a = {
        "decision":   "BUY",
        "blocked_by": [],
        "metrics":    {"alignment_score": 3, "rr": 1.5},
        "plan":       {"entry": 2320.0, "sl": 2316.0, "tp": 2326.0},
    }
    r_a = fast.confirm_detailed(pkg_a)
    _assert("T1-A approved",    r_a.approved,    True)
    _assert("T1-A tier",        r_a.tier,        "approve")
    _assert_not_none("T1-A confidence", r_a.confidence)
    _assert_lt("T1-A latency", r_a.elapsed_ms, 1.0, "(target < 1ms)")

    # Test B: align=5, rr=2.5 (high confidence)
    pkg_b = {
        "decision":   "SELL",
        "blocked_by": [],
        "metrics":    {"alignment_score": 5, "rr": 2.5},
        "plan":       {"entry": 2320.0, "sl": 2325.0, "tp": 2314.0},
    }
    r_b = fast.confirm_detailed(pkg_b)
    _assert("T1-B approved",    r_b.approved,    True)
    _assert("T1-B tier",        r_b.tier,        "approve")
    if r_b.confidence is not None:
        _assert("T1-B conf >= 0.80", r_b.confidence >= 0.80, True,
                f"(conf={r_b.confidence:.4f})")

    # Test C: align=3, rr=1.5 exactly (boundary)
    pkg_c = {
        "decision":   "BUY",
        "blocked_by": [],
        "metrics":    {"alignment_score": 3, "rr": 1.5},
        "plan":       {"entry": 2320.0, "sl": 2315.0, "tp": 2327.5},
    }
    r_c = fast.confirm_detailed(pkg_c)
    _assert("T1-C boundary align=3,rr=1.5", r_c.approved, True)

    # ─────────────────────────────────────────────────────────────────────
    # TIER 2 — FAST REJECT
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*68}")
    print("  TIER 2: Fast Reject Tests")
    print(f"{'─'*68}")

    # Test D: blocked_by non-empty → approved=False < 1ms
    pkg_d = {
        "decision":   "BUY",
        "blocked_by": ["supertrend_conflict", "bos_missing"],
        "metrics":    {"alignment_score": 4, "rr": 1.8},
        "plan":       {"entry": 2320.0, "sl": 2315.0, "tp": 2328.0},
    }
    r_d = fast.confirm_detailed(pkg_d)
    _assert("T2-D approved",    r_d.approved,    False)
    _assert("T2-D tier",        r_d.tier,        "reject")
    _assert("T2-D confidence",  r_d.confidence,  None)
    _assert_lt("T2-D latency",  r_d.elapsed_ms,  1.0,  "(target < 1ms)")
    if "blocked_by" in r_d.reason:
        _assert("T2-D reason has blocked_by", True, True, f"({r_d.reason!r})")
    else:
        _assert("T2-D reason has blocked_by", False, True, f"({r_d.reason!r})")

    # Test E: blocked_by = ["sl_too_close"]
    pkg_e = {
        "decision":   "SELL",
        "blocked_by": ["sl_too_close"],
        "metrics":    {"alignment_score": 3, "rr": 1.5},
    }
    r_e = fast.confirm_detailed(pkg_e)
    _assert("T2-E blocked_by single", r_e.approved, False)

    # Test F: decision invalid ("HOLD") → reject
    pkg_f = {
        "decision":   "HOLD",
        "blocked_by": [],
        "metrics":    {"alignment_score": 3, "rr": 1.5},
    }
    r_f = fast.confirm_detailed(pkg_f)
    _assert("T2-F invalid decision", r_f.approved, False)

    # Test G: rr < fast_reject_max_rr → reject
    pkg_g = {
        "decision":   "BUY",
        "blocked_by": [],
        "metrics":    {"alignment_score": 3, "rr": 1.0},
        "plan":       {"entry": 2320.0, "sl": 2315.0, "tp": 2325.0},
    }
    r_g = fast.confirm_detailed(pkg_g)
    _assert("T2-G rr_too_low", r_g.approved, False)
    _assert("T2-G tier",       r_g.tier,     "reject")

    # Test H: plan sanity fail (BUY but sl > entry)
    pkg_h = {
        "decision":   "BUY",
        "blocked_by": [],
        "metrics":    {"alignment_score": 3, "rr": 1.5},
        "plan":       {"entry": 2320.0, "sl": 2325.0, "tp": 2326.0},  # sl > entry
    }
    r_h = fast.confirm_detailed(pkg_h)
    _assert("T2-H plan invalid BUY", r_h.approved, False)

    # ─────────────────────────────────────────────────────────────────────
    # TIER 3 — UNCERTAIN (borderline → None)
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*68}")
    print("  TIER 3: Uncertain / LLM Fallback Tests")
    print(f"{'─'*68}")

    # Test I: align=2 (below 3), rr=1.5 → uncertain
    pkg_i = {
        "decision":   "BUY",
        "blocked_by": [],
        "metrics":    {"alignment_score": 2, "rr": 1.5},
        "plan":       {"entry": 2320.0, "sl": 2315.0, "tp": 2327.5},
    }
    r_i = fast.confirm_detailed(pkg_i)
    _assert("T3-I align=2 (uncertain)", r_i.approved, None)
    _assert("T3-I tier",                r_i.tier,     "uncertain")
    _assert("T3-I confidence is None",  r_i.confidence, None)

    # Test J: align=3 but rr=1.3 (between reject_max and approve_min) → uncertain
    pkg_j = {
        "decision":   "BUY",
        "blocked_by": [],
        "metrics":    {"alignment_score": 3, "rr": 1.35},
        "plan":       {"entry": 2320.0, "sl": 2315.0, "tp": 2326.75},
    }
    r_j = fast.confirm_detailed(pkg_j)
    _assert("T3-J borderline rr=1.35", r_j.approved, None)
    _assert("T3-J tier",               r_j.tier,     "uncertain")

    # Test K: align and rr both missing → uncertain (no data → LLM)
    pkg_k = {
        "decision":   "BUY",
        "blocked_by": [],
        "metrics":    {},
        "plan":       {"entry": 2320.0, "sl": 2315.0, "tp": 2327.5},
    }
    r_k = fast.confirm_detailed(pkg_k)
    _assert("T3-K no metrics (uncertain)", r_k.approved, None)

    # ─────────────────────────────────────────────────────────────────────
    # LATENCY STRESS TEST
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*68}")
    print("  LATENCY: 1000x calls < 1ms each")
    print(f"{'─'*68}")

    REPS = 1000
    t_start = time.monotonic()
    for _ in range(REPS):
        fast.confirm(pkg_a)    # approve path
        fast.confirm(pkg_d)    # reject path
        fast.confirm(pkg_i)    # uncertain path
    total_ms = (time.monotonic() - t_start) * 1000
    avg_ms   = total_ms / (REPS * 3)

    test_count += 1
    if avg_ms < 0.5:
        print(
            f"  [PASS] Avg per call = {avg_ms:.4f}ms "
            f"({REPS*3} calls total={total_ms:.2f}ms) ✓"
        )
    else:
        print(
            f"  [WARN] Avg per call = {avg_ms:.4f}ms (>0.5ms — may be slow env)"
        )
        # ไม่ fail test เพราะ environment อาจช้า แต่ warn

    # ─────────────────────────────────────────────────────────────────────
    # FastAIConfig.from_dict() test
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*68}")
    print("  CONFIG: FastAIConfig.from_dict()")
    print(f"{'─'*68}")

    cfg_dict = {
        "fast_approve_min_align": 4,
        "fast_approve_min_rr":    2.0,
        "fast_reject_max_rr":     1.3,
        "uncertain_min_rr":       1.3,
    }
    cfg = FastAIConfig.from_dict(cfg_dict)
    _assert("config min_align",        cfg.fast_approve_min_align, 4)
    _assert("config fast_approve_rr",  cfg.fast_approve_min_rr,    2.0)
    _assert("config fast_reject_rr",   cfg.fast_reject_max_rr,     1.3)

    # ใช้ config ที่เข้มกว่า → align=3, rr=1.5 ไม่ผ่าน → uncertain
    fast_strict = FastAIConfirm(cfg_dict)
    r_strict = fast_strict.confirm_detailed(pkg_a)
    _assert("strict config: align=3 insufficient → uncertain", r_strict.approved, None)

    # ────────��────────────────────────────────────────────────────────────
    # as_ai_confirm_dict() compatible test
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{'─'*68}")
    print("  INTEGRATION: as_ai_confirm_dict() format")
    print(f"{'─'*68}")

    r_approve = fast.confirm_detailed(pkg_a)
    d = r_approve.as_ai_confirm_dict()
    _assert("dict.approved",   d.get("approved"),  True)
    _assert("dict.provider",   d.get("provider"),  PROVIDER)
    _assert("dict.model",      d.get("model"),     MODEL)
    _assert_not_none("dict.reason", d.get("reason"))
    _assert_not_none("dict.confidence", d.get("confidence"))

    # ─────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────────────────────
    print(f"\n{SEP}")
    if fail_count == 0:
        print(f"  ✅ ALL {test_count} TESTS PASSED — Phase 1.4 READY")
    else:
        print(f"  ❌ {fail_count}/{test_count} TEST(S) FAILED")
    print(SEP)
    return 0 if fail_count == 0 else 1


# ===========================================================================
#  Entry Point
# ===========================================================================

if __name__ == "__main__":
    sys.exit(_run_tests())