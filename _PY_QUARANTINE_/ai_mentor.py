"""
AI Mentor - Spec v2 (Mode D: Bounded Adjustment) - Version: 2.1.1
Changelog:
- 2.1.1 (2026-02-27):
  - NEW: position SL suggestion endpoint (tighten-only, NEVER widen) for Hybrid SL Manager (Mode C)
  - KEEP: execution/analysis/mentor blocks for new trades
  - SAFETY: fail-closed, JSON-only, constraints enforced

Design intent:
- Think big (full context reasoning)
- Act small (bounded authority)
- Position SL: tighten-only to reduce risk in event/whipsaw conditions
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _sf(v: Any, d: float = 0.0) -> float:
    try:
        if v is None:
            return float(d)
        return float(v)
    except Exception:
        return float(d)


def _si(v: Any, d: int = 0) -> int:
    try:
        if v is None:
            return int(d)
        return int(v)
    except Exception:
        return int(d)


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _is_finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


@dataclass(frozen=True)
class AIMentorConfig:
    min_rr: float = 1.50
    entry_shift_max_atr: float = 0.20
    sl_atr_min: float = 1.20
    sl_atr_max: float = 1.80
    conf_execute_threshold: int = 70
    event_conf_cap_high: int = 72

    # Position SL tighten-only
    pos_sl_tighten_max_atr: float = 0.30  # tighten at most 0.30*ATR in one step
    pos_sl_min_improve_atr: float = 0.03  # ignore tiny changes (< 0.03*ATR)

    # Confidence scoring weights (sum 100)
    w_technical: int = 25
    w_structure: int = 20
    w_context: int = 20
    w_execution: int = 15
    w_portfolio: int = 20


class AIMentor:
    """
    MOCK AI (rule-based) ที่ทำหน้าที่เหมือน LLM:
    - new trade: evaluate() -> {execution, analysis, mentor}
    - position SL tighten-only: evaluate_position_sl() -> {position, analysis, mentor}
    """

    def __init__(self, cfg: Optional[AIMentorConfig] = None):
        self.cfg = cfg or AIMentorConfig()

    # -------------------------
    # New trade evaluation
    # -------------------------
    def evaluate(self, package: Dict[str, Any]) -> Dict[str, Any]:
        safe_out = self._fail_closed_output(package, reason="init")

        try:
            baseline = (package.get("baseline") or {})
            direction = str(baseline.get("dir", "NONE")).upper()
            entry0 = _sf(baseline.get("entry"))
            sl0 = _sf(baseline.get("sl"))
            tp0 = _sf(baseline.get("tp"))
            rr0 = _sf(baseline.get("rr"))
            atr = max(_sf(baseline.get("atr"), 0.0), 1e-9)

            c = self._merge_constraints(package.get("constraints") or {})

            if direction not in ("BUY", "SELL"):
                return self._fail_closed_output(package, reason="invalid_direction")
            if not self._sanity(direction, entry0, sl0, tp0):
                return self._fail_closed_output(package, reason="baseline_sanity_failed")
            if rr0 < c.min_rr:
                return self._fail_closed_output(package, reason="baseline_rr_below_min")

            tech_score, tech_flags = self._score_technical(package)
            struct_score, struct_flags = self._score_structure(package)
            ctx_score, ctx_flags = self._score_context(package)
            exec_score, exec_flags = self._score_execution(package)
            port_score, port_flags = self._score_portfolio(package)

            risk_flags = self._dedupe_flags(tech_flags + struct_flags + ctx_flags + exec_flags + port_flags)

            breakdown = {
                "technical": _clamp(tech_score, 0, self.cfg.w_technical),
                "structure": _clamp(struct_score, 0, self.cfg.w_structure),
                "context": _clamp(ctx_score, 0, self.cfg.w_context),
                "execution": _clamp(exec_score, 0, self.cfg.w_execution),
                "portfolio": _clamp(port_score, 0, self.cfg.w_portfolio),
            }
            conf_raw = int(sum(breakdown.values()))
            conf = self._apply_event_cap(package, conf_raw, c)

            decision = "APPROVE" if conf >= c.conf_execute_threshold else "REJECT"

            entry1, sl1, tp1, rr1, adjust_notes = self._bounded_adjust(
                direction=direction,
                entry=entry0,
                sl=sl0,
                tp=tp0,
                rr=rr0,
                atr=atr,
                constraints=c,
                package=package,
                risk_flags=risk_flags,
            )

            checks = self._constraint_check(
                direction=direction,
                entry0=entry0,
                sl0=sl0,
                tp0=tp0,
                entry1=entry1,
                sl1=sl1,
                tp1=tp1,
                rr1=rr1,
                atr=atr,
                constraints=c,
            )

            if not checks["all_ok"]:
                entry1, sl1, tp1, rr1 = entry0, sl0, tp0, rr0
                decision = "REJECT"
                conf = min(conf, 45)
                risk_flags = self._dedupe_flags(risk_flags + ["CONSTRAINT_FAIL_REVERT"])

            invalidations = self._build_invalidations(direction, entry1, sl1, tp1, package)

            mentor = self._build_mentor_message(
                direction=direction,
                entry=entry1,
                sl=sl1,
                tp=tp1,
                conf=conf,
                risk_flags=risk_flags,
                adjust_notes=adjust_notes,
                package=package,
            )

            return {
                "execution": {
                    "entry": float(entry1),
                    "sl": float(sl1),
                    "tp": float(tp1),
                    "rr": float(rr1),
                    "conf": int(conf),
                    "decision": str(decision),
                },
                "analysis": {
                    "confidence_breakdown": breakdown,
                    "risk_flags": risk_flags,
                    "invalidations": invalidations,
                    "adjust_notes": adjust_notes,
                    "constraints_check": checks,
                },
                "mentor": mentor,
            }

        except Exception:
            return safe_out

    # -------------------------
    # Position SL tighten-only (Hybrid Mode C)
    # -------------------------
    def evaluate_position_sl(self, package: Dict[str, Any]) -> Dict[str, Any]:
        """
        Input expected:
        {
          "symbol": "...",
          "position": { "dir": "BUY/SELL", "entry":..., "sl":..., "tp":..., "price_now":..., "atr":... },
          "context": {...}, "technical": {...}, "structure": {...}, "portfolio": {...},
          "constraints": { "sl_atr_min":1.2, "sl_atr_max":1.8, ... }
        }

        Output:
        {
          "position": { "new_sl": <float>, "conf": <int>, "decision": "TIGHTEN"|"NO_CHANGE" },
          "analysis": { "risk_flags":[...], "reason":[...], "constraints_check":{...} },
          "mentor": {...}
        }
        """
        safe = {
            "position": {"new_sl": None, "conf": 0, "decision": "NO_CHANGE"},
            "analysis": {"risk_flags": ["FAIL_CLOSED"], "reason": ["init"], "constraints_check": {"all_ok": False}},
            "mentor": {
                "headline": "Position SL: fail-closed",
                "explanation": "Safety triggered.",
                "action_guidance": "No SL change.",
                "confidence_reasoning": "conf=0",
            },
        }

        try:
            pos = (package.get("position") or {})
            direction = str(pos.get("dir", "NONE")).upper()
            entry = _sf(pos.get("entry"))
            sl0 = _sf(pos.get("sl"))
            price_now = _sf(pos.get("price_now"))
            atr = max(_sf(pos.get("atr"), 0.0), 1e-9)

            c = self._merge_constraints(package.get("constraints") or {})

            if direction not in ("BUY", "SELL"):
                return safe
            if not (_is_finite(entry) and _is_finite(sl0) and _is_finite(price_now)):
                return safe

            # Score context to decide "should tighten?"
            _, tech_flags = self._score_technical(package)
            _, struct_flags = self._score_structure(package)
            _, ctx_flags = self._score_context(package)
            _, exec_flags = self._score_execution(package)
            _, port_flags = self._score_portfolio(package)
            risk_flags = self._dedupe_flags(tech_flags + struct_flags + ctx_flags + exec_flags + port_flags)

            reason = []
            want_tighten = False

            # Trigger tighten in high risk environment (event/spread/whipsaw)
            if "EVENT_RISK_HIGH" in risk_flags or "SPREAD_SPIKE" in risk_flags or "SPREAD_ELEVATED" in risk_flags:
                want_tighten = True
                reason.append("high_risk_env")

            # Only tighten if position is at least slightly in profit
            # BUY: price_now > entry ; SELL: price_now < entry
            in_profit = (price_now > entry) if direction == "BUY" else (price_now < entry)
            if not in_profit:
                return {
                    "position": {"new_sl": None, "conf": 40, "decision": "NO_CHANGE"},
                    "analysis": {"risk_flags": risk_flags, "reason": ["not_in_profit"], "constraints_check": {"all_ok": True}},
                    "mentor": {
                        "headline": "Position SL: no change",
                        "explanation": "Not in profit; tightening would increase stop-out risk.",
                        "action_guidance": "Keep current SL.",
                        "confidence_reasoning": "conf=40",
                    },
                }

            if not want_tighten:
                return {
                    "position": {"new_sl": None, "conf": 55, "decision": "NO_CHANGE"},
                    "analysis": {"risk_flags": risk_flags, "reason": ["no_tighten_trigger"], "constraints_check": {"all_ok": True}},
                    "mentor": {
                        "headline": "Position SL: no change",
                        "explanation": "No high-risk trigger detected.",
                        "action_guidance": "Keep current SL; Python BE/Trail will manage.",
                        "confidence_reasoning": "conf=55",
                    },
                }

            # Tighten amount bounded
            tighten_max = self.cfg.pos_sl_tighten_max_atr * atr
            min_improve = self.cfg.pos_sl_min_improve_atr * atr

            if direction == "BUY":
                # move SL up (closer to entry/price) but NEVER above price_now - small buffer
                target = sl0 + tighten_max
                cap = price_now - 0.05 * atr
                new_sl = min(target, cap)
                # Must be > sl0 to be a tighten
                if (new_sl - sl0) < min_improve:
                    return {
                        "position": {"new_sl": None, "conf": 58, "decision": "NO_CHANGE"},
                        "analysis": {"risk_flags": risk_flags, "reason": ["tighten_too_small"], "constraints_check": {"all_ok": True}},
                        "mentor": {
                            "headline": "Position SL: no change",
                            "explanation": "Suggested tighten too small; skipped.",
                            "action_guidance": "Keep current SL.",
                            "confidence_reasoning": "conf=58",
                        },
                    }
            else:
                # SELL: move SL down (closer) but NEVER below price_now + buffer
                target = sl0 - tighten_max
                cap = price_now + 0.05 * atr
                new_sl = max(target, cap)
                if (sl0 - new_sl) < min_improve:
                    return {
                        "position": {"new_sl": None, "conf": 58, "decision": "NO_CHANGE"},
                        "analysis": {"risk_flags": risk_flags, "reason": ["tighten_too_small"], "constraints_check": {"all_ok": True}},
                        "mentor": {
                            "headline": "Position SL: no change",
                            "explanation": "Suggested tighten too small; skipped.",
                            "action_guidance": "Keep current SL.",
                            "confidence_reasoning": "conf=58",
                        },
                    }

            # Constraint check: keep SL distance within ATR bounds relative to entry (not perfect but consistent)
            sl_dist = abs(new_sl - entry)
            sl_min = c.sl_atr_min * atr
            sl_max = c.sl_atr_max * atr
            ok = (sl_dist >= (sl_min - 1e-9)) and (sl_dist <= (sl_max + 1e-9))

            if not ok:
                # If violates ATR window, do NOT apply
                return {
                    "position": {"new_sl": None, "conf": 50, "decision": "NO_CHANGE"},
                    "analysis": {"risk_flags": self._dedupe_flags(risk_flags + ["POS_SL_CONSTRAINT_BLOCK"]),
                                "reason": ["sl_atr_window_fail"],
                                "constraints_check": {"all_ok": False, "sl_atr_window_ok": False}},
                    "mentor": {
                        "headline": "Position SL: blocked by constraints",
                        "explanation": "Tighten would violate SL ATR window.",
                        "action_guidance": "No SL change.",
                        "confidence_reasoning": "conf=50",
                    },
                }

            conf = self._apply_event_cap(package, 72, c)  # tighten action is cautious but useful
            return {
                "position": {"new_sl": float(new_sl), "conf": int(conf), "decision": "TIGHTEN"},
                "analysis": {
                    "risk_flags": risk_flags,
                    "reason": reason + ["tighten_only_bounded"],
                    "constraints_check": {"all_ok": True, "sl_atr_window_ok": True},
                },
                "mentor": {
                    "headline": "Position SL: tighten suggested",
                    "explanation": "High-risk conditions detected; tightening SL reduces tail risk.",
                    "action_guidance": f"Move SL to {new_sl:.5f} (tighten-only).",
                    "confidence_reasoning": f"conf={conf} (bounded tighten).",
                },
            }

        except Exception:
            return safe

    # -------------------------
    # Helpers
    # -------------------------
    def _merge_constraints(self, c: Dict[str, Any]) -> AIMentorConfig:
        return AIMentorConfig(
            min_rr=_sf(c.get("min_rr", self.cfg.min_rr), self.cfg.min_rr),
            entry_shift_max_atr=_sf(c.get("entry_shift_max_atr", self.cfg.entry_shift_max_atr), self.cfg.entry_shift_max_atr),
            sl_atr_min=_sf(c.get("sl_atr_min", self.cfg.sl_atr_min), self.cfg.sl_atr_min),
            sl_atr_max=_sf(c.get("sl_atr_max", self.cfg.sl_atr_max), self.cfg.sl_atr_max),
            conf_execute_threshold=_si(c.get("conf_execute_threshold", self.cfg.conf_execute_threshold), self.cfg.conf_execute_threshold),
            event_conf_cap_high=_si(c.get("event_conf_cap_high", self.cfg.event_conf_cap_high), self.cfg.event_conf_cap_high),
            pos_sl_tighten_max_atr=_sf(c.get("pos_sl_tighten_max_atr", self.cfg.pos_sl_tighten_max_atr), self.cfg.pos_sl_tighten_max_atr),
            pos_sl_min_improve_atr=_sf(c.get("pos_sl_min_improve_atr", self.cfg.pos_sl_min_improve_atr), self.cfg.pos_sl_min_improve_atr),
            w_technical=self.cfg.w_technical,
            w_structure=self.cfg.w_structure,
            w_context=self.cfg.w_context,
            w_execution=self.cfg.w_execution,
            w_portfolio=self.cfg.w_portfolio,
        )

    def _fail_closed_output(self, package: Dict[str, Any], reason: str) -> Dict[str, Any]:
        baseline = (package.get("baseline") or {})
        entry = _sf(baseline.get("entry"))
        sl = _sf(baseline.get("sl"))
        tp = _sf(baseline.get("tp"))
        rr = _sf(baseline.get("rr"))
        return {
            "execution": {"entry": float(entry), "sl": float(sl), "tp": float(tp), "rr": float(rr), "conf": 0, "decision": "REJECT"},
            "analysis": {
                "confidence_breakdown": {"technical": 0, "structure": 0, "context": 0, "execution": 0, "portfolio": 0},
                "risk_flags": ["FAIL_CLOSED", f"REASON_{reason}"],
                "invalidations": [],
                "adjust_notes": [f"fail_closed: {reason}"],
                "constraints_check": {"all_ok": False},
            },
            "mentor": {
                "headline": "AI fail-closed (no trade)",
                "explanation": f"System safety triggered: {reason}",
                "action_guidance": "Skip this signal.",
                "confidence_reasoning": "Confidence=0 due to fail-closed safety.",
            },
        }

    def _sanity(self, direction: str, entry: float, sl: float, tp: float) -> bool:
        if not (_is_finite(entry) and _is_finite(sl) and _is_finite(tp)):
            return False
        if direction == "BUY":
            return sl < entry < tp
        if direction == "SELL":
            return tp < entry < sl
        return False

    def _dedupe_flags(self, flags: List[str]) -> List[str]:
        out, seen = [], set()
        for f in flags:
            f = str(f).strip()
            if not f or f in seen:
                continue
            out.append(f)
            seen.add(f)
        return out[:30]

    # ---- scoring (same as before, compact) ----
    def _score_technical(self, package: Dict[str, Any]) -> Tuple[int, List[str]]:
        t = (package.get("technical") or {})
        flags, score = [], self.cfg.w_technical
        rsi = _sf(t.get("rsi"), 50.0)
        bb = str(t.get("bb_state", "")).lower()
        adx = _sf(t.get("adx"), 20.0)

        if bb not in ("lower_touch", "upper_touch"):
            score -= 4
            flags.append("BB_NOT_ALIGNED")
        if not (rsi < 30 or rsi > 70):
            score -= 3
            flags.append("RSI_NOT_EXTREME")
        if adx < 17:
            score -= 4
            flags.append("LOW_ADX_RANGE")
        elif adx <= 30:
            score -= 2
        return int(_clamp(score, 0, self.cfg.w_technical)), flags

    def _score_structure(self, package: Dict[str, Any]) -> Tuple[int, List[str]]:
        s = (package.get("structure") or {})
        flags, score = [], self.cfg.w_structure
        bos = bool(s.get("bos", False))
        choch = bool(s.get("choch", False))
        regime = str((package.get("context") or {}).get("regime", "")).lower()
        if not bos:
            score -= 8
            flags.append("NO_BOS")
        if choch:
            score -= 3
            flags.append("CHOCH_PRESENT")
        if regime == "sideways":
            score -= 2
        return int(_clamp(score, 0, self.cfg.w_structure)), flags

    def _score_context(self, package: Dict[str, Any]) -> Tuple[int, List[str]]:
        c = (package.get("context") or {})
        flags, score = [], self.cfg.w_context
        event_risk = str(c.get("event_risk", "")).upper()
        session = str(c.get("session", "")).lower()
        corr = (c.get("correlations") or {})
        if event_risk.endswith("HIGH"):
            score -= 8
            flags.append("EVENT_RISK_HIGH")
        elif event_risk:
            score -= 4
            flags.append("EVENT_RISK_MED")
        if session not in ("london", "newyork", "ny", "ny_overlap"):
            score -= 2
        if isinstance(corr, dict) and corr:
            mx = max(abs(_sf(v, 0.0)) for v in corr.values())
            if mx >= 0.6:
                score -= 5
                flags.append("CORRELATION_RISK_HIGH")
            elif mx >= 0.3:
                score -= 3
                flags.append("CORRELATION_RISK_MED")
        return int(_clamp(score, 0, self.cfg.w_context)), flags

    def _score_execution(self, package: Dict[str, Any]) -> Tuple[int, List[str]]:
        e = (package.get("execution_context") or {})
        flags, score = [], self.cfg.w_execution
        spread_z = _sf(e.get("spread_z"), 0.0)
        slip = _sf(e.get("slippage_estimate"), 0.0)
        if spread_z >= 2.5:
            score -= 8
            flags.append("SPREAD_SPIKE")
        elif spread_z >= 1.5:
            score -= 4
            flags.append("SPREAD_ELEVATED")
        if slip >= 1.0:
            score -= 4
            flags.append("SLIPPAGE_RISK")
        elif slip >= 0.5:
            score -= 2
        return int(_clamp(score, 0, self.cfg.w_execution)), flags

    def _score_portfolio(self, package: Dict[str, Any]) -> Tuple[int, List[str]]:
        p = (package.get("portfolio") or {})
        flags, score = [], self.cfg.w_portfolio
        dd = _sf(p.get("equity_drawdown_pct"), 0.0)
        corr_risk = _sf(p.get("correlation_risk"), 0.0)
        open_pos = _si(p.get("open_positions_symbol"), 0)
        if dd >= 3.0:
            score -= 10
            flags.append("DD_ELEVATED")
        elif dd >= 1.5:
            score -= 5
        if corr_risk >= 0.6:
            score -= 6
            flags.append("PORT_CORR_RISK_HIGH")
        elif corr_risk >= 0.3:
            score -= 3
            flags.append("PORT_CORR_RISK_MED")
        if open_pos >= 2:
            score -= 4
            flags.append("MULTI_POS_EXPOSURE")
        return int(_clamp(score, 0, self.cfg.w_portfolio)), flags

    def _apply_event_cap(self, package: Dict[str, Any], conf: int, c: AIMentorConfig) -> int:
        ctx = (package.get("context") or {})
        event_risk = str(ctx.get("event_risk", "")).upper()
        if event_risk.endswith("HIGH"):
            return int(min(conf, c.event_conf_cap_high))
        return int(_clamp(conf, 0, 100))

    def _bounded_adjust(
        self,
        direction: str,
        entry: float,
        sl: float,
        tp: float,
        rr: float,
        atr: float,
        constraints: AIMentorConfig,
        package: Dict[str, Any],
        risk_flags: List[str],
    ) -> Tuple[float, float, float, float, List[str]]:
        notes: List[str] = []
        entry1, sl1, tp1 = entry, sl, tp

        ctx = (package.get("context") or {})
        liq = str(ctx.get("liquidity", "")).lower()
        max_shift = constraints.entry_shift_max_atr * atr

        if "wall" in liq and max_shift > 0:
            shift = 0.10 * max_shift
            if direction == "BUY":
                entry1, sl1, tp1 = entry + shift, sl + shift, tp + shift
            else:
                entry1, sl1, tp1 = entry - shift, sl - shift, tp - shift
            notes.append(f"entry_shift_liq: {shift:.5f}")

        if "EVENT_RISK_HIGH" in risk_flags:
            tighten = 0.05 * atr
            if direction == "BUY":
                sl1 = sl1 + tighten
            else:
                sl1 = sl1 - tighten
            notes.append(f"sl_tighten_event: {tighten:.5f}")

        rr1 = self._calc_rr(direction, entry1, sl1, tp1)
        if rr1 < constraints.min_rr:
            sl_dist = abs(sl1 - entry1)
            if sl_dist > 0:
                tp_dist = constraints.min_rr * sl_dist
                tp1 = (entry1 + tp_dist) if direction == "BUY" else (entry1 - tp_dist)
                rr1 = self._calc_rr(direction, entry1, sl1, tp1)
                notes.append("tp_adjust_restore_min_rr")

        return float(entry1), float(sl1), float(tp1), float(rr1), notes

    def _calc_rr(self, direction: str, entry: float, sl: float, tp: float) -> float:
        risk = abs(entry - sl)
        if risk <= 0:
            return 0.0
        reward = abs(tp - entry)
        return float(reward / risk)

    def _constraint_check(
        self,
        direction: str,
        entry0: float,
        sl0: float,
        tp0: float,
        entry1: float,
        sl1: float,
        tp1: float,
        rr1: float,
        atr: float,
        constraints: AIMentorConfig,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        max_shift = constraints.entry_shift_max_atr * atr
        out["entry_shift_ok"] = abs(entry1 - entry0) <= (max_shift + 1e-9)

        sl_dist = abs(sl1 - entry1)
        out["sl_atr_min_ok"] = sl_dist >= (constraints.sl_atr_min * atr - 1e-9)
        out["sl_atr_max_ok"] = sl_dist <= (constraints.sl_atr_max * atr + 1e-9)

        out["rr_min_ok"] = rr1 >= (constraints.min_rr - 1e-9)
        out["sanity_ok"] = self._sanity(direction, entry1, sl1, tp1)

        out["all_ok"] = bool(out["entry_shift_ok"] and out["sl_atr_min_ok"] and out["sl_atr_max_ok"] and out["rr_min_ok"] and out["sanity_ok"])
        return out

    def _build_invalidations(self, direction: str, entry: float, sl: float, tp: float, package: Dict[str, Any]) -> List[str]:
        inv = []
        inv.append(f"break_{'below' if direction=='BUY' else 'above'}_sl_{sl:.5f}")
        inv.append(f"break_{'above' if direction=='BUY' else 'below'}_tp_{tp:.5f}")
        return inv[:10]

    def _build_mentor_message(
        self,
        direction: str,
        entry: float,
        sl: float,
        tp: float,
        conf: int,
        risk_flags: List[str],
        adjust_notes: List[str],
        package: Dict[str, Any],
    ) -> Dict[str, str]:
        ctx = (package.get("context") or {})
        t = (package.get("technical") or {})
        s = (package.get("structure") or {})
        event_risk = str(ctx.get("event_risk", "")).upper() or "NONE"
        regime = str(ctx.get("regime", "")).lower() or "unknown"
        session = str(ctx.get("session", "")).lower() or "unknown"
        rsi = _sf(t.get("rsi"), 0.0)
        bb = str(t.get("bb_state", "")).lower()
        bos = bool(s.get("bos", False))

        headline = f"{direction} setup | conf={conf}% | regime={regime} | event={event_risk}"
        explanation = f"BB={bb}, RSI={rsi:.1f}, BOS={bos}. Session={session}. RiskFlags={','.join(risk_flags[:6]) or 'none'}."
        action = f"Entry={entry:.5f} SL={sl:.5f} TP={tp:.5f}. " + (f"Adjustments={','.join(adjust_notes)}" if adjust_notes else "No adjustment.")
        conf_reason = "Confidence reflects multi-dimension alignment; reduced by event/execution/portfolio risks."
        return {
            "headline": headline,
            "explanation": explanation,
            "action_guidance": action,
            "confidence_reasoning": conf_reason,
        }


def _selftest() -> int:
    ai = AIMentor()

    pkg_trade = {
        "baseline": {"dir": "SELL", "entry": 1.10000, "sl": 1.10500, "tp": 1.09500, "rr": 1.67, "atr": 0.0020},
        "technical": {"bb_state": "lower_touch", "rsi": 28.5, "adx": 18.2},
        "structure": {"bos": True, "choch": False},
        "context": {"regime": "sideways", "session": "london", "event_risk": "FOMC_HIGH", "liquidity": "wall_1.0980"},
        "execution_context": {"spread_z": 1.2, "slippage_estimate": 0.3},
        "portfolio": {"open_positions_symbol": 1, "equity_drawdown_pct": 0.8, "correlation_risk": 0.3},
        "constraints": {"min_rr": 1.5, "entry_shift_max_atr": 0.2, "sl_atr_min": 1.2, "sl_atr_max": 1.8, "event_conf_cap_high": 72},
    }
    out_trade = ai.evaluate(pkg_trade)
    print("=== SELFTEST: NEW TRADE ===")
    print(json.dumps(out_trade, indent=2))

    pkg_pos = {
        "position": {"dir": "BUY", "entry": 1.10000, "sl": 1.09650, "tp": 1.10600, "price_now": 1.10120, "atr": 0.0020},
        "context": {"event_risk": "FOMC_HIGH", "regime": "sideways", "session": "london"},
        "execution_context": {"spread_z": 1.7, "slippage_estimate": 0.4},
        "constraints": {"sl_atr_min": 1.2, "sl_atr_max": 1.8, "event_conf_cap_high": 72},
    }
    out_pos = ai.evaluate_position_sl(pkg_pos)
    print("\n=== SELFTEST: POSITION SL ===")
    print(json.dumps(out_pos, indent=2))

    ok = ("execution" in out_trade) and ("position" in out_pos)
    return 0 if ok else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    raise SystemExit(_selftest() if args.selftest else 0)
