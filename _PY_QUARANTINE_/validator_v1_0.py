
"""
Hybrid Intelligence Mentor (HIM)
AI Schema Validator v1.0
File: validator_v1_0.py
Version: 1.0.0

CHANGELOG
- 1.0.0 (2026-02-28)
  - Implement deterministic AI schema validation (Spec v1.0)
  - Enforce confirm-only: direction lock, lot lock, optional mode lock
  - Enforce tighten-only SL, RR floor, bounded entry shift, confidence bounds
  - Stable error codes + normalized output

BACKTEST / EVIDENCE
- N/A (Validator layer; deterministic enforcement only)

FROZEN DECISIONS
- RR floor default = 1.5
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

Number = Union[int, float]

E_SCHEMA = "E_SCHEMA"
E_VERSION = "E_VERSION"
E_DECISION = "E_DECISION"
E_CONFIDENCE = "E_CONFIDENCE"
E_DIR_LOCK = "E_DIR_LOCK"
E_LOT_LOCK = "E_LOT_LOCK"
E_MODE_LOCK = "E_MODE_LOCK"
E_ENTRY_SHIFT = "E_ENTRY_SHIFT"
E_SL_TIGHTEN_ONLY = "E_SL_TIGHTEN_ONLY"
E_RR_FLOOR = "E_RR_FLOOR"
E_NUMERIC = "E_NUMERIC"


@dataclass(frozen=True)
class ValidationPolicy:
    rr_floor: float = 1.5
    entry_shift_max_atr_mult: float = 0.25
    entry_shift_max_pct: float = 0.001
    enforce_mode_lock: bool = False


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    decision: str  # CONFIRM | REJECT
    errors: Tuple[str, ...]
    reasons: Tuple[str, ...]
    normalized: Dict[str, Any]


def validate_ai_response_v1_0(
    ai_payload: Dict[str, Any],
    engine_order: Dict[str, Any],
    *,
    policy: Optional[ValidationPolicy] = None,
) -> ValidationResult:
    pol = policy or ValidationPolicy()
    errors: List[str] = []
    reasons: List[str] = []

    def add_err(code: str, reason: str) -> None:
        errors.append(code)
        reasons.append(reason)

    def get_required(d: Dict[str, Any], k: str) -> Any:
        if k not in d:
            add_err(E_SCHEMA, f"missing_required_field:{k}")
            return None
        return d.get(k)

    def is_number(x: Any) -> bool:
        return isinstance(x, (int, float)) and not isinstance(x, bool)

    def to_float(x: Any, field: str) -> Optional[float]:
        if x is None:
            return None
        if not is_number(x):
            add_err(E_NUMERIC, f"non_numeric:{field}")
            return None
        return float(x)

    def norm_dir(x: Any) -> Optional[str]:
        if x is None or not isinstance(x, str):
            return None
        u = x.strip().upper()
        return u if u in ("BUY", "SELL") else None

    schema_version = get_required(ai_payload, "schema_version")
    decision = get_required(ai_payload, "decision")
    confidence = get_required(ai_payload, "confidence")

    if schema_version != "1.0":
        add_err(E_VERSION, f"unsupported_schema_version:{schema_version}")

    if not isinstance(decision, str) or decision.strip().upper() not in ("CONFIRM", "REJECT"):
        add_err(E_DECISION, f"invalid_decision:{decision}")
        decision_norm = "REJECT"
    else:
        decision_norm = decision.strip().upper()

    conf_f = to_float(confidence, "confidence")
    if conf_f is None or conf_f < 0.0 or conf_f > 1.0:
        add_err(E_CONFIDENCE, f"confidence_out_of_range:{confidence}")

    eng_dir = norm_dir(engine_order.get("direction"))
    eng_entry = to_float(engine_order.get("entry"), "engine.entry")
    eng_sl = to_float(engine_order.get("sl"), "engine.sl")
    eng_tp = to_float(engine_order.get("tp"), "engine.tp")
    eng_lot = to_float(engine_order.get("lot"), "engine.lot")
    eng_mode = engine_order.get("mode")

    if eng_dir is None:
        add_err(E_SCHEMA, "engine_missing_or_invalid:direction")
    if eng_entry is None:
        add_err(E_SCHEMA, "engine_missing_or_invalid:entry")
    if eng_sl is None:
        add_err(E_SCHEMA, "engine_missing_or_invalid:sl")
    if eng_tp is None:
        add_err(E_SCHEMA, "engine_missing_or_invalid:tp")
    if eng_lot is None:
        add_err(E_SCHEMA, "engine_missing_or_invalid:lot")

    if errors:
        return _finalize_reject(ai_payload, engine_order, errors, reasons)

    ai_dir = norm_dir(ai_payload.get("direction"))
    if ai_dir is not None and ai_dir != eng_dir:
        add_err(E_DIR_LOCK, f"direction_changed:{ai_dir}->{eng_dir}")

    ai_lot = to_float(ai_payload.get("lot"), "ai.lot")
    if ai_lot is not None and abs(ai_lot - eng_lot) > 0.0:
        add_err(E_LOT_LOCK, f"lot_changed:{ai_lot}->{eng_lot}")

    if pol.enforce_mode_lock:
        ai_mode = ai_payload.get("mode")
        if ai_mode is not None and eng_mode is not None and ai_mode != eng_mode:
            add_err(E_MODE_LOCK, f"mode_changed:{ai_mode}->{eng_mode}")

    ai_entry = to_float(ai_payload.get("entry"), "ai.entry")
    if ai_entry is not None:
        shift = abs(ai_entry - eng_entry)
        atr = to_float(engine_order.get("atr"), "engine.atr")
        allowed_by_atr = atr * pol.entry_shift_max_atr_mult if atr is not None and atr > 0 else None
        allowed_by_pct = abs(eng_entry) * pol.entry_shift_max_pct
        allowed = max(allowed_by_pct, allowed_by_atr) if allowed_by_atr is not None else allowed_by_pct
        if shift > allowed:
            add_err(E_ENTRY_SHIFT, f"entry_shift_exceeded:shift={shift:.10f} allowed={allowed:.10f}")

    ai_sl = to_float(ai_payload.get("sl"), "ai.sl")
    if ai_sl is not None:
        if not _is_sl_tighten_only(eng_dir, eng_entry, eng_sl, ai_sl):
            add_err(E_SL_TIGHTEN_ONLY, f"sl_not_tighten_only:ai_sl={ai_sl} engine_sl={eng_sl} dir={eng_dir}")

    eff_entry = ai_entry if ai_entry is not None else eng_entry
    eff_sl = ai_sl if ai_sl is not None else eng_sl
    ai_tp = to_float(ai_payload.get("tp"), "ai.tp")
    eff_tp = ai_tp if ai_tp is not None else eng_tp

    rr = _calc_rr(eff_entry, eff_sl, eff_tp)
    if rr is None:
        add_err(E_NUMERIC, "rr_undefined_or_invalid")
    elif rr < pol.rr_floor:
        add_err(E_RR_FLOOR, f"rr_below_floor:rr={rr:.6f} floor={pol.rr_floor}")

    if errors:
        return _finalize_reject(ai_payload, engine_order, errors, reasons)

    normalized = _normalized_payload(ai_payload, engine_order, rr=rr)
    normalized["decision"] = decision_norm

    if decision_norm == "REJECT":
        return ValidationResult(ok=True, decision="REJECT", errors=tuple(), reasons=tuple(), normalized=normalized)

    return ValidationResult(ok=True, decision="CONFIRM", errors=tuple(), reasons=tuple(), normalized=normalized)


def _finalize_reject(ai_payload: Dict[str, Any], engine_order: Dict[str, Any], errors: List[str], reasons: List[str]) -> ValidationResult:
    normalized = _normalized_payload(ai_payload, engine_order, rr=None)
    normalized["decision"] = "REJECT"
    normalized["validator_errors"] = list(errors)
    normalized["validator_reasons"] = list(reasons)
    return ValidationResult(ok=False, decision="REJECT", errors=tuple(errors), reasons=tuple(reasons), normalized=normalized)


def _normalized_payload(ai_payload: Dict[str, Any], engine_order: Dict[str, Any], *, rr: Optional[float]) -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "decision": str(ai_payload.get("decision", "")).strip().upper() if isinstance(ai_payload.get("decision"), str) else "REJECT",
        "confidence": ai_payload.get("confidence"),
        "direction": engine_order.get("direction"),
        "lot": engine_order.get("lot"),
        "mode": engine_order.get("mode"),
        "engine": {
            "entry": engine_order.get("entry"),
            "sl": engine_order.get("sl"),
            "tp": engine_order.get("tp"),
            "atr": engine_order.get("atr"),
        },
        "ai": {
            "entry": ai_payload.get("entry"),
            "sl": ai_payload.get("sl"),
            "tp": ai_payload.get("tp"),
            "note": ai_payload.get("note") or ai_payload.get("reason"),
        },
        "computed": {"rr": rr},
    }


def _calc_rr(entry: float, sl: float, tp: float) -> Optional[float]:
    try:
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk <= 0:
            return None
        return reward / risk
    except Exception:
        return None


def _is_sl_tighten_only(direction: str, entry: float, sl_engine: float, sl_ai: float) -> bool:
    if direction == "BUY":
        if sl_ai < sl_engine:
            return False
        if sl_ai >= entry:
            return False
        return True
    if direction == "SELL":
        if sl_ai > sl_engine:
            return False
        if sl_ai <= entry:
            return False
        return True
    return False
