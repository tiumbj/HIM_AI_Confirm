"""
HIM - Unit Tests for AI Schema Validator v1.0
File: tests/test_validator_v1_0.py
Version: 1.0.0

CHANGELOG
- 1.0.0 (2026-02-28)
  - Deterministic tests for schema + confirm-only constraints

BACKTEST / EVIDENCE
- N/A (validator tests only)
"""

import pytest

from validator_v1_0 import (
    ValidationPolicy,
    validate_ai_response_v1_0,
    E_VERSION,
    E_DECISION,
    E_CONFIDENCE,
    E_DIR_LOCK,
    E_LOT_LOCK,
    E_ENTRY_SHIFT,
    E_SL_TIGHTEN_ONLY,
    E_RR_FLOOR,
    E_SCHEMA,
)


def base_engine():
    return {
        "direction": "BUY",
        "entry": 2000.0,
        "sl": 1990.0,
        "tp": 2020.0,   # RR = 20/10 = 2.0
        "lot": 0.10,
        "mode": "sideway_scalp",
        "atr": 10.0,
    }


def base_ai_confirm():
    return {
        "schema_version": "1.0",
        "decision": "CONFIRM",
        "confidence": 0.8,
        # optional fields omitted -> should pass
    }


def test_ok_confirm_minimal():
    r = validate_ai_response_v1_0(base_ai_confirm(), base_engine())
    assert r.ok is True
    assert r.decision == "CONFIRM"
    assert r.errors == tuple()


def test_ok_reject_respected_when_no_violation():
    ai = base_ai_confirm()
    ai["decision"] = "REJECT"
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is True
    assert r.decision == "REJECT"
    assert r.errors == tuple()


def test_bad_version_reject():
    ai = base_ai_confirm()
    ai["schema_version"] = "9.9"
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is False
    assert r.decision == "REJECT"
    assert E_VERSION in r.errors


def test_bad_decision_reject():
    ai = base_ai_confirm()
    ai["decision"] = "MAYBE"
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is False
    assert E_DECISION in r.errors


def test_confidence_out_of_range_reject():
    ai = base_ai_confirm()
    ai["confidence"] = 1.5
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is False
    assert E_CONFIDENCE in r.errors


def test_direction_lock_reject():
    ai = base_ai_confirm()
    ai["direction"] = "SELL"
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is False
    assert E_DIR_LOCK in r.errors


def test_lot_lock_reject():
    ai = base_ai_confirm()
    ai["lot"] = 0.20
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is False
    assert E_LOT_LOCK in r.errors


def test_entry_shift_exceeded_reject_by_atr_bound():
    # ATR=10, entry_shift_max_atr_mult=0.25 -> allowed=2.5
    ai = base_ai_confirm()
    ai["entry"] = 2005.1  # shift=5.1 > 2.5
    r = validate_ai_response_v1_0(ai, base_engine(), policy=ValidationPolicy(entry_shift_max_atr_mult=0.25, entry_shift_max_pct=0.0))
    assert r.ok is False
    assert E_ENTRY_SHIFT in r.errors


def test_entry_shift_ok_within_bound():
    ai = base_ai_confirm()
    ai["entry"] = 2002.0  # shift=2.0 <= 2.5
    r = validate_ai_response_v1_0(ai, base_engine(), policy=ValidationPolicy(entry_shift_max_atr_mult=0.25, entry_shift_max_pct=0.0))
    assert r.ok is True
    assert r.decision == "CONFIRM"


def test_sl_tighten_only_buy_reject_if_loosened():
    ai = base_ai_confirm()
    # engine SL=1990, loosen -> 1985 (further from entry)
    ai["sl"] = 1985.0
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is False
    assert E_SL_TIGHTEN_ONLY in r.errors


def test_sl_tighten_only_buy_reject_if_crosses_entry():
    ai = base_ai_confirm()
    ai["sl"] = 2000.0  # equal entry -> invalid
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is False
    assert E_SL_TIGHTEN_ONLY in r.errors


def test_sl_tighten_only_buy_ok_if_tightened():
    ai = base_ai_confirm()
    ai["sl"] = 1995.0
    r = validate_ai_response_v1_0(ai, base_engine())
    assert r.ok is True


def test_rr_floor_reject_when_below():
    eng = base_engine()
    eng["tp"] = 2005.0  # reward=5, risk=10 -> RR=0.5
    ai = base_ai_confirm()
    r = validate_ai_response_v1_0(ai, eng, policy=ValidationPolicy(rr_floor=1.5))
    assert r.ok is False
    assert E_RR_FLOOR in r.errors


def test_missing_engine_fields_reject():
    eng = base_engine()
    del eng["entry"]
    ai = base_ai_confirm()
    r = validate_ai_response_v1_0(ai, eng)
    assert r.ok is False
    assert E_SCHEMA in r.errors