"""
test_phase7_kill_switch.py — Phase 7 Kill-Switch Runtime Tests
Purpose: Verify synchronous kill-switch enforcement without strategy changes
Scope: Tests _kill_switch_active() and mentor run_once() kill-switch behavior only
"""

import sys
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Mock MetaTrader5 and mt5_executor before importing mentor_executor
_mock_mt5_mod = MagicMock()
sys.modules.setdefault("MetaTrader5", MagicMock())
sys.modules.setdefault("mt5_executor", _mock_mt5_mod)

from mentor_executor import (  # noqa: E402
    _kill_switch_active,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_kill_switch(tmp_path):
    """Temporary KILL_SWITCH.txt for testing — patches global KILL_SWITCH_PATH."""
    kill_switch_path = tmp_path / "KILL_SWITCH.txt"

    import mentor_executor
    original_path = mentor_executor.KILL_SWITCH_PATH
    mentor_executor.KILL_SWITCH_PATH = str(kill_switch_path)

    yield kill_switch_path

    mentor_executor.KILL_SWITCH_PATH = original_path
    if kill_switch_path.exists():
        kill_switch_path.unlink()


@pytest.fixture
def mock_mentor():
    """MentorExecutor with MT5, fetch_signal and ai_confirm mocked out."""
    import mentor_executor

    # Ensure mt5_executor stays mocked for the lazy import inside __init__
    mock_mt5_executor_mod = MagicMock()
    with patch.dict(sys.modules, {"mt5_executor": mock_mt5_executor_mod}):
        mentor = mentor_executor.MentorExecutor()

    # Replace the mt5 attribute with a fresh MagicMock so we can track calls
    mentor.mt5 = MagicMock()
    # Patch method-level helpers so no real network calls happen
    mentor.fetch_signal = MagicMock()
    mentor.ai_confirm = MagicMock()

    yield mentor


# ---------------------------------------------------------------------------
# Helper — minimal valid BUY signal that passes build_execution_package
# ---------------------------------------------------------------------------

def _buy_signal():
    return {
        "decision": "BUY",
        "plan": {"entry": 1.1000, "sl": 1.0950, "tp": 1.1100},
        "request_id": "test-req-001",
        "blocked_by": [],
        "metrics": {},
        "context": {},
    }


def _approved_ai():
    return {"approved": True, "reason": "test_ok", "confidence": 0.9}


# ---------------------------------------------------------------------------
# Group 1: _kill_switch_active() unit tests (5 tests)
# ---------------------------------------------------------------------------

def test_kill_switch_not_exists(temp_kill_switch):
    """Returns (False, '') when KILL_SWITCH.txt does not exist."""
    # Ensure the file is absent
    assert not temp_kill_switch.exists()

    active, reason = _kill_switch_active()

    assert active is False
    assert reason == ""


def test_kill_switch_exists_with_content(temp_kill_switch):
    """Returns (True, content) when KILL_SWITCH.txt exists with content."""
    test_reason = "Emergency stop: volatility spike"
    temp_kill_switch.write_text(test_reason, encoding="utf-8")

    active, reason = _kill_switch_active()

    assert active is True
    assert reason == test_reason


def test_kill_switch_exists_empty(temp_kill_switch):
    """Returns (True, '') when KILL_SWITCH.txt exists but is empty."""
    temp_kill_switch.write_text("", encoding="utf-8")

    active, reason = _kill_switch_active()

    assert active is True
    assert reason == ""


def test_kill_switch_truncates_long_reason(temp_kill_switch):
    """Truncates reason to 500 characters."""
    long_reason = "X" * 1000
    temp_kill_switch.write_text(long_reason, encoding="utf-8")

    active, reason = _kill_switch_active()

    assert active is True
    assert len(reason) == 500
    assert reason == "X" * 500


def test_kill_switch_fail_closed_unreadable(temp_kill_switch):
    """Returns (True, 'KILL_SWITCH_READ_ERROR') when file exists but cannot be read."""
    temp_kill_switch.write_text("some reason", encoding="utf-8")

    import builtins
    real_open = builtins.open

    def raise_on_kill_switch(path, *args, **kwargs):
        if str(path) == str(temp_kill_switch):
            raise PermissionError("simulated read error")
        return real_open(path, *args, **kwargs)

    with patch("builtins.open", side_effect=raise_on_kill_switch):
        active, reason = _kill_switch_active()

    assert active is True
    assert reason == "KILL_SWITCH_READ_ERROR"


# ---------------------------------------------------------------------------
# Group 2: Mentor run_once() behaviour tests (3 tests)
# ---------------------------------------------------------------------------

def test_mentor_blocks_execution_when_kill_switch_active(mock_mentor, temp_kill_switch):
    """Blocks execution, returns SKIP with kill_switch_active, never calls mt5.execute()."""
    test_reason = "MANUAL HALT — risk limit exceeded"
    temp_kill_switch.write_text(test_reason, encoding="utf-8")

    # Valid trade signal + approved AI so only the kill-switch stops execution
    mock_mentor.fetch_signal.return_value = (True, _buy_signal())
    mock_mentor.ai_confirm.return_value = (True, _approved_ai())
    mock_mentor.dry_run = False

    result = mock_mentor.run_once()

    assert result["status"] == "SKIP"
    assert result["reason"] == "kill_switch_active"
    assert result["kill_switch_reason"] == test_reason
    mock_mentor.mt5.execute.assert_not_called()  # CRITICAL: prove execution blocked


def test_mentor_proceeds_when_kill_switch_inactive_no_trade(mock_mentor, temp_kill_switch):
    """Normal flow when no kill-switch file and signal is HOLD (no-trade decision)."""
    # Kill-switch file absent
    assert not temp_kill_switch.exists()

    # HOLD signal — build_execution_package returns decision_not_trade
    hold_signal = {"decision": "HOLD", "plan": None}
    mock_mentor.fetch_signal.return_value = (True, hold_signal)
    mock_mentor.dry_run = False

    result = mock_mentor.run_once()

    # Should have skipped due to no-trade decision, not kill-switch
    assert result["status"] == "SKIP"
    assert result["reason"] == "decision_not_trade"
    # MT5 must not have been called either
    mock_mentor.mt5.execute.assert_not_called()


def test_mentor_dry_run_skips_kill_switch_check(mock_mentor, temp_kill_switch):
    """Dry-run exits before kill-switch check; kill-switch file is ignored."""
    # Plant an active kill-switch file
    temp_kill_switch.write_text("should be ignored in dry run", encoding="utf-8")

    mock_mentor.fetch_signal.return_value = (True, _buy_signal())
    mock_mentor.ai_confirm.return_value = (True, _approved_ai())
    mock_mentor.dry_run = True  # activate dry-run

    result = mock_mentor.run_once()

    # Dry-run should return DRY_RUN, never reaching the kill-switch check
    assert result["status"] == "DRY_RUN"
    mock_mentor.mt5.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Group 3: Integration — enforcement order test (1 test)
# ---------------------------------------------------------------------------

def test_kill_switch_enforcement_position_in_flow(mock_mentor, temp_kill_switch):
    """Verifies order: Signal → AI → Kill-Switch → MT5.

    Specifically: when the kill-switch is armed AFTER a valid AI-approved signal
    is prepared, execution must still be blocked (kill-switch is the last gate
    before execute(), not a pre-filter).
    """
    test_reason = "Integration guard active"
    temp_kill_switch.write_text(test_reason, encoding="utf-8")

    call_order = []

    # Wrap fetch_signal to record call order
    original_fetch = mock_mentor.fetch_signal
    original_fetch.return_value = (True, _buy_signal())
    mock_mentor.fetch_signal = MagicMock(
        side_effect=lambda: (call_order.append("fetch_signal"), (True, _buy_signal()))[1]
    )

    # Wrap ai_confirm to record call order
    mock_mentor.ai_confirm = MagicMock(
        side_effect=lambda pkg: (call_order.append("ai_confirm"), (True, _approved_ai()))[1]
    )

    # mt5.execute should never be reached
    mock_mentor.mt5.execute = MagicMock(
        side_effect=lambda pkg: (call_order.append("mt5_execute"), {})[1]
    )

    mock_mentor.dry_run = False

    result = mock_mentor.run_once()

    # Kill-switch must have blocked execution
    assert result["status"] == "SKIP"
    assert result["reason"] == "kill_switch_active"

    # Signal was fetched and AI was consulted — both happened before the block
    assert "fetch_signal" in call_order
    assert "ai_confirm" in call_order

    # MT5 execute must NOT have been called
    assert "mt5_execute" not in call_order
    mock_mentor.mt5.execute.assert_not_called()
