"""
HIM
File: tests/test_event_gating.py
Version: v2.12.4
Purpose: Regression tests for event gating (no MT5 dependency).
"""

import unittest
from dataclasses import dataclass
from typing import Dict, Optional, List, Tuple


@dataclass
class EventState:
    last_bar_time: Dict[str, int]
    last_signature: Optional[str]


def default_state() -> EventState:
    return EventState(last_bar_time={}, last_signature=None)


def simulate_detect_events(
    tfs: List[str],
    st: EventState,
    bar_times: Dict[str, int],
    signature: Optional[str],
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    # NEW_BAR
    new_bar = False
    for tf in tfs:
        bt = bar_times.get(tf)
        if bt is None:
            continue
        prev = st.last_bar_time.get(tf)
        if prev is None:
            st.last_bar_time[tf] = bt  # init only
        elif bt != prev:
            st.last_bar_time[tf] = bt
            new_bar = True
    if new_bar:
        reasons.append("NEW_BAR")

    # SIGNAL_CHANGE
    if signature is not None:
        if st.last_signature is None:
            st.last_signature = signature  # init only
        elif signature != st.last_signature:
            st.last_signature = signature
            reasons.append("SIGNAL_CHANGE")

    return (len(reasons) > 0), reasons


class TestEventGating(unittest.TestCase):
    def test_initial_no_event(self):
        st = default_state()
        fire, reasons = simulate_detect_events(["M5"], st, {"M5": 1000}, "a")
        self.assertFalse(fire)
        self.assertEqual(reasons, [])

    def test_new_bar_event(self):
        st = default_state()
        simulate_detect_events(["M5"], st, {"M5": 1000}, "a")  # init
        fire, reasons = simulate_detect_events(["M5"], st, {"M5": 2000}, "a")
        self.assertTrue(fire)
        self.assertIn("NEW_BAR", reasons)
        self.assertNotIn("SIGNAL_CHANGE", reasons)

    def test_signal_change_event(self):
        st = default_state()
        simulate_detect_events(["M5"], st, {"M5": 1000}, "a")  # init
        fire, reasons = simulate_detect_events(["M5"], st, {"M5": 1000}, "b")
        self.assertTrue(fire)
        self.assertIn("SIGNAL_CHANGE", reasons)

    def test_both_events(self):
        st = default_state()
        simulate_detect_events(["M5"], st, {"M5": 1000}, "a")  # init
        fire, reasons = simulate_detect_events(["M5"], st, {"M5": 2000}, "b")
        self.assertTrue(fire)
        self.assertIn("NEW_BAR", reasons)
        self.assertIn("SIGNAL_CHANGE", reasons)


if __name__ == "__main__":
    unittest.main()