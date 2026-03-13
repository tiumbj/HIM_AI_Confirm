"""
HIM Risk Guard - Unit Tests
File: test_risk_guard_v1_0.py
Version: v1.0.0
"""

import unittest
from datetime import datetime, timezone, timedelta, date

from risk_guard_v1_0 import (
    RiskGuard, GuardConfig,
    TradePlan, MarketSnapshot, AccountSnapshot, PerformanceSnapshot,
    GuardAction,
)


class TestRiskGuardV1(unittest.TestCase):
    def setUp(self):
        self.cfg = GuardConfig(
            tick_max_age_sec=8,
            max_spread_points=100,
            max_positions=1,
            dedup_window_sec=90,
            cooldown_sec=45,
            daily_loss_limit_pct=0.01,
            max_consecutive_losses=3,
            max_slippage_points=10,
            min_rr_exec=None,
            fail_closed=True,
        )
        self.rg = RiskGuard(self.cfg)

        self.now = datetime.now(timezone.utc)

        self.plan = TradePlan(
            symbol="GOLD",
            direction="BUY",
            entry=2000.0,
            sl=1990.0,
            tp=2020.0,
            min_rr=1.9,
            created_utc=self.now,
        )

        self.market_ok = MarketSnapshot(
            tick_time_utc=self.now,
            bid=1999.9,
            ask=2000.1,   # spread 0.2
            point=0.01,
            digits=2,
            atr=5.0,
            bb_width=2.0,
        )

        self.account_ok = AccountSnapshot(
            equity=10000.0,
            balance=10000.0,
            positions_count=0,
            net_position_direction=None,
        )

        self.perf_ok = PerformanceSnapshot(
            today=date.today(),
            realized_pl_today=0.0,
            consecutive_losses=0,
        )

    def test_allow_when_all_ok(self):
        d = self.rg.evaluate_pre_exec(self.plan, self.market_ok, self.account_ok, self.perf_ok, now_utc=self.now)
        self.assertEqual(d.action, GuardAction.ALLOW)

    def test_block_stale_tick(self):
        market = MarketSnapshot(
            tick_time_utc=self.now - timedelta(seconds=30),
            bid=self.market_ok.bid,
            ask=self.market_ok.ask,
            point=self.market_ok.point,
            digits=self.market_ok.digits,
        )
        d = self.rg.evaluate_pre_exec(self.plan, market, self.account_ok, self.perf_ok, now_utc=self.now)
        self.assertEqual(d.action, GuardAction.BLOCK_SOFT)
        self.assertTrue(any(r.code == "TICK_STALE" for r in d.reasons))

    def test_block_spread_too_wide(self):
        market = MarketSnapshot(
            tick_time_utc=self.now,
            bid=2000.0,
            ask=2005.0,  # spread 5.0 => 500 points (point=0.01)
            point=0.01,
            digits=2,
        )
        d = self.rg.evaluate_pre_exec(self.plan, market, self.account_ok, self.perf_ok, now_utc=self.now)
        self.assertEqual(d.action, GuardAction.BLOCK_SOFT)
        self.assertTrue(any(r.code == "SPREAD_TOO_WIDE" for r in d.reasons))

    def test_block_max_positions(self):
        acc = AccountSnapshot(
            equity=10000.0,
            balance=10000.0,
            positions_count=1,  # max=1 => reached
            net_position_direction="BUY",
        )
        d = self.rg.evaluate_pre_exec(self.plan, self.market_ok, acc, self.perf_ok, now_utc=self.now)
        self.assertEqual(d.action, GuardAction.BLOCK_SOFT)
        self.assertTrue(any(r.code == "MAX_POSITIONS_REACHED" for r in d.reasons))

    def test_block_hedge(self):
        acc = AccountSnapshot(
            equity=10000.0,
            balance=10000.0,
            positions_count=1,
            net_position_direction="SELL",
        )
        d = self.rg.evaluate_pre_exec(self.plan, self.market_ok, acc, self.perf_ok, now_utc=self.now)
        self.assertEqual(d.action, GuardAction.BLOCK_SOFT)
        self.assertTrue(any(r.code == "HEDGE_BLOCKED" for r in d.reasons))

    def test_block_dedup(self):
        sent = {}
        # first eval to get fingerprint
        d1 = self.rg.evaluate_pre_exec(self.plan, self.market_ok, self.account_ok, self.perf_ok, now_utc=self.now, sent_fingerprints=sent)
        sent[d1.fingerprint] = self.now  # pretend we sent it now
        d2 = self.rg.evaluate_pre_exec(self.plan, self.market_ok, self.account_ok, self.perf_ok, now_utc=self.now + timedelta(seconds=10), sent_fingerprints=sent)
        self.assertEqual(d2.action, GuardAction.BLOCK_SOFT)
        self.assertTrue(any(r.code == "DEDUP_BLOCK" for r in d2.reasons))

    def test_hard_block_daily_loss_cutoff(self):
        # daily loss limit = 1% * equity = 100. realized <= -100 triggers
        perf = PerformanceSnapshot(
            today=date.today(),
            realized_pl_today=-150.0,
            consecutive_losses=0,
        )
        d = self.rg.evaluate_pre_exec(self.plan, self.market_ok, self.account_ok, perf, now_utc=self.now)
        self.assertEqual(d.action, GuardAction.BLOCK_HARD)
        self.assertTrue(any(r.code == "DAILY_LOSS_CUTOFF" for r in d.reasons))

    def test_block_rr_after_slippage(self):
        # Make RR tight so slippage breaks it
        plan = TradePlan(
            symbol="GOLD",
            direction="BUY",
            entry=2000.0,
            sl=1995.0,   # risk 5
            tp=2007.0,   # reward 7 => RR 1.4 (already under 1.9)
            min_rr=1.9,
            created_utc=self.now,
        )
        d = self.rg.evaluate_pre_exec(plan, self.market_ok, self.account_ok, self.perf_ok, now_utc=self.now)
        self.assertEqual(d.action, GuardAction.BLOCK_SOFT)
        self.assertTrue(any(r.code == "RR_DEGRADES_AFTER_SLIPPAGE" for r in d.reasons))


if __name__ == "__main__":
    unittest.main()