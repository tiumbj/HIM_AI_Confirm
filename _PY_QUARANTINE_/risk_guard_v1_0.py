"""
Hybrid Intelligence Mentor (HIM)
Module: Risk Guard
File: risk_guard_v1_0.py
Version: v1.0.0
Date: 2026-03-01 (Asia/Bangkok)

CHANGELOG
- v1.0.0
  - Add deterministic PRE-EXEC risk guards: tick age, spread, max positions, no-hedge, dedup, daily loss cutoff, loss streak,
    slippage gate, RR-after-slippage gate, cooldown/throttle.
  - Add POST-EXEC checks: SL/TP presence expectation interface.
  - Provide structured decision output for audit-friendly logging.

PARAMETER RATIONALE (คำอธิบายภาษาคน + ศัพท์เทคนิคแปลไทย)
- fail-closed (ปิดทางไว้ก่อน): ข้อมูลไม่พอ/ไม่แน่ใจ => BLOCK
- dedup_window_sec (หน้าต่างกันส่งซ้ำ): กัน loop/retry ส่ง order ซ้ำ
- daily_loss_limit_pct (เพดานขาดทุนรายวัน): ตัดไฟก่อนล้างพอร์ต
- max_positions=1 (จำกัดจำนวนโพสิชัน): จนกว่าจะมี position management จริง
- spread/tick_age guard: กันสภาพคล่องผิดปกติและข้อมูล stale

BACKTEST / EVIDENCE
- N/A (commissioning phase). ตัวเลขจะเติมหลังเชื่อม performance tracker และเก็บสถิติจริง.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, date
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple


# =========================
# Decision Model
# =========================

class GuardAction(str, Enum):
    ALLOW = "ALLOW"
    BLOCK_SOFT = "BLOCK_SOFT"       # block this cycle, retry next cycle ok (e.g., high spread)
    BLOCK_HARD = "BLOCK_HARD"       # lockout until reset condition (e.g., daily loss breached)
    DEGRADE_TO_DRY_RUN = "DEGRADE_TO_DRY_RUN"  # optional: allow analysis but prohibit LIVE execute


@dataclass(frozen=True)
class GuardReason:
    code: str
    message: str
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GuardDecision:
    action: GuardAction
    reasons: Tuple[GuardReason, ...] = ()
    fingerprint: Optional[str] = None
    decision_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action.value,
            "reasons": [asdict(r) for r in self.reasons],
            "fingerprint": self.fingerprint,
            "decision_id": self.decision_id,
        }


# =========================
# Input Snapshots
# =========================

@dataclass(frozen=True)
class TradePlan:
    symbol: str
    direction: str           # "BUY" | "SELL"
    entry: float
    sl: float
    tp: float
    min_rr: float            # required RR floor (strategy/validator requirement)
    created_utc: datetime    # when plan was made (UTC recommended)


@dataclass(frozen=True)
class MarketSnapshot:
    tick_time_utc: datetime
    bid: float
    ask: float
    point: float            # MT5 symbol point
    digits: int
    atr: Optional[float] = None
    bb_width: Optional[float] = None


@dataclass(frozen=True)
class AccountSnapshot:
    equity: float
    balance: float
    positions_count: int
    net_position_direction: Optional[str] = None
    # net_position_direction: "BUY" | "SELL" | None (no position)


@dataclass(frozen=True)
class PerformanceSnapshot:
    today: date
    realized_pl_today: float
    consecutive_losses: int


@dataclass
class GuardConfig:
    # Market integrity
    tick_max_age_sec: int = 8
    max_spread_points: int = 450     # start conservative; tune from your broker statistics
    max_volatility_atr: Optional[float] = None  # if set, ATR > this => block
    # Exposure
    max_positions: int = 1
    no_hedge: bool = True
    # Dedup / throttle
    dedup_window_sec: int = 90
    cooldown_sec: int = 45
    # Risk budget
    daily_loss_limit_pct: float = 0.01          # 1% equity default (commissioning-safe)
    max_consecutive_losses: int = 3
    # Execution quality
    max_slippage_points: int = 80
    min_rr_exec: Optional[float] = None         # if None -> use plan.min_rr
    # Fail closed
    fail_closed: bool = True


# =========================
# Risk Guard Engine
# =========================

class RiskGuard:
    """
    Deterministic risk guards for HIM.

    Usage:
      rg = RiskGuard(cfg)
      decision = rg.evaluate_pre_exec(plan, market, account, perf, now_utc, last_exec_utc, recently_sent_cache)
    """

    def __init__(self, cfg: GuardConfig):
        self.cfg = cfg

    # ---------- Utility ----------
    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _round_to_digits(x: float, digits: int) -> float:
        return round(float(x), int(digits))

    @staticmethod
    def _spread_points(bid: float, ask: float, point: float) -> float:
        if point <= 0:
            return float("inf")
        return (ask - bid) / point

    @staticmethod
    def _rr(direction: str, entry: float, sl: float, tp: float) -> float:
        # RR = reward / risk
        risk = abs(entry - sl)
        reward = abs(tp - entry)
        if risk <= 0:
            return 0.0
        # sanity: direction should align with tp side but we only compute magnitude
        return reward / risk

    def _fingerprint(self, plan: TradePlan, market: MarketSnapshot, time_bucket_sec: int = 60) -> str:
        """
        Create a stable fingerprint to prevent duplicate orders.
        Uses rounded prices by digits and a time bucket to avoid infinite uniqueness.
        """
        bucket = int(plan.created_utc.timestamp()) // int(time_bucket_sec)
        payload = {
            "symbol": plan.symbol,
            "direction": plan.direction.upper(),
            "entry": self._round_to_digits(plan.entry, market.digits),
            "sl": self._round_to_digits(plan.sl, market.digits),
            "tp": self._round_to_digits(plan.tp, market.digits),
            "bucket": bucket,
        }
        raw = repr(payload).encode("utf-8")
        return sha256(raw).hexdigest()

    def _decision_id(self, fingerprint: str, now_utc: datetime) -> str:
        raw = f"{fingerprint}:{now_utc.timestamp():.3f}".encode("utf-8")
        return sha256(raw).hexdigest()[:16]

    # ---------- Guards ----------
    def evaluate_pre_exec(
        self,
        plan: TradePlan,
        market: MarketSnapshot,
        account: AccountSnapshot,
        perf: PerformanceSnapshot,
        now_utc: Optional[datetime] = None,
        last_exec_utc: Optional[datetime] = None,
        sent_fingerprints: Optional[Dict[str, datetime]] = None,
        prefer_dry_run_on_soft_block: bool = False,
    ) -> GuardDecision:
        """
        PRE-EXEC evaluation.
        - sent_fingerprints: dict[fingerprint] = last_sent_time_utc (persist in runner memory)
        - last_exec_utc: last successful execution time (for cooldown)
        """
        now = now_utc or self._utc_now()
        reasons: list[GuardReason] = []

        # Build fingerprint early for dedup/audit
        fp = self._fingerprint(plan, market)
        did = self._decision_id(fp, now)

        # Fail-closed basic sanity
        if plan.symbol.strip() == "" or market.point <= 0:
            reasons.append(GuardReason(
                code="SANITY_FAIL",
                message="Invalid symbol or point size; fail-closed.",
                data={"symbol": plan.symbol, "point": market.point},
            ))
            return GuardDecision(GuardAction.BLOCK_HARD if self.cfg.fail_closed else GuardAction.BLOCK_SOFT,
                                 tuple(reasons), fp, did)

        # Guard A1: Tick age
        tick_age = (now - market.tick_time_utc).total_seconds()
        if tick_age > self.cfg.tick_max_age_sec:
            reasons.append(GuardReason(
                code="TICK_STALE",
                message="Market tick is stale; block this cycle.",
                data={"tick_age_sec": tick_age, "tick_max_age_sec": self.cfg.tick_max_age_sec},
            ))

        # Guard A2: Spread
        spread_pts = self._spread_points(market.bid, market.ask, market.point)
        if spread_pts > self.cfg.max_spread_points:
            reasons.append(GuardReason(
                code="SPREAD_TOO_WIDE",
                message="Spread too wide; block this cycle.",
                data={"spread_points": spread_pts, "max_spread_points": self.cfg.max_spread_points},
            ))

        # Guard A3: Volatility ATR cap (optional)
        if self.cfg.max_volatility_atr is not None and market.atr is not None:
            if market.atr > self.cfg.max_volatility_atr:
                reasons.append(GuardReason(
                    code="VOLATILITY_TOO_HIGH",
                    message="ATR volatility exceeds cap; block this cycle.",
                    data={"atr": market.atr, "max_volatility_atr": self.cfg.max_volatility_atr},
                ))

        # Guard B1: Max positions
        if account.positions_count >= self.cfg.max_positions:
            reasons.append(GuardReason(
                code="MAX_POSITIONS_REACHED",
                message="Max positions reached; block.",
                data={"positions_count": account.positions_count, "max_positions": self.cfg.max_positions},
            ))

        # Guard B2: No hedge rule
        if self.cfg.no_hedge and account.net_position_direction is not None:
            if account.net_position_direction.upper() != plan.direction.upper():
                reasons.append(GuardReason(
                    code="HEDGE_BLOCKED",
                    message="Existing position is opposite direction; hedging blocked.",
                    data={"net_position_direction": account.net_position_direction, "plan_direction": plan.direction},
                ))

        # Guard C: Dedup
        if sent_fingerprints is not None and fp in sent_fingerprints:
            last_sent = sent_fingerprints[fp]
            age = (now - last_sent).total_seconds()
            if age < self.cfg.dedup_window_sec:
                reasons.append(GuardReason(
                    code="DEDUP_BLOCK",
                    message="Duplicate signal fingerprint in dedup window; block this cycle.",
                    data={"age_sec": age, "dedup_window_sec": self.cfg.dedup_window_sec},
                ))

        # Guard D1: Daily loss cutoff (hard)
        daily_loss_limit_money = abs(account.equity) * float(self.cfg.daily_loss_limit_pct)
        if perf.realized_pl_today <= -daily_loss_limit_money:
            reasons.append(GuardReason(
                code="DAILY_LOSS_CUTOFF",
                message="Daily loss limit breached; hard block until next day/reset.",
                data={
                    "realized_pl_today": perf.realized_pl_today,
                    "daily_loss_limit_money": daily_loss_limit_money,
                    "daily_loss_limit_pct": self.cfg.daily_loss_limit_pct,
                },
            ))

        # Guard D2: Loss streak lock
        if perf.consecutive_losses >= self.cfg.max_consecutive_losses:
            reasons.append(GuardReason(
                code="LOSS_STREAK_LOCK",
                message="Consecutive loss lock triggered; block this cycle or cooldown.",
                data={"consecutive_losses": perf.consecutive_losses, "max_consecutive_losses": self.cfg.max_consecutive_losses},
            ))

        # Guard D3: Cooldown after last execution
        if last_exec_utc is not None:
            since_exec = (now - last_exec_utc).total_seconds()
            if since_exec < self.cfg.cooldown_sec:
                reasons.append(GuardReason(
                    code="COOLDOWN_ACTIVE",
                    message="Cooldown active after last execution; block this cycle.",
                    data={"since_exec_sec": since_exec, "cooldown_sec": self.cfg.cooldown_sec},
                ))

        # Guard E1: Slippage & RR-after-slippage (estimate worst-case with max_slippage_points)
        # If plan.entry is intended, in live market order you may get worse price.
        # We approximate worst-case entry shift against the trade by max_slippage_points.
        slip_price = float(self.cfg.max_slippage_points) * float(market.point)
        if slip_price > 0:
            if plan.direction.upper() == "BUY":
                worst_entry = plan.entry + slip_price
            else:
                worst_entry = plan.entry - slip_price
            rr_worst = self._rr(plan.direction, worst_entry, plan.sl, plan.tp)
            rr_floor = float(self.cfg.min_rr_exec) if self.cfg.min_rr_exec is not None else float(plan.min_rr)
            if rr_worst < rr_floor:
                reasons.append(GuardReason(
                    code="RR_DEGRADES_AFTER_SLIPPAGE",
                    message="Worst-case slippage reduces RR below floor; block this cycle.",
                    data={
                        "rr_worst": rr_worst,
                        "rr_floor": rr_floor,
                        "max_slippage_points": self.cfg.max_slippage_points,
                    },
                ))

        # Decide action by severity
        if not reasons:
            return GuardDecision(GuardAction.ALLOW, tuple(), fp, did)

        # Hard blocks if daily loss cutoff or sanity fail (already returned) present
        hard_codes = {"DAILY_LOSS_CUTOFF"}
        if any(r.code in hard_codes for r in reasons):
            return GuardDecision(GuardAction.BLOCK_HARD, tuple(reasons), fp, did)

        if prefer_dry_run_on_soft_block:
            return GuardDecision(GuardAction.DEGRADE_TO_DRY_RUN, tuple(reasons), fp, did)

        return GuardDecision(GuardAction.BLOCK_SOFT, tuple(reasons), fp, did)

    def evaluate_post_exec(
        self,
        has_position: bool,
        sl_present: bool,
        tp_present: bool,
        now_utc: Optional[datetime] = None,
    ) -> GuardDecision:
        """
        POST-EXEC evaluation.
        For now: enforce SL presence if a position exists.
        (Integration point: after MT5 order fill, verify SL/TP attached.)
        """
        now = now_utc or self._utc_now()
        reasons: list[GuardReason] = []
        fp = None
        did = self._decision_id("post_exec", now)

        if has_position and not sl_present:
            reasons.append(GuardReason(
                code="POST_EXEC_MISSING_SL",
                message="Position exists but SL is missing; emergency action required.",
                data={"has_position": has_position, "sl_present": sl_present, "tp_present": tp_present},
            ))
            return GuardDecision(GuardAction.BLOCK_HARD, tuple(reasons), fp, did)

        return GuardDecision(GuardAction.ALLOW, tuple(reasons), fp, did)