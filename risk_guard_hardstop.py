# ============================================================
# ชื่อโค้ด: HIM Risk Guard Hardstop (risk_guard_hardstop.py)
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\risk_guard_hardstop.py
# คำสั่งรัน: python risk_guard_hardstop.py
# เวอร์ชัน: v1.1.0
# ============================================================
"""
risk_guard_hardstop.py — Production Risk Guard (MT5-based)
Version: v1.1.1
Purpose:
  - Enforce real MT5-based constraints and write KILL_SWITCH.txt on breach (fail-closed)
  - Supervisor (watchdog_supervisor.py) forces mentor_executor DRY_RUN=1 when KILL_SWITCH exists

Changelog:
  - v1.1.1 (Phase 7):
      * Use __file__-based PROJECT_ROOT for deterministic KILL_SWITCH path
  - v1.1.0: Production MT5-based enforcement
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None

VERSION = "v1.1.1"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
HARDSTOP_LOG = os.path.join(LOG_DIR, "risk_guard.jsonl")
RISK_GUARD_STATE_PATH = os.path.join(PROJECT_ROOT, ".risk_guard_state.json")
KILL_SWITCH_PATH = os.path.join(PROJECT_ROOT, "KILL_SWITCH.txt")


def _ensure_dirs() -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        pass


_ensure_dirs()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "risk_guard.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("RiskGuard")


def _now_ts() -> int:
    return int(time.time())


def _utc_iso() -> str:
    return datetime.utcnow().isoformat()


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _write_json(path: str, data: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _resolve_config_path() -> str:
    raw = (os.environ.get("HIM_CONFIG") or os.environ.get("HIM_CONFIG_PATH") or "config.json").strip()
    if not raw:
        raw = "config.json"
    if os.path.isabs(raw):
        return raw
    return os.path.join(PROJECT_ROOT, raw)


def _log_event(event: str, data: Dict[str, Any]) -> None:
    try:
        rec = {"ts": _now_ts(), "datetime": _utc_iso(), "event": event, "version": VERSION, **data}
        with open(HARDSTOP_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def _read_kill_switch() -> Tuple[bool, str]:
    if not os.path.exists(KILL_SWITCH_PATH):
        return False, ""
    try:
        with open(KILL_SWITCH_PATH, "r", encoding="utf-8", errors="replace") as f:
            note = f.read().strip()
        return True, note[:500]
    except Exception:
        return True, "KILL_SWITCH present"


def _write_kill_switch(reason: str) -> None:
    if os.path.exists(KILL_SWITCH_PATH):
        return
    try:
        with open(KILL_SWITCH_PATH, "w", encoding="utf-8") as f:
            f.write(reason.strip()[:2000])
    except Exception:
        pass


def _mt5_init() -> Tuple[bool, str]:
    if mt5 is None:
        return False, "mt5_module_missing"
    if mt5.initialize():
        return True, "ok"
    try:
        return False, f"mt5_initialize_failed:{mt5.last_error()}"
    except Exception:
        return False, "mt5_initialize_failed"


def _mt5_shutdown() -> None:
    try:
        if mt5 is not None:
            mt5.shutdown()
    except Exception:
        pass


@dataclass(frozen=True)
class GuardLimits:
    enabled: bool
    poll_interval_sec: float
    max_daily_dd_pct: float
    max_total_dd_pct: float
    max_open_positions: Optional[int]
    max_total_volume: Optional[float]
    symbol: Optional[str]
    initial_equity: Optional[float]


def _parse_limits(cfg: Dict[str, Any]) -> GuardLimits:
    rg = cfg.get("risk_guard", {}) if isinstance(cfg.get("risk_guard", {}), dict) else {}
    enabled = bool(rg.get("enabled", True))
    poll_interval_sec = float(rg.get("poll_interval_sec", rg.get("poll_interval", 5)))
    if poll_interval_sec <= 0:
        poll_interval_sec = 5.0

    max_daily_dd_pct = float(rg.get("max_daily_dd_pct", 2.0))
    max_total_dd_pct = float(rg.get("max_total_dd_pct", 8.0))
    if max_daily_dd_pct < 0:
        max_daily_dd_pct = 0.0
    if max_total_dd_pct < 0:
        max_total_dd_pct = 0.0

    max_open_positions: Optional[int]
    try:
        v = rg.get("max_open_positions", None)
        max_open_positions = int(v) if v is not None else None
        if max_open_positions is not None and max_open_positions <= 0:
            max_open_positions = None
    except Exception:
        max_open_positions = None

    max_total_volume: Optional[float]
    try:
        v2 = rg.get("max_total_volume", None)
        max_total_volume = float(v2) if v2 is not None else None
        if max_total_volume is not None and max_total_volume <= 0:
            max_total_volume = None
    except Exception:
        max_total_volume = None

    symbol = cfg.get("symbol", None)
    if not isinstance(symbol, str) or not symbol.strip():
        symbol = None
    else:
        symbol = symbol.strip()

    initial_equity: Optional[float]
    try:
        ie = rg.get("initial_equity", None)
        initial_equity = float(ie) if ie is not None else None
        if initial_equity is not None and initial_equity <= 0:
            initial_equity = None
    except Exception:
        initial_equity = None

    return GuardLimits(
        enabled=enabled,
        poll_interval_sec=float(poll_interval_sec),
        max_daily_dd_pct=float(max_daily_dd_pct),
        max_total_dd_pct=float(max_total_dd_pct),
        max_open_positions=max_open_positions,
        max_total_volume=max_total_volume,
        symbol=symbol,
        initial_equity=initial_equity,
    )


def _local_midnight() -> datetime:
    now = datetime.now()
    return datetime(year=now.year, month=now.month, day=now.day)


def _state_load() -> Dict[str, Any]:
    return _load_json(RISK_GUARD_STATE_PATH) if os.path.exists(RISK_GUARD_STATE_PATH) else {}


def _state_get_today(state: Dict[str, Any]) -> Tuple[str, Optional[float]]:
    today = datetime.now().strftime("%Y-%m-%d")
    if str(state.get("day", "")) != today:
        return today, None
    try:
        v = state.get("day_start_equity", None)
        return today, float(v) if v is not None else None
    except Exception:
        return today, None


def _state_update_day_start(today: str, day_start_equity: float, initial_equity: float) -> None:
    _write_json(
        RISK_GUARD_STATE_PATH,
        {
            "day": today,
            "day_start_equity": float(day_start_equity),
            "initial_equity": float(initial_equity),
            "updated_utc": _utc_iso(),
            "version": VERSION,
        },
    )


def _account_snapshot() -> Tuple[bool, Dict[str, Any], str]:
    ok, reason = _mt5_init()
    if not ok:
        return False, {}, reason

    try:
        acc = mt5.account_info()
        if acc is None:
            return False, {}, "account_info_none"

        balance = float(getattr(acc, "balance", 0.0) or 0.0)
        equity = float(getattr(acc, "equity", 0.0) or 0.0)
        profit = float(getattr(acc, "profit", 0.0) or 0.0)
        margin_free = float(getattr(acc, "margin_free", 0.0) or 0.0)
        login = int(getattr(acc, "login", 0) or 0)
        currency = str(getattr(acc, "currency", "") or "")
        company = str(getattr(acc, "company", "") or "")
        server = str(getattr(acc, "server", "") or "")

        positions = mt5.positions_get()
        pos_list = list(positions) if positions else []
        pos_count = len(pos_list)
        total_volume = 0.0
        for p in pos_list:
            try:
                total_volume += float(getattr(p, "volume", 0.0) or 0.0)
            except Exception:
                continue

        return True, {
            "login": login,
            "currency": currency,
            "company": company,
            "server": server,
            "balance": balance,
            "equity": equity,
            "profit": profit,
            "margin_free": margin_free,
            "open_positions": pos_count,
            "open_volume": total_volume,
        }, "ok"
    except Exception as e:
        return False, {}, f"snapshot_error:{type(e).__name__}"
    finally:
        _mt5_shutdown()


class RiskGuardHardstop:
    def __init__(self) -> None:
        self.running = True

        self.config_path = _resolve_config_path()
        self.config = _load_json(self.config_path) if os.path.exists(self.config_path) else {}
        self.limits = _parse_limits(self.config)

        logger.info(f"Risk Guard Hardstop initialized {VERSION}")
        logger.info(f"config_path={self.config_path}")
        logger.info(f"enabled={self.limits.enabled}")
        logger.info(f"poll_interval_sec={self.limits.poll_interval_sec}")
        logger.info(f"max_daily_dd_pct={self.limits.max_daily_dd_pct}")
        logger.info(f"max_total_dd_pct={self.limits.max_total_dd_pct}")
        logger.info(f"max_open_positions={self.limits.max_open_positions}")
        logger.info(f"max_total_volume={self.limits.max_total_volume}")
        logger.info(f"kill_switch_path={KILL_SWITCH_PATH}")

        _log_event("initialized", {
            "config_path": self.config_path,
            "enabled": self.limits.enabled,
            "poll_interval_sec": self.limits.poll_interval_sec,
            "max_daily_dd_pct": self.limits.max_daily_dd_pct,
            "max_total_dd_pct": self.limits.max_total_dd_pct,
            "max_open_positions": self.limits.max_open_positions,
            "max_total_volume": self.limits.max_total_volume,
            "kill_switch_path": KILL_SWITCH_PATH,
        })

    def _signal_handler(self, signum, frame) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _evaluate(self) -> Tuple[bool, Dict[str, Any], str]:
        kill_on, kill_note = _read_kill_switch()
        if kill_on:
            return True, {"kill_switch": True, "note": kill_note}, "kill_switch_active"

        if not self.limits.enabled:
            return True, {"enabled": False}, "disabled"

        ok, snap, reason = _account_snapshot()
        if not ok:
            return False, {"kill_switch": False, "enabled": True}, reason

        state = _state_load()
        today, day_start_eq = _state_get_today(state)

        equity = float(snap.get("equity", 0.0) or 0.0)
        if equity <= 0:
            return False, snap, "invalid_equity"

        initial_equity = None
        try:
            initial_equity = float(state.get("initial_equity")) if state.get("initial_equity") is not None else None
        except Exception:
            initial_equity = None
        if initial_equity is None:
            initial_equity = self.limits.initial_equity
        if initial_equity is None or initial_equity <= 0:
            initial_equity = equity

        if day_start_eq is None or day_start_eq <= 0:
            day_start_eq = equity
            _state_update_day_start(today, day_start_eq, initial_equity)

        daily_dd_pct = max(0.0, (day_start_eq - equity) / day_start_eq * 100.0) if day_start_eq > 0 else 0.0
        total_dd_pct = max(0.0, (initial_equity - equity) / initial_equity * 100.0) if initial_equity > 0 else 0.0

        breach = []
        if daily_dd_pct >= self.limits.max_daily_dd_pct:
            breach.append("daily_dd_pct")
        if total_dd_pct >= self.limits.max_total_dd_pct:
            breach.append("total_dd_pct")

        pos_count = int(snap.get("open_positions", 0) or 0)
        pos_vol = float(snap.get("open_volume", 0.0) or 0.0)
        if self.limits.max_open_positions is not None and pos_count > int(self.limits.max_open_positions):
            breach.append("max_open_positions")
        if self.limits.max_total_volume is not None and pos_vol > float(self.limits.max_total_volume):
            breach.append("max_total_volume")

        out = {
            **snap,
            "day": today,
            "day_start_equity": float(day_start_eq),
            "initial_equity": float(initial_equity),
            "daily_dd_pct": float(round(daily_dd_pct, 6)),
            "total_dd_pct": float(round(total_dd_pct, 6)),
            "breach": breach,
        }
        return True, out, ("breach" if breach else "ok")

    def enforce_once(self) -> None:
        ok, status, reason = self._evaluate()
        _log_event("check", {"ok": ok, "reason": reason, **status})

        if not ok:
            msg = f"RISK_GUARD_FAIL_CLOSED\nreason={reason}\nutc={_utc_iso()}"
            logger.critical(msg)
            _write_kill_switch(msg)
            _log_event("kill_switch_written", {"reason": reason, "mode": "fail_closed"})
            return

        if reason == "breach":
            breach = status.get("breach")
            msg = (
                "RISK_GUARD_HARDSTOP\n"
                f"breach={breach}\n"
                f"daily_dd_pct={status.get('daily_dd_pct')}\n"
                f"total_dd_pct={status.get('total_dd_pct')}\n"
                f"open_positions={status.get('open_positions')}\n"
                f"open_volume={status.get('open_volume')}\n"
                f"utc={_utc_iso()}"
            )
            logger.critical(msg)
            _write_kill_switch(msg)
            _log_event("kill_switch_written", {"reason": "breach", "breach": breach})
            return

    def run(self) -> None:
        logger.info("Risk Guard Hardstop monitor started")
        _log_event("started", {})

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        while self.running:
            try:
                self.enforce_once()
            except Exception as e:
                logger.error(f"Error in main loop: {type(e).__name__}: {str(e)[:200]}")
            time.sleep(float(self.limits.poll_interval_sec))

        logger.info("Risk Guard Hardstop shutting down")
        _log_event("stopped", {})


def main() -> int:
    guard = RiskGuardHardstop()

    if "--daemon" in sys.argv:
        logger.info("Running in daemon mode")

    try:
        guard.run()
        return 0
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        return 0
    except Exception as e:
        logger.error(f"Fatal error: {type(e).__name__}: {str(e)[:300]}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
