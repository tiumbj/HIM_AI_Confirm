# ============================================================
# ชื่อโค้ด: HIM Cascade Runner (cascade_runner.py)
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\cascade_runner.py
# คำสั่งรัน: python cascade_runner.py
# เวอร์ชัน: v1.0.0
# ============================================================
"""
cascade_runner.py
Version: v1.0.0
Purpose: HIM v3 MTF Cascade Exit Runner
         - Standalone process for Phase 3.2
         - Reads him_v3.cascade_exit from config.json
         - Exits immediately with code 0 when disabled
         - Initializes MT5
         - Registers current open positions for this symbol/magic
         - Runs MTFCascadeExitSystem loop safely
         - Writes CASCADE_WARN / CASCADE_EXIT style events to logs/cascade_exit.jsonl

CHANGELOG (v1.0.0)
- ADD: standalone Phase 3.2 cascade runner
- ADD: fail-safe config loading
- ADD: MT5 init/shutdown lifecycle
- ADD: bootstrap registration from open MT5 positions
- ADD: JSONL cascade event logging
- KEEP: no change to production files
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import MetaTrader5 as mt5

VERSION = "v1.0.0"

DEFAULT_CONFIG_PATH = "config.json"
DEFAULT_SYMBOL = "GOLD"
DEFAULT_MAGIC = 202603
DEFAULT_BOOTSTRAP_INTERVAL_SEC = 5.0
DEFAULT_IDLE_SLEEP_SEC = 1.0
CASCADE_LOG_FILE = os.path.join("logs", "cascade_exit.jsonl")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_str(v: Any, default: str = "") -> str:
    try:
        if v is None:
            return default
        return str(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _resolve_config_path(raw_path: str) -> str:
    p = _safe_str(raw_path, DEFAULT_CONFIG_PATH).strip()
    if not p:
        p = DEFAULT_CONFIG_PATH
    if os.path.isabs(p):
        return p
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, p)


def _load_config(path: str) -> Dict[str, Any]:
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_him_v3_cfg(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    try:
        cfg = _load_config(_resolve_config_path(config_path))
        return cfg.get("him_v3", {}) if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _load_execution_cfg(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    try:
        cfg = _load_config(_resolve_config_path(config_path))
        exec_cfg = cfg.get("execution", {})
        return exec_cfg if isinstance(exec_cfg, dict) else {}
    except Exception:
        return {}


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _json_default(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if hasattr(obj, "_asdict"):
        return obj._asdict()
    return str(obj)


def _append_jsonl(path: str, payload: Dict[str, Any]) -> None:
    _ensure_parent_dir(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=_json_default) + "\n")


def _print_event(event: Dict[str, Any]) -> None:
    print(json.dumps(event, ensure_ascii=False, default=_json_default), flush=True)


def _kill_switch_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "KILL_SWITCH.txt")


def _kill_switch_active() -> bool:
    return os.path.exists(_kill_switch_path())


class CascadeRunner:
    def __init__(
        self,
        *,
        config_path: str = DEFAULT_CONFIG_PATH,
        symbol: Optional[str] = None,
        magic: Optional[int] = None,
        log_path: str = CASCADE_LOG_FILE,
    ) -> None:
        self.config_path = _resolve_config_path(config_path)
        self.him_v3_cfg = _load_him_v3_cfg(self.config_path)
        self.exec_cfg = _load_execution_cfg(self.config_path)

        self.symbol = _safe_str(
            symbol or self.exec_cfg.get("symbol") or os.environ.get("HIM_SYMBOL"),
            DEFAULT_SYMBOL,
        ).strip() or DEFAULT_SYMBOL

        self.magic = _safe_int(
            magic if magic is not None else self.exec_cfg.get("magic"),
            DEFAULT_MAGIC,
        )
        if self.magic <= 0:
            self.magic = DEFAULT_MAGIC

        self.log_path = log_path
        self.bootstrap_interval_sec = _safe_float(
            self.him_v3_cfg.get("cascade_exit", {}).get("bootstrap_interval_sec"),
            DEFAULT_BOOTSTRAP_INTERVAL_SEC,
        )
        if self.bootstrap_interval_sec <= 0:
            self.bootstrap_interval_sec = DEFAULT_BOOTSTRAP_INTERVAL_SEC

        self.idle_sleep_sec = _safe_float(
            self.him_v3_cfg.get("cascade_exit", {}).get("idle_sleep_sec"),
            DEFAULT_IDLE_SLEEP_SEC,
        )
        if self.idle_sleep_sec <= 0:
            self.idle_sleep_sec = DEFAULT_IDLE_SLEEP_SEC

        self._last_bootstrap_ts = 0.0
        self._registered_tickets: set[int] = set()
        self._cascade_sys: Any = None

    def _cfg_enabled(self) -> bool:
        return bool(self.him_v3_cfg.get("cascade_exit", {}).get("enabled", False))

    def _mt5_init(self) -> bool:
        try:
            return bool(mt5.initialize())
        except Exception:
            return False

    def _mt5_shutdown(self) -> None:
        try:
            mt5.shutdown()
        except Exception:
            pass

    def _log_event(self, *, status: str, reason: str, **extra: Any) -> None:
        payload: Dict[str, Any] = {
            "ts": _utc_now_iso(),
            "version": VERSION,
            "runner": "cascade_runner",
            "symbol": self.symbol,
            "magic": self.magic,
            "status": status,
            "reason": reason,
        }
        payload.update(extra)
        _append_jsonl(self.log_path, payload)
        _print_event(payload)

    def _get_cascade_system(self) -> Any:
        if self._cascade_sys is not None:
            return self._cascade_sys

        from mtf_cascade_exit import get_cascade_system  # local import by design

        self._cascade_sys = get_cascade_system(self.symbol, self.magic)
        return self._cascade_sys

    def _positions_for_magic(self) -> List[Any]:
        try:
            positions = mt5.positions_get(symbol=self.symbol)
        except Exception:
            positions = None
        if not positions:
            return []
        out: List[Any] = []
        for p in positions:
            if _safe_int(getattr(p, "magic", None), 0) != self.magic:
                continue
            out.append(p)
        return out

    def _position_direction(self, pos: Any) -> Optional[str]:
        p_type = getattr(pos, "type", None)
        if p_type == 0:
            return "BUY"
        if p_type == 1:
            return "SELL"
        return None

    def _register_open_positions(self) -> None:
        now_ts = time.time()
        if (now_ts - self._last_bootstrap_ts) < self.bootstrap_interval_sec:
            return
        self._last_bootstrap_ts = now_ts

        try:
            from mtf_cascade_exit import PositionCtx
        except Exception as exc:
            self._log_event(status="ERROR", reason="cascade_import_failed", detail=str(exc)[:300])
            return

        try:
            cascade_sys = self._get_cascade_system()
        except Exception as exc:
            self._log_event(status="ERROR", reason="cascade_system_init_failed", detail=str(exc)[:300])
            return

        positions = self._positions_for_magic()
        live_tickets = set()

        for pos in positions:
            ticket = _safe_int(getattr(pos, "ticket", None), 0)
            if ticket <= 0:
                continue
            live_tickets.add(ticket)
            if ticket in self._registered_tickets:
                continue

            direction = self._position_direction(pos)
            if direction not in ("BUY", "SELL"):
                continue

            entry_price = _safe_float(getattr(pos, "price_open", None), 0.0)
            volume = _safe_float(getattr(pos, "volume", None), 0.0)
            if entry_price <= 0 or volume <= 0:
                continue

            atr_at_entry = 1.0
            try:
                pos_ctx = PositionCtx(
                    ticket=ticket,
                    direction=direction,
                    entry_price=entry_price,
                    atr_at_entry=atr_at_entry,
                    volume=volume,
                )
                cascade_sys.register(pos_ctx)
                self._registered_tickets.add(ticket)
                self._log_event(
                    status="INFO",
                    reason="CASCADE_REGISTERED",
                    ticket=ticket,
                    direction=direction,
                    entry_price=entry_price,
                    volume=volume,
                    source="bootstrap",
                )
            except Exception as exc:
                self._log_event(
                    status="ERROR",
                    reason="cascade_register_failed",
                    ticket=ticket,
                    detail=str(exc)[:300],
                )

        stale = self._registered_tickets - live_tickets
        for ticket in sorted(stale):
            self._registered_tickets.discard(ticket)
            self._log_event(
                status="INFO",
                reason="CASCADE_UNREGISTERED",
                ticket=ticket,
                source="bootstrap_cleanup",
            )

    def _run_cascade_loop_once(self) -> None:
        try:
            cascade_sys = self._get_cascade_system()
        except Exception as exc:
            self._log_event(status="ERROR", reason="cascade_system_unavailable", detail=str(exc)[:300])
            time.sleep(self.idle_sleep_sec)
            return

        loop_fn = getattr(cascade_sys, "loop_once", None)
        if callable(loop_fn):
            try:
                result = loop_fn()
                if result is not None:
                    self._log_event(status="INFO", reason="CASCADE_LOOP_ONCE", result=result)
            except Exception as exc:
                self._log_event(status="ERROR", reason="cascade_loop_once_failed", detail=str(exc)[:300])
            time.sleep(self.idle_sleep_sec)
            return

        loop_forever = getattr(cascade_sys, "loop", None)
        if callable(loop_forever):
            try:
                self._log_event(status="INFO", reason="CASCADE_LOOP_STARTED")
                loop_forever()
            except Exception as exc:
                self._log_event(status="ERROR", reason="cascade_loop_failed", detail=str(exc)[:300])
            return

        self._log_event(status="ERROR", reason="cascade_loop_method_missing")
        time.sleep(self.idle_sleep_sec)

    def run(self) -> int:
        if not self._cfg_enabled():
            self._log_event(status="INFO", reason="cascade_disabled_exit0")
            return 0

        if _kill_switch_active():
            self._log_event(status="INFO", reason="kill_switch_active_exit0")
            return 0

        if not self._mt5_init():
            self._log_event(status="ERROR", reason="mt5_initialize_failed")
            return 1

        self._log_event(status="INFO", reason="cascade_runner_started")

        try:
            while True:
                if _kill_switch_active():
                    self._log_event(status="INFO", reason="kill_switch_stop")
                    return 0

                self.him_v3_cfg = _load_him_v3_cfg(self.config_path)
                if not self._cfg_enabled():
                    self._log_event(status="INFO", reason="cascade_disabled_runtime_exit0")
                    return 0

                self._register_open_positions()
                self._run_cascade_loop_once()

                loop_fn = getattr(self._get_cascade_system(), "loop_once", None)
                if not callable(loop_fn):
                    return 0
        finally:
            self._log_event(status="INFO", reason="cascade_runner_stopped")
            self._mt5_shutdown()


if __name__ == "__main__":
    runner = CascadeRunner(
        config_path=os.environ.get("HIM_CONFIG_PATH", DEFAULT_CONFIG_PATH),
        symbol=os.environ.get("HIM_SYMBOL"),
        magic=_safe_int(os.environ.get("HIM_MAGIC"), 0) or None,
    )
    sys.exit(runner.run())