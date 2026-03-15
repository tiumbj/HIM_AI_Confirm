# ============================================================
# ชื่อโค้ด: HIM MT5 Executor (mt5_executor.py)
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\mt5_executor.py
# คำสั่งรัน: python mt5_executor.py
# เวอร์ชัน: v1.4.2
# ============================================================
"""
mt5_executor.py
Version: v1.4.2
Purpose: MT5 Execution Guard + AI Final Confirm Gate + Request Dedup + Execution Logging
         (Production Safe Execution for HIM)

========================================================
CHANGELOG (v1.4.2)
========================================================
- FIX: Start MTF Cascade Exit background loop in production process (exit monitoring actually runs)
- ADD: Bootstrap-register existing open positions for cascade monitoring (best-effort, fail-safe)

========================================================
CHANGELOG (v1.4.1)
========================================================
- ADD: Phase 3.1 HIM v3 config loader (_load_him_v3_cfg)
- ADD: Phase 3.1 MTF Cascade Exit registration after enforce_sltp_after_send()
- KEEP: Existing spread/stops/cooldown/order_send behavior unchanged
- FAIL-SAFE: Cascade registration is wrapped in try/except and never affects order execution

========================================================
CHANGELOG (v1.4.0)
========================================================
- FIX: Execution parameters are loaded from config.json (single source-of-truth under config.execution)
  Targeted parameters: magic, lot, max_spread_points, deviation, cooldown, ATR spread params
- KEEP: v1.3.1 behavior and interfaces unless config overrides are provided

========================================================
CHANGELOG (v1.3.1)
========================================================
- FIX: Directional stops validation (prevents MT5 retcode=10016 'Invalid stops')
  BUY requires: SL < price and TP > price
  SELL requires: SL > price and TP < price
- KEEP: v1.3.0 features (AI gate, dedup, JSONL logs, SLTP enforcement, ATR spread)
- Fail-closed: invalid stop side => SKIP("invalid_stops_side")

========================================================
INPUT CONTRACT (AI -> mt5_executor)
========================================================
Required minimal fields:
{
  "request_id": "unique_string",
  "decision": "BUY" | "SELL" | "HOLD" | ...,
  "plan": {"entry": <num>, "sl": <num>, "tp": <num>},
  "ai_confirm": {"approved": true|false, "reason": "...", "confidence": 0..1}
}
"""

from __future__ import annotations

import os
import time
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import MetaTrader5 as mt5
import numpy as np

VERSION = "v1.4.2"

_UNSET = object()

DEFAULT_CONFIG_PATH = "config.json"
DEFAULT_MAGIC_NUMBER = 202603
DEFAULT_COOLDOWN_SECONDS = 20
DEFAULT_LOT = 0.01
DEFAULT_MAX_SPREAD_POINTS = 80
DEFAULT_ATR_PERIOD = 14
DEFAULT_ATR_MULTIPLIER = 0.2
DEFAULT_TIMEFRAME = mt5.TIMEFRAME_M5
DEFAULT_DEVIATION_MIN = 20
DEFAULT_DEVIATION_SPREAD_MULT = 2.0
DEFAULT_SLTP_VERIFY_TIMEOUT_SEC = 3.0
DEFAULT_SLTP_VERIFY_RETRY_INTERVAL_SEC = 0.25

DEDUP_STATE_FILE = ".execution_dedup_state.json"
EXEC_LOG_FILE = os.path.join("logs", "execution_orders.jsonl")


@dataclass(frozen=True)
class ExecutionSettings:
    config_path: str
    magic: int
    cooldown_seconds: float
    lot: float
    max_spread_points: int
    atr_period: int
    atr_multiplier: float
    timeframe: int
    deviation_min: int
    deviation_spread_mult: float
    sltp_verify_timeout_sec: float
    sltp_verify_retry_interval_sec: float


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def _safe_str(v: Any, default: str = "") -> str:
    try:
        if v is None:
            return default
        return str(v)
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
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _load_him_v3_cfg(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Phase 3.1: Load him_v3 section from config.json. Fail-safe: returns {} on any error."""
    try:
        cfg = _load_config(_resolve_config_path(config_path))
        return cfg.get("him_v3", {}) if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _tf_from_config(v: Any) -> Optional[int]:
    if isinstance(v, int):
        return v
    s = _safe_str(v, "").strip().upper()
    if not s:
        return None
    tf_map: Dict[str, Optional[int]] = {
        "M1": mt5.TIMEFRAME_M1,
        "M5": mt5.TIMEFRAME_M5,
        "M10": getattr(mt5, "TIMEFRAME_M10", None),
        "M15": mt5.TIMEFRAME_M15,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
    }
    tf = tf_map.get(s)
    return int(tf) if tf is not None else None


def _load_execution_settings(config_path: str) -> ExecutionSettings:
    cfg_path = _resolve_config_path(config_path)
    cfg = _load_config(cfg_path)
    exec_cfg = cfg.get("execution", {}) if isinstance(cfg.get("execution", {}), dict) else {}

    magic = _safe_int(exec_cfg.get("magic"), None)
    if magic is None:
        magic = DEFAULT_MAGIC_NUMBER

    cooldown_seconds = _safe_float(exec_cfg.get("cooldown_seconds"), None)
    if cooldown_seconds is None:
        cooldown_seconds = float(DEFAULT_COOLDOWN_SECONDS)

    lot = _safe_float(exec_cfg.get("lot"), None)
    if lot is None:
        lot = _safe_float(exec_cfg.get("volume"), None)
    if lot is None:
        lot = _safe_float(cfg.get("lot"), None)
    if lot is None:
        lot = float(DEFAULT_LOT)

    max_spread_points = _safe_int(exec_cfg.get("max_spread_points"), None)
    if max_spread_points is None:
        max_spread_points = DEFAULT_MAX_SPREAD_POINTS

    atr_period = _safe_int(exec_cfg.get("atr_period"), None)
    if atr_period is None:
        atr_period = DEFAULT_ATR_PERIOD

    atr_multiplier = _safe_float(exec_cfg.get("atr_multiplier"), None)
    if atr_multiplier is None:
        atr_multiplier = float(DEFAULT_ATR_MULTIPLIER)

    tf = _tf_from_config(exec_cfg.get("timeframe"))
    if tf is None:
        tf = int(DEFAULT_TIMEFRAME)

    deviation_min = _safe_int(exec_cfg.get("deviation_min"), None)
    if deviation_min is None:
        deviation_min = _safe_int(exec_cfg.get("max_deviation_points"), None)
    if deviation_min is None:
        deviation_min = DEFAULT_DEVIATION_MIN

    deviation_spread_mult = _safe_float(exec_cfg.get("deviation_spread_mult"), None)
    if deviation_spread_mult is None:
        deviation_spread_mult = float(DEFAULT_DEVIATION_SPREAD_MULT)

    sltp_verify_timeout_sec = _safe_float(exec_cfg.get("sltp_verify_timeout_sec"), None)
    if sltp_verify_timeout_sec is None:
        sltp_verify_timeout_sec = float(DEFAULT_SLTP_VERIFY_TIMEOUT_SEC)

    sltp_verify_retry_interval_sec = _safe_float(exec_cfg.get("sltp_verify_retry_interval_sec"), None)
    if sltp_verify_retry_interval_sec is None:
        sltp_verify_retry_interval_sec = float(DEFAULT_SLTP_VERIFY_RETRY_INTERVAL_SEC)

    return ExecutionSettings(
        config_path=cfg_path,
        magic=int(magic),
        cooldown_seconds=float(cooldown_seconds),
        lot=float(lot),
        max_spread_points=int(max_spread_points),
        atr_period=int(atr_period),
        atr_multiplier=float(atr_multiplier),
        timeframe=int(tf),
        deviation_min=int(deviation_min),
        deviation_spread_mult=float(deviation_spread_mult),
        sltp_verify_timeout_sec=float(sltp_verify_timeout_sec),
        sltp_verify_retry_interval_sec=float(sltp_verify_retry_interval_sec),
    )


class MT5Executor:
    def __init__(
        self,
        symbol: str = "GOLD",
        lot: Any = _UNSET,
        max_spread_points: Any = _UNSET,
        atr_period: Any = _UNSET,
        atr_multiplier: Any = _UNSET,
        timeframe: Any = _UNSET,
        deviation_min: Any = _UNSET,
        deviation_spread_mult: Any = _UNSET,
        sltp_verify_timeout_sec: Any = _UNSET,
        sltp_verify_retry_interval_sec: Any = _UNSET,
        dedup_state_file: str = DEDUP_STATE_FILE,
        exec_log_file: str = EXEC_LOG_FILE,
        config_path: Optional[str] = None,
        magic: Any = _UNSET,
        cooldown_seconds: Any = _UNSET,
    ) -> None:
        self.symbol = symbol

        cfg_raw = config_path
        if not cfg_raw:
            cfg_raw = os.environ.get("HIM_CONFIG") or os.environ.get("HIM_CONFIG_PATH") or DEFAULT_CONFIG_PATH
        settings = _load_execution_settings(str(cfg_raw))
        self.config_path = str(settings.config_path)

        self.magic = int(settings.magic if magic is _UNSET else int(float(magic)))
        self.cooldown_seconds = float(settings.cooldown_seconds if cooldown_seconds is _UNSET else float(cooldown_seconds))

        self.lot = float(settings.lot if lot is _UNSET else float(lot))

        self.max_spread_points = int(settings.max_spread_points if max_spread_points is _UNSET else int(float(max_spread_points)))
        self.atr_period = int(settings.atr_period if atr_period is _UNSET else int(float(atr_period)))
        self.atr_multiplier = float(settings.atr_multiplier if atr_multiplier is _UNSET else float(atr_multiplier))
        self.timeframe = int(settings.timeframe if timeframe is _UNSET else int(float(timeframe)))

        self.deviation_min = int(settings.deviation_min if deviation_min is _UNSET else int(float(deviation_min)))
        self.deviation_spread_mult = float(settings.deviation_spread_mult if deviation_spread_mult is _UNSET else float(deviation_spread_mult))

        self.sltp_verify_timeout_sec = float(settings.sltp_verify_timeout_sec if sltp_verify_timeout_sec is _UNSET else float(sltp_verify_timeout_sec))
        self.sltp_verify_retry_interval_sec = float(settings.sltp_verify_retry_interval_sec if sltp_verify_retry_interval_sec is _UNSET else float(sltp_verify_retry_interval_sec))

        self.dedup_state_file = str(dedup_state_file)
        self.exec_log_file = str(exec_log_file)

        self.last_trade_time = 0.0

        os.makedirs(os.path.dirname(self.exec_log_file), exist_ok=True)

        self._dedup = self._load_dedup_state()

        self._tg: Optional[Any] = None
        try:
            from telegram_notifier import TelegramNotifier  # type: ignore

            base_dir = os.path.dirname(os.path.abspath(__file__))
            tg_cfg_path = os.path.join(base_dir, "config.json")
            self._tg = TelegramNotifier(config_path=tg_cfg_path)
        except Exception:
            self._tg = None

        if not mt5.initialize():
            raise RuntimeError("MT5 initialize failed")

        self._cascade_sys: Optional[Any] = None
        self._cascade_mtf: Optional[Any] = None
        self._maybe_start_cascade_exit()

    def _maybe_start_cascade_exit(self) -> None:
        try:
            _him_v3 = _load_him_v3_cfg(self.config_path)
            cascade_cfg = _him_v3.get("cascade_exit", {}) if isinstance(_him_v3, dict) else {}
            if not (isinstance(cascade_cfg, dict) and cascade_cfg.get("enabled", False)):
                return

            from mtf_supertrend import MTFSupertrend
            from mtf_cascade_exit import PositionCtx, get_cascade_system

            self._cascade_mtf = MTFSupertrend(symbol=self.symbol, config_path=self.config_path)
            self._cascade_sys = get_cascade_system(self.symbol, int(self.magic))

            try:
                self._cascade_sys.start_background(self._cascade_mtf)
            except Exception:
                pass

            try:
                registered = set(self._cascade_sys.registered_tickets())
            except Exception:
                registered = set()

            positions = mt5.positions_get(symbol=self.symbol)
            if not positions:
                return

            info = mt5.symbol_info(self.symbol)
            point = float(getattr(info, "point", 0.0) or 0.0) if info is not None else 0.0
            atr_price = 1.0
            if point > 0:
                atr_points = self.get_atr_points(point)
                if atr_points is not None and atr_points > 0:
                    atr_price = float(atr_points) * float(point)

            for p in positions:
                if getattr(p, "magic", None) != int(self.magic):
                    continue
                ticket = int(getattr(p, "ticket", 0) or 0)
                if ticket <= 0 or ticket in registered:
                    continue
                p_type = getattr(p, "type", None)
                direction = "BUY" if int(p_type) == 0 else "SELL"
                entry_price = float(getattr(p, "price_open", 0.0) or 0.0)
                volume = float(getattr(p, "volume", 0.0) or 0.0)
                if entry_price <= 0 or volume <= 0:
                    continue

                pos_ctx = PositionCtx(
                    ticket=ticket,
                    direction=direction,
                    entry_price=float(entry_price),
                    atr_at_entry=float(atr_price),
                    volume=float(volume),
                )
                self._cascade_sys.register(pos_ctx)
        except Exception:
            return

    # -----------------------------
    # Helpers
    # -----------------------------

    @staticmethod
    def _is_number(x: Any) -> bool:
        try:
            float(x)
            return True
        except Exception:
            return False

    @staticmethod
    def _now() -> float:
        return time.time()

    @staticmethod
    def _round_to_digits(price: float, digits: int) -> float:
        return float(round(float(price), int(digits)))

    @staticmethod
    def _safe_json(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False, default=str)

    def _append_jsonl(self, record: Dict[str, Any]) -> None:
        line = self._safe_json(record)
        with open(self.exec_log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    @staticmethod
    def _utc_ts_str() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _tg_send(self, text: str, event_type: str) -> None:
        if self._tg is None:
            return
        try:
            self._tg.send_text(text=text, event_type=event_type, parse_mode=None)
        except Exception:
            return

    def _format_trade_alert(
        self,
        *,
        status: str,
        direction: str,
        request_id: str,
        price: Optional[float],
        sl: Optional[float],
        tp: Optional[float],
        position_ticket: Optional[int],
        ai_confirm: Any,
        extra: str = "",
    ) -> str:
        conf = ""
        if isinstance(ai_confirm, dict):
            c = ai_confirm.get("confidence", None)
            if isinstance(c, (int, float)):
                conf = f"\nconfidence={float(c):.2f}"
        ticket_str = str(position_ticket) if position_ticket is not None else "-"
        price_str = f"{price:.2f}" if isinstance(price, (int, float)) else "-"
        sl_str = f"{sl:.2f}" if isinstance(sl, (int, float)) else "-"
        tp_str = f"{tp:.2f}" if isinstance(tp, (int, float)) else "-"
        x = f"\n{extra}" if extra else ""
        return (
            f"HIM TRADE | {status}\n"
            f"time_utc={self._utc_ts_str()}\n"
            f"symbol={self.symbol}\n"
            f"side={direction}\n"
            f"volume={float(self.lot):g}\n"
            f"price={price_str}\n"
            f"sl={sl_str}\n"
            f"tp={tp_str}\n"
            f"ticket={ticket_str}\n"
            f"request_id={request_id}"
            f"{conf}"
            f"{x}"
        )

    # -----------------------------
    # Dedup State
    # -----------------------------

    def _load_dedup_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.dedup_state_file):
            return {"version": VERSION, "executed": {}}

        try:
            with open(self.dedup_state_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {"version": VERSION, "executed": {}}
            if "executed" not in data or not isinstance(data["executed"], dict):
                data["executed"] = {}
            return data
        except Exception:
            return {"version": VERSION, "executed": {}, "warning": "dedup_state_load_failed"}

    def _save_dedup_state(self) -> None:
        tmp = self.dedup_state_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._dedup, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.dedup_state_file)

    def _dedup_is_done(self, request_id: str) -> bool:
        return str(request_id) in self._dedup.get("executed", {})

    def _dedup_mark_done(self, request_id: str, payload: Dict[str, Any]) -> None:
        self._dedup.setdefault("executed", {})[str(request_id)] = payload
        self._dedup["version"] = VERSION
        self._save_dedup_state()

    # -----------------------------
    # AI Confirm Gate
    # -----------------------------

    def ai_confirm_check(self, signal: Dict[str, Any]) -> Tuple[bool, str]:
        ai_confirm = signal.get("ai_confirm", None)
        if not isinstance(ai_confirm, dict):
            return False, "ai_confirm_missing"
        if ai_confirm.get("approved", None) is not True:
            return False, "ai_denied"
        return True, "ai_approved"

    # -----------------------------
    # Symbol / Environment Checks
    # -----------------------------

    def symbol_check(self) -> Tuple[bool, Any]:
        info = mt5.symbol_info(self.symbol)
        if info is None:
            return False, "symbol_not_found"

        if not info.visible:
            ok = mt5.symbol_select(self.symbol, True)
            if not ok:
                return False, "symbol_select_failed"

        if getattr(info, "trade_mode", None) == mt5.SYMBOL_TRADE_MODE_DISABLED:
            return False, "trade_disabled"

        return True, info

    def cooldown_check(self) -> Tuple[bool, Optional[str]]:
        if self._now() - self.last_trade_time < float(self.cooldown_seconds):
            return False, "cooldown_active"
        return True, None

    def margin_check(self) -> Tuple[bool, Optional[str]]:
        acc = mt5.account_info()
        if acc is None:
            return False, "account_error"
        if acc.margin_free <= 0:
            return False, "no_margin"
        return True, None

    # -----------------------------
    # Duplicate / Pending Guards
    # -----------------------------

    def duplicate_position_check(self, direction: str) -> Tuple[bool, Optional[str]]:
        positions = mt5.positions_get(symbol=self.symbol)
        if positions:
            for p in positions:
                if getattr(p, "magic", None) != self.magic:
                    continue
                if direction == "BUY" and p.type == 0:
                    return False, "duplicate_buy_magic"
                if direction == "SELL" and p.type == 1:
                    return False, "duplicate_sell_magic"
        return True, None

    def _block_opposite_enabled(self) -> bool:
        return os.environ.get("EXECUTION_BLOCK_OPPOSITE", "1").strip() in ("1", "true", "TRUE", "yes", "YES")

    def opposite_position_check(self, direction: str) -> Tuple[bool, Optional[str]]:
        if not self._block_opposite_enabled():
            return True, None
        d = str(direction).upper().strip()
        positions = mt5.positions_get(symbol=self.symbol)
        if not positions:
            return True, None
        for p in positions:
            if getattr(p, "magic", None) != self.magic:
                continue
            p_type = getattr(p, "type", None)
            if d == "BUY" and p_type == 1:
                return False, "opposite_sell_open"
            if d == "SELL" and p_type == 0:
                return False, "opposite_buy_open"
        return True, None

    def _adaptive_reverse_enabled(self) -> bool:
        return os.environ.get("EXECUTION_ADAPTIVE_REVERSE", "1").strip() in ("1", "true", "TRUE", "yes", "YES")

    def _reverse_required_votes(self) -> int:
        try:
            v = int(float(os.environ.get("EXECUTION_REVERSE_MIN_VOTES", "2")))
        except Exception:
            v = 2
        return max(1, v)

    def _reverse_max_st_distance_atr(self) -> float:
        try:
            return max(0.0, float(os.environ.get("EXECUTION_REVERSE_MAX_ST_DISTANCE_ATR", "2.8")))
        except Exception:
            return 2.8

    def _reverse_confirmed(self, direction: str, signal: Dict[str, Any]) -> bool:
        d = str(direction).upper().strip()
        if d not in ("BUY", "SELL"):
            return False
        metrics = signal.get("metrics") if isinstance(signal.get("metrics"), dict) else {}
        try:
            align = int(metrics.get("alignment_score") or 0)
        except Exception:
            align = 0
        if align < 2:
            return False
        try:
            st_dir = int(metrics.get("supertrend_dir_event") or 0)
        except Exception:
            st_dir = 0
        if d == "BUY" and st_dir <= 0:
            return False
        if d == "SELL" and st_dir >= 0:
            return False
        st_dist = metrics.get("supertrend_distance_atr")
        try:
            st_dist_f = float(st_dist)
        except Exception:
            st_dist_f = None
        if st_dist_f is not None and st_dist_f > self._reverse_max_st_distance_atr():
            return False
        regime = str(metrics.get("regime") or metrics.get("regime_candidate") or "").upper().strip()
        if regime and regime not in self._pyramid_allowed_regimes():
            return False
        votes = signal.get("decision_votes") if isinstance(signal.get("decision_votes"), dict) else {}
        if votes:
            want = int(votes.get("BUY", 0) if d == "BUY" else votes.get("SELL", 0))
            opp = int(votes.get("SELL", 0) if d == "BUY" else votes.get("BUY", 0))
            if opp > 0:
                return False
            if want < self._reverse_required_votes():
                return False
        return True

    def _close_position_by_ticket(self, p: Any) -> Tuple[bool, str]:
        p_type = getattr(p, "type", None)
        ticket = int(getattr(p, "ticket", 0) or 0)
        vol = float(getattr(p, "volume", 0.0) or 0.0)
        if ticket <= 0 or vol <= 0:
            return False, "invalid_position_ticket_or_volume"
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return False, "tick_error"
        if p_type == 0:
            close_type = mt5.ORDER_TYPE_SELL
            px = float(tick.bid)
        elif p_type == 1:
            close_type = mt5.ORDER_TYPE_BUY
            px = float(tick.ask)
        else:
            return False, "unknown_position_type"
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "position": ticket,
            "volume": vol,
            "type": close_type,
            "price": px,
            "deviation": int(self.deviation_min),
            "magic": int(self.magic),
            "comment": "HIM_CLOSE_REVERSE",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        res = mt5.order_send(req)
        if res is None:
            return False, "close_order_send_none"
        if int(getattr(res, "retcode", 0)) != int(mt5.TRADE_RETCODE_DONE):
            rc = int(getattr(res, "retcode", 0))
            return False, f"close_fail:{rc}"
        return True, "closed"

    def adaptive_reverse_opposite(self, direction: str, signal: Dict[str, Any]) -> Tuple[bool, Optional[str], int]:
        d = str(direction).upper().strip()
        opposite = "SELL" if d == "BUY" else "BUY"
        opp_positions = self._our_positions_side(opposite)
        if not opp_positions:
            return True, None, 0
        if not self._adaptive_reverse_enabled():
            return False, f"opposite_{opposite.lower()}_open", 0
        if not self._reverse_confirmed(d, signal):
            return False, "opposite_signal_not_confirmed", 0
        closed = 0
        opp_positions.sort(key=lambda x: getattr(x, "time", 0) or 0)
        for p in opp_positions:
            ok, rs = self._close_position_by_ticket(p)
            if not ok:
                return False, rs, closed
            closed += 1
        return True, None, closed

    def _pyramid_enabled(self) -> bool:
        return os.environ.get("EXECUTION_PYRAMID_ENABLE", "1").strip() in ("1", "true", "TRUE", "yes", "YES")

    def _pyramid_step_atr(self) -> float:
        try:
            return max(0.0, float(os.environ.get("EXECUTION_PYRAMID_STEP_ATR", "0.8")))
        except Exception:
            return 0.8

    def _pyramid_min_align(self) -> int:
        try:
            return max(0, int(float(os.environ.get("EXECUTION_PYRAMID_MIN_ALIGN", "2"))))
        except Exception:
            return 2

    def _pyramid_allowed_regimes(self) -> set[str]:
        raw = os.environ.get("EXECUTION_PYRAMID_ALLOW_REGIMES", "TREND,EXPANSION").strip()
        parts = [p.strip().upper() for p in raw.split(",") if p.strip()]
        return set(parts) if parts else {"TREND", "EXPANSION"}

    def _pyramid_max_st_distance_atr(self) -> float:
        try:
            return max(0.0, float(os.environ.get("EXECUTION_PYRAMID_MAX_ST_DISTANCE_ATR", "3.2")))
        except Exception:
            return 3.2

    def _pyramid_margin_buffer(self) -> float:
        try:
            return max(1.0, float(os.environ.get("EXECUTION_PYRAMID_MARGIN_BUFFER", "1.6")))
        except Exception:
            return 1.6

    def _abs_max_positions(self) -> int:
        try:
            return max(1, int(float(os.environ.get("EXECUTION_ABS_MAX_POSITIONS", "50"))))
        except Exception:
            return 50

    def _our_positions(self) -> list[Any]:
        pos = mt5.positions_get(symbol=self.symbol)
        if not pos:
            return []
        out: list[Any] = []
        for p in pos:
            if getattr(p, "magic", None) != self.magic:
                continue
            out.append(p)
        return out

    def _our_positions_side(self, direction: str) -> list[Any]:
        d = str(direction).upper().strip()
        want_type = 0 if d == "BUY" else 1
        out: list[Any] = []
        for p in self._our_positions():
            if getattr(p, "type", None) != want_type:
                continue
            out.append(p)
        return out

    def _latest_entry_price(self, positions: list[Any]) -> Optional[float]:
        if not positions:
            return None
        positions.sort(key=lambda x: getattr(x, "time", 0) or 0, reverse=True)
        p0 = positions[0]
        try:
            return float(getattr(p0, "price_open", 0.0) or 0.0)
        except Exception:
            return None

    def _order_calc_margin(self, order_type: int, price: float) -> Optional[float]:
        try:
            m = mt5.order_calc_margin(order_type, self.symbol, float(self.lot), float(price))
            if m is None:
                return None
            m = float(m)
            return m if m > 0 else None
        except Exception:
            return None

    def adaptive_position_check(self, *, direction: str, order_type: int, exec_price: float, info: Any, signal: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        d = str(direction).upper().strip()
        metrics = signal.get("metrics") if isinstance(signal.get("metrics"), dict) else {}
        regime = str(metrics.get("regime") or metrics.get("regime_candidate") or "").upper().strip()
        st_dist = metrics.get("supertrend_distance_atr")
        try:
            st_dist_f = float(st_dist)
        except Exception:
            st_dist_f = None
        if st_dist_f is not None and st_dist_f > self._pyramid_max_st_distance_atr():
            return False, "pyramid_block_chase"

        try:
            align = int(metrics.get("alignment_score") or 0)
        except Exception:
            align = 0

        st_dir = metrics.get("supertrend_dir_event")
        try:
            st_dir_i = int(st_dir)
        except Exception:
            st_dir_i = 0
        if d == "BUY" and st_dir_i <= 0:
            return False, "pyramid_block_st_dir"
        if d == "SELL" and st_dir_i >= 0:
            return False, "pyramid_block_st_dir"

        side_positions = self._our_positions_side(d)
        if not side_positions:
            total = len(self._our_positions())
            if total >= self._abs_max_positions():
                return False, "abs_max_positions_reached"
            return True, None

        if not self._pyramid_enabled():
            return self.duplicate_position_check(d)

        if regime and regime not in self._pyramid_allowed_regimes():
            return False, "pyramid_block_regime"

        if align < self._pyramid_min_align():
            return False, "pyramid_block_alignment"

        atr = metrics.get("atr")
        try:
            atr_f = float(atr)
        except Exception:
            atr_f = None
        if atr_f is None or atr_f <= 0:
            point = float(getattr(info, "point", 0.0) or 0.0)
            atr_pts = self.get_atr_points(point) if point > 0 else None
            atr_f = (float(atr_pts) * point) if (atr_pts is not None and point > 0) else None
        if atr_f is None or atr_f <= 0:
            return False, "pyramid_no_atr"

        last_entry = self._latest_entry_price(side_positions)
        if last_entry is None:
            return False, "pyramid_no_last_entry"

        step = self._pyramid_step_atr() * float(atr_f)
        if d == "BUY":
            if float(exec_price) < (float(last_entry) + step):
                return False, "pyramid_wait_step"
        else:
            if float(exec_price) > (float(last_entry) - step):
                return False, "pyramid_wait_step"

        total = len(self._our_positions())
        if total >= self._abs_max_positions():
            return False, "abs_max_positions_reached"

        acc = mt5.account_info()
        if acc is None:
            return False, "account_error"
        margin_per_order = self._order_calc_margin(order_type, float(exec_price))
        if margin_per_order is not None:
            buffer = self._pyramid_margin_buffer()
            max_orders_by_margin = int(math.floor(float(acc.margin_free) / (float(margin_per_order) * float(buffer))))
            if max_orders_by_margin <= 0:
                return False, "pyramid_no_margin_room"
            if total >= max_orders_by_margin:
                return False, "pyramid_margin_cap"

        return True, None

    def pending_orders_check(self) -> Tuple[bool, Optional[str]]:
        orders = mt5.orders_get(symbol=self.symbol)
        if not orders:
            return True, None
        for o in orders:
            if getattr(o, "magic", None) != self.magic:
                continue
            return False, "pending_order_exists_magic"
        return True, None

    # -----------------------------
    # ATR-based Spread Filter
    # -----------------------------

    def get_atr_points(self, point: float) -> Optional[float]:
        n = self.atr_period + 1
        rates = mt5.copy_rates_from_pos(self.symbol, self.timeframe, 0, n)
        if rates is None or len(rates) < n:
            return None

        try:
            highs = np.asarray(rates["high"], dtype=float)
            lows = np.asarray(rates["low"], dtype=float)
            closes = np.asarray(rates["close"], dtype=float)
        except Exception:
            return None

        if highs.size < n or lows.size < n or closes.size < n:
            return None

        tr = np.maximum(
            highs[1:] - lows[1:],
            np.maximum(
                np.abs(highs[1:] - closes[:-1]),
                np.abs(lows[1:] - closes[:-1]),
            ),
        )

        atr_price = float(np.mean(tr))
        if point <= 0:
            return None
        return atr_price / point

    def get_live_spread_points(self, info: Any) -> Tuple[Optional[int], Optional[str]]:
        point = float(getattr(info, "point", 0.0))
        if point <= 0:
            return None, "invalid_point"
        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            return None, "tick_error"
        spread_price = float(tick.ask) - float(tick.bid)
        spread_points = int(round(spread_price / point))
        if spread_points < 0:
            spread_points = 0
        return spread_points, None

    def spread_check(self, info: Any) -> Tuple[bool, Optional[str]]:
        point = float(getattr(info, "point", 0.0))
        if point <= 0:
            return False, "invalid_point"

        spread_points, _ = self.get_live_spread_points(info)
        if spread_points is None:
            spread_points = int(getattr(info, "spread", 0))

        atr_points = self.get_atr_points(point)
        if atr_points is not None and atr_points > 0:
            dynamic_limit = atr_points * float(self.atr_multiplier)
            if spread_points > dynamic_limit:
                return False, f"spread>ATR_limit:{spread_points}>{dynamic_limit:.1f}"
            return True, None

        if spread_points > int(self.max_spread_points):
            return False, f"spread_too_high:{spread_points}"
        return True, None

    # -----------------------------
    # Stops / Levels (Directional + Distance)
    # -----------------------------

    def stops_check(self, direction: str, info: Any, exec_price: float, sl: float, tp: float) -> Tuple[bool, Optional[str]]:
        point = float(getattr(info, "point", 0.0))
        digits = int(getattr(info, "digits", 2))
        stops_level_points = int(getattr(info, "trade_stops_level", 0))
        stop_level_price = float(stops_level_points) * point

        if point <= 0:
            return False, "invalid_point"

        if not (self._is_number(exec_price) and self._is_number(sl) and self._is_number(tp)):
            return False, "invalid_plan_numbers"

        px = self._round_to_digits(float(exec_price), digits)
        slx = self._round_to_digits(float(sl), digits)
        tpx = self._round_to_digits(float(tp), digits)

        if slx <= 0 or tpx <= 0:
            return False, "sl_tp_must_be_positive"

        # Directional stop-side validation (KEY FIX for retcode=10016)
        if direction == "BUY":
            if not (slx < px and tpx > px):
                return False, "invalid_stops_side"
        elif direction == "SELL":
            if not (slx > px and tpx < px):
                return False, "invalid_stops_side"

        # Distance validation
        if abs(px - slx) < stop_level_price:
            return False, "sl_too_close"
        if abs(tpx - px) < stop_level_price:
            return False, "tp_too_close"

        return True, None

    # -----------------------------
    # SL/TP Post-Trade Enforcement (P0)
    # -----------------------------

    def _position_has_sltp(self, p: Any) -> bool:
        slv = float(getattr(p, "sl", 0.0) or 0.0)
        tpv = float(getattr(p, "tp", 0.0) or 0.0)
        return (slv > 0.0) and (tpv > 0.0)

    def _find_latest_our_position(self, direction: str) -> Optional[Any]:
        positions = mt5.positions_get(symbol=self.symbol)
        if not positions:
            return None

        candidates = []
        for p in positions:
            if getattr(p, "magic", None) != self.magic:
                continue
            if direction == "BUY" and p.type != 0:
                continue
            if direction == "SELL" and p.type != 1:
                continue
            candidates.append(p)

        if not candidates:
            return None

        candidates.sort(key=lambda x: getattr(x, "time", 0), reverse=True)
        return candidates[0]

    def _sltp_modify(self, position_ticket: int, sl: float, tp: float) -> Tuple[bool, str]:
        req = {
            "action": mt5.TRADE_ACTION_SLTP,
            "position": int(position_ticket),
            "sl": float(sl),
            "tp": float(tp),
            "magic": int(self.magic),
            "comment": "HIM_SLTP_FALLBACK",
        }
        res = mt5.order_send(req)
        if res is None:
            return False, "sltp_modify_none"
        if res.retcode != mt5.TRADE_RETCODE_DONE:
            return False, f"sltp_modify_fail:{res.retcode}"
        return True, "sltp_modified"

    def enforce_sltp_after_send(self, direction: str, sl: float, tp: float) -> Tuple[bool, str, Optional[int]]:
        deadline = self._now() + self.sltp_verify_timeout_sec

        position = None
        while self._now() < deadline:
            position = self._find_latest_our_position(direction)
            if position is not None:
                break
            time.sleep(self.sltp_verify_retry_interval_sec)

        if position is None:
            return False, "position_not_found_after_send", None

        pos_ticket = int(getattr(position, "ticket", 0) or 0)
        if pos_ticket <= 0:
            return False, "invalid_position_ticket", None

        if self._position_has_sltp(position):
            return True, "sltp_ok", pos_ticket

        ok, msg = self._sltp_modify(pos_ticket, sl, tp)
        if not ok:
            return False, msg, pos_ticket

        deadline2 = self._now() + self.sltp_verify_timeout_sec
        while self._now() < deadline2:
            pos2 = mt5.positions_get(ticket=pos_ticket)
            if pos2 and self._position_has_sltp(pos2[0]):
                return True, "sltp_enforced", pos_ticket
            time.sleep(self.sltp_verify_retry_interval_sec)

        return False, "sltp_verify_failed_after_modify", pos_ticket

    # -----------------------------
    # Execution
    # -----------------------------

    def execute(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        ts = self._now()

        request_id = signal.get("request_id", None)
        if not isinstance(request_id, str) or not request_id.strip():
            out = {"status": "SKIP", "reason": "missing_request_id"}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "status": out["status"], "reason": out["reason"]})
            return out
        request_id = request_id.strip()

        if self._dedup_is_done(request_id):
            out = {"status": "SKIP", "reason": "duplicate_request_id", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out

        ok_ai, ai_reason = self.ai_confirm_check(signal)
        if not ok_ai:
            out = {"status": "SKIP", "reason": ai_reason, "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "ai_confirm": signal.get("ai_confirm", None)})
            return out

        direction = str(signal.get("decision", "")).upper()
        if direction not in ("BUY", "SELL"):
            out = {"status": "SKIP", "reason": "no_trade_signal", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "decision": direction})
            return out

        plan = signal.get("plan", {}) if isinstance(signal.get("plan", {}), dict) else {}
        entry = plan.get("entry", None)
        sl = plan.get("sl", None)
        tp = plan.get("tp", None)

        ok, info_or_reason = self.symbol_check()
        if not ok:
            out = {"status": "SKIP", "reason": str(info_or_reason), "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out
        info = info_or_reason

        ok, reason, closed_n = self.adaptive_reverse_opposite(direction, signal)
        if not ok:
            out = {"status": "SKIP", "reason": reason or "opposite_position_blocked", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "decision": direction})
            return out
        if closed_n > 0:
            self._append_jsonl({
                "ts": ts,
                "version": VERSION,
                "symbol": self.symbol,
                "request_id": request_id,
                "status": "INFO",
                "reason": "adaptive_reverse_closed_opposite",
                "direction": direction,
                "closed_positions": int(closed_n),
            })

        ok, reason = self.spread_check(info)
        if not ok:
            out = {"status": "SKIP", "reason": reason or "spread_blocked", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out

        ok, reason = self.cooldown_check()
        if not ok:
            out = {"status": "SKIP", "reason": reason or "cooldown_blocked", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out

        ok, reason = self.pending_orders_check()
        if not ok:
            out = {"status": "SKIP", "reason": reason or "pending_blocked", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out

        ok, reason = self.margin_check()
        if not ok:
            out = {"status": "SKIP", "reason": reason or "margin_blocked", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out

        if not (self._is_number(entry) and self._is_number(sl) and self._is_number(tp)):
            out = {"status": "SKIP", "reason": "plan_missing_or_invalid", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "plan": plan})
            return out

        sl_f = float(sl)
        tp_f = float(tp)
        if sl_f <= 0 or tp_f <= 0:
            out = {"status": "SKIP", "reason": "plan_sl_tp_must_be_positive", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "plan": plan})
            return out

        tick = mt5.symbol_info_tick(self.symbol)
        if tick is None:
            out = {"status": "SKIP", "reason": "tick_error", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out

        digits = int(getattr(info, "digits", 2))
        point = float(getattr(info, "point", 0.0))
        if point <= 0:
            out = {"status": "SKIP", "reason": "invalid_point", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            return out

        if direction == "BUY":
            order_type = mt5.ORDER_TYPE_BUY
            price = float(tick.ask)
        else:
            order_type = mt5.ORDER_TYPE_SELL
            price = float(tick.bid)

        price = self._round_to_digits(price, digits)
        sl_f = self._round_to_digits(sl_f, digits)
        tp_f = self._round_to_digits(tp_f, digits)

        ok, reason = self.adaptive_position_check(direction=direction, order_type=order_type, exec_price=price, info=info, signal=signal)
        if not ok:
            out = {"status": "SKIP", "reason": reason or "position_policy_blocked", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "price": price})
            return out

        ok, reason = self.stops_check(direction, info, price, sl_f, tp_f)
        if not ok:
            out = {"status": "SKIP", "reason": reason or "stops_blocked", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "price": price, "sl": sl_f, "tp": tp_f})
            return out

        spread_points, _ = self.get_live_spread_points(info)
        if spread_points is None:
            spread_points = int(getattr(info, "spread", 0))
        deviation = max(self.deviation_min, int(round(spread_points * self.deviation_spread_mult)))

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "volume": float(self.lot),
            "type": order_type,
            "price": price,
            "sl": sl_f,
            "tp": tp_f,
            "deviation": int(deviation),
            "magic": int(self.magic),
            "comment": "HIM_MT5_EXECUTOR",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }

        result = mt5.order_send(request)
        if result is None:
            out = {"status": "SKIP", "reason": "order_send_none", "request_id": request_id}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            self._tg_send(
                self._format_trade_alert(
                    status=out["status"],
                    direction=direction,
                    request_id=request_id,
                    price=price,
                    sl=sl_f,
                    tp=tp_f,
                    position_ticket=None,
                    ai_confirm=signal.get("ai_confirm", None),
                    extra="reason=order_send_none",
                ),
                "error",
            )
            return out

        if result.retcode != mt5.TRADE_RETCODE_DONE:
            out = {"status": "SKIP", "reason": f"order_fail:{result.retcode}", "request_id": request_id, "mt5_comment": getattr(result, "comment", "")}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out})
            self._tg_send(
                self._format_trade_alert(
                    status=out["status"],
                    direction=direction,
                    request_id=request_id,
                    price=price,
                    sl=sl_f,
                    tp=tp_f,
                    position_ticket=None,
                    ai_confirm=signal.get("ai_confirm", None),
                    extra=f"reason={out['reason']}\nmt5_comment={out.get('mt5_comment','')}",
                ),
                "error",
            )
            return out

        self.last_trade_time = self._now()

        ok_sltp, sltp_reason, pos_ticket = self.enforce_sltp_after_send(direction, sl_f, tp_f)
        if not ok_sltp:
            out = {"status": "ORDER_SENT_BUT_UNSAFE", "request_id": request_id, "price": price, "position_ticket": pos_ticket, "reason": sltp_reason}
            self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "direction": direction, "ai_confirm": signal.get("ai_confirm", None), "plan": plan})
            self._tg_send(
                self._format_trade_alert(
                    status=out["status"],
                    direction=direction,
                    request_id=request_id,
                    price=price,
                    sl=sl_f,
                    tp=tp_f,
                    position_ticket=pos_ticket,
                    ai_confirm=signal.get("ai_confirm", None),
                    extra=f"sltp={sltp_reason}",
                ),
                "trade",
            )
            return out

        # ── MTF Cascade Exit Registration (Phase 3.1) ──
        try:
            _him_v3 = _load_him_v3_cfg(self.config_path)
            if _him_v3.get("cascade_exit", {}).get("enabled", False):
                from mtf_cascade_exit import PositionCtx, get_cascade_system

                cascade_sys = get_cascade_system(self.symbol, self.magic)
                atr_val = float((signal.get("price") or {}).get("atr") or 1.0)

                cascade_ticket = int(pos_ticket or getattr(result, "order", 0) or 0)
                if cascade_ticket > 0:
                    pos_ctx = PositionCtx(
                        ticket=cascade_ticket,
                        direction=direction,
                        entry_price=float(price),
                        atr_at_entry=atr_val,
                        volume=float(self.lot),
                    )
                    cascade_sys.register(pos_ctx)
                    self._append_jsonl({
                        "ts": ts,
                        "version": VERSION,
                        "symbol": self.symbol,
                        "request_id": request_id,
                        "status": "INFO",
                        "reason": "CASCADE_REGISTERED",
                        "ticket": cascade_ticket,
                        "direction": direction,
                        "entry_price": float(price),
                        "atr_at_entry": atr_val,
                        "volume": float(self.lot),
                    })
        except Exception:
            pass

        out = {"status": "ORDER_SENT", "request_id": request_id, "price": price, "position_ticket": pos_ticket, "sltp": sltp_reason}
        self._append_jsonl({"ts": ts, "version": VERSION, "symbol": self.symbol, "request_id": request_id, **out, "direction": direction, "ai_confirm": signal.get("ai_confirm", None), "plan": plan})
        self._dedup_mark_done(request_id, {"ts": ts, "status": out["status"], "position_ticket": pos_ticket, "price": price, "sltp": sltp_reason})
        self._tg_send(
            self._format_trade_alert(
                status=out["status"],
                direction=direction,
                request_id=request_id,
                price=price,
                sl=sl_f,
                tp=tp_f,
                position_ticket=pos_ticket,
                ai_confirm=signal.get("ai_confirm", None),
                extra=f"sltp={sltp_reason}",
            ),
            "trade",
        )
        return out

    def skip(self, reason: str) -> Dict[str, Any]:
        return {"status": "SKIP", "reason": reason}


if __name__ == "__main__":
    print(f"[MT5Executor] file={os.path.abspath(__file__)} version={VERSION}")

    executor = MT5Executor()

    example_signal = {
        "request_id": "TEST_DENY_001",
        "decision": "BUY",
        "plan": {"entry": 0, "sl": 5154.1, "tp": 5175.1},
        "ai_confirm": {"approved": False, "reason": "deny_test", "confidence": 0.0},
    }

    print("[MT5Executor] example_signal =", json.dumps(example_signal, ensure_ascii=False))
    result = executor.execute(example_signal)
    print(json.dumps(result, indent=2, ensure_ascii=False))
