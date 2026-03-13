"""
Force Execution Test - MT5 Commissioning Tool
Code Name: force_execution_test.py
Version: 1.0.1 (2026-02-27)

Changelog:
- 1.0.1:
  - FIX: Auto-negotiate supported MT5 filling mode (RETURN/IOC/FOK) using order_check.
    Prevents retcode=10030 "Unsupported filling mode".
  - IMPROVE: Log which filling mode is selected.
  - SAFETY: Keep existing gates (spread, stop/freeze, position guard, cooldown/dedupe, Sunday block).
- 1.0.0:
  - Initial release.

Rules:
- Commissioning ONLY: bypasses strategy gates by design.
- Use small lot (default 0.01) for first live test.
- If enable_execution=false => dry-run only (never send order).
- Skip Sundays (system rule).

"""

from __future__ import annotations

import copy
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Tuple

import MetaTrader5 as mt5
from zoneinfo import ZoneInfo

from config_resolver import resolve_effective_config
from telegram_notifier import TelegramNotifier


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("HIM_FORCE")


# -----------------------------
# Utilities
# -----------------------------
def _safe_float(v: Any, default: float) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return int(default)
        return int(v)
    except Exception:
        return int(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _is_market_open_basic(tz_name: str = "Asia/Bangkok") -> Tuple[bool, str]:
    """
    Basic guard (not a holiday calendar):
    - Skip Sundays
    """
    try:
        now = datetime.now(ZoneInfo(tz_name))
        if now.weekday() == 6:
            return False, "sunday_market_closed"
        return True, "ok"
    except Exception:
        # Fail-safe: block
        return False, "timezone_error_blocked"


def _ensure_mt5() -> None:
    if mt5.terminal_info() is not None:
        return
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize failed: {mt5.last_error()}")
    logger.info("MT5 Connected")


def _get_info(symbol: str):
    info = mt5.symbol_info(symbol)
    if info is None:
        raise RuntimeError(f"Symbol not found: {symbol}")
    if not info.visible:
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"Cannot select symbol: {symbol} last_error={mt5.last_error()}")
    return info


def _get_tick(symbol: str):
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        raise RuntimeError(f"Cannot get tick for symbol: {symbol} last_error={mt5.last_error()}")
    return tick


def _spread_points(symbol: str) -> float:
    info = _get_info(symbol)
    tick = _get_tick(symbol)
    if float(info.point) <= 0:
        return float("inf")
    return float((tick.ask - tick.bid) / float(info.point))


def _positions_exist(symbol: str) -> bool:
    pos = mt5.positions_get(symbol=symbol)
    # Conservative: if MT5 cannot return positions, block to avoid duplicate risk
    if pos is None:
        return True
    return bool(len(pos) > 0)


def _validate_stops_levels(symbol: str, direction: str, entry: float, sl: float, tp: float) -> Tuple[bool, str]:
    info = _get_info(symbol)
    point = float(info.point) if info.point else 0.0
    if point <= 0:
        return False, "invalid_symbol_point"

    stops_level_points = int(getattr(info, "trade_stops_level", 0) or 0)
    freeze_level_points = int(getattr(info, "trade_freeze_level", 0) or 0)
    min_dist = float(stops_level_points) * point

    if direction == "BUY":
        if not (sl < entry < tp):
            return False, "sanity_failed_buy"
        if (entry - sl) < min_dist:
            return False, f"stop_level_failed_sl dist={entry - sl:.5f} min={min_dist:.5f}"
        if (tp - entry) < min_dist:
            return False, f"stop_level_failed_tp dist={tp - entry:.5f} min={min_dist:.5f}"
    elif direction == "SELL":
        if not (tp < entry < sl):
            return False, "sanity_failed_sell"
        if (sl - entry) < min_dist:
            return False, f"stop_level_failed_sl dist={sl - entry:.5f} min={min_dist:.5f}"
        if (entry - tp) < min_dist:
            return False, f"stop_level_failed_tp dist={entry - tp:.5f} min={min_dist:.5f}"
    else:
        return False, "invalid_direction"

    # Freeze-level: conservative check
    if freeze_level_points > 0:
        tick = _get_tick(symbol)
        freeze_dist = float(freeze_level_points) * point
        cur = float(tick.ask if direction == "BUY" else tick.bid)
        if abs(cur - entry) <= freeze_dist:
            return False, f"freeze_level_too_close cur-entry={abs(cur-entry):.5f} freeze={freeze_dist:.5f}"

    return True, "ok"


def _filling_name(fill: int) -> str:
    # Common MT5 mapping:
    # 0=FOK, 1=IOC, 2=RETURN (but keep generic)
    if fill == mt5.ORDER_FILLING_FOK:
        return "FOK"
    if fill == mt5.ORDER_FILLING_IOC:
        return "IOC"
    if fill == mt5.ORDER_FILLING_RETURN:
        return "RETURN"
    return str(fill)


def _order_check_ok(retcode: int) -> bool:
    # MetaTrader5 order_check retcodes vary; accept common "ok" forms
    return retcode in (
        0,
        int(mt5.TRADE_RETCODE_DONE),
        int(mt5.TRADE_RETCODE_PLACED),
    )


def _negotiate_filling_mode(request: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Try RETURN/IOC/FOK using order_check, pick first supported/ok.
    Handles retcode=10030 Unsupported filling mode.
    """
    candidates: List[int] = [
        int(mt5.ORDER_FILLING_RETURN),
        int(mt5.ORDER_FILLING_IOC),
        int(mt5.ORDER_FILLING_FOK),
    ]

    last_fail: Dict[str, Any] = {}
    for fill in candidates:
        req = copy.deepcopy(request)
        req["type_filling"] = int(fill)

        chk = mt5.order_check(req)
        if chk is None:
            last_fail = {
                "ok": False,
                "error": "order_check returned None",
                "last_error": mt5.last_error(),
                "request": req,
            }
            continue

        chk_ret = int(getattr(chk, "retcode", -1))
        chk_comm = str(getattr(chk, "comment", ""))

        if _order_check_ok(chk_ret):
            logger.info(f"FILLING_SELECTED: {_filling_name(fill)} (retcode={chk_ret})")
            return True, {"ok": True, "request": req, "check_retcode": chk_ret, "check_comment": chk_comm}

        # If unsupported filling, just try next
        if chk_ret == 10030:
            logger.warning(f"FILLING_UNSUPPORTED: {_filling_name(fill)} (retcode=10030)")
            last_fail = {
                "ok": False,
                "error": "order_check_failed",
                "retcode": chk_ret,
                "comment": chk_comm,
                "request": req,
            }
            continue

        # Other errors: still try next (conservative), but record
        last_fail = {
            "ok": False,
            "error": "order_check_failed",
            "retcode": chk_ret,
            "comment": chk_comm,
            "request": req,
        }

    return False, last_fail


@dataclass
class ForceDecision:
    side: str
    entry: float
    sl: float
    tp: float
    rr_target: float
    sl_dist_points: float
    stops_level_points: int


def _build_force_decision(cfg_eff: Dict[str, Any], symbol: str) -> ForceDecision:
    test_cfg = (cfg_eff.get("execution_test") or {}) if isinstance(cfg_eff, dict) else {}

    side = str(test_cfg.get("side", "BUY")).upper().strip()
    if side not in ("BUY", "SELL"):
        side = "BUY"

    rr_target = _safe_float(test_cfg.get("rr_target", 2.0), 2.0)
    rr_target = _clamp(rr_target, 1.0, 5.0)

    sl_dist_points_cfg = _safe_float(test_cfg.get("sl_dist_points", 300), 300.0)
    stop_buffer_points = _safe_float(test_cfg.get("stop_buffer_points", 10), 10.0)
    min_stop_mult = _safe_float(test_cfg.get("min_stop_mult", 2.0), 2.0)
    min_stop_mult = _clamp(min_stop_mult, 1.0, 10.0)

    info = _get_info(symbol)
    point = float(info.point) if info.point else 0.0
    if point <= 0:
        raise RuntimeError("invalid_symbol_point")

    tick = _get_tick(symbol)
    entry = float(tick.ask if side == "BUY" else tick.bid)

    stops_level_points = int(getattr(info, "trade_stops_level", 0) or 0)
    min_points = float(stops_level_points) * float(min_stop_mult) + float(stop_buffer_points)

    sl_dist_points = float(max(sl_dist_points_cfg, min_points))
    sl_dist = sl_dist_points * point
    tp_dist = sl_dist * rr_target

    if side == "BUY":
        sl = entry - sl_dist
        tp = entry + tp_dist
    else:
        sl = entry + sl_dist
        tp = entry - tp_dist

    return ForceDecision(
        side=side,
        entry=entry,
        sl=sl,
        tp=tp,
        rr_target=rr_target,
        sl_dist_points=sl_dist_points,
        stops_level_points=stops_level_points,
    )


def _place_market_order(
    symbol: str,
    direction: str,
    lot: float,
    sl: float,
    tp: float,
    cfg_eff: Dict[str, Any],
) -> Dict[str, Any]:
    tick = _get_tick(symbol)
    price = float(tick.ask if direction == "BUY" else tick.bid)

    exec_cfg = (cfg_eff.get("execution") or {}) if isinstance(cfg_eff, dict) else {}
    deviation = _safe_int(exec_cfg.get("deviation", 30), 30)
    magic = _safe_int(exec_cfg.get("magic", 240227), 240227)
    comment = str(exec_cfg.get("comment", "HIM:EXEC"))[:30]

    # If user forced a filling mode (int), respect it.
    # Otherwise, use auto negotiation to avoid retcode=10030.
    forced_filling = exec_cfg.get("type_filling", None)
    forced_filling_int = None
    if isinstance(forced_filling, int):
        forced_filling_int = int(forced_filling)

    order_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL

    base_request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(lot),
        "type": order_type,
        "price": float(price),
        "sl": float(sl),
        "tp": float(tp),
        "deviation": int(deviation),
        "magic": int(magic),
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,
        # type_filling set below
    }

    if forced_filling_int is not None:
        base_request["type_filling"] = forced_filling_int
        chk = mt5.order_check(base_request)
        if chk is None:
            return {"ok": False, "error": "order_check returned None", "last_error": mt5.last_error(), "request": base_request}

        chk_ret = int(getattr(chk, "retcode", -1))
        chk_comm = str(getattr(chk, "comment", ""))
        if not _order_check_ok(chk_ret):
            return {"ok": False, "error": "order_check_failed", "retcode": chk_ret, "comment": chk_comm, "request": base_request}

        result = mt5.order_send(base_request)
        if result is None:
            return {"ok": False, "error": "order_send returned None", "last_error": mt5.last_error(), "request": base_request}

        ok = (int(result.retcode) == int(mt5.TRADE_RETCODE_DONE))
        return {
            "ok": bool(ok),
            "retcode": int(result.retcode),
            "order": int(getattr(result, "order", 0)),
            "deal": int(getattr(result, "deal", 0)),
            "price": float(price),
            "comment": str(getattr(result, "comment", "")),
            "request": base_request,
        }

    # AUTO: negotiate
    ok_fill, payload = _negotiate_filling_mode(base_request)
    if not ok_fill:
        return payload

    request = payload["request"]
    result = mt5.order_send(request)
    if result is None:
        return {"ok": False, "error": "order_send returned None", "last_error": mt5.last_error(), "request": request}

    ok = (int(result.retcode) == int(mt5.TRADE_RETCODE_DONE))
    return {
        "ok": bool(ok),
        "retcode": int(result.retcode),
        "order": int(getattr(result, "order", 0)),
        "deal": int(getattr(result, "deal", 0)),
        "price": float(price),
        "comment": str(getattr(result, "comment", "")),
        "request": request,
    }


def _build_message(symbol: str, decision: ForceDecision, enable_execution: bool, lot: float) -> str:
    mode = "LIVE" if enable_execution else "DRY-RUN"
    return (
        f"HIM FORCE EXECUTION TEST\n"
        f"symbol={symbol} side={decision.side} lot={lot:.2f} mode={mode}\n"
        f"entry={decision.entry:.2f} sl={decision.sl:.2f} tp={decision.tp:.2f} rr_target={decision.rr_target:.2f}\n"
        f"sl_dist_points={decision.sl_dist_points:.1f} stops_level_points={decision.stops_level_points}\n"
        f"NOTE: This bypasses strategy gates. Execution safety gates still apply."
    )


def _load_effective_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f) or {}
    return resolve_effective_config(raw)


def main() -> None:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.json")

    cfg_eff = _load_effective_config(config_path)
    symbol = str(cfg_eff.get("symbol", "GOLD"))
    enable_execution = bool(cfg_eff.get("enable_execution", False))

    exec_cfg = (cfg_eff.get("execution") or {}) if isinstance(cfg_eff, dict) else {}
    max_spread_points = _safe_float(exec_cfg.get("max_spread_points", 60), 60.0)
    block_if_position_exists = bool(exec_cfg.get("block_if_position_exists", True))
    cooldown_sec = _safe_int(exec_cfg.get("cooldown_sec", 30), 30)

    test_cfg = (cfg_eff.get("execution_test") or {}) if isinstance(cfg_eff, dict) else {}
    if not bool(test_cfg.get("enabled", False)):
        logger.info("execution_test.enabled=false -> no action (safe).")
        return

    ok_mkt, why_mkt = _is_market_open_basic()
    if not ok_mkt:
        logger.warning(f"BLOCK: market closed ({why_mkt}).")
        return

    _ensure_mt5()
    _get_info(symbol)  # ensure symbol visible

    # Spread filter
    spread_pts = _spread_points(symbol)
    if spread_pts > max_spread_points:
        logger.warning(f"BLOCK: spread too high {spread_pts:.1f} > {max_spread_points:.1f}")
        return

    # Duplicate protection
    if block_if_position_exists and _positions_exist(symbol):
        logger.warning("BLOCK: position exists (duplicate protection).")
        return

    # Build decision
    decision = _build_force_decision(cfg_eff, symbol)

    # Stops/freeze check
    ok_levels, why_levels = _validate_stops_levels(symbol, decision.side, decision.entry, decision.sl, decision.tp)
    if not ok_levels:
        logger.warning(f"BLOCK: levels invalid: {why_levels}")
        return

    lot = _safe_float(cfg_eff.get("lot", 0.01), 0.01)

    # Execution dedupe/cooldown (local file-based)
    sig = f"FORCE|{symbol}|{decision.side}|{round(decision.entry,2)}|{round(decision.sl,2)}|{round(decision.tp,2)}"
    stamp_path = os.path.join(base_dir, ".force_exec_last_sig.json")
    now_ts = time.time()
    try:
        if os.path.exists(stamp_path):
            with open(stamp_path, "r", encoding="utf-8") as f:
                d = json.load(f) or {}
            last_sig = str(d.get("sig", ""))
            last_ts = _safe_float(d.get("ts", 0.0), 0.0)
            if last_sig == sig and (now_ts - last_ts) < float(cooldown_sec):
                logger.warning("BLOCK: execution dedupe/cooldown.")
                return
    except Exception:
        pass

    msg = _build_message(symbol, decision, enable_execution, lot)

    # Telegram (trade channel)
    tg = TelegramNotifier(config_path=config_path)
    tg.send_text(msg, event_type="trade")

    # Save stamp early (even for dry-run, to prevent spam)
    try:
        with open(stamp_path, "w", encoding="utf-8") as f:
            json.dump({"sig": sig, "ts": now_ts}, f)
    except Exception:
        pass

    if not enable_execution:
        logger.info("DRY-RUN: enable_execution=false, skipping order_send.")
        logger.info(msg)
        return

    result = _place_market_order(symbol, decision.side, lot, decision.sl, decision.tp, cfg_eff=cfg_eff)
    logger.info(f"EXEC_RESULT: ok={result.get('ok')} retcode={result.get('retcode')} comment={result.get('comment')}")
    logger.info(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()