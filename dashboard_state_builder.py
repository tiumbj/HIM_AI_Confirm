"""
ชื่อโค้ด: HIM Dashboard State Builder
ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\dashboard_state_builder.py
คำสั่งรัน: python dashboard_state_builder.py
เวอร์ชัน: v1.3.0
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import MetaTrader5 as mt5
except Exception:
    mt5 = None


VERSION = "v1.3.0"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=False)
except Exception:
    pass

RUNTIME_DIR = os.path.join(PROJECT_ROOT, "runtime")
STATE_PATH = os.environ.get("DASH_STATE_PATH") or os.path.join(RUNTIME_DIR, "dashboard_state.json")

DEFAULT_CONFIG_PATH = os.environ.get("HIM_CONFIG") or os.environ.get("HIM_CONFIG_PATH") or "config.json"
DAILY_REFRESH_SECONDS = float(os.environ.get("DASH_DAILY_REFRESH_SECONDS", "30") or "30")

LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
CASCADE_LOG = os.path.join(LOG_DIR, "cascade_exit.jsonl")
EXEC_LOG = os.path.join(LOG_DIR, "execution_orders.jsonl")
MENTOR_LOG = os.path.join(LOG_DIR, "mentor_executor.jsonl")
API_LOG = os.path.join(LOG_DIR, "api_server.jsonl")
RISK_LOG = os.path.join(LOG_DIR, "risk_guard.jsonl")


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(float(v))
    except Exception:
        return None


def _load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def _tail_jsonl(path: str, max_lines: int = 50) -> List[Dict[str, Any]]:
    if max_lines <= 0:
        return []
    if not os.path.exists(path):
        return []
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            block = 64 * 1024
            data = b""
            pos = size
            while pos > 0 and data.count(b"\n") <= max_lines:
                step = block if pos >= block else pos
                pos -= step
                f.seek(pos)
                data = f.read(step) + data
        lines = data.splitlines()[-max_lines:]
        out: List[Dict[str, Any]] = []
        for raw in lines:
            s = raw.decode("utf-8", errors="replace").strip()
            if not s:
                continue
            try:
                j = json.loads(s)
            except Exception:
                continue
            if isinstance(j, dict):
                out.append(j)
        return out
    except Exception:
        return []


def _mt5_snapshot(symbol: str, magic: int) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    t0 = time.time()
    if mt5 is None:
        return {"ok": False, "reason": "mt5_module_missing", "latency_ms": int((time.time() - t0) * 1000)}, []
    if not mt5.initialize():
        try:
            return {"ok": False, "reason": f"mt5_initialize_failed:{mt5.last_error()}", "latency_ms": int((time.time() - t0) * 1000)}, []
        except Exception:
            return {"ok": False, "reason": "mt5_initialize_failed", "latency_ms": int((time.time() - t0) * 1000)}, []

    try:
        term = mt5.terminal_info()
        acc = mt5.account_info()
        if acc is None:
            return {"ok": False, "reason": "account_info_none", "latency_ms": int((time.time() - t0) * 1000)}, []

        info = mt5.symbol_info(symbol)
        point = float(getattr(info, "point", 0.0) or 0.0) if info is not None else 0.0
        digits = int(getattr(info, "digits", 0) or 0) if info is not None else 0

        tick = mt5.symbol_info_tick(symbol)
        bid = float(getattr(tick, "bid", 0.0) or 0.0) if tick is not None else 0.0
        ask = float(getattr(tick, "ask", 0.0) or 0.0) if tick is not None else 0.0
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
        spread_points = int(round((ask - bid) / point)) if (point > 0 and ask > 0 and bid > 0) else None

        positions = mt5.positions_get(symbol=symbol)
        pos_list = list(positions) if positions else []
        ours: List[Dict[str, Any]] = []
        for p in pos_list:
            if magic and int(getattr(p, "magic", 0) or 0) != int(magic):
                continue
            t = int(getattr(p, "ticket", 0) or 0)
            ty = int(getattr(p, "type", 0) or 0)
            ours.append(
                {
                    "ticket": t,
                    "type": "BUY" if ty == 0 else "SELL",
                    "volume": float(getattr(p, "volume", 0.0) or 0.0),
                    "price_open": float(getattr(p, "price_open", 0.0) or 0.0),
                    "sl": float(getattr(p, "sl", 0.0) or 0.0),
                    "tp": float(getattr(p, "tp", 0.0) or 0.0),
                    "profit": float(getattr(p, "profit", 0.0) or 0.0),
                    "time": int(getattr(p, "time", 0) or 0),
                }
            )

        snap = {
            "ok": True,
            "latency_ms": int((time.time() - t0) * 1000),
            "connected": bool(getattr(term, "connected", False)) if term is not None else None,
            "trade_allowed": bool(getattr(term, "trade_allowed", False)) if term is not None else None,
            "login": int(getattr(acc, "login", 0) or 0),
            "server": str(getattr(acc, "server", "") or ""),
            "balance": float(getattr(acc, "balance", 0.0) or 0.0),
            "equity": float(getattr(acc, "equity", 0.0) or 0.0),
            "profit": float(getattr(acc, "profit", 0.0) or 0.0),
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "point": point,
            "digits": digits,
            "spread_points": spread_points,
        }
        return snap, ours
    finally:
        try:
            mt5.shutdown()
        except Exception:
            pass


def _mt5_daily_report(symbol: str, magic: int) -> Dict[str, Any]:
    if mt5 is None:
        return {"ok": False, "reason": "mt5_module_missing"}
    if not mt5.initialize():
        try:
            return {"ok": False, "reason": f"mt5_initialize_failed:{mt5.last_error()}"}
        except Exception:
            return {"ok": False, "reason": "mt5_initialize_failed"}

    try:
        now = datetime.now().astimezone()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        deals = mt5.history_deals_get(start, now)
        deal_list = list(deals) if deals else []

        wins = 0
        losses = 0
        total = 0
        net = 0.0

        for d in deal_list:
            if symbol and str(getattr(d, "symbol", "") or "") != str(symbol):
                continue
            if magic and int(getattr(d, "magic", 0) or 0) != int(magic):
                continue

            try:
                entry = int(getattr(d, "entry", -1))
            except Exception:
                entry = -1
            if entry != getattr(mt5, "DEAL_ENTRY_OUT", 1):
                continue

            profit = float(getattr(d, "profit", 0.0) or 0.0)
            commission = float(getattr(d, "commission", 0.0) or 0.0)
            swap = float(getattr(d, "swap", 0.0) or 0.0)
            pnl = profit + commission + swap

            total += 1
            net += pnl
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1

        win_rate = (float(wins) / float(total) * 100.0) if total > 0 else 0.0
        return {
            "ok": True,
            "date": start.strftime("%Y-%m-%d"),
            "trades": int(total),
            "wins": int(wins),
            "losses": int(losses),
            "win_rate": float(round(win_rate, 2)),
            "net_pnl": float(round(net, 2)),
        }
    except Exception as e:
        return {"ok": False, "reason": f"daily_report_error:{type(e).__name__}"}
    finally:
        try:
            mt5.shutdown()
        except Exception:
            pass


def _ai_llm_status(cfg: Dict[str, Any]) -> Dict[str, Any]:
    ai = cfg.get("ai_confirm", {}) if isinstance(cfg.get("ai_confirm", {}), dict) else {}
    enabled = bool(ai.get("use_llm") is True)
    provider = str(ai.get("provider") or "").strip()
    model = str(ai.get("llm_model") or "").strip()

    deepseek_key_set = bool(os.environ.get("DEEPSEEK_API_KEY"))
    openai_key_set = bool(os.environ.get("OPENAI_API_KEY"))
    generic_key_set = bool(os.environ.get("AI_API_KEY"))
    key_set = deepseek_key_set or openai_key_set or generic_key_set

    mode = "DISABLED"
    if enabled and key_set:
        mode = "ENABLED"
    elif enabled and not key_set:
        mode = "NO_KEY"

    return {
        "enabled": enabled,
        "mode": mode,
        "provider": provider or None,
        "model": model or None,
        "key_set": bool(key_set),
    }


def _market_view(
    *,
    system: str,
    spread_points: Optional[int],
    max_spread_points: Optional[int],
    cascade_enabled: bool,
    exit_risk: str,
) -> Dict[str, Any]:
    state = "UNKNOWN"
    lines: List[str] = []

    if system in ("DEGRADED", "STALE"):
        state = "NO_SIGNAL"
        lines.append("Data stale/degraded: do not trust entries.")
    else:
        state = "NORMAL"
        if spread_points is not None and max_spread_points is not None:
            if spread_points > max_spread_points:
                state = "HIGH_SPREAD"
                lines.append("Spread high: execution likely blocked.")
        if cascade_enabled:
            lines.append("Exit monitoring: ENABLED (cascade).")
        else:
            lines.append("Exit monitoring: DISABLED.")

        if exit_risk == "EXIT_TRIGGERED":
            state = "REVERSAL_EXIT"
            lines.append("Reversal detected: EXIT triggered.")
        elif exit_risk == "ELEVATED":
            if state == "NORMAL":
                state = "REVERSAL_RISK"
            lines.append("Reversal risk elevated (early warnings).")

    return {"state": state, "lines": lines[:4]}


def _derive_system_status(now_ts: float, last_write_ts: Optional[float], stale_sec: float) -> str:
    if last_write_ts is None:
        return "DEGRADED"
    age = now_ts - last_write_ts
    if age > stale_sec:
        return "STALE"
    return "NOMINAL"


def _infer_exit_risk(last_cascade: Optional[Dict[str, Any]]) -> str:
    if not isinstance(last_cascade, dict):
        return "UNKNOWN"
    ev = str(last_cascade.get("event") or "")
    action = str(last_cascade.get("action") or "")
    if ev == "CASCADE_EXIT":
        return "EXIT_TRIGGERED"
    if ev == "CASCADE_WARN" or action == "EARLY_WARN":
        return "ELEVATED"
    return "NORMAL"


def build_state(prev_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    cfg_path = DEFAULT_CONFIG_PATH
    if not os.path.isabs(cfg_path):
        cfg_path = os.path.join(PROJECT_ROOT, cfg_path)

    cfg = _load_json(cfg_path)

    symbol = str(cfg.get("symbol") or "GOLD").strip() or "GOLD"
    exec_cfg = cfg.get("execution", {}) if isinstance(cfg.get("execution", {}), dict) else {}
    magic = _safe_int(exec_cfg.get("magic")) or 0
    max_spread_points = _safe_int(exec_cfg.get("max_spread_points"))

    him_v3 = cfg.get("him_v3", {}) if isinstance(cfg.get("him_v3", {}), dict) else {}
    cascade_cfg = him_v3.get("cascade_exit", {}) if isinstance(him_v3.get("cascade_exit", {}), dict) else {}
    cascade_enabled = bool(cascade_cfg.get("enabled", False))

    mt5_snap, positions = _mt5_snapshot(symbol, magic)

    cascade_events = _tail_jsonl(CASCADE_LOG, max_lines=40)
    exec_events = _tail_jsonl(EXEC_LOG, max_lines=80)
    mentor_events = _tail_jsonl(MENTOR_LOG, max_lines=20)
    api_events = _tail_jsonl(API_LOG, max_lines=20)
    risk_events = _tail_jsonl(RISK_LOG, max_lines=20)

    last_cascade = cascade_events[-1] if cascade_events else None
    last_exec = exec_events[-1] if exec_events else None

    now_ts = time.time()
    last_write_ts = _safe_float((prev_state or {}).get("_meta", {}).get("last_write_ts"))
    system_status = _derive_system_status(now_ts, last_write_ts, stale_sec=8.0)

    action = "WAIT"
    reason = ""
    if isinstance(last_cascade, dict) and str(last_cascade.get("event", "")).startswith("CASCADE_"):
        action = str(last_cascade.get("event"))
        reason = str(last_cascade.get("reason") or last_cascade.get("action") or "")
    elif isinstance(last_exec, dict):
        action = str(last_exec.get("status") or "WAIT")
        reason = str(last_exec.get("reason") or "")

    event_lines: List[str] = []
    for src, items in (
        ("cascade", cascade_events[-10:]),
        ("exec", exec_events[-6:]),
        ("risk", risk_events[-6:]),
        ("api", api_events[-4:]),
        ("mentor", mentor_events[-4:]),
    ):
        for it in items:
            if not isinstance(it, dict):
                continue
            ts = it.get("ts") or it.get("ts_utc") or it.get("datetime") or ""
            ev = it.get("event") or it.get("status") or ""
            msg = it.get("reason") or it.get("note") or ""
            ticket = it.get("ticket") or it.get("position_ticket") or ""
            line = f"[{src}] {ts} {ev} ticket={ticket} {msg}"
            event_lines.append(line[:240])
    event_lines = event_lines[-40:]

    spread_points = mt5_snap.get("spread_points") if isinstance(mt5_snap, dict) else None
    if spread_points is None and isinstance(mt5_snap, dict):
        point = _safe_float(mt5_snap.get("point")) or 0.0
        ask = _safe_float(mt5_snap.get("ask")) or 0.0
        bid = _safe_float(mt5_snap.get("bid")) or 0.0
        if point > 0 and ask > 0 and bid > 0:
            spread_points = int(round((ask - bid) / point))

    exit_risk = _infer_exit_risk(last_cascade if isinstance(last_cascade, dict) else None)
    prev_daily = None
    prev_daily_ts = None
    if isinstance(prev_state, dict):
        prev_daily = prev_state.get("daily_report") if isinstance(prev_state.get("daily_report"), dict) else None
        try:
            prev_daily_ts = float((prev_state.get("_meta") or {}).get("daily_report_ts")) if isinstance(prev_state.get("_meta"), dict) else None
        except Exception:
            prev_daily_ts = None

    if prev_daily is not None and prev_daily_ts is not None and (now_ts - prev_daily_ts) < max(5.0, DAILY_REFRESH_SECONDS):
        daily_report = dict(prev_daily)
        daily_report_ts = float(prev_daily_ts)
    else:
        daily_report = _mt5_daily_report(symbol, magic)
        daily_report_ts = float(now_ts)
    mt5_status = {
        "ok": bool(mt5_snap.get("ok")),
        "connected": mt5_snap.get("connected"),
        "trade_allowed": mt5_snap.get("trade_allowed"),
        "reason": mt5_snap.get("reason"),
    }
    ai_llm = _ai_llm_status(cfg)
    market_view = _market_view(
        system=str(system_status if mt5_snap.get("ok") else "DEGRADED").upper(),
        spread_points=_safe_int(spread_points),
        max_spread_points=max_spread_points,
        cascade_enabled=cascade_enabled,
        exit_risk=str(exit_risk),
    )

    return {
        "_meta": {
            "version": VERSION,
            "generated_utc": _utc_iso(),
            "last_write_ts": now_ts,
            "daily_report_ts": daily_report_ts,
            "config_path": cfg_path,
        },
        "header": {
            "system": system_status if mt5_snap.get("ok") else "DEGRADED",
            "mode": "AUTO",
            "symbol": symbol,
            "time_utc": _utc_iso(),
            "loop": "RUNNING",
            "broker": mt5_snap.get("server") if mt5_snap.get("ok") else None,
            "position": len(positions),
            "magic": magic,
            "cascade_exit_enabled": cascade_enabled,
            "mt5_latency_ms": mt5_snap.get("latency_ms"),
        },
        "data_ingest": {
            "feed": "MT5" if mt5_snap.get("ok") else "MISSING",
            "ticks": "OK" if (mt5_snap.get("ok") and mt5_snap.get("bid") and mt5_snap.get("ask")) else "NO_DATA",
            "spread_points": spread_points,
        },
        "mt5_status": mt5_status,
        "ai_llm_status": ai_llm,
        "market_view": market_view,
        "live_analytics": {
            "bid": mt5_snap.get("bid"),
            "ask": mt5_snap.get("ask"),
            "price": mt5_snap.get("mid"),
            "equity": mt5_snap.get("equity"),
            "balance": mt5_snap.get("balance"),
            "profit": mt5_snap.get("profit"),
            "digits": mt5_snap.get("digits"),
            "point": mt5_snap.get("point"),
        },
        "execution_guard": {
            "action": action,
            "reason": reason,
            "last_exec_status": last_exec.get("status") if isinstance(last_exec, dict) else None,
            "last_exec_reason": last_exec.get("reason") if isinstance(last_exec, dict) else None,
        },
        "position_monitor": {
            "status": "OPEN" if positions else "FLAT",
            "positions": positions,
            "last_cascade_event": last_cascade,
            "exit_risk": exit_risk,
        },
        "daily_report": daily_report,
        "event_stream": {
            "lines": event_lines,
        },
        "raw": {
            "mt5": mt5_snap,
        },
    }


def main() -> int:
    interval_sec = float(os.environ.get("DASH_STATE_INTERVAL_SEC", "1.0") or "1.0")
    if interval_sec <= 0:
        interval_sec = 1.0

    prev: Optional[Dict[str, Any]] = None
    while True:
        try:
            state = build_state(prev)
            _atomic_write_json(STATE_PATH, state)
            prev = state
        except Exception:
            pass
        time.sleep(interval_sec)


if __name__ == "__main__":
    raise SystemExit(main())
