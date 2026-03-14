# ============================================================
# ชื่อโค้ด: HIM Mentor Executor (mentor_executor.py)
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\mentor_executor.py
# คำสั่งรัน: python mentor_executor.py
# เวอร์ชัน: v1.0.3
# ============================================================
"""
mentor_executor.py
Version: v1.0.3
Purpose: Production Orchestrator for HIM
         Signal Source -> AI Final Confirm -> MT5 Executor

========================================================
DESIGN PRINCIPLES (Production)
========================================================
- Confirm-only: AI must NOT modify plan (entry/sl/tp). It only approves/denies.
- Fail-closed: Missing critical fields -> SKIP
- Single execution entry: always call mt5_executor.execute(execution_package)
- Observability: JSONL logging for every cycle
- Minimal assumptions: Signal/AI accessed via HTTP endpoints (configurable)

========================================================
FILES
========================================================
- logs/mentor_executor.jsonl          : orchestrator audit log
- uses mt5_executor.py (v1.3.1+)      : final gate + order_send + dedup/log

========================================================
CONFIG (env overrides)
========================================================
SIGNAL_URL      default: http://127.0.0.1:5000/api/signal_preview
AI_CONFIRM_URL  default: http://127.0.0.1:5000/api/ai_confirm
POLL_INTERVAL   default: 2.0 seconds
DRY_RUN         default: 0 (0/1)
SYMBOL          default: GOLD

Notes:
- You can keep SIGNAL_URL pointing to your engine endpoint.
- You can keep AI_CONFIRM_URL pointing to your AI confirmation service endpoint.

========================================================
CHANGELOG
========================================================
- v1.0.3 (Phase 2.2):
    * Add candle-close trigger mode in loop()
    * Add _load_him_v3_config() helper (reads config.json him_v3 section, fail-safe)
    * Add _run_once_and_print() wrapper for CandleCloseTrigger callback
    * Falls back to legacy poll mode if candle_trigger disabled or import fails
    * loop() logs candle_trigger_mode and candle_trigger_tf at startup
- v1.0.2 (Phase 7):
    * Add synchronous KILL_SWITCH.txt check before execution
    * Use __file__-based path for deterministic kill-switch location
    * Fail-closed: block if file exists or read fails
    * Zero post-breach trades guarantee
- v1.0.1: Initial production orchestrator
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
import json
import uuid
import hashlib
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple, cast
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse


# -----------------------------
# VERSION
# -----------------------------
VERSION = "v1.0.3"

# Phase 7: Deterministic path resolution
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
KILL_SWITCH_PATH = os.path.join(PROJECT_ROOT, "KILL_SWITCH.txt")


# -----------------------------
# DEFAULT CONFIG
# -----------------------------
DEFAULT_SIGNAL_URL = "http://127.0.0.1:5000/api/signal_preview"
DEFAULT_AI_CONFIRM_URL = "http://127.0.0.1:5000/api/ai_confirm"
DEFAULT_POLL_INTERVAL = 2.0
DEFAULT_DRY_RUN = "0"
DEFAULT_SYMBOL = "GOLD"

MENTOR_LOG_FILE = os.path.join("logs", "mentor_executor.jsonl")


# -----------------------------
# Utilities
# -----------------------------
def now_epoch() -> float:
    return time.time()

def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()

def safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)

def ensure_logs_dir() -> None:
    os.makedirs(os.path.dirname(MENTOR_LOG_FILE), exist_ok=True)

def append_jsonl(path: str, record: Dict[str, Any]) -> None:
    line = safe_json(record)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def load_env_file(env_path: str, override: bool = True) -> None:
    try:
        if not os.path.exists(env_path):
            return
        with open(env_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                key = k.strip()
                val = v.strip().strip("'").strip('"')
                if not key:
                    continue
                if (not override) and (key in os.environ):
                    continue
                os.environ[key] = val
    except Exception:
        return


def http_get_json(url: str, timeout_sec: float = 8.0) -> Tuple[bool, Any]:
    try:
        req = Request(url, method="GET", headers={"Accept": "application/json"})
        with urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return True, json.loads(body)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
        return False, str(e)

def http_post_json(url: str, payload: Dict[str, Any], timeout_sec: float = 12.0) -> Tuple[bool, Any]:
    try:
        data = safe_json(payload).encode("utf-8")
        req = Request(
            url,
            method="POST",
            data=data,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        with urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return True, json.loads(body)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
        return False, str(e)


def telegram_send_text(text: str, chat_id_env: str, token_env: str = "TELEGRAM_BOT_TOKEN") -> Tuple[bool, int, str]:
    token_candidates: List[str] = []
    if chat_id_env == "TELEGRAM_MENTOR_CHAT_ID":
        token_candidates.extend(
            [
                os.environ.get("TELEGRAM_BOT_TOKEN_MENTOR") or "",
                os.environ.get("TELEGRAM_BOT_TOKEN_2") or "",
                os.environ.get(token_env) or "",
                os.environ.get("TELEGRAM_BOT_TOKEN") or "",
                os.environ.get("TELEGRAM_TOKEN") or "",
                os.environ.get("TELEGRAM_API_TOKEN") or "",
            ]
        )
    else:
        token_candidates.extend(
            [
                os.environ.get("TELEGRAM_BOT_TOKEN_TRADE") or "",
                os.environ.get(token_env) or "",
                os.environ.get("TELEGRAM_BOT_TOKEN") or "",
                os.environ.get("TELEGRAM_BOT_TOKEN_2") or "",
                os.environ.get("TELEGRAM_TOKEN") or "",
                os.environ.get("TELEGRAM_API_TOKEN") or "",
            ]
        )

    token = next((t.strip() for t in token_candidates if isinstance(t, str) and t.strip()), "")

    chat_id_candidates: List[str] = []
    if chat_id_env == "TELEGRAM_MENTOR_CHAT_ID":
        chat_id_candidates.extend(
            [
                os.environ.get(chat_id_env) or "",
                os.environ.get("MENTOR_CHAT_ID") or "",
            ]
        )
    else:
        chat_id_candidates.extend(
            [
                os.environ.get(chat_id_env) or "",
                os.environ.get("TELEGRAM_TRADE_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("CHAT_ID") or "",
            ]
        )
    chat_id = next((c.strip() for c in chat_id_candidates if isinstance(c, str) and c.strip()), "")
    if not token or not chat_id:
        return False, 0, f"missing token/chat_id env ({token_env}/{chat_id_env})"

    mentor_id = (os.environ.get("TELEGRAM_MENTOR_CHAT_ID") or os.environ.get("MENTOR_CHAT_ID") or "").strip()
    trade_id = (os.environ.get("TELEGRAM_TRADE_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("CHAT_ID") or "").strip()
    if chat_id_env == "TELEGRAM_MENTOR_CHAT_ID" and mentor_id and trade_id and mentor_id == trade_id:
        return False, 0, "mentor_chat_id_equals_trade_chat_id"
    if chat_id_env != "TELEGRAM_MENTOR_CHAT_ID" and mentor_id and trade_id and mentor_id == trade_id:
        return False, 0, "trade_chat_id_equals_mentor_chat_id"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    data = safe_json(payload).encode("utf-8")
    req = Request(url, method="POST", data=data, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            status = int(getattr(resp, "status", 0) or 0)
            return (status == 200), status, body
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = str(e)
        code = int(getattr(e, "code", 0) or 0)
        return False, code, body
    except (URLError, TimeoutError) as e:
        return False, 0, str(e)


def normalize_decision(x: Any) -> str:
    return str(x or "").upper().strip()

def _as_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None

def is_number(x: Any) -> bool:
    try:
        float(x)
        return True
    except Exception:
        return False

def minimal_plan_ok(plan: Any) -> bool:
    if not isinstance(plan, dict):
        return False
    return all(k in plan for k in ("entry", "sl", "tp"))

def make_request_id(symbol: str, decision: str, plan: Dict[str, Any]) -> str:
    """
    request_id must be unique and stable enough for dedup.
    Strategy:
    - UTC timestamp (to seconds)
    - short hash of (symbol, decision, sl, tp) to help trace
    - random suffix to avoid collision in same second
    """
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{symbol}|{decision}|{plan.get('sl')}|{plan.get('tp')}"
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:8]
    r = uuid.uuid4().hex[:6]
    return f"{ts}_{symbol}_{decision}_{h}_{r}"


def _kill_switch_active() -> Tuple[bool, str]:
    """
    Phase 7: Synchronous kill-switch check for fail-safe enforcement.
    Returns: (active: bool, reason: str)

    Fail-closed behavior:
    - If KILL_SWITCH.txt exists and can be read -> (True, content)
    - If KILL_SWITCH.txt exists but cannot be read -> (True, "KILL_SWITCH_READ_ERROR")
    - If KILL_SWITCH.txt does not exist -> (False, "")

    Uses PROJECT_ROOT from __file__ for deterministic path resolution.
    """
    if not os.path.exists(KILL_SWITCH_PATH):
        return False, ""
    try:
        with open(KILL_SWITCH_PATH, "r", encoding="utf-8", errors="replace") as f:
            reason = f.read().strip()[:500]
        return True, reason
    except Exception:
        # Fail-closed: if file exists but cannot be read, assume active
        return True, "KILL_SWITCH_READ_ERROR"


# -----------------------------
# Core Orchestrator
# -----------------------------
class MentorExecutor:
    def __init__(self) -> None:
        ensure_logs_dir()

        base_dir = os.path.dirname(os.path.abspath(__file__))
        load_env_file(os.path.join(base_dir, ".env"), override=True)

        self.signal_url = os.environ.get("SIGNAL_URL", DEFAULT_SIGNAL_URL).strip()
        self.ai_confirm_url = os.environ.get("AI_CONFIRM_URL", DEFAULT_AI_CONFIRM_URL).strip()
        self.poll_interval = float(os.environ.get("POLL_INTERVAL", str(DEFAULT_POLL_INTERVAL)))
        self.dry_run = os.environ.get("DRY_RUN", DEFAULT_DRY_RUN).strip() in ("1", "true", "TRUE", "yes", "YES")
        self.symbol = os.environ.get("SYMBOL", DEFAULT_SYMBOL).strip()
        self.signal_timeout_sec = float(os.environ.get("SIGNAL_TIMEOUT_SEC", "15"))
        self.ai_timeout_sec = float(os.environ.get("AI_TIMEOUT_SEC", "25"))
        self.verbose_status = os.environ.get("MENTOR_VERBOSE_STATUS", "0").strip() in ("1", "true", "TRUE", "yes", "YES")

        # import mt5_executor lazily to keep mentor stable even if MT5 not ready
        from mt5_executor import MT5Executor  # type: ignore

        cfg_raw = (os.environ.get("HIM_CONFIG") or os.environ.get("HIM_CONFIG_PATH") or "config.json").strip()
        if not cfg_raw:
            cfg_raw = "config.json"
        if os.path.isabs(cfg_raw):
            self.config_path = cfg_raw
        else:
            self.config_path = os.path.join(base_dir, cfg_raw)
        self.mt5 = MT5Executor(symbol=self.symbol, config_path=self.config_path)
        self.mentor_telegram_enabled = os.environ.get("MENTOR_TELEGRAM_ENABLED", "").strip()
        if self.mentor_telegram_enabled:
            self.mentor_telegram_enabled = self.mentor_telegram_enabled in ("1", "true", "TRUE", "yes", "YES")
        else:
            self.mentor_telegram_enabled = bool(os.environ.get("TELEGRAM_MENTOR_CHAT_ID"))
        self.startup_notify_mentor = os.environ.get("MENTOR_STARTUP_NOTIFY", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
        raw_trade_notify = os.environ.get("AUTOTRADE_STARTUP_NOTIFY", "").strip()
        if raw_trade_notify:
            self.startup_notify_trade = raw_trade_notify in ("1", "true", "TRUE", "yes", "YES")
        else:
            self.startup_notify_trade = False
        self.intel_enabled = os.environ.get("MENTOR_INTEL_ENABLED", "1").strip() in ("1", "true", "TRUE", "yes", "YES")
        self.intel_interval_sec = float(os.environ.get("MENTOR_INTEL_INTERVAL_SEC", "1800"))
        self._last_intel_ts = 0.0
        self._last_conn_status: Dict[str, Any] = {}
        self.rich_intel_enabled = os.environ.get("MENTOR_RICH_INTEL_ENABLED", "1").strip() in ("1", "true", "TRUE", "yes", "YES")
        self._rich_intel_mod: Optional[Any] = None
        self._rich_intel_mod_failed = False
        self.intel_min_interval_sec = float(os.environ.get("MENTOR_INTEL_MIN_INTERVAL_SEC", "900"))
        self.intel_max_interval_sec = float(os.environ.get("MENTOR_INTEL_MAX_INTERVAL_SEC", "3600"))
        self.intel_require_trend = os.environ.get("MENTOR_INTEL_REQUIRE_TREND", "1").strip() in ("1", "true", "TRUE", "yes", "YES")
        self._last_intel_sig = ""
        self.intel_min_move_points = float(os.environ.get("MENTOR_INTEL_MIN_MOVE_POINTS", "0"))
        self.intel_min_move_atr = float(os.environ.get("MENTOR_INTEL_MIN_MOVE_ATR", "0.35"))
        self._last_intel_close: Optional[float] = None
        self.intel_trend_change_only = os.environ.get("MENTOR_INTEL_TREND_CHANGE_ONLY", "1").strip() in ("1", "true", "TRUE", "yes", "YES")
        raw_tf = os.environ.get("MENTOR_INTEL_TFS", "M15,M30,H1,H4")
        self.intel_trend_tfs = [x.strip().upper() for x in raw_tf.split(",") if x.strip()]
        self.intel_trend_min_interval_sec = float(os.environ.get("MENTOR_INTEL_TREND_MIN_INTERVAL_SEC", "60"))
        self._last_trend_state: Dict[str, int] = {}
        self._last_trend_notify_ts = 0.0
        self.conn_alert_enabled = os.environ.get("MENTOR_CONN_ALERT_ENABLED", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
        self.conn_alert_min_interval_sec = float(os.environ.get("MENTOR_CONN_ALERT_MIN_INTERVAL_SEC", "1800"))
        self._last_conn_alert_ts = 0.0
        self._pending_intel_text: Optional[str] = None

    def log(self, record: Dict[str, Any]) -> None:
        record.setdefault("ts", now_epoch())
        record.setdefault("utc", now_utc_iso())
        record.setdefault("mentor_version", VERSION)
        append_jsonl(MENTOR_LOG_FILE, record)

    def _mentor_msg(self, raw_signal: Dict[str, Any], pkg: Dict[str, Any], mt5_out: Optional[Dict[str, Any]]) -> str:
        ts = now_utc_iso()
        decision = normalize_decision(pkg.get("decision"))
        plan = pkg.get("plan") if isinstance(pkg.get("plan"), dict) else {}
        ai = pkg.get("ai_confirm") if isinstance(pkg.get("ai_confirm"), dict) else {}
        ctx = raw_signal.get("context") if isinstance(raw_signal.get("context"), dict) else {}

        approved = ai.get("approved", None)
        conf = ai.get("confidence", None)
        conf_str = "-" if conf is None else f"{float(conf):.2f}"
        blocked_by = pkg.get("blocked_by")
        if not isinstance(blocked_by, list):
            blocked_by = raw_signal.get("blocked_by")
        if not isinstance(blocked_by, list):
            blocked_by = ctx.get("blocked_by", None)
        watch_state = ctx.get("watch_state", None)
        mode = ctx.get("mode", raw_signal.get("mode", None))
        rr = raw_signal.get("rr", None)

        status = None
        reason = None
        if isinstance(mt5_out, dict):
            status = mt5_out.get("status", None)
            reason = mt5_out.get("reason", None)

        ai_reason = ai.get("reason", None)
        bullets = ai.get("bullets", None)
        if isinstance(bullets, list):
            bullets = [str(x)[:140] for x in bullets[:3]]
        else:
            bullets = None

        lines = [
            "HIM MENTOR | EXECUTION",
            f"time_utc={ts}",
            f"mentor_version={VERSION}",
            f"symbol={self.symbol}",
            f"mode={mode}",
            f"decision={decision}",
            f"approved={approved}",
            f"confidence={conf_str}",
            f"rr={rr}",
            f"watch_state={watch_state}",
            f"blocked_by={blocked_by}",
            f"entry={plan.get('entry','-')}",
            f"sl={plan.get('sl','-')}",
            f"tp={plan.get('tp','-')}",
        ]
        if ai_reason is not None:
            lines.append(f"ai_reason={str(ai_reason)[:180]}")
        if bullets:
            lines.append(f"ai_bullets={bullets}")
        if status is not None:
            lines.append(f"mt5_status={status}")
        if reason is not None:
            lines.append(f"mt5_reason={reason}")
        lines.append(f"request_id={pkg.get('request_id')}")
        return "\n".join(lines)

    def _send_mentor_telegram(self, text: str) -> Tuple[bool, int, str]:
        if not self.mentor_telegram_enabled:
            return False, 0, "mentor_telegram_disabled"
        ok, status, body = telegram_send_text(text=text, chat_id_env="TELEGRAM_MENTOR_CHAT_ID")
        if not ok:
            self.log({"event": "mentor_telegram_failed", "status": status, "detail": str(body)[:400]})
        return ok, status, str(body)[:200]

    def _send_trade_telegram(self, text: str) -> Tuple[bool, int, str]:
        ok, status, body = telegram_send_text(text=text, chat_id_env="TELEGRAM_TRADE_CHAT_ID")
        if not ok:
            self.log({"event": "trade_telegram_failed", "status": status, "detail": str(body)[:400]})
        return ok, status, str(body)[:200]

    def _conn_check(self) -> Dict[str, Any]:
        def base_url(u: str) -> str:
            try:
                pu = urlparse(u)
                if pu.scheme and pu.netloc:
                    return f"{pu.scheme}://{pu.netloc}"
            except Exception:
                pass
            return "http://127.0.0.1:5000"

        base = base_url(self.signal_url)
        out: Dict[str, Any] = {
            "ts": now_epoch(),
            "utc": now_utc_iso(),
            "symbol": self.symbol,
            "dry_run": self.dry_run,
            "poll_interval": self.poll_interval,
            "signal_url": self.signal_url,
            "ai_confirm_url": self.ai_confirm_url,
            "telegram": {
                "token_1_set": bool(os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TELEGRAM_API_TOKEN")),
                "token_2_set": bool(os.environ.get("TELEGRAM_BOT_TOKEN_2") or os.environ.get("TELEGRAM_BOT_TOKEN_MENTOR") or os.environ.get("TELEGRAM_BOT_TOKEN_TRADE")),
                "mentor_chat_set": bool(os.environ.get("TELEGRAM_MENTOR_CHAT_ID") or os.environ.get("MENTOR_CHAT_ID")),
                "mentor_enabled": bool(self.mentor_telegram_enabled),
            },
            "llm": {
                "deepseek_api_key_set": bool(os.environ.get("DEEPSEEK_API_KEY")),
                "openai_api_key_set": bool(os.environ.get("OPENAI_API_KEY")),
            },
        }

        ok_health, health = http_get_json(f"{base}/api/health")
        out["api_health"] = {"ok": bool(ok_health), "detail": health if ok_health else str(health)[:200]}

        ok_status, status = http_get_json(f"{base}/api/status")
        out["api_status"] = {"ok": bool(ok_status), "detail": status if ok_status else str(status)[:200]}

        ok_cfg, cfg = http_get_json(f"{base}/api/config")
        if ok_cfg and isinstance(cfg, dict):
            d = cfg.get("data") if isinstance(cfg.get("data"), dict) else {}
            ai = d.get("ai_confirm") if isinstance(d.get("ai_confirm"), dict) else {}
            out["config"] = {
                "ok": True,
                "min_rr": d.get("min_rr"),
                "rr_sl_atr": d.get("rr_sl_atr"),
                "rr_base_tp_atr": d.get("rr_base_tp_atr"),
                "ai_use_llm": ai.get("use_llm"),
                "ai_provider": ai.get("provider"),
                "ai_model": ai.get("llm_model"),
                "ai_url": ai.get("llm_api_url"),
            }
        else:
            out["config"] = {"ok": False, "detail": str(cfg)[:200]}

        try:
            import MetaTrader5 as mt5  # type: ignore

            ti = mt5.terminal_info()
            ai2 = mt5.account_info()
            tick = mt5.symbol_info_tick(self.symbol)
            out["mt5"] = {
                "ok": bool(ti is not None and ai2 is not None),
                "trade_allowed": bool(getattr(ti, "trade_allowed", False)) if ti is not None else False,
                "connected": bool(getattr(ti, "connected", False)) if ti is not None else False,
                "tick_ok": bool(tick is not None),
            }
        except Exception as e:
            out["mt5"] = {"ok": False, "detail": f"{type(e).__name__}: {str(e)[:180]}"}

        ok_sig, sig = self.fetch_signal()
        if ok_sig and isinstance(sig, dict):
            out["signal_preview"] = {
                "ok": True,
                "decision": sig.get("decision"),
                "blocked_by": sig.get("blocked_by"),
                "request_id": sig.get("request_id"),
                "latency_ms": (sig.get("api_meta") or {}).get("latency_ms") if isinstance(sig.get("api_meta"), dict) else None,
            }
        else:
            out["signal_preview"] = {"ok": False, "detail": str(sig)[:200]}

        return out

    def _conn_status_changed(self, s: Dict[str, Any]) -> bool:
        keys = ("api_health", "api_status", "signal_preview", "telegram")
        slim = {k: s.get(k) for k in keys}
        prev = {k: self._last_conn_status.get(k) for k in keys}
        if slim != prev:
            self._last_conn_status = slim
            return True
        return False

    def _format_conn_text(self, s: Dict[str, Any]) -> str:
        health = s.get("api_health", {})
        status = s.get("api_status", {})
        sig = s.get("signal_preview", {})
        tg = (s.get("telegram") or {})
        mentor_chat_id = (os.environ.get("TELEGRAM_MENTOR_CHAT_ID") or os.environ.get("MENTOR_CHAT_ID") or "").strip()
        trade_chat_id = (os.environ.get("TELEGRAM_TRADE_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("CHAT_ID") or "").strip()
        return (
            "HIM MENTOR STATUS\n"
            f"time_utc={s.get('utc')}\n"
            f"symbol={s.get('symbol')}\n"
            f"dry_run={s.get('dry_run')}\n"
            f"poll_interval={s.get('poll_interval')}\n"
            f"api_health_ok={health.get('ok')}\n"
            f"api_status_ok={status.get('ok')}\n"
            f"signal_preview_ok={sig.get('ok')}\n"
            f"signal_decision={sig.get('decision','-')}\n"
            f"telegram_token_1_set={tg.get('token_1_set')}\n"
            f"telegram_token_2_set={tg.get('token_2_set')}\n"
            f"telegram_mentor_chat_set={tg.get('mentor_chat_set')}\n"
            f"telegram_mentor_enabled={tg.get('mentor_enabled')}\n"
            f"mentor_chat_id={mentor_chat_id}\n"
            f"trade_chat_id={trade_chat_id}"
        )

    def _can_send_conn_alert(self) -> bool:
        now = now_epoch()
        if (now - self._last_conn_alert_ts) < max(self.conn_alert_min_interval_sec, 60.0):
            return False
        self._last_conn_alert_ts = now
        return True

    def _format_trade_startup_text(self, s: Dict[str, Any]) -> str:
        health = s.get("api_health", {})
        status = s.get("api_status", {})
        mt5s = s.get("mt5", {})
        return (
            "HIM TRADE | STARTUP\n"
            f"time_utc={s.get('utc')}\n"
            f"symbol={s.get('symbol')}\n"
            f"dry_run={s.get('dry_run')}\n"
            f"api_health_ok={health.get('ok')}\n"
            f"api_status_ok={status.get('ok')}\n"
            f"mt5_ok={mt5s.get('ok')}\n"
            f"trade_allowed={mt5s.get('trade_allowed')}\n"
            f"connected={mt5s.get('connected')}"
        )

    def _rich_intel_text(self, raw_signal: Dict[str, Any]) -> Optional[str]:
        if not self.rich_intel_enabled:
            return None
        if self._rich_intel_mod_failed:
            return None
        if self._rich_intel_mod is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            p = os.path.join(base_dir, "_PY_QUARANTINE_", "intelligent_mentor_readonly.py")
            if not os.path.exists(p):
                self._rich_intel_mod_failed = True
                return None
            try:
                spec = importlib.util.spec_from_file_location("intelligent_mentor_readonly_quarantine", p)
                if spec is None or spec.loader is None:
                    self._rich_intel_mod_failed = True
                    return None
                mod = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = mod
                spec.loader.exec_module(mod)
                self._rich_intel_mod = mod
            except Exception:
                self._rich_intel_mod_failed = True
                return None

        mod = self._rich_intel_mod
        try:
            cls = getattr(mod, "IntelligentMentorReadOnly", None)
            if cls is None:
                return None
            if not os.environ.get("MENTOR_STYLE"):
                os.environ["MENTOR_STYLE"] = "beginner"
            ev_tf = str(raw_signal.get("event_timeframe") or "M1").upper()
            mentor = cls(config_path="config.json", symbol=self.symbol, event_timeframe=ev_tf, signal_url=self.signal_url)
            result = mentor.analyze()
            text = mentor.format_message(result)
            return cast(str, text)
        except Exception:
            return None

    def _signal_url_for_tf(self, tf: str) -> str:
        p = urlparse(self.signal_url)
        qs = dict(parse_qsl(p.query, keep_blank_values=True))
        qs["event_timeframe"] = tf
        nq = urlencode(qs)
        return urlunparse((p.scheme, p.netloc, p.path, p.params, nq, p.fragment))

    def _trend_dir_from_signal(self, sig: Dict[str, Any]) -> int:
        metrics = sig.get("metrics") if isinstance(sig.get("metrics"), dict) else {}
        st_dir = metrics.get("supertrend_dir_event")
        try:
            d = int(st_dir)
        except Exception:
            d = 0
        if d > 0:
            return 1
        if d < 0:
            return -1
        return 0

    def _trend_change_text(self) -> Optional[str]:
        if not self.intel_trend_tfs:
            return None
        now = now_epoch()
        if (now - self._last_trend_notify_ts) < max(self.intel_trend_min_interval_sec, 15.0):
            return None
        changed: List[Tuple[str, int, int]] = []
        new_state = dict(self._last_trend_state)
        for tf in self.intel_trend_tfs:
            ok, sig = http_get_json(self._signal_url_for_tf(tf), timeout_sec=self.signal_timeout_sec)
            if not ok or not isinstance(sig, dict):
                continue
            curr = self._trend_dir_from_signal(sig)
            prev = new_state.get(tf)
            new_state[tf] = curr
            if prev is None:
                continue
            if prev != curr:
                changed.append((tf, prev, curr))
        self._last_trend_state = new_state
        if not changed:
            return None
        def name(v: int) -> str:
            if v > 0:
                return "UP"
            if v < 0:
                return "DOWN"
            return "NEUTRAL"
        lines = [
            "HIM MENTOR TREND CHANGE",
            f"time_utc={now_utc_iso()}",
            f"symbol={self.symbol}",
        ]
        for tf, prev, curr in changed:
            lines.append(f"{tf}: {name(prev)} -> {name(curr)}")
        return "\n".join(lines)

    def _intel_text(self, raw_signal: Dict[str, Any]) -> str:
        if isinstance(self._pending_intel_text, str) and self._pending_intel_text.strip():
            msg = self._pending_intel_text
            self._pending_intel_text = None
            return msg
        rich = self._rich_intel_text(raw_signal)
        if isinstance(rich, str) and rich.strip():
            return rich
        return self._mentor_intel_msg(raw_signal)

    def _intel_has_trend(self, raw_signal: Dict[str, Any]) -> bool:
        metrics = raw_signal.get("metrics") if isinstance(raw_signal.get("metrics"), dict) else {}
        blocked_by = raw_signal.get("blocked_by")
        if not isinstance(blocked_by, list):
            blocked_by = []

        align = metrics.get("alignment_score")
        try:
            align_i = int(align)
        except Exception:
            align_i = 0

        st_dir = metrics.get("supertrend_dir_event")
        try:
            st_dir_i = int(st_dir)
        except Exception:
            st_dir_i = 0

        if "supertrend_conflict" in blocked_by:
            return False
        return (align_i >= 2) and (st_dir_i in (-1, 1))

    def _intel_signature(self, raw_signal: Dict[str, Any]) -> str:
        metrics = raw_signal.get("metrics") if isinstance(raw_signal.get("metrics"), dict) else {}
        blocked_by = raw_signal.get("blocked_by")
        if not isinstance(blocked_by, list):
            blocked_by = []
        sig_obj = {
            "decision": raw_signal.get("decision"),
            "blocked_by": sorted([str(x) for x in blocked_by])[:8],
            "regime": metrics.get("regime"),
            "alignment": metrics.get("alignment_score"),
            "st_dir": metrics.get("supertrend_dir_event"),
            "bos_up": metrics.get("bos_break_up_atr"),
            "bos_dn": metrics.get("bos_break_dn_atr"),
        }
        h = hashlib.sha1(safe_json(sig_obj).encode("utf-8")).hexdigest()
        return h[:16]

    def _intel_price_ok(self, raw_signal: Dict[str, Any]) -> bool:
        price = raw_signal.get("price") if isinstance(raw_signal.get("price"), dict) else {}
        close = price.get("close") if isinstance(price, dict) else None
        atr = price.get("atr") if isinstance(price, dict) else None

        try:
            close_f = float(close)
        except Exception:
            return False

        atr_f: Optional[float]
        try:
            atr_f = float(atr)
        except Exception:
            atr_f = None

        thr = float(self.intel_min_move_points)
        if atr_f is not None and atr_f > 0 and self.intel_min_move_atr > 0:
            thr = max(thr, float(self.intel_min_move_atr) * float(atr_f))

        if self._last_intel_close is None:
            self._last_intel_close = close_f
            return True

        delta = abs(close_f - float(self._last_intel_close))
        if delta >= thr:
            self._last_intel_close = close_f
            return True
        return False

    def _should_send_intel(self, raw_signal: Dict[str, Any]) -> bool:
        if self.intel_trend_change_only:
            msg = self._trend_change_text()
            if isinstance(msg, str) and msg.strip():
                self._last_intel_ts = now_epoch()
                self._last_trend_notify_ts = self._last_intel_ts
                self._pending_intel_text = msg
                return True
            return False

        now = now_epoch()
        if (now - self._last_intel_ts) < max(self.intel_min_interval_sec, 60.0):
            return False
        if self.intel_require_trend and (not self._intel_has_trend(raw_signal)):
            return False
        if not self._intel_price_ok(raw_signal):
            return False

        sig = self._intel_signature(raw_signal)
        self._last_intel_ts = now
        self._last_intel_sig = sig
        return True

    def _mentor_intel_msg(self, raw_signal: Dict[str, Any]) -> str:
        ts = now_utc_iso()
        decision = normalize_decision(raw_signal.get("decision"))
        plan = raw_signal.get("plan") if isinstance(raw_signal.get("plan"), dict) else {}
        metrics = raw_signal.get("metrics") if isinstance(raw_signal.get("metrics"), dict) else {}
        rr = metrics.get("rr") if isinstance(metrics, dict) else None
        blocked_by = raw_signal.get("blocked_by")
        regime = metrics.get("regime") if isinstance(metrics, dict) else None
        st_dir = metrics.get("supertrend_dir_event") if isinstance(metrics, dict) else None
        bbwa = metrics.get("bb_width_atr") if isinstance(metrics, dict) else None
        return (
            "HIM INTELLIGENT MENTOR (READ-ONLY)\n"
            f"time_utc={ts}\n"
            f"symbol={self.symbol}\n"
            f"decision={decision}\n"
            f"regime={regime}\n"
            f"st_dir={st_dir}\n"
            f"bb_width_atr={bbwa}\n"
            f"rr={rr}\n"
            f"blocked_by={blocked_by}\n"
            f"entry={plan.get('entry','-')}\n"
            f"sl={plan.get('sl','-')}\n"
            f"tp={plan.get('tp','-')}"
        )

    def fetch_signal(self) -> Tuple[bool, Any]:
        return http_get_json(self.signal_url, timeout_sec=self.signal_timeout_sec)

    def ai_confirm(self, execution_package: Dict[str, Any]) -> Tuple[bool, Any]:
        """
        Sends only what AI needs to approve/deny.
        IMPORTANT: AI must not modify plan; mentor will ignore plan changes if any.
        """
        payload = {
            "request_id": execution_package.get("request_id"),
            "symbol": self.symbol,
            "decision": execution_package.get("decision"),
            "plan": execution_package.get("plan"),
            "metrics": execution_package.get("metrics", {}),
            "context": execution_package.get("context", {}),
            "blocked_by": execution_package.get("blocked_by", []),
            "status": execution_package.get("status"),
            "bias": execution_package.get("bias"),
            "event_timeframe": execution_package.get("event_timeframe"),
            "decision_votes": execution_package.get("decision_votes"),
        }
        return http_post_json(self.ai_confirm_url, payload, timeout_sec=self.ai_timeout_sec)

    def build_execution_package(self, raw_signal: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], str]:
        decision = normalize_decision(raw_signal.get("decision"))
        plan = raw_signal.get("plan")

        # Only trade signals proceed to AI/MT5
        if decision not in ("BUY", "SELL"):
            return False, {}, "decision_not_trade"

        # Fail-closed: required plan
        if not minimal_plan_ok(plan):
            return False, {}, "signal_missing_plan"

        if not (is_number(plan.get("entry")) and is_number(plan.get("sl")) and is_number(plan.get("tp"))):
            return False, {}, "signal_plan_invalid_numbers"

        entry_v = float(plan.get("entry"))
        sl_v = float(plan.get("sl"))
        tp_v = float(plan.get("tp"))

        if (sl_v <= 0.0) or (tp_v <= 0.0):
            return False, {}, "signal_plan_invalid_values"

        if decision == "BUY" and not (sl_v < entry_v < tp_v):
            return False, {}, "signal_plan_invalid_side_buy"

        if decision == "SELL" and not (tp_v < entry_v < sl_v):
            return False, {}, "signal_plan_invalid_side_sell"

        # request_id unique per cycle
        req_id = raw_signal.get("request_id")
        if not isinstance(req_id, str) or not req_id.strip():
            req_id = make_request_id(self.symbol, decision, plan)

        metrics = raw_signal.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}

        ctx = raw_signal.get("context", {})
        if not isinstance(ctx, dict):
            ctx = {}
        tfs = raw_signal.get("timeframes")
        if isinstance(tfs, list):
            compact_tfs = []
            for row in tfs:
                if not isinstance(row, dict):
                    continue
                tf = str(row.get("tf") or "").upper()
                if tf not in ("D1", "H4", "H1", "M30", "M15", "M5", "M1"):
                    continue
                compact_tfs.append(
                    {
                        "tf": tf,
                        "st": row.get("st_dir_label") or row.get("st_dir"),
                        "bbw": row.get("bb_width_atr"),
                        "atr": row.get("atr"),
                    }
                )
            ctx = {**ctx, "tfs": compact_tfs[:7]}

        pkg = {
            "request_id": req_id,
            "decision": decision,
            "plan": {
                "entry": float(plan.get("entry")),
                "sl": float(plan.get("sl")),
                "tp": float(plan.get("tp")),
            },
            "metrics": metrics,
            # optional: any engine metadata / indicators snapshot
            "context": ctx,
            "source": raw_signal.get("source", {}),
            "blocked_by": raw_signal.get("blocked_by", []),
            "status": raw_signal.get("status"),
            "bias": raw_signal.get("bias"),
            "event_timeframe": raw_signal.get("event_timeframe"),
            "decision_votes": raw_signal.get("decision_votes") if isinstance(raw_signal.get("decision_votes"), dict) else None,
        }
        return True, pkg, "ok"

    def enforce_confirm_only(self, pkg: Dict[str, Any], ai_resp: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge AI response into pkg, but DO NOT allow AI to overwrite plan.
        Accept only ai_confirm fields:
          - approved (bool)
          - reason (str)
          - confidence (0..1)
        """
        ai_confirm = {}
        if isinstance(ai_resp, dict):
            approved = ai_resp.get("approved")
            reason = ai_resp.get("reason", "")
            conf = ai_resp.get("confidence", None)

            ai_confirm["approved"] = bool(approved is True)
            ai_confirm["reason"] = str(reason)[:500]

            if is_number(conf):
                c = float(conf)
                if c < 0:
                    c = 0.0
                if c > 1:
                    c = 1.0
                ai_confirm["confidence"] = c
            else:
                ai_confirm["confidence"] = None

            if isinstance(ai_resp.get("confirmed_plan"), dict):
                ai_confirm["confirmed_plan"] = ai_resp.get("confirmed_plan")
            if isinstance(ai_resp.get("bullets"), list):
                ai_confirm["bullets"] = [str(x)[:140] for x in ai_resp.get("bullets")[:3]]
            if isinstance(ai_resp.get("provider"), str):
                ai_confirm["provider"] = str(ai_resp.get("provider"))[:80]
            if isinstance(ai_resp.get("model"), str):
                ai_confirm["model"] = str(ai_resp.get("model"))[:80]

        pkg["ai_confirm"] = ai_confirm
        return pkg

    def run_once(self) -> Dict[str, Any]:
        # 1) Fetch signal
        ok, raw = self.fetch_signal()
        if not ok:
            out = {"status": "SKIP", "reason": "signal_fetch_failed", "detail": raw}
            self.log({"event": "signal_fetch_failed", "out": out})
            s = self._conn_check()
            if self.conn_alert_enabled and self._conn_status_changed(s) and self._can_send_conn_alert():
                self._send_mentor_telegram(self._format_conn_text(s))
            return out

        if not isinstance(raw, dict):
            out = {"status": "SKIP", "reason": "signal_invalid_format"}
            self.log({"event": "signal_invalid_format", "raw": raw, "out": out})
            return out

        # 2) Build execution package (pre-AI)
        ok_pkg, pkg, why = self.build_execution_package(raw)
        if not ok_pkg:
            # No trade signal (normal) vs true schema failure
            if why == "decision_not_trade":
                if self.verbose_status and isinstance(raw, dict):
                    out = {
                        "status": "SKIP",
                        "reason": "decision_not_trade",
                        "decision": raw.get("decision"),
                        "blocked_by": raw.get("blocked_by"),
                        "latency_ms": (raw.get("api_meta") or {}).get("latency_ms") if isinstance(raw.get("api_meta"), dict) else None,
                    }
                else:
                    out = {"status": "SKIP", "reason": "decision_not_trade"}
                self.log({"event": "no_trade_signal", "out": out})
                if self.intel_enabled:
                    if self._should_send_intel(raw):
                        self._send_mentor_telegram(self._intel_text(raw))
                return out

            out = {"status": "SKIP", "reason": why}
            self.log({"event": "build_pkg_failed", "raw": raw, "out": out})
            return out

        # 3) AI final confirm
        ok_ai, ai = self.ai_confirm(pkg)
        if not ok_ai:
            # Fail-closed: AI unreachable -> deny by default
            pkg["ai_confirm"] = {"approved": False, "reason": "ai_unreachable", "confidence": None}
            self.log({"event": "ai_call_failed", "request_id": pkg["request_id"], "detail": ai})

            if self.dry_run:
                out = {"status": "DRY_RUN", "request_id": pkg["request_id"], "reason": "ai_unreachable"}
                self.log({"event": "dry_run_end", "pkg": pkg, "out": out})
                return out

            out = {
                "status": "SKIP",
                "reason": "ai_unreachable",
                "request_id": pkg["request_id"],
                "detail": ai,
            }
            self.log({"event": "ai_unreachable_skip", "pkg": pkg, "out": out})
            return out

        if not isinstance(ai, dict):
            ai = {"approved": False, "reason": "ai_invalid_response", "confidence": None}

        pkg = self.enforce_confirm_only(pkg, ai)
        ai_confirm = pkg.get("ai_confirm") if isinstance(pkg.get("ai_confirm"), dict) else {}
        if not bool(ai_confirm.get("approved") is True):
            out = {
                "status": "SKIP",
                "reason": "ai_denied",
                "request_id": pkg.get("request_id"),
                "ai_confirm": ai_confirm,
                "blocked_by": pkg.get("blocked_by"),
                "decision": pkg.get("decision"),
                "plan": pkg.get("plan"),
            }
            self.log({"event": "ai_denied_skip", "pkg": pkg, "out": out})
            return out

        # 4) Dry-run option
        if self.dry_run:
            out = {"status": "DRY_RUN", "request_id": pkg["request_id"], "ai_confirm": pkg.get("ai_confirm")}
            self.log({"event": "dry_run_end", "pkg": pkg, "out": out})
            return out

        # ========================================
        # PHASE 7: SYNCHRONOUS KILL-SWITCH CHECK
        # ========================================
        # Production fail-safe: Check kill switch immediately before execution
        # Fail-closed: If KILL_SWITCH.txt exists (or cannot be read), block execution
        # Uses __file__-based path for deterministic resolution
        kill_active, kill_reason = _kill_switch_active()
        if kill_active:
            out = {
                "status": "SKIP",
                "reason": "kill_switch_active",
                "kill_switch_reason": kill_reason,
                "request_id": pkg.get("request_id"),
            }
            self.log({
                "event": "kill_switch_block",
                "request_id": pkg.get("request_id"),
                "kill_switch_reason": kill_reason,
                "out": out,
            })
            return out
        # ========================================

        # 5) Execute via mt5_executor (final authority)
        mt5_out = self.mt5.execute(pkg)
        if self.verbose_status and isinstance(mt5_out, dict) and mt5_out.get("reason") == "ai_denied":
            mt5_out = {**mt5_out, "ai_confirm": pkg.get("ai_confirm"), "blocked_by": pkg.get("blocked_by")}
        self.log({"event": "mt5_execute", "request_id": pkg["request_id"], "pkg": pkg, "mt5_out": mt5_out})
        return mt5_out

    # -----------------------------
    # Phase 2.2: HIM v3 helpers
    # -----------------------------

    def _load_him_v3_config(self) -> Dict[str, Any]:
        """
        โหลด him_v3 section จาก config.json (self.config_path)
        Fail-safe: คืน {} ถ้าไม่มีไฟล์ / key ไม่มี / parse error
        ไม่ raise exception — ไม่กระทบ existing behavior
        """
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            return cfg.get("him_v3", {}) if isinstance(cfg, dict) else {}
        except Exception:
            return {}

    def _run_once_and_print(self, _event: Any = None) -> None:
        """
        Wrapper สำหรับ CandleCloseTrigger.on_new_candle callback:
        เรียก run_once() แล้ว print ผล เหมือนกับ poll loop เดิม
        _event: NewBarEvent จาก CandleCloseTrigger (รับแต่ไม่ใช้)
        Exception ใน run_once() ถูก catch — ไม่ทำให้ trigger loop หยุด
        """
        try:
            out = self.run_once()
            print(safe_json(out))
        except Exception as e:
            print(safe_json({"status": "ERROR", "reason": "run_once_exception", "detail": str(e)[:200]}))

    def loop(self) -> None:
        s = self._conn_check()
        self.log({"event": "startup_status", "status": s})
        print(safe_json({"event": "startup_status", "status": s}))
        if self.startup_notify_mentor and self.conn_alert_enabled and (not self.intel_trend_change_only):
            ok, status, detail = self._send_mentor_telegram(self._format_conn_text(s))
            evt = {"event": "startup_telegram", "target": "mentor", "ok": ok, "status": status, "detail": detail}
            self.log(evt)
            print(safe_json(evt))
        if self.startup_notify_trade:
            ok, status, detail = self._send_trade_telegram(self._format_trade_startup_text(s))
            evt = {"event": "startup_telegram", "target": "trade", "ok": ok, "status": status, "detail": detail}
            self.log(evt)
            print(safe_json(evt))

        # ---- Phase 2.2: read him_v3.candle_trigger ----------------------
        him_v3_cfg = self._load_him_v3_config()
        candle_cfg = him_v3_cfg.get("candle_trigger", {}) if isinstance(him_v3_cfg, dict) else {}
        use_candle_trigger = bool(candle_cfg.get("enabled", False))
        candle_tf = str(candle_cfg.get("timeframe", "M1")).strip().upper() or "M1"
        # -----------------------------------------------------------------

        self.log(
            {
                "event": "mentor_start",
                "signal_url": self.signal_url,
                "ai_confirm_url": self.ai_confirm_url,
                "poll_interval": self.poll_interval,
                "dry_run": self.dry_run,
                "symbol": self.symbol,
                "kill_switch_path": KILL_SWITCH_PATH,
                "candle_trigger_mode": use_candle_trigger,
                "candle_trigger_tf": candle_tf,
            }
        )

        # ---- Phase 2.2: candle-close mode OR legacy poll mode -----------
        if use_candle_trigger:
            try:
                from candle_close_trigger import CandleCloseTrigger  # type: ignore
            except ImportError as exc:
                print(safe_json({
                    "event": "candle_trigger_import_error",
                    "detail": str(exc),
                    "fallback": "poll_mode",
                }))
                use_candle_trigger = False

        if use_candle_trigger:
            print(safe_json({
                "event": "mentor_loop_mode",
                "mode": "candle_trigger",
                "symbol": self.symbol,
                "timeframe": candle_tf,
            }))
            trigger = CandleCloseTrigger(  # type: ignore[name-defined]
                symbol=self.symbol,
                timeframe=candle_tf,
                on_new_candle=self._run_once_and_print,
            )
            trigger.run()   # blocking — same lifetime as old while True
        else:
            print(safe_json({
                "event": "mentor_loop_mode",
                "mode": "poll",
                "poll_interval": self.poll_interval,
            }))
            while True:
                out = self.run_once()
                print(safe_json(out))
                time.sleep(self.poll_interval)
        # -----------------------------------------------------------------


# -----------------------------
# Entry
# -----------------------------
if __name__ == "__main__":
    print(f"[MentorExecutor] file={os.path.abspath(__file__)} version={VERSION}")
    print(f"[MENTOR] kill_switch_path={KILL_SWITCH_PATH}")
    ap = argparse.ArgumentParser(add_help=True)
    ap.add_argument("--poll", default=None)
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--signal-url", default=None)
    ap.add_argument("--ai-confirm-url", default=None)
    ap.add_argument("--symbol", default=None)
    args = ap.parse_args()

    poll = _as_float(args.poll)
    if poll is not None and poll > 0:
        os.environ["POLL_INTERVAL"] = str(poll)
    if args.symbol:
        os.environ["SYMBOL"] = str(args.symbol).strip()
    if args.signal_url:
        os.environ["SIGNAL_URL"] = str(args.signal_url).strip()
    if args.ai_confirm_url:
        os.environ["AI_CONFIRM_URL"] = str(args.ai_confirm_url).strip()
    if args.dry_run:
        os.environ["DRY_RUN"] = "1"
    if args.live:
        os.environ["DRY_RUN"] = "0"
    try:
        m = MentorExecutor()
    except Exception as e:
        print(safe_json({"status": "FATAL", "reason": "init_failed", "detail": str(e)}))
        sys.exit(1)

    if args.once:
        out = m.run_once()
        print(safe_json(out))
        raise SystemExit(0)

    m.loop()