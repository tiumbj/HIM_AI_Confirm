# ============================================================
# ชื่อโค้ด: HIM API Server (api_server.py)
# ที่อยู่ไฟล์: c:\Data\Bot\HIM_AI_Confirm\api_server.py
# คำสั่งรัน: python api_server.py
# เวอร์ชัน: v4.2.0
# ============================================================
#  api_server.py — Production-grade API Server for HIM
#  Version: v4.2.0
#  Changelog:
#   - v4.2.0 (2026-03-13): Phase 8.1 - Enforce AI LLM-only confirmation
#       * Remove ALL local_policy execution approval paths (_local_confirm removed)
#       * Add _local_precheck(): non-authoritative validation only, never returns approved=True
#       * Add _llm_confirm_with_priority(): strict provider chain DeepSeek->OpenAI->Groq->Claude->Gemini
#       * Add _llm_call_provider(): handles OpenAI-compatible, Claude, and Gemini API formats
#       * Add _normalize_llm_out(): normalize LLM JSON output fields
#       * Rewrite confirm(): LLM-only approval, fail-closed when all providers fail
#       * All API keys from .env only: DEEPSEEK_API_KEY, OPENAI_API_KEY, GROQ_API_KEY,
#         CLAUDE_API_KEY, GEMINI_API_KEY
#       * Remove old _llm_confirm() single-provider method
#   - v4.1.0 (2026-03-13):
#       * Make AI confirm pipeline confirm-only: remove confirmed_plan from LLM prompt and API response
#   - v4.0.1 (2026-03-13):
#       * Fix commissioning event timeframe config: support commissioning.event_timeframes list
#   - v4.0.0 (2026-03-05):
#       * Add missing endpoints:
#           - GET  /api/signal_preview  -> engine.generate_signal_package()
#           - POST /api/ai_confirm      -> confirm-only policy (fail-closed)
#       * Keep existing endpoints: /api/health, /api/status, /api/config (GET/POST)
#       * Thread-safe config loader with reload-on-change
#       * JSONL audit logging to logs/api_server.jsonl
#       * Dashboard redirect support via config.dashboard.external_url
# ============================================================

from __future__ import annotations

import json
import os
import re
import sys
import time
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Callable

from flask import Flask, jsonify, request, redirect
from urllib.request import Request, urlopen

# ----------------------------
# Constants
# ----------------------------
APP_VERSION = "v4.2.0"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000
DEFAULT_CONFIG_PATH = "config.json"

LOG_DIR = os.path.join(os.getcwd(), "logs")
API_AUDIT_LOG = os.path.join(LOG_DIR, "api_server.jsonl")


# ----------------------------
# Helpers: JSON, Logging
# ----------------------------
def _now_ts() -> int:
    return int(time.time())


def _ensure_log_dir() -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        # do not crash server for logging directory errors
        pass


def _append_jsonl(path: str, obj: Dict[str, Any]) -> None:
    _ensure_log_dir()
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        # do not crash server for logging errors
        pass


def _telegram_send_startup(text: str) -> None:
    if os.environ.get("API_STARTUP_NOTIFY", "1").strip() not in ("1", "true", "TRUE", "yes", "YES"):
        return
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN") or os.environ.get("TELEGRAM_API_TOKEN") or "").strip()
    chat_id = (os.environ.get("TELEGRAM_TRADE_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("CHAT_ID") or "").strip()
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(url, method="POST", data=data, headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=10) as _:
            return
    except Exception:
        return


def _ok(data: Any = None, **meta: Any):
    out = {"ok": True, "ts": _now_ts(), "version": APP_VERSION}
    if data is not None:
        out["data"] = data
    if meta:
        out.update(meta)
    return jsonify(out), 200


def _err(code: str, message: str, http_status: int = 400, **meta: Any):
    out = {
        "ok": False,
        "ts": _now_ts(),
        "version": APP_VERSION,
        "error": {"code": code, "message": message},
    }
    if meta:
        out.update(meta)
    return jsonify(out), http_status


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _upper_str(x: Any) -> str:
    try:
        return str(x).strip().upper()
    except Exception:
        return ""


def _first_present(d: Dict[str, Any], keys: Tuple[str, ...], default: Any = None) -> Any:
    for k in keys:
        if k in d:
            return d.get(k)
    return default


# ----------------------------
# Config Loader (thread-safe, reload-on-change)
# ----------------------------
@dataclass
class ConfigState:
    path: str
    mtime: float
    data: Dict[str, Any]


class ConfigManager:
    def __init__(self, config_path: str):
        self._lock = threading.Lock()
        self._state = ConfigState(path=config_path, mtime=0.0, data={})

    def get(self) -> Dict[str, Any]:
        # Reload if file changed
        with self._lock:
            path = self._state.path
            try:
                st = os.stat(path)
                if st.st_mtime > self._state.mtime:
                    self._state = ConfigState(path=path, mtime=st.st_mtime, data=self._load_file(path))
            except FileNotFoundError:
                # keep old config but expose error via status
                if not self._state.data:
                    self._state = ConfigState(path=path, mtime=0.0, data={})
            except Exception:
                # keep old config
                pass
            return dict(self._state.data)

    def set(self, new_data: Dict[str, Any]) -> Tuple[bool, str]:
        with self._lock:
            path = self._state.path
            try:
                # basic validation: must be dict
                if not isinstance(new_data, dict):
                    return False, "config_must_be_object"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(new_data, f, ensure_ascii=False, indent=2)
                st = os.stat(path)
                self._state = ConfigState(path=path, mtime=st.st_mtime, data=new_data)
                return True, "ok"
            except Exception as e:
                return False, f"write_failed: {e}"

    @staticmethod
    def _load_file(path: str) -> Dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}


# ----------------------------
# Engine Adapter (best-effort import)
# ----------------------------
class EngineAdapter:
    """
    Goal: obtain a callable generate_signal_package() from engine.py
    Supports:
      - from engine import TradingEngine; TradingEngine(cfg).generate_signal_package(...)
      - from engine import generate_signal_package (function)
      - from engine import TradingEngine (instance method) with flexible signature
    """

    def __init__(self, config_mgr: ConfigManager):
        self.config_mgr = config_mgr
        self._lock = threading.Lock()
        self._cached: Optional[Callable[..., Dict[str, Any]]] = None
        self._cached_signature: str = ""

    @staticmethod
    def _normalize_tf(v: Any) -> str:
        s = str(v or "").strip().upper()
        if s in ("M10", "M15", "M30", "H1", "H4", "M5", "M1", "D1"):
            return s
        return ""

    def _decision_timeframes(self, cfg: Dict[str, Any]) -> list[str]:
        raw = cfg.get("decision_timeframes")
        out: list[str] = []
        if isinstance(raw, list):
            for x in raw:
                tf = self._normalize_tf(x)
                if tf and tf not in out:
                    out.append(tf)
        if not out:
            out = ["M10", "M15", "M30", "H1"]
        return out

    def _commissioning_event_timeframes(self, cfg: Dict[str, Any]) -> list[str]:
        try:
            com = cfg.get("commissioning", {})
            if not isinstance(com, dict):
                com = {}
        except Exception:
            com = {}

        out: list[str] = []

        single = com.get("event_timeframe") or com.get("event_tf")
        tf_single = self._normalize_tf(single)
        if tf_single:
            out.append(tf_single)

        raw_list = com.get("event_timeframes")
        if isinstance(raw_list, list):
            for x in raw_list:
                tf = self._normalize_tf(x)
                if tf and tf not in out:
                    out.append(tf)

        return out

    def _decision_min_agree(self, cfg: Dict[str, Any], n: int) -> int:
        try:
            v = int(cfg.get("decision_min_agree", 2))
        except Exception:
            v = 2
        if v < 1:
            v = 1
        if v > n:
            v = n
        return v

    def generate_signal_package(self, event_timeframe_override: Optional[str] = None) -> Tuple[bool, Dict[str, Any], str]:
        """
        Returns: (ok, payload, reason)
        """
        try:
            fn = self._get_callable()
            cfg = self.config_mgr.get()

            override_tf = self._normalize_tf(event_timeframe_override) if event_timeframe_override else ""
            if override_tf:
                try:
                    pkg = fn(event_timeframe=str(override_tf))
                    return True, self._normalize_signal(pkg), "ok"
                except TypeError:
                    pass
                except Exception:
                    return False, {}, "engine_error: event_timeframe_call_failed"

            commissioning_tfs = self._commissioning_event_timeframes(cfg)
            if len(commissioning_tfs) == 1:
                try:
                    pkg = fn(event_timeframe=str(commissioning_tfs[0]))
                    return True, self._normalize_signal(pkg), "ok"
                except TypeError:
                    pass
                except Exception:
                    return False, {}, "engine_error: event_timeframe_call_failed"

            if len(commissioning_tfs) >= 2:
                tfs = commissioning_tfs
            else:
                tfs = self._decision_timeframes(cfg)

            min_agree = self._decision_min_agree(cfg, len(tfs))
            per_tf: dict[str, Dict[str, Any]] = {}
            for tf in tfs:
                try:
                    pkg = fn(event_timeframe=tf)
                except TypeError:
                    try:
                        pkg = fn()
                    except Exception:
                        continue
                except Exception:
                    continue
                norm = self._normalize_signal(pkg)
                norm["event_timeframe"] = tf
                per_tf[tf] = norm

            if per_tf:
                votes_buy = [tf for tf, p in per_tf.items() if str(p.get("decision")).upper() == "BUY"]
                votes_sell = [tf for tf, p in per_tf.items() if str(p.get("decision")).upper() == "SELL"]
                final_side = "HOLD"
                if len(votes_buy) >= min_agree and len(votes_sell) == 0:
                    final_side = "BUY"
                elif len(votes_sell) >= min_agree and len(votes_buy) == 0:
                    final_side = "SELL"

                chosen_tf = tfs[-1]
                if final_side in ("BUY", "SELL"):
                    agree_tfs = votes_buy if final_side == "BUY" else votes_sell
                    for tf in reversed(tfs):
                        if tf in agree_tfs:
                            chosen_tf = tf
                            break
                chosen = dict(per_tf.get(chosen_tf) or next(iter(per_tf.values())))
                if final_side == "HOLD":
                    chosen["decision"] = "HOLD"
                    chosen["blocked_by"] = ["multi_tf_no_consensus"]
                else:
                    chosen["decision"] = final_side
                    chosen["blocked_by"] = []
                chosen["event_timeframe"] = chosen_tf
                chosen["multi_tf"] = {
                    tf: {
                        "decision": per_tf[tf].get("decision"),
                        "blocked_by": per_tf[tf].get("blocked_by"),
                        "regime": (per_tf[tf].get("metrics") or {}).get("regime") if isinstance(per_tf[tf].get("metrics"), dict) else None,
                        "st_dir": (per_tf[tf].get("metrics") or {}).get("supertrend_dir_event") if isinstance(per_tf[tf].get("metrics"), dict) else None,
                        "align": (per_tf[tf].get("metrics") or {}).get("alignment_score") if isinstance(per_tf[tf].get("metrics"), dict) else None,
                    }
                    for tf in tfs
                    if tf in per_tf
                }
                chosen["decision_votes"] = {"BUY": len(votes_buy), "SELL": len(votes_sell), "min_agree": min_agree}
                return True, chosen, "ok"

            pkg = fn()
            return True, self._normalize_signal(pkg), "ok"
        except Exception as e:
            return False, {}, f"engine_error: {e}"

    def _get_callable(self) -> Callable[..., Dict[str, Any]]:
        with self._lock:
            if self._cached is not None:
                return self._cached

            # Ensure project root in sys.path
            if os.getcwd() not in sys.path:
                sys.path.insert(0, os.getcwd())

            import importlib

            eng = importlib.import_module("engine")

            # Case A: module-level function
            if hasattr(eng, "generate_signal_package") and callable(getattr(eng, "generate_signal_package")):
                self._cached = getattr(eng, "generate_signal_package")
                self._cached_signature = "engine.generate_signal_package (module function)"
                return self._cached

            # Case B: TradingEngine class
            if hasattr(eng, "TradingEngine"):
                TradingEngine = getattr(eng, "TradingEngine")
                if callable(TradingEngine):
                    cfg = self.config_mgr.get()
                    try:
                        obj = TradingEngine(cfg)
                    except TypeError:
                        # Some engines use TradingEngine(config_path=...) or no-arg
                        try:
                            obj = TradingEngine(config_path=self.config_mgr._state.path)  # type: ignore[attr-defined]
                        except Exception:
                            obj = TradingEngine()

                    if hasattr(obj, "generate_signal_package") and callable(getattr(obj, "generate_signal_package")):
                        self._cached = getattr(obj, "generate_signal_package")
                        self._cached_signature = "TradingEngine(cfg).generate_signal_package"
                        return self._cached

                    # fallback method names
                    for name in ("eval_signal", "generate_signal", "run_once", "analyze"):
                        if hasattr(obj, name) and callable(getattr(obj, name)):
                            self._cached = getattr(obj, name)
                            self._cached_signature = f"TradingEngine(cfg).{name}"
                            return self._cached

            raise RuntimeError("engine_callable_not_found")

    @staticmethod
    def _normalize_signal(raw: Any) -> Dict[str, Any]:
        """
        Normalize to mentor_executor expected fields:
          request_id, decision, plan{entry,sl,tp}, confidence?, metrics?, context?, source?
        """
        if not isinstance(raw, dict):
            return {"request_id": f"RAW-{_now_ts()}", "decision": "HOLD", "plan": {"entry": 0.0, "sl": 0.0, "tp": 0.0}, "raw": raw}

        decision = _upper_str(_first_present(raw, ("decision", "action", "signal", "side"), "HOLD"))
        if decision not in ("BUY", "SELL", "HOLD", "NONE"):
            # map common variants
            if decision in ("LONG", "BULL", "UP"):
                decision = "BUY"
            elif decision in ("SHORT", "BEAR", "DOWN"):
                decision = "SELL"
            else:
                decision = "HOLD"

        plan = raw.get("plan", None)
        if not isinstance(plan, dict):
            # some engines nest under dry_run_order/order/signal/result/payload
            for k in ("dry_run_order", "order", "result", "payload", "signal"):
                v = raw.get(k)
                if isinstance(v, dict) and isinstance(v.get("plan"), dict):
                    plan = v.get("plan")
                    break
            if not isinstance(plan, dict):
                plan = {}

        entry = _safe_float(_first_present(plan, ("entry", "price", "open"), 0.0)) or 0.0
        sl = _safe_float(_first_present(plan, ("sl", "stop_loss"), 0.0)) or 0.0
        tp = _safe_float(_first_present(plan, ("tp", "take_profit"), 0.0)) or 0.0

        request_id = raw.get("request_id") or raw.get("id") or f"REQ-{_now_ts()}"
        request_id = str(request_id)[:80]

        out: Dict[str, Any] = {
            "request_id": request_id,
            "decision": "HOLD" if decision == "NONE" else decision,
            "plan": {"entry": entry, "sl": sl, "tp": tp},
        }

        # Optional fields (keep if present)
        for k in (
            "confidence",
            "metrics",
            "blocked_by",
            "context",
            "source",
            "symbol",
            "timeframe",
            "event_timeframe",
            "timeframes",
            "engine_version",
            "bias",
            "status",
            "gates",
            "price",
            "ts",
            "ts_ms",
            "latency_ms",
        ):
            if k in raw:
                out[k] = raw.get(k)

        if isinstance(out.get("price"), dict):
            atr = _safe_float((out.get("price") or {}).get("atr"))
            if atr is not None:
                metrics = out.get("metrics") if isinstance(out.get("metrics"), dict) else {}
                if "atr" not in metrics:
                    metrics = {**metrics, "atr": atr}
                out["metrics"] = metrics

        return out


# ----------------------------
# AI Confirm (confirm-only, pluggable)
# ----------------------------
class AIConfirmer:
    """
    confirm-only policy:
      - LLM confirmation is REQUIRED for execution approval.
      - Local precheck performs non-authoritative validation only (never approves execution).
      - Provider priority: DeepSeek -> OpenAI -> Groq -> Claude -> Gemini
      - If all providers fail, returns approved=False (fail-closed, no silent fallback).
    """

    def __init__(self, config_mgr: ConfigManager):
        self.config_mgr = config_mgr

    def confirm(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        cfg = self.config_mgr.get()
        ai_cfg = cfg.get("ai_confirm", {}) if isinstance(cfg.get("ai_confirm", {}), dict) else {}

        # Non-authoritative precheck (validation metadata only, never approves execution)
        pre = self._local_precheck(payload, ai_cfg)
        if not pre["valid"]:
            return {
                "approved": False,
                "reason": pre["reason"],
                "confidence": None,
                "provider": "validation_failed",
                "model": "",
            }

        # LLM confirmation is REQUIRED
        use_llm = bool(ai_cfg.get("use_llm") is True) or (os.environ.get("AI_CONFIRM_USE_LLM", "0").strip() in ("1", "true", "TRUE", "yes", "YES"))

        if not use_llm:
            return {
                "approved": False,
                "reason": "llm_required",
                "confidence": None,
                "provider": "none",
                "model": "",
            }

        # Try provider priority chain: DeepSeek -> OpenAI -> Groq -> Claude -> Gemini
        ok_llm, llm_out, provider_trace = self._llm_confirm_with_priority(payload, ai_cfg)

        if ok_llm and isinstance(llm_out, dict):
            out = self._sanitize_ai_response(llm_out)
            out["provider"] = str(llm_out.get("provider", "unknown"))[:80]
            out["model"] = str(llm_out.get("model", ""))[:80]
            out["provider_trace"] = provider_trace
            if isinstance(llm_out.get("bullets"), list):
                out["bullets"] = [str(x)[:140] for x in llm_out.get("bullets")[:3]]
            return out

        # All providers failed - DENY (fail-closed)
        return {
            "approved": False,
            "reason": "ai_llm_unavailable",
            "confidence": None,
            "provider": "none",
            "model": "",
            "provider_trace": provider_trace,
        }

    @staticmethod
    def _local_precheck(payload: Dict[str, Any], ai_cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        NON-AUTHORITATIVE validation only.
        NEVER returns approved=True.
        Returns: {"valid": bool, "reason": str}
        """
        decision = _upper_str(payload.get("decision"))
        plan = payload.get("plan", {}) if isinstance(payload.get("plan", {}), dict) else {}
        blocked_by = payload.get("blocked_by")
        if blocked_by is None:
            blocked_by = payload.get("blocked_list") or payload.get("blocked") or []
        if not isinstance(blocked_by, list):
            blocked_by = []

        if blocked_by:
            return {"valid": False, "reason": "blocked_by_present"}

        if decision not in ("BUY", "SELL"):
            return {"valid": False, "reason": "decision_not_trade"}

        entry = _safe_float(plan.get("entry"))
        sl = _safe_float(plan.get("sl"))
        tp = _safe_float(plan.get("tp"))
        if entry is None or sl is None or tp is None:
            return {"valid": False, "reason": "plan_missing"}

        if decision == "BUY" and not (sl < entry < tp):
            return {"valid": False, "reason": "plan_invalid_buy"}
        if decision == "SELL" and not (tp < entry < sl):
            return {"valid": False, "reason": "plan_invalid_sell"}

        return {"valid": True, "reason": "precheck_ok"}

    def _llm_confirm_with_priority(
        self, payload: Dict[str, Any], ai_cfg: Dict[str, Any]
    ) -> Tuple[bool, Dict[str, Any], list]:
        """
        Try AI providers in strict priority order: DeepSeek -> OpenAI -> Groq -> Claude -> Gemini.
        Returns: (success, response_dict, provider_trace)
        provider_trace = [{"provider": "deepseek", "status": "skipped", "reason": "no_api_key"}, ...]
        All API keys are sourced from environment variables only.
        """
        providers = [
            {
                "name": "deepseek",
                "url": "https://api.deepseek.com/v1/chat/completions",
                "key_env": "DEEPSEEK_API_KEY",
                "model": "deepseek-chat",
                "format": "openai",
            },
            {
                "name": "openai",
                "url": "https://api.openai.com/v1/chat/completions",
                "key_env": "OPENAI_API_KEY",
                "model": "gpt-4",
                "format": "openai",
            },
            {
                "name": "groq",
                "url": "https://api.groq.com/openai/v1/chat/completions",
                "key_env": "GROQ_API_KEY",
                "model": "mixtral-8x7b-32768",
                "format": "openai",
            },
            {
                "name": "claude",
                "url": "https://api.anthropic.com/v1/messages",
                "key_env": "CLAUDE_API_KEY",
                "model": "claude-3-sonnet-20240229",
                "format": "claude",
            },
            {
                "name": "gemini",
                "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent",
                "key_env": "GEMINI_API_KEY",
                "model": "gemini-pro",
                "format": "gemini",
            },
        ]

        provider_trace: list = []
        timeout_sec = float(ai_cfg.get("llm_timeout_sec") or ai_cfg.get("timeout_sec") or 8.0)
        max_tokens = int(ai_cfg.get("llm_max_tokens") or 220)
        temperature = float(ai_cfg.get("llm_temperature") or 0.1)
        policy = {"min_rr": ai_cfg.get("min_rr")}

        for p in providers:
            key = os.environ.get(p["key_env"], "").strip()
            if not key:
                provider_trace.append({"provider": p["name"], "status": "skipped", "reason": "no_api_key"})
                continue

            ok, response = self._llm_call_provider(
                p, key, payload, policy, timeout_sec, max_tokens, temperature
            )
            if ok and isinstance(response, dict):
                response["provider"] = p["name"]
                response["model"] = p["model"]
                provider_trace.append({"provider": p["name"], "status": "success", "model": p["model"]})
                return True, response, provider_trace
            else:
                provider_trace.append({
                    "provider": p["name"],
                    "status": "failed",
                    "reason": str(response)[:100] if not ok else "invalid_response",
                })

        return False, {}, provider_trace

    def _llm_call_provider(
        self,
        provider: Dict[str, Any],
        key: str,
        payload: Dict[str, Any],
        policy: Dict[str, Any],
        timeout_sec: float,
        max_tokens: int,
        temperature: float,
    ) -> Tuple[bool, Any]:
        """
        Call a single LLM provider. Handles OpenAI-compatible, Claude, and Gemini API formats.
        Returns (success, parsed_response_dict_or_error_str).
        """
        fmt = provider.get("format", "openai")
        url = provider["url"]
        model = provider["model"]
        user_text = self._build_llm_prompt(payload, policy)
        system_text = "You are a strict risk manager. Return JSON only. No markdown."

        try:
            if fmt == "openai":
                ok, raw = self._llm_http_chat_completions(
                    url=url,
                    api_key=key,
                    model=model,
                    system_text=system_text,
                    user_text=user_text,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    timeout_sec=timeout_sec,
                )
                if not ok:
                    return False, raw
                okj, out = self._extract_llm_json(raw)
                if not okj:
                    return False, "llm_json_parse_failed"
                return True, self._normalize_llm_out(out, model)

            elif fmt == "claude":
                req_payload = {
                    "model": model,
                    "max_tokens": max_tokens,
                    "system": system_text,
                    "messages": [{"role": "user", "content": user_text}],
                }
                data = json.dumps(req_payload, ensure_ascii=False).encode("utf-8")
                req = Request(
                    url=url,
                    data=data,
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": key,
                        "anthropic-version": "2023-06-01",
                    },
                    method="POST",
                )
                with urlopen(req, timeout=timeout_sec) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                try:
                    raw = json.loads(body)
                except Exception:
                    return False, "claude_json_body_parse_failed"
                # Claude response: {"content": [{"type": "text", "text": "..."}]}
                content_blocks = raw.get("content") if isinstance(raw, dict) else None
                if isinstance(content_blocks, list) and content_blocks:
                    text = content_blocks[0].get("text", "") if isinstance(content_blocks[0], dict) else ""
                else:
                    text = ""
                okj, out = self._extract_llm_json(text)
                if not okj:
                    return False, "claude_llm_json_parse_failed"
                return True, self._normalize_llm_out(out, model)

            elif fmt == "gemini":
                gemini_url = f"{url}?key={key}"
                req_payload = {
                    "contents": [{"parts": [{"text": f"{system_text}\n\n{user_text}"}]}],
                    "generationConfig": {
                        "temperature": temperature,
                        "maxOutputTokens": max_tokens,
                    },
                }
                data = json.dumps(req_payload, ensure_ascii=False).encode("utf-8")
                req = Request(
                    url=gemini_url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(req, timeout=timeout_sec) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                try:
                    raw = json.loads(body)
                except Exception:
                    return False, "gemini_json_body_parse_failed"
                # Gemini response: {"candidates": [{"content": {"parts": [{"text": "..."}]}}]}
                candidates = raw.get("candidates") if isinstance(raw, dict) else None
                text = ""
                if isinstance(candidates, list) and candidates:
                    cand = candidates[0]
                    if isinstance(cand, dict):
                        content = cand.get("content", {})
                        if isinstance(content, dict):
                            parts = content.get("parts", [])
                            if isinstance(parts, list) and parts:
                                text = parts[0].get("text", "") if isinstance(parts[0], dict) else ""
                okj, out = self._extract_llm_json(text)
                if not okj:
                    return False, "gemini_llm_json_parse_failed"
                return True, self._normalize_llm_out(out, model)

            else:
                return False, f"unknown_provider_format_{fmt}"

        except Exception as e:
            return False, str(e)[:200]

    @staticmethod
    def _normalize_llm_out(out: Dict[str, Any], model: str) -> Dict[str, Any]:
        """Normalize and clamp LLM JSON output fields."""
        approved = out.get("approved")
        decision = str(out.get("decision") or "").strip().upper()
        if approved is None and decision:
            out["approved"] = decision == "CONFIRM"

        try:
            c = float(out.get("confidence") or 0.0)
            out["confidence"] = max(0.0, min(1.0, c))
        except Exception:
            out["confidence"] = 0.0

        reason = out.get("reason")
        out["reason"] = str(reason or "").strip()[:120]

        bullets = out.get("bullets")
        if isinstance(bullets, list):
            cleaned = []
            for b in bullets:
                if not isinstance(b, str):
                    continue
                words = [w for w in b.strip().split() if w]
                cleaned.append(" ".join(words[:8])[:140])
                if len(cleaned) >= 3:
                    break
            out["bullets"] = cleaned
        else:
            out["bullets"] = []

        out["model"] = model
        return out

    @staticmethod
    def _sanitize_ai_response(raw: Any) -> Dict[str, Any]:
        """
        Normalize external response into:
          {approved: bool, reason: str, confidence: float|None}
        """
        if isinstance(raw, dict):
            approved = raw.get("approved", None)
            reason = raw.get("reason", raw.get("message", ""))
            confidence = raw.get("confidence", None)
            return {
                "approved": bool(approved is True),
                "reason": str(reason)[:500],
                "confidence": _safe_float(confidence),
            }
        return {"approved": False, "reason": "ai_response_invalid", "confidence": None}

    @staticmethod
    def _build_llm_prompt(payload: Dict[str, Any], policy: Dict[str, Any]) -> str:
        decision = _upper_str(payload.get("decision"))
        symbol = str(payload.get("symbol") or payload.get("sym") or "GOLD")[:32]
        plan = payload.get("plan", {}) if isinstance(payload.get("plan", {}), dict) else {}
        metrics = payload.get("metrics", {}) if isinstance(payload.get("metrics", {}), dict) else {}
        blocked_by = payload.get("blocked_by")
        if blocked_by is None:
            blocked_by = payload.get("blocked_list") or payload.get("blocked") or []
        if not isinstance(blocked_by, list):
            blocked_by = []

        entry = _safe_float(plan.get("entry"))
        sl = _safe_float(plan.get("sl"))
        tp = _safe_float(plan.get("tp"))
        rr = _safe_float(metrics.get("rr"))
        regime = str(metrics.get("regime") or metrics.get("regime_candidate") or "")[:24]
        align = metrics.get("alignment_score")
        st_dir = metrics.get("supertrend_dir_event")
        st_dist = _safe_float(metrics.get("supertrend_distance_atr"))
        bbw = _safe_float(metrics.get("bb_width_atr"))
        atr = _safe_float(metrics.get("atr") or metrics.get("atr_event") or payload.get("atr"))
        if atr is None:
            price = payload.get("price", {}) if isinstance(payload.get("price", {}), dict) else {}
            atr = _safe_float(price.get("atr"))
        if atr is None:
            ctx = payload.get("context", {}) if isinstance(payload.get("context", {}), dict) else {}
            event_tf = str(payload.get("event_timeframe") or payload.get("timeframe") or ctx.get("event_timeframe") or "M1").upper()
            tfs = ctx.get("tfs")
            if isinstance(tfs, list):
                for row in tfs:
                    if isinstance(row, dict) and str(row.get("tf") or "").upper() == event_tf:
                        atr = _safe_float(row.get("atr"))
                        break

        min_rr = _safe_float(policy.get("min_rr"))

        def f(x: Any) -> str:
            v = _safe_float(x)
            if v is None:
                return "-"
            return f"{float(v):.4f}"

        blocked_str = ",".join([str(x)[:24] for x in blocked_by[:6]])
        prompt = (
            "TASK: You are the FINAL CONFIRM layer for an automated trading bot.\n"
            "Output JSON only.\n\n"
            "Goal:\n"
            "Approve or deny the locked trade plan.\n\n"
            "Rules:\n"
            f"- symbol={symbol}\n"
            "- direction is LOCKED; do not flip BUY/SELL\n"
            f"- if blocked_by is not empty => deny\n"
            f"- RR must be >= {f(min_rr)} if provided\n"
            "- deny if direction conflicts with supertrend_dir_event OR RR fails OR blocked_by not empty\n"
            "- keep explanation concise to reduce token cost\n\n"
            "Output JSON schema:\n"
            "{\n"
            '  \"approved\": true|false,\n'
            '  \"confidence\": 0.0-1.0,\n'
            '  \"reason\": \"short <=120 chars\",\n'
            '  \"bullets\": [\"<=3 concise technical suggestions\"]\n'
            "}\n\n"
            "Guidelines for bullets:\n"
            "- short technical advice only\n"
            "- max 8 words each\n"
            "- no long sentences\n\n"
            "Input snapshot:\n"
            f"- decision={decision}\n"
            f"- plan: entry={f(entry)} sl={f(sl)} tp={f(tp)}\n"
            f"- atr={f(atr)} rr={f(rr)} regime={regime or '-'} alignment={str(align)[:8]}\n"
            f"- supertrend_dir_event={str(st_dir)[:8]}\n"
            f"- supertrend_distance_atr={f(st_dist)}\n"
            f"- bb_width_atr={f(bbw)}\n"
            f"- blocked_by={blocked_str or '-'}\n"
        )
        return prompt

    @staticmethod
    def _llm_http_chat_completions(
        *,
        url: str,
        api_key: str,
        model: str,
        system_text: str,
        user_text: str,
        temperature: float,
        max_tokens: int,
        timeout_sec: float,
    ) -> Tuple[bool, Any]:
        try:
            req_payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": system_text},
                    {"role": "user", "content": user_text},
                ],
                "temperature": float(temperature),
                "max_tokens": int(max_tokens),
            }
            data = json.dumps(req_payload, ensure_ascii=False).encode("utf-8")
            req = Request(
                url=url,
                data=data,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
                method="POST",
            )
            with urlopen(req, timeout=timeout_sec) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            try:
                return True, json.loads(body)
            except Exception:
                return True, body
        except Exception as e:
            return False, str(e)

    @staticmethod
    def _extract_llm_json(raw: Any) -> Tuple[bool, Dict[str, Any]]:
        if isinstance(raw, dict):
            try:
                choices = raw.get("choices")
                if isinstance(choices, list) and choices:
                    msg = choices[0].get("message", {})
                    if isinstance(msg, dict):
                        content = msg.get("content")
                        if isinstance(content, str) and content.strip():
                            s = content.strip()
                            try:
                                return True, json.loads(s)
                            except Exception:
                                pass

                            if s.startswith("```"):
                                s2 = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
                                s2 = re.sub(r"\s*```$", "", s2)
                                s2 = s2.strip()
                                try:
                                    return True, json.loads(s2)
                                except Exception:
                                    pass

                            m = re.search(r"\{[\s\S]*\}", s)
                            if m:
                                try:
                                    return True, json.loads(m.group(0))
                                except Exception:
                                    pass
            except Exception:
                pass
        if isinstance(raw, str):
            s = raw.strip()
            if not s:
                return False, {}
            try:
                return True, json.loads(s)
            except Exception:
                if s.startswith("```"):
                    s2 = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
                    s2 = re.sub(r"\s*```$", "", s2)
                    s2 = s2.strip()
                    try:
                        return True, json.loads(s2)
                    except Exception:
                        pass

                m = re.search(r"\{[\s\S]*\}", s)
                if m:
                    try:
                        return True, json.loads(m.group(0))
                    except Exception:
                        pass
                return False, {}
        return False, {}

    @staticmethod
    def _proxy_confirm(url: str, payload: Dict[str, Any], timeout_sec: float) -> Tuple[bool, Any]:
        """
        Best-effort HTTP POST JSON without extra deps.
        Uses urllib to avoid requests dependency assumptions.
        """
        try:
            import urllib.request

            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                url=url,
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                body = resp.read().decode("utf-8", errors="replace")
            try:
                return True, json.loads(body)
            except Exception:
                return True, {"approved": False, "reason": "ai_proxy_non_json", "raw": body[:800]}
        except Exception:
            return False, None


# ----------------------------
# Flask App
# ----------------------------
app = Flask(__name__)

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

_base_dir = os.path.dirname(os.path.abspath(__file__))
load_env_file(os.path.join(_base_dir, ".env"), override=True)

_config_path = os.environ.get("HIM_CONFIG_PATH", DEFAULT_CONFIG_PATH).strip() or DEFAULT_CONFIG_PATH
_host = os.environ.get("HIM_HOST", DEFAULT_HOST).strip() or DEFAULT_HOST
_port = int(os.environ.get("HIM_PORT", str(DEFAULT_PORT)))

config_mgr = ConfigManager(_config_path)
engine_adapter = EngineAdapter(config_mgr)
ai_confirmer = AIConfirmer(config_mgr)


def _audit(event: str, **fields: Any) -> None:
    _append_jsonl(API_AUDIT_LOG, {"ts": _now_ts(), "event": event, "version": APP_VERSION, **fields})


@app.route("/", methods=["GET"])
def root():
    cfg = config_mgr.get()
    # If dashboard external url provided, redirect root too (optional behavior)
    ext = None
    try:
        ext = cfg.get("dashboard", {}).get("external_url")
    except Exception:
        ext = None
    if isinstance(ext, str) and ext.strip():
        return redirect(ext.strip(), code=302)
    return _ok(
        {
            "service": "HIM API Server",
            "version": APP_VERSION,
            "endpoints": [
                "GET /api/health",
                "GET /api/status",
                "GET /api/config",
                "POST /api/config",
                "GET /api/signal_preview",
                "POST /api/ai_confirm",
            ],
        }
    )


@app.route("/dashboard", methods=["GET"])
def dashboard():
    cfg = config_mgr.get()
    ext = None
    try:
        ext = cfg.get("dashboard", {}).get("external_url")
    except Exception:
        ext = None
    if isinstance(ext, str) and ext.strip():
        return redirect(ext.strip(), code=302)
    # local dashboard removed (per v3 note): return informative JSON
    return _err(
        code="dashboard_removed",
        message="Local dashboard removed; set config.dashboard.external_url to redirect.",
        http_status=404,
    )


@app.route("/api/health", methods=["GET"])
def api_health():
    # minimal health check
    _audit("api_health")
    return _ok({"status": "healthy", "host": _host, "port": _port, "config_path": _config_path})


@app.route("/api/status", methods=["GET"])
def api_status():
    cfg = config_mgr.get()
    # Try quick engine callable detection without generating signal
    engine_ok = True
    engine_sig = ""
    try:
        fn = engine_adapter._get_callable()  # noqa
        engine_sig = getattr(engine_adapter, "_cached_signature", "")  # noqa
        _ = fn
    except Exception as e:
        engine_ok = False
        engine_sig = f"engine_unavailable: {e}"

    out = {
        "service": "HIM API Server",
        "host": _host,
        "port": _port,
        "config_path": _config_path,
        "engine": {"ok": engine_ok, "binding": engine_sig},
        "ai_confirm": {
            "mode": "proxy" if (isinstance(cfg.get("ai_confirm", {}), dict) and (cfg.get("ai_confirm", {}).get("external_url") or cfg.get("ai_confirm", {}).get("api_url"))) else "local_policy",
        },
    }
    _audit("api_status", engine_ok=engine_ok)
    return _ok(out)


@app.route("/api/config", methods=["GET"])
def api_get_config():
    cfg = config_mgr.get()
    _audit("api_get_config")
    return _ok(cfg)


@app.route("/api/config", methods=["POST", "OPTIONS"])
def api_set_config():
    if request.method == "OPTIONS":
        return "", 204

    try:
        d = request.get_json(force=True, silent=False)
    except Exception:
        return _err("invalid_json", "Body must be valid JSON object", 400)

    ok, reason = config_mgr.set(d if isinstance(d, dict) else {})
    _audit("api_set_config", ok=ok, reason=reason)
    if not ok:
        return _err("config_write_failed", reason, 500)
    return _ok({"status": "saved", "config_path": _config_path})


@app.route("/api/signal_preview", methods=["GET"])
def api_signal_preview():
    t0 = time.time()
    event_tf = request.args.get("event_timeframe") or request.args.get("tf")
    ok, sig, reason = engine_adapter.generate_signal_package(event_timeframe_override=event_tf)

    latency_ms = int((time.time() - t0) * 1000)
    _audit(
        "api_signal_preview",
        ok=ok,
        reason=reason,
        latency_ms=latency_ms,
        request_id=sig.get("request_id") if isinstance(sig, dict) else None,
    )

    if not ok:
        return _err("engine_failed", reason, 500, latency_ms=latency_ms)

    # ensure minimal schema exists
    if not isinstance(sig, dict) or "request_id" not in sig or "decision" not in sig or "plan" not in sig:
        return _err("signal_schema_invalid", "engine output missing required fields", 500, raw_type=str(type(sig)))

    # Return "signal object" directly (mentor_executor expects raw signal JSON)
    sig["api_meta"] = {"latency_ms": latency_ms, "version": APP_VERSION}
    return jsonify(sig), 200


@app.route("/api/ai_confirm", methods=["POST", "OPTIONS"])
def api_ai_confirm():
    if request.method == "OPTIONS":
        return "", 204

    try:
        payload = request.get_json(force=True, silent=False)
    except Exception:
        return _err("invalid_json", "Body must be valid JSON object", 400)

    if not isinstance(payload, dict):
        return _err("invalid_payload", "Payload must be JSON object", 400)

    request_id = str(payload.get("request_id") or payload.get("id") or "")[:80]
    if not request_id:
        # fail-closed
        _audit("api_ai_confirm", ok=False, reason="request_id_missing")
        return jsonify({"approved": False, "reason": "request_id_missing", "confidence": None}), 200

    t0 = time.time()
    ai = ai_confirmer.confirm(payload)
    latency_ms = int((time.time() - t0) * 1000)

    # enforce confirm-only schema
    out = {
        "approved": bool(ai.get("approved") is True),
        "reason": str(ai.get("reason", ""))[:500],
        "confidence": _safe_float(ai.get("confidence")),
        "request_id": request_id,
        "api_meta": {"latency_ms": latency_ms, "version": APP_VERSION},
    }
    if isinstance(ai.get("bullets"), list):
        out["bullets"] = [str(x)[:140] for x in ai.get("bullets")[:3]]
    if isinstance(ai.get("provider"), str):
        out["provider"] = str(ai.get("provider"))[:80]
    if isinstance(ai.get("model"), str):
        out["model"] = str(ai.get("model"))[:80]

    _audit(
        "api_ai_confirm",
        ok=True,
        approved=out["approved"],
        latency_ms=latency_ms,
        request_id=request_id,
        reason=out["reason"][:120],
    )

    return jsonify(out), 200


# ----------------------------
# Error handlers (consistent JSON)
# ----------------------------
@app.errorhandler(404)
def not_found(e):
    _audit("http_404", path=request.path)
    return _err("not_found", f"Route not found: {request.path}", 404)


@app.errorhandler(500)
def internal_error(e):
    _audit("http_500", path=request.path)
    return _err("internal_error", "Internal server error", 500)


def _startup_log():
    note = "Local dashboard removed; redirect to config.dashboard.external_url"
    _append_jsonl(
        API_AUDIT_LOG,
        {
            "ts": _now_ts(),
            "msg": "api_server_start",
            "version": APP_VERSION,
            "host": _host,
            "port": _port,
            "config_path": _config_path,
            "note": note,
        },
    )
    _telegram_send_startup(
        "HIM API STARTED\n"
        f"host={_host}\n"
        f"port={_port}\n"
        f"version={APP_VERSION}\n"
        f"config={_config_path}"
    )


if __name__ == "__main__":
    _startup_log()
    # Flask dev server (local). For true production WSGI, serve app via gunicorn/waitress.
    app.run(host=_host, port=_port, debug=False, threaded=True)
