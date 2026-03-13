"""
telegram_notifier.py
Version: 1.1.0
Changelog:
- 1.1.0:
  - load_dotenv override=True (use .env as source of truth for commissioning)
  - send_text supports parse_mode=None (omit parse_mode)
  - add send_text_debug() to return Telegram HTTP status + response body
Rules:
- Never freeze silently: return False + log on any failure
- config.json stores ENV KEY NAMES (token_env/chat_id_env), not raw secrets
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Dict, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore


logger = logging.getLogger("HIM")


class TelegramNotifier:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.base_dir = os.path.dirname(os.path.abspath(config_path))

        env_path = os.path.join(self.base_dir, ".env")
        self._load_env(env_path, override=True)

    @staticmethod
    def _load_env(env_path: str, override: bool) -> None:
        try:
            if load_dotenv is not None:
                load_dotenv(env_path, override=override)
                return

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

    @staticmethod
    def _post_json(url: str, payload: Dict[str, Any], timeout: float) -> Tuple[int, str]:
        if requests is not None:
            resp = requests.post(url, json=payload, timeout=timeout)
            return int(getattr(resp, "status_code", 0) or 0), str(getattr(resp, "text", "") or "")

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = Request(url, method="POST", data=data, headers={"Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                status = int(getattr(resp, "status", 0) or 0)
                return status, body
        except HTTPError as e:
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                body = str(e)
            return int(getattr(e, "code", 0) or 0), body
        except (URLError, TimeoutError) as e:
            return 0, str(e)

    def _load_config(self) -> Dict[str, Any]:
        try:
            if not os.path.exists(self.config_path):
                return {}
            with open(self.config_path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception as e:
            logger.error(f"CRITICAL: TelegramNotifier config load failed: {e}")
            return {}

    def _resolve_credentials(self) -> tuple[bool, Optional[str], Optional[str], list[str]]:
        cfg = self._load_config()
        tg = (cfg.get("telegram") or {}) if isinstance(cfg, dict) else {}

        enabled = bool(tg.get("enabled", False))
        notify_on = tg.get("notify_on", ["signal", "trade", "error"])
        if not isinstance(notify_on, list):
            notify_on = ["signal", "trade", "error"]

        token_env = tg.get("token_env")
        chat_id_env = tg.get("chat_id_env")

        token = os.getenv(str(token_env)) if token_env else None
        chat_id = os.getenv(str(chat_id_env)) if chat_id_env else None

        if not token:
            token = (
                os.getenv("TELEGRAM_BOT_TOKEN_TRADE")
                or os.getenv("TELEGRAM_BOT_TOKEN")
                or os.getenv("TELEGRAM_TOKEN")
                or os.getenv("TELEGRAM_API_TOKEN")
            )
        if not chat_id:
            chat_id = os.getenv("TELEGRAM_TRADE_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID") or os.getenv("CHAT_ID")

        return enabled, token, chat_id, notify_on

    def send_text_debug(
        self,
        text: str,
        event_type: str = "signal",
        parse_mode: Optional[str] = None,
    ) -> Tuple[bool, int, str]:
        """
        Returns: (ok, http_status, response_text)
        """
        try:
            enabled, token, chat_id, notify_on = self._resolve_credentials()

            if not enabled:
                return False, 0, "telegram.enabled=false"

            if event_type not in notify_on:
                return False, 0, f"event_type={event_type} not in notify_on={notify_on}"

            if not token or not chat_id:
                return False, 0, "missing token/chat_id (check .env + config.json token_env/chat_id_env)"

            url = f"https://api.telegram.org/bot{token}/sendMessage"

            payload = {"chat_id": chat_id, "text": text}
            if parse_mode:
                payload["parse_mode"] = parse_mode

            status, body = self._post_json(url=url, payload=payload, timeout=10)
            ok = (status == 200)
            return ok, status, body

        except Exception as e:
            return False, 0, f"exception: {e}"

    def send_text(self, text: str, event_type: str = "signal", parse_mode: Optional[str] = None) -> bool:
        ok, status, body = self.send_text_debug(text=text, event_type=event_type, parse_mode=parse_mode)
        if not ok:
            logger.warning(f"Telegram send failed: status={status} body={body[:400]}")
        return ok

    def get_updates_debug(self, limit: int = 20) -> Tuple[bool, int, str]:
        try:
            enabled, token, _, _ = self._resolve_credentials()
            if not enabled:
                return False, 0, "telegram.enabled=false"
            if not token:
                return False, 0, "missing token (check .env + config.json token_env)"

            url = f"https://api.telegram.org/bot{token}/getUpdates?limit={int(limit)}"
            req = Request(url, method="GET")
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
                return False, int(getattr(e, "code", 0) or 0), body
            except (URLError, TimeoutError) as e:
                return False, 0, str(e)
        except Exception as e:
            return False, 0, f"exception: {e}"

    def list_chat_ids(self, limit: int = 20) -> Tuple[bool, list[str], str]:
        ok, status, body = self.get_updates_debug(limit=limit)
        if not ok:
            return False, [], f"status={status} body={body[:400]}"
        try:
            data = json.loads(body)
            results = data.get("result", []) if isinstance(data, dict) else []
            out: list[str] = []
            if isinstance(results, list):
                for upd in results:
                    if not isinstance(upd, dict):
                        continue
                    msg = upd.get("message") or upd.get("channel_post") or {}
                    if not isinstance(msg, dict):
                        continue
                    chat = msg.get("chat", {})
                    if not isinstance(chat, dict):
                        continue
                    cid = chat.get("id", None)
                    if cid is None:
                        continue
                    out.append(str(cid))
            dedup = sorted(set(out))
            return True, dedup, "ok"
        except Exception as e:
            return False, [], f"parse_error: {e}"
