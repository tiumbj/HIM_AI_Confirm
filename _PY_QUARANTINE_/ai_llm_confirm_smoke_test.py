from __future__ import annotations

import json
import os
from typing import Any, Dict, Tuple
from urllib.error import HTTPError
from urllib.request import Request, urlopen


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


def post_chat_completions(*, url: str, api_key: str, payload: Dict[str, Any], timeout_sec: float) -> Tuple[bool, str]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        return True, body
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return False, f"HTTPError status={getattr(e,'code',None)} body={body[:1200]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)[:300]}"


def main() -> int:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    load_env_file(os.path.join(base_dir, ".env"), override=True)

    api_key = (os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("OPENAI_API_KEY") or os.environ.get("AI_API_KEY") or "").strip()
    if not api_key:
        print("missing_api_key")
        return 2

    url = (os.environ.get("AI_API_URL") or "https://api.deepseek.com/v1/chat/completions").strip()
    model = (os.environ.get("AI_MODEL") or "deepseek-chat").strip()

    prompt = (
        "TASK: You are the FINAL CONFIRM layer for an automated trading bot.\n"
        "Output JSON only.\n\n"
        "Goal:\n"
        "Approve or deny the locked trade plan and return a confirmed entry/sl/tp.\n\n"
        "Rules:\n"
        "- symbol=GOLD\n"
        "- direction is LOCKED; do not flip BUY/SELL\n"
        "- if blocked_by is not empty => deny\n"
        "- entry shift <= 0.2000 * ATR; if ATR missing, do not shift entry\n"
        "- SL tighten-only=true; never widen risk\n"
        "- RR must be >= 1.6000 if provided\n"
        "- deny if thesis is broken or TP unrealistic for regime/volatility\n"
        "- keep explanation concise to reduce token cost\n\n"
        "Output JSON schema:\n"
        "{\n"
        '  "approved": true|false,\n'
        '  "confidence": 0.0-1.0,\n'
        '  "reason": "short <=120 chars",\n'
        '  "bullets": ["<=3 concise technical suggestions"],\n'
        '  "confirmed_plan":{"entry":number,"sl":number,"tp":number}\n'
        "}\n\n"
        "Guidelines for bullets:\n"
        "- short technical advice only\n"
        "- max 8 words each\n"
        "- no long sentences\n\n"
        "Input snapshot:\n"
        "- decision=BUY\n"
        "- plan: entry=2034.9000 sl=2030.7000 tp=2045.3000\n"
        "- atr=3.2000 rr=1.6000 regime=EXPANSION alignment=2\n"
        "- supertrend_dir_event=-1\n"
        "- supertrend_distance_atr=2.4000\n"
        "- bb_width_atr=5.6000\n"
        "- blocked_by=-\n"
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a strict risk manager. Return JSON only. No markdown."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.1,
        "max_tokens": 220,
    }

    ok, body = post_chat_completions(url=url, api_key=api_key, payload=payload, timeout_sec=15.0)
    if not ok:
        print("llm_call_failed:", body)
        return 1

    try:
        js = json.loads(body)
        content = js["choices"][0]["message"]["content"]
    except Exception:
        print("llm_response_not_parseable:", body[:1200])
        return 1

    print(content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
