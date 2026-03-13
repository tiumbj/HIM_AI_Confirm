"""
mentor_telegram_sender.py
Version: v1.0.1

Send mentor analysis messages to a separate Telegram chat
without touching the autotrade notification system.
"""

import os
import requests
import time

from dotenv import load_dotenv


# load .env
load_dotenv()


class MentorTelegramSender:

    def __init__(self):

        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_MENTOR_CHAT_ID")

        if not self.token:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN")

        if not self.chat_id:
            raise ValueError("Missing TELEGRAM_MENTOR_CHAT_ID")

        self.url = f"https://api.telegram.org/bot{self.token}/sendMessage"


    def send(self, message: str):

        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML"
        }

        try:

            r = requests.post(
                self.url,
                json=payload,
                timeout=10
            )

            if r.status_code != 200:
                print("Mentor telegram failed:", r.text)

        except Exception as e:

            print("Mentor telegram error:", e)


def build_mentor_message(data: dict) -> str:

    symbol = data.get("symbol", "UNKNOWN")
    direction = data.get("direction", "NONE")
    entry = data.get("entry", "-")
    sl = data.get("sl", "-")
    tp = data.get("tp", "-")
    rr = data.get("rr", "-")
    confidence = data.get("confidence", "-")

    msg = f"""
<b>MENTOR ANALYSIS</b>

Symbol: {symbol}

Direction: {direction}

Entry: {entry}
SL: {sl}
TP: {tp}

RR: {rr}

Confidence: {confidence}

Time: {time.strftime('%Y-%m-%d %H:%M:%S')}
"""

    return msg


if __name__ == "__main__":

    sender = MentorTelegramSender()

    test_data = {
        "symbol": "GOLD",
        "direction": "SELL",
        "entry": 2341.80,
        "sl": 2344.20,
        "tp": 2336.90,
        "rr": 2.4,
        "confidence": 0.63
    }

    msg = build_mentor_message(test_data)

    sender.send(msg)

    print("Mentor message sent")