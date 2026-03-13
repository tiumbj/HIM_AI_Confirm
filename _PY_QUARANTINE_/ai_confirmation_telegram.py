"""
AI Confirmation with Telegram Alert
Version: 1.2.0 - Fixed timeout + retry logic
"""

import os
import json
import time
import requests
import re
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import argparse

# โหลด environment variables
load_dotenv()

class AIConfirmationBot:
    def __init__(self):
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        
        if not all([self.api_key, self.telegram_token, self.telegram_chat_id]):
            print("❌ Missing environment variables:")
            print(f"  DEEPSEEK_API_KEY: {'✅' if self.api_key else '❌'}")
            print(f"  TELEGRAM_BOT_TOKEN: {'✅' if self.telegram_token else '❌'}")
            print(f"  TELEGRAM_CHAT_ID: {'✅' if self.telegram_chat_id else '❌'}")
            raise ValueError("Please check your .env file")
    
    def extract_json(self, text):
        """Extract JSON from text that might have markdown code blocks"""
        # ลอง pattern ต่างๆ
        patterns = [
            r'```json\n(.*?)\n```',  # ```json ... ```
            r'```\n(.*?)\n```',      # ``` ... ```
            r'\{.*\}',                 # { ... } (json object)
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.DOTALL)
            if match:
                json_str = match.group(1) if pattern != r'\{.*\}' else match.group(0)
                try:
                    return json.loads(json_str)
                except:
                    continue
        
        # ถ้าไม่เจอ pattern ลองทั้ง string
        try:
            return json.loads(text)
        except:
            return None
    
    def send_telegram(self, message, parse_mode="HTML"):
        """ส่งข้อความไปยัง Telegram"""
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        data = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": parse_mode
        }
        
        try:
            r = requests.post(url, json=data, timeout=10)
            if r.status_code == 200:
                print(f"✅ Telegram sent")
                return True
            else:
                print(f"❌ Telegram error: {r.text}")
                return False
        except Exception as e:
            print(f"❌ Telegram failed: {e}")
            return False
    
    def format_telegram_message(self, trade_data, ai_response):
        """จัดรูปแบบข้อความสำหรับ Telegram"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # ส่วนหัว
        message = f"""
<b>🤖 AI TRADE CONFIRMATION</b>
<i>{timestamp}</i>

<b>📊 TRADE DETAILS</b>
"""
        # รายละเอียดการเทรด
        message += f"""
Symbol: {trade_data.get('symbol', 'GOLD')}
Direction: <b>{trade_data.get('direction', 'NONE')}</b>
Entry: <b>{trade_data.get('entry', 0):.2f}</b>
Stop Loss: <b>{trade_data.get('sl', 0):.2f}</b>
Take Profit: <b>{trade_data.get('tp', 0):.2f}</b>
RR Ratio: <b>{trade_data.get('rr', 0):.2f}</b>
"""
        
        # Market Context (ถ้ามี)
        if trade_data.get('context'):
            message += f"""
<b>📈 MARKET CONTEXT</b>
HTF: {trade_data['context'].get('HTF_trend', 'N/A')}
MTF: {trade_data['context'].get('MTF_trend', 'N/A')}
LTF: {trade_data['context'].get('LTF_trend', 'N/A')}
BOS: {trade_data['context'].get('bos', False)}
SuperTrend: {trade_data['context'].get('supertrend_dir', 'N/A')}
Proximity: {trade_data['context'].get('proximity_score', 0):.1f}%
"""
        
        # AI Analysis
        action_emoji = {
            "APPROVE": "✅",
            "REJECT": "❌",
            "NEUTRAL": "🤔"
        }.get(ai_response.get('action', 'NEUTRAL'), "🤔")
        
        risk_emoji = {
            "HIGH": "🔴",
            "MEDIUM": "🟡",
            "LOW": "🟢"
        }.get(ai_response.get('risk', 'MEDIUM'), "⚪")
        
        message += f"""
<b>🤖 AI ANALYSIS</b>
{action_emoji} Action: <b>{ai_response.get('action', 'NONE')}</b>
Confidence: <b>{ai_response.get('confidence', 0)}%</b>

<b>📝 REASON</b>
{ai_response.get('reason', 'N/A')}

<b>⚠️ RISK ASSESSMENT</b>
{risk_emoji} {ai_response.get('risk', 'N/A')}

<b>✅ RECOMMENDATION</b>
{ai_response.get('recommendation', 'N/A')}
"""
        
        return message
    
    def ask_ai_with_retry(self, trade_data, max_retries=3, timeout=30):
        """ส่งข้อมูลให้ AI วิเคราะห์ พร้อม retry และ timeout ที่นานขึ้น"""
        
        for attempt in range(max_retries):
            try:
                print(f"\n🔄 Attempt {attempt + 1}/{max_retries}...")
                
                # คำนวณ RR ให้ชัดเจน
                rr = trade_data.get('rr', 0)
                
                # สร้าง prompt สำหรับ AI
                prompt = f"""You are an AI Trading Mentor. Analyze this trade setup:

Symbol: {trade_data.get('symbol', 'GOLD')}
Direction: {trade_data.get('direction', 'NONE')}
Entry: {trade_data.get('entry', 0)}
Stop Loss: {trade_data.get('sl', 0)}
Take Profit: {trade_data.get('tp', 0)}
Risk/Reward: {rr:.2f}

Market Context:
{trade_data.get('context', 'No context provided')}

Rules - IMPORTANT:
1. Return ONLY valid JSON object
2. NO markdown formatting, NO backticks
3. NO explanations outside JSON
4. Use this exact format:
{{
    "action": "APPROVE" or "REJECT" or "NEUTRAL",
    "confidence": 0-100,
    "reason": "brief technical reason (1 sentence)",
    "risk": "HIGH" or "MEDIUM" or "LOW",
    "recommendation": "execute" or "wait" or "skip"
}}

Analyze based on:
- Risk/Reward ratio (target >=2.0 is good, {rr:.2f} is {'good' if rr >= 2 else 'poor'})
- Market alignment with context
- Technical confluence
- Current price action
- BOS confirmation
- Proximity score
"""
                
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                }
                
                data = {
                    "model": "deepseek-chat",
                    "messages": [
                        {"role": "system", "content": "You are a strict JSON-only trading mentor. Never use markdown formatting in your responses. Return ONLY raw JSON object."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 300
                }
                
                # เพิ่ม timeout เป็น 30 วินาที
                r = requests.post(
                    "https://api.deepseek.com/v1/chat/completions",
                    headers=headers,
                    json=data,
                    timeout=timeout
                )
                
                if r.status_code == 200:
                    result = r.json()
                    ai_reply = result['choices'][0]['message']['content']
                    
                    print(f"\n📨 Raw AI response:")
                    print(ai_reply)
                    print()
                    
                    # Extract JSON
                    ai_response = self.extract_json(ai_reply)
                    
                    if ai_response:
                        print(f"✅ JSON parsed successfully")
                        return ai_response
                    else:
                        print(f"❌ Could not parse JSON from response")
                        
                else:
                    print(f"❌ AI API error: {r.status_code}")
                    print(r.text)
                    
            except requests.exceptions.Timeout:
                print(f"⏱️ Timeout on attempt {attempt + 1}")
            except requests.exceptions.ConnectionError:
                print(f"🔌 Connection error on attempt {attempt + 1}")
            except Exception as e:
                print(f"❌ Error: {e}")
            
            # รอก่อน retry (ยกเว้นครั้งสุดท้าย)
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # exponential backoff: 1, 2, 4, ...
                print(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
        
        print(f"❌ All {max_retries} attempts failed")
        return {
            "action": "NEUTRAL",
            "confidence": 0,
            "reason": "AI service unavailable after retries",
            "risk": "HIGH",
            "recommendation": "wait"
        }
    
    def manual_input(self):
        """รับข้อมูลจาก manual input"""
        print("\n📝 Manual Trade Input")
        print("-" * 40)
        
        trade_data = {
            "symbol": input("Symbol (default GOLD): ").upper() or "GOLD",
            "direction": input("Direction (BUY/SELL): ").upper(),
            "entry": float(input("Entry price: ")),
            "sl": float(input("Stop Loss: ")),
            "tp": float(input("Take Profit: ")),
            "rr": 0.0
        }
        
        # คำนวณ RR
        if trade_data["direction"] == "BUY":
            risk = trade_data["entry"] - trade_data["sl"]
            reward = trade_data["tp"] - trade_data["entry"]
        else:
            risk = trade_data["sl"] - trade_data["entry"]
            reward = trade_data["entry"] - trade_data["tp"]
        
        trade_data["rr"] = round(reward / risk, 2) if risk > 0 else 0
        
        # Optional context
        add_context = input("\nAdd market context? (y/n): ").lower()
        if add_context == 'y':
            trade_data["context"] = {
                "HTF_trend": input("HTF trend (bullish/bearish/ranging): "),
                "MTF_trend": input("MTF trend (bullish/bearish/ranging): "),
                "LTF_trend": input("LTF trend (bullish/bearish/ranging): "),
                "bos": input("BOS? (true/false): ").lower() == 'true',
                "supertrend_dir": input("SuperTrend direction: "),
                "proximity_score": float(input("Proximity score (0-100): "))
            }
        
        return trade_data
    
    def auto_from_engine(self):
        """อ่านข้อมูลจาก engine output"""
        try:
            # รัน engine และอ่านผล
            import subprocess
            result = subprocess.run(
                ["python", "engine.py"], 
                capture_output=True, 
                text=True,
                timeout=10
            )
            
            # Parse JSON output
            for line in result.stdout.split('\n'):
                if line.strip().startswith('{'):
                    pkg = json.loads(line)
                    
                    trade_data = {
                        "symbol": pkg.get("symbol", "GOLD"),
                        "direction": pkg.get("direction", "NONE"),
                        "entry": pkg.get("entry_candidate", 0),
                        "sl": pkg.get("stop_candidate", 0),
                        "tp": pkg.get("tp_candidate", 0),
                        "rr": pkg.get("rr", 0),
                        "context": pkg.get("context", {})
                    }
                    return trade_data
            
            print("❌ No valid engine output found")
            return None
            
        except Exception as e:
            print(f"❌ Failed to read engine: {e}")
            return None
    
    def run_manual_mode(self):
        """โหมด manual input"""
        print("\n" + "="*60)
        print("🤖 AI CONFIRMATION BOT - MANUAL MODE")
        print("="*60)
        
        # รับ input
        trade_data = self.manual_input()
        
        print("\n📊 Trade Summary:")
        print(json.dumps(trade_data, indent=2, ensure_ascii=False))
        
        # Ask AI with retry
        ai_response = self.ask_ai_with_retry(trade_data)
        
        # Send Telegram
        message = self.format_telegram_message(trade_data, ai_response)
        self.send_telegram(message)
        
        # Show result
        print(f"\n✅ Process complete!")
        print(f"AI Decision: {ai_response.get('action')} ({ai_response.get('confidence')}%)")
        
    def run_auto_mode(self, interval=60):
        """โหมด auto - อ่านจาก engine ทุก interval วินาที"""
        print("\n" + "="*60)
        print(f"🤖 AI CONFIRMATION BOT - AUTO MODE (interval: {interval}s)")
        print("="*60)
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                # อ่านจาก engine
                trade_data = self.auto_from_engine()
                
                if trade_data and trade_data["direction"] != "NONE":
                    print(f"\n🔄 Found signal: {trade_data['direction']} {trade_data['symbol']}")
                    
                    # Ask AI with retry
                    ai_response = self.ask_ai_with_retry(trade_data)
                    
                    # Send Telegram
                    message = self.format_telegram_message(trade_data, ai_response)
                    self.send_telegram(message)
                    
                    print(f"✅ AI: {ai_response.get('action')} ({ai_response.get('confidence')}%)")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n\n👋 Stopped by user")
    
    def run_single_check(self):
        """ตรวจสอบ engine ครั้งเดียว"""
        trade_data = self.auto_from_engine()
        
        if not trade_data:
            print("❌ No trade data from engine")
            return
        
        if trade_data["direction"] == "NONE":
            print("ℹ️ No signal from engine")
            return
        
        print(f"\n📊 Found signal: {trade_data['direction']} {trade_data['symbol']}")
        
        # Ask AI with retry
        ai_response = self.ask_ai_with_retry(trade_data)
        
        # Send Telegram
        message = self.format_telegram_message(trade_data, ai_response)
        self.send_telegram(message)
        
        print("\n✅ Done!")

def main():
    parser = argparse.ArgumentParser(description='AI Confirmation Bot with Telegram')
    parser.add_argument('--mode', choices=['manual', 'auto', 'single'], 
                       default='manual', help='Run mode')
    parser.add_argument('--interval', type=int, default=60,
                       help='Auto mode interval (seconds)')
    
    args = parser.parse_args()
    
    try:
        bot = AIConfirmationBot()
        
        if args.mode == 'manual':
            bot.run_manual_mode()
        elif args.mode == 'auto':
            bot.run_auto_mode(args.interval)
        else:  # single
            bot.run_single_check()
            
    except KeyboardInterrupt:
        print("\n\n👋 Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()