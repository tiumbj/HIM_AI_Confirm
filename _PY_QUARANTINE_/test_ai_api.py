import os
import requests
import json
from dotenv import load_dotenv
from pathlib import Path

# โหลด .env
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

def get_api_key():
    """อ่าน API key จาก .env"""
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        # ลองชื่ออื่นที่อาจใช้
        api_key = os.getenv("OPENAI_API_KEY")
    return api_key

def test_connection():
    """ทดสอบการเชื่อมต่อ API อย่างง่าย"""
    api_key = get_api_key()
    
    if not api_key:
        print("❌ ไม่พบ API key ใน .env")
        print("   ตรวจสอบว่ามี: DEEPSEEK_API_KEY=your-key-here")
        return False
    
    print(f"✅ พบ API key: {api_key[:5]}...{api_key[-5:]}")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # ทดสอบแบบง่ายๆ
    data = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": "You are a trading mentor."},
            {"role": "user", "content": "Say 'OK' if you can hear me."}
        ],
        "max_tokens": 10,
        "temperature": 0.0
    }
    
    try:
        print("🔄 กำลังทดสอบ connection...")
        r = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=10
        )
        
        if r.status_code == 200:
            result = r.json()
            reply = result['choices'][0]['message']['content']
            print(f"✅ Connection OK! Response: {reply}")
            print(f"📊 Token usage: {result['usage']}")
            return True
        else:
            print(f"❌ Error {r.status_code}: {r.text}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Timeout - API ช้าเกินไป")
        return False
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def test_trading_prompt():
    """ทดสอบ prompt จริงที่ใช้ใน HIM"""
    api_key = get_api_key()
    
    # ตัวอย่าง market context จาก engine
    market_context = {
        "symbol": "GOLD",
        "htf_trend": "bullish",
        "mtf_trend": "bearish",
        "ltf_trend": "bullish",
        "proximity_score": 65,
        "bos": False,
        "supertrend_dir": "bearish"
    }
    
    prompt = f"""You are an AI Trading Mentor. Analyze this market:

Context: {json.dumps(market_context, indent=2)}

Rules:
- Return ONLY valid JSON
- No explanations
- Format: {{"action": "BUY/SELL/NONE", "confidence": 0-100, "reason": "summary"}}

Analysis:"""
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    data = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": "You are a strict JSON-only trading mentor."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.1,  # low for consistency
        "max_tokens": 100
    }
    
    try:
        print("\n🔄 กำลังทดสอบ trading prompt...")
        r = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=15
        )
        
        if r.status_code == 200:
            result = r.json()
            reply = result['choices'][0]['message']['content']
            print(f"✅ AI Response:")
            print(json.dumps(json.loads(reply), indent=2, ensure_ascii=False))
            print(f"📊 Token usage: {result['usage']}")
            return True
        else:
            print(f"❌ Error {r.status_code}: {r.text}")
            return False
            
    except json.JSONDecodeError:
        print("❌ AI ไม่ได้ตอบกลับเป็น JSON")
        print(f"Response: {reply}")
        return False
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

def benchmark_speed():
    """ทดสอบความเร็ว API"""
    import time
    
    api_key = get_api_key()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    times = []
    for i in range(3):
        data = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "user", "content": "Say 'test'"}
            ],
            "max_tokens": 5
        }
        
        start = time.time()
        r = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers=headers,
            json=data,
            timeout=10
        )
        end = time.time()
        
        if r.status_code == 200:
            times.append(end - start)
            print(f"  ✅ Request {i+1}: {end-start:.2f} seconds")
    
    if times:
        avg = sum(times) / len(times)
        print(f"\n📊 Average response time: {avg:.2f} seconds")
        return avg
    return None

if __name__ == "__main__":
    print("="*50)
    print("🔍 HIM AI API Test Script")
    print("="*50)
    
    # 1. ทดสอบ connection
    print("\n1️⃣ Testing basic connection...")
    if not test_connection():
        print("\n❌ Connection test failed. ตรวจสอบ:")
        print("   - .env file มีอยู่ไหม")
        print("   - DEEPSEEK_API_KEY ใน .env ถูกต้อง")
        print("   - internet connection")
        exit(1)
    
    # 2. ทดสอบ trading prompt
    print("\n2️⃣ Testing trading prompt...")
    test_trading_prompt()
    
    # 3. ทดสอบความเร็ว
    print("\n3️⃣ Benchmarking speed...")
    benchmark_speed()
    
    print("\n" + "="*50)
    print("✅ Test complete!")