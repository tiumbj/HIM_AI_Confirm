"""
ทดสอบ MarketAux News API
"""

import requests
from datetime import datetime, timedelta

API_KEY = "ygBt0Cds219wKrjAG87OvnZNXpUw6mhYfx0rFg1w"
SYMBOL = "XAU"  # หรือ "GOLD"

def test_marketaux_api():
    """ทดสอบการเชื่อมต่อ MarketAux API"""
    
    print("\n" + "="*60)
    print("📰 TESTING MARKETAUX NEWS API")
    print("="*60)
    
    # 1. ทดสอบ connection พื้นฐาน
    print(f"\n🔑 API Key: {API_KEY[:5]}...{API_KEY[-5:]}")
    
    url = "https://api.marketaux.com/v1/news/all"
    
    # 2. ทดสอบแบบไม่ระบุ symbol
    print("\n🌐 Testing basic connection...")
    
    params = {
        'api_token': API_KEY,
        'limit': 1
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"📡 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Basic connection successful!")
            data = response.json()
            print(f"📊 Total available: {data.get('meta', {}).get('total', 0)}")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 3. ทดสอบดึงข่าวทองคำ
    print(f"\n📰 Fetching news for {SYMBOL}...")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    params = {
        'api_token': API_KEY,
        'symbols': SYMBOL,
        'filter_entities': 'true',
        'language': 'en',
        'published_after': start_date.strftime('%Y-%m-%d'),
        'limit': 5
    }
    
    try:
        response = requests.get(url, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            articles = data.get('data', [])
            
            print(f"✅ Found {len(articles)} articles")
            
            if articles:
                print("\n📰 Latest News:")
                for i, article in enumerate(articles[:3], 1):
                    print(f"\n  {i}. {article.get('title', 'No title')}")
                    print(f"     Source: {article.get('source', 'Unknown')}")
                    print(f"     Published: {article.get('published_at', '')[:10]}")
                    
                    # หา sentiment
                    entities = article.get('entities', [])
                    for entity in entities:
                        if entity.get('symbol') == SYMBOL:
                            print(f"     Sentiment: {entity.get('sentiment_score', 0)}")
                            break
            else:
                print("⚠️ No articles found for gold")
                
        elif response.status_code == 429:
            print("❌ Rate Limit Exceeded! (100/100 used)")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 4. ตรวจสอบ usage
    print("\n📊 USAGE INFO:")
    print("-" * 40)
    print("Free Tier: 100 requests per day")
    print("Current Usage: Unknown (API doesn't show usage)")
    print("Reset: Daily at 00:00 UTC")
    print("-" * 40)
    
    return

def test_without_api_key():
    """ทดสอบ RSS แทน (ไม่ใช้ API Key)"""
    
    print("\n" + "="*60)
    print("📰 TESTING RSS FALLBACK")
    print("="*60)
    
    import urllib.request
    import xml.etree.ElementTree as ET
    
    # Yahoo Finance RSS
    print("\n🔄 Yahoo Finance RSS...")
    
    try:
        url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=XAUUSD&region=US&lang=en-US"
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = response.read().decode('utf-8')
            
            # Parse แบบง่าย
            import re
            titles = re.findall(r'<title>(.*?)</title>', data)
            
            if len(titles) > 1:
                print(f"✅ Found {len(titles)-1} news items")
                for title in titles[1:4]:
                    print(f"   • {title[:80]}...")
            else:
                print("⚠️ No news")
                
    except Exception as e:
        print(f"❌ RSS Error: {e}")

if __name__ == "__main__":
    print("🚀 MarketAux API Tester")
    print("="*60)
    
    print("\n📋 Options:")
    print("1. Test MarketAux API")
    print("2. Test RSS Fallback")
    
    choice = input("\nSelect option: ").strip()
    
    if choice == '1':
        test_marketaux_api()
    elif choice == '2':
        test_without_api_key()
    else:
        print("❌ Invalid choice")