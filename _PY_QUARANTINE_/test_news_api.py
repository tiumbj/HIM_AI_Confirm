"""
News API Tester - ทดสอบการเชื่อมต่อ News API
"""

import requests
import json
from datetime import datetime, timedelta
import sys
import os

# กำหนดค่าต่างๆ
NEWS_API_KEY = "ygBt0Cds219wKrjAG87OvnZNXpUw6mhYfx0rFg1w"  # Key จาก config.py
SYMBOL = "XAU"  # หรือ "GOLD", "XAUUSD"

def test_news_api():
    """ทดสอบการเชื่อมต่อ News API แบบละเอียด"""
    
    print("\n" + "="*60)
    print("📰 NEWS API CONNECTION TESTER")
    print("="*60)
    
    # 1. ตรวจสอบ API Key
    print(f"\n🔑 API Key: {NEWS_API_KEY[:5]}...{NEWS_API_KEY[-5:] if NEWS_API_KEY else 'None'}")
    
    if not NEWS_API_KEY or NEWS_API_KEY == "your_marketaux_api_key":
        print("❌ API Key ไม่ถูกต้องหรือไม่ได้ตั้งค่า")
        return False
    
    # 2. ทดสอบ connection พื้นฐาน
    print(f"\n🌐 Testing connection to MarketAux...")
    
    test_url = "https://api.marketaux.com/v1/news/all"
    params = {
        'api_token': NEWS_API_KEY,
        'limit': 1
    }
    
    try:
        response = requests.get(test_url, params=params, timeout=10)
        print(f"📡 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Connection successful!")
            data = response.json()
            print(f"📊 Total articles: {data.get('meta', {}).get('total', 0)}")
        else:
            print(f"❌ Connection failed: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
    except requests.exceptions.Timeout:
        print("❌ Connection Timeout - Server ไม่ตอบสนอง")
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error - ไม่สามารถเชื่อมต่อได้")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # 3. ทดสอบดึงข่าวจริง
    print(f"\n📰 Fetching news for {SYMBOL}...")
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)
    
    params = {
        'api_token': NEWS_API_KEY,
        'symbols': SYMBOL,
        'filter_entities': 'true',
        'language': 'en',
        'published_after': start_time.strftime('%Y-%m-%d'),
        'limit': 5
    }
    
    try:
        response = requests.get(test_url, params=params, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            articles = data.get('data', [])
            
            print(f"✅ Found {len(articles)} articles")
            
            if articles:
                print("\n📰 Latest News:")
                for i, article in enumerate(articles[:3], 1):
                    print(f"\n  {i}. {article.get('title', 'No title')}")
                    print(f"     Source: {article.get('source', 'Unknown')}")
                    
                    # หา sentiment จาก entities
                    entities = article.get('entities', [])
                    sentiment = 0
                    for entity in entities:
                        if entity.get('symbol') == SYMBOL:
                            sentiment = entity.get('sentiment_score', 0)
                            break
                    
                    print(f"     Sentiment: {sentiment}")
                    print(f"     Published: {article.get('published_at', '')[:10]}")
            else:
                print("⚠️ No articles found for this symbol")
                
        else:
            print(f"❌ Failed to fetch news: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
    except Exception as e:
        print(f"❌ Error fetching news: {e}")

def test_without_api_key():
    """ทดสอบโดยไม่ใช้ API Key (RSS)"""
    
    print("\n" + "="*60)
    print("📰 TESTING WITHOUT API KEY (RSS)")
    print("="*60)
    
    # ลองใช้ RSS แทน
    import urllib.request
    import xml.etree.ElementTree as ET
    
    # Yahoo Finance RSS
    print("\n🔄 Trying Yahoo Finance RSS...")
    
    try:
        rss_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=XAUUSD&region=US&lang=en-US"
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        req = urllib.request.Request(rss_url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=10) as response:
            rss_data = response.read().decode('utf-8')
            
            # Parse XML แบบง่าย
            import re
            titles = re.findall(r'<title>(.*?)</title>', rss_data)
            
            if len(titles) > 1:  # ข้าม title แรกที่เป็นชื่อ feed
                print(f"✅ Found {len(titles)-1} news items")
                for title in titles[1:4]:
                    print(f"\n  • {title}")
            else:
                print("⚠️ No news found")
                
    except Exception as e:
        print(f"❌ RSS Error: {e}")
    
    # Investing.com RSS
    print("\n🔄 Trying Investing.com RSS...")
    
    try:
        rss_url = "https://www.investing.com/rss/news.rss"
        
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        req = urllib.request.Request(rss_url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=10) as response:
            rss_data = response.read().decode('utf-8')
            
            # Parse XML แบบง่าย
            import re
            titles = re.findall(r'<title>(.*?)</title>', rss_data)
            
            # กรองข่าวที่เกี่ยวกับทองคำ
            gold_titles = [t for t in titles if 'gold' in t.lower() or 'xau' in t.lower()]
            
            if gold_titles:
                print(f"✅ Found {len(gold_titles)} gold-related news")
                for title in gold_titles[:3]:
                    print(f"\n  • {title}")
            else:
                print("⚠️ No gold news found")
                
    except Exception as e:
        print(f"❌ RSS Error: {e}")

def create_mock_news_api():
    """สร้าง Mock News API เผื่อ API จริงใช้ไม่ได้"""
    
    print("\n" + "="*60)
    print("🎭 CREATING MOCK NEWS API (FALLBACK)")
    print("="*60)
    
    mock_news = [
        {
            'title': 'Gold Prices Rise on US Dollar Weakness',
            'description': 'Gold prices climbed as the US dollar softened against major currencies, making bullion cheaper for foreign buyers.',
            'published': datetime.now().isoformat(),
            'source': 'Reuters (Mock)',
            'sentiment': 0.75,
            'impact': 'medium'
        },
        {
            'title': 'Federal Reserve Signals Potential Rate Cut',
            'description': 'Fed officials hint at possible rate cuts later this year, boosting non-yielding assets like gold.',
            'published': (datetime.now() - timedelta(hours=2)).isoformat(),
            'source': 'Bloomberg (Mock)',
            'sentiment': 0.82,
            'impact': 'high'
        },
        {
            'title': 'Central Banks Continue Gold Purchases',
            'description': 'Global central banks added to their gold reserves in the latest quarter, supporting prices.',
            'published': (datetime.now() - timedelta(hours=5)).isoformat(),
            'source': 'Financial Times (Mock)',
            'sentiment': 0.68,
            'impact': 'medium'
        },
        {
            'title': 'Technical Analysis: Gold Breaks Resistance',
            'description': 'Gold prices broke above key resistance level, suggesting further upside potential.',
            'published': (datetime.now() - timedelta(hours=8)).isoformat(),
            'source': 'DailyFX (Mock)',
            'sentiment': 0.71,
            'impact': 'low'
        },
        {
            'title': 'Geopolitical Tensions Boost Safe-Haven Demand',
            'description': 'Rising tensions in Middle East drive investors toward safe-haven assets including gold.',
            'published': (datetime.now() - timedelta(hours=12)).isoformat(),
            'source': 'CNBC (Mock)',
            'sentiment': 0.64,
            'impact': 'high'
        }
    ]
    
    # บันทึกเป็นไฟล์ JSON เผื่อใช้
    with open('mock_news.json', 'w', encoding='utf-8') as f:
        json.dump(mock_news, f, indent=2, ensure_ascii=False)
    
    print("✅ Mock news created and saved to mock_news.json")
    print("\n📰 Sample:")
    for news in mock_news[:2]:
        print(f"\n  • {news['title']}")
        print(f"    Source: {news['source']} | Sentiment: {news['sentiment']}")
    
    return mock_news

def load_mock_news():
    """โหลดข่าวจาก mock_news.json"""
    try:
        if os.path.exists('mock_news.json'):
            with open('mock_news.json', 'r', encoding='utf-8') as f:
                return json.load(f)
    except:
        pass
    return create_mock_news_api()

if __name__ == "__main__":
    print("🚀 News API Diagnostic Tool")
    print("="*60)
    
    # ตรวจสอบการติดตั้ง
    print("\n📋 Checking environment...")
    print(f"Python version: {sys.version}")
    print(f"Requests installed: {'✅' if 'requests' in sys.modules else '❌'}")
    
    # เมนู
    print("\n📋 Options:")
    print("1. Test MarketAux API")
    print("2. Test RSS (No API Key)")
    print("3. Create Mock News API")
    print("4. Load Mock News")
    print("5. Run All Tests")
    
    choice = input("\nSelect option (1-5): ").strip()
    
    if choice == '1':
        test_news_api()
    elif choice == '2':
        test_without_api_key()
    elif choice == '3':
        create_mock_news_api()
    elif choice == '4':
        news = load_mock_news()
        print(f"\n📰 Loaded {len(news)} mock news:")
        for n in news[:3]:
            print(f"\n  • {n['title']}")
    elif choice == '5':
        test_news_api()
        test_without_api_key()
        create_mock_news_api()
    else:
        print("❌ Invalid choice")