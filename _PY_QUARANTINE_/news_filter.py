import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# โหลด .env
load_dotenv()

class NewsFilter:
    def __init__(self):
        self.api_key = os.getenv('MARKET_AUX_API_KEY')  # โหลด Marketaux API Key จาก .env
        self.base_url = "https://api.marketaux.com/v1/news/all"
        
        if not self.api_key:
            print("⚠️ ไม่พบ Marketaux API Key")
    
    def get_high_impact_news(self, symbols=['XAUUSD'], hours_ahead=2):
        """
        ดึงข่าวที่มีผลกระทบสูง
        Get high impact news
        """
        
        if not self.api_key:
            print("⚠️ ไม่มี Marketaux API Key - ข้ามการตรวจสอบข่าว")
            return []
        
        try:
            # กำหนดช่วงเวลา
            now = datetime.now()
            future = now + timedelta(hours=hours_ahead)
            
            # สร้าง Request
            params = {
                'api_token': self.api_key,
                'symbols': ','.join(symbols),
                'filter_entities': 'true',
                'published_after': now.strftime('%Y-%m-%dT%H:%M'),
                'published_before': future.strftime('%Y-%m-%dT%H:%M'),
                'limit': 10
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                news_list = data.get('data', [])
                
                # กรองเฉพาะข่าวสำคัญ
                high_impact = []
                for news in news_list:
                    if news.get('entities', []):
                        high_impact.append({
                            'title': news.get('title'),
                            'published': news.get('published_at'),
                            'entities': news.get('entities', []),
                            'sentiment': news.get('sentiment')
                        })
                
                return high_impact
            else:
                print(f"⚠️ Marketaux API Error: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"⚠️ Error fetching news: {e}")
            return []
    
    def should_avoid_trading(self, symbol, hours_ahead=1):
        """
        ตรวจสอบว่าควรหลีกเลี่ยงการเทรดหรือไม่
        Check if should avoid trading
        """
        
        # แยก Symbol (EURUSD -> EUR, USD)
        base = symbol[:3]
        quote = symbol[3:6] if len(symbol) >= 6 else 'USD'
        
        news = self.get_high_impact_news([base, quote], hours_ahead)
        
        if news:
            print(f"\n⚠️ พบข่าวสำคัญใน {hours_ahead} ชั่วโมงข้างหน้า:")
            for item in news:
                print(f"  📰 {item['title']}")
            return True
        
        return False