"""
ทดสอบ Marketaux API
"""

from news_filter import NewsFilter
from dotenv import load_dotenv

# โหลด .env
load_dotenv()

print("🧪 ทดสอบ Marketaux API...\n")

# สร้าง News Filter
news_filter = NewsFilter()

# ทดสอบดึงข่าว
print("📰 ดึงข่าว XAU/USD ใน 2 ชั่วโมงข้างหน้า:")
news = news_filter.get_high_impact_news(['XAUUSD'], hours_ahead=2)

if news:
    print(f"\n✅ พบข่าว {len(news)} รายการ:\n")
    for i, item in enumerate(news, 1):
        print(f"{i}. {item['title']}")
        print(f"   เวลา: {item['published']}")
        print(f"   Sentiment: {item.get('sentiment', 'N/A')}")
        print()
else:
    print("\n✅ ไม่มีข่าวสำคัญในช่วงนี้")

# ทดสอบการตัดสินใจ
print("\n" + "="*60)
print("🔍 ตรวจสอบว่าควรเทรด ) XAU/USD หรือไม่:")
should_avoid = news_filter.should_avoid_trading('XAUUSD', hours_ahead=1)

if should_avoid:
    print("❌ ไม่ควรเทรด - มีข่าวสำคัญ")
else:
    print("✅ ปลอดภัย - ไม่มีข่าวสำคัญ")