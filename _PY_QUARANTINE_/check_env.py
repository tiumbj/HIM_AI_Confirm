import os
from dotenv import load_dotenv

print("🔍 กำลังตรวจสอบ .env...\n")

if os.path.exists('.env'):
    print("✅ พบไฟล์ .env\n")
    with open('.env', 'r', encoding='utf-8') as f:
        print("📄 เนื้อหา:")
        print(f.read())
else:
    print("❌ ไม่พบไฟล์ .env")
    exit()

print("\n🔄 กำลังโหลด .env...")
load_dotenv()

print("\n🔑 Environment Variables:")
print(f"OPENAI_API_KEY: {os.getenv('OPENAI_API_KEY', 'ไม่พบ')}")
news_key = os.getenv('NEWS_API_KEY')
if news_key:
    print(f"NEWS_API_KEY: {news_key[:10]}... ✅")
else:
    print("NEWS_API_KEY: ไม่พบ ❌")