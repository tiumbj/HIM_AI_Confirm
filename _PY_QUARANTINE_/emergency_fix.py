# emergency_fix.py
import json
import os
from datetime import datetime

print("🔧 HIM Emergency Fix")
print("="*50)

# 1. อ่านไฟล์ปัจจุบัน
with open('config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"📂 Current file type: {type(data)}")

# 2. ถ้าเป็น list ให้แยก
if isinstance(data, list):
    print(f"📊 Found {len(data)} items in list")
    
    # ค้นหา config (อันแรกที่ไม่มี type หรือมี structure เหมือน config)
    config_data = None
    log_entries = []
    
    for item in data:
        if isinstance(item, dict):
            # ถ้ามี 'type' และ 'decision' น่าจะเป็น log
            if 'type' in item and 'decision' in item:
                log_entries.append(item)
            else:
                # อันอื่น предположимเป็น config
                config_data = item
    
    # ถ้าไม่เจอ config ชัดเจน ให้ใช้อันแรก
    if config_data is None and len(data) > 0:
        config_data = data[0]
        # ถ้าอันแรกเป็น log ให้เอาอันต่อไป
        if 'type' in config_data:
            config_data = data[1] if len(data) > 1 else {}
    
    print(f"✅ Found config: {config_data is not None}")
    print(f"📝 Found {len(log_entries)} log entries")

else:
    config_data = data
    log_entries = []

# 3. Backup ไฟล์เดิม
backup_name = f"config.json.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
os.rename('config.json', backup_name)
print(f"💾 Backup saved as: {backup_name}")

# 4. เขียน config ใหม่
if config_data and isinstance(config_data, dict):
    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    print("✅ config.json restored")
else:
    # สร้าง config ใหม่ถ้าไม่มี
    default_config = {
        "mode": "B",
        "symbol": "GOLD",
        "enable_execution": False,
        "confidence_threshold": 75,
        "min_score": 7.0,
        "min_rr": 2.0,
        "lot": 0.01,
        "timeframes": {
            "htf": "H4",
            "mtf": "H1",
            "ltf": "M15"
        },
        "risk": {
            "atr_period": 14,
            "atr_sl_mult": 1.8
        },
        "breakout": {
            "confirm_buffer_atr": 0.05,
            "require_retest": True,
            "min_proximity_score": 10
        },
        "continuation": {
            "enabled": True,
            "min_proximity": 20,
            "require_mtf_ltf_aligned": True,
            "require_htf_not_opposite": True
        },
        "ai": {
            "enabled": False,
            "provider": "deepseek",
            "api_key_env": "DEEPSEEK_API_KEY"
        },
        "telegram": {
            "enabled": True,
            "token_env": "TELEGRAM_BOT_TOKEN",
            "chat_id_env": "TELEGRAM_CHAT_ID"
        }
    }
    with open('config.json', 'w', encoding='utf-8') as f:
        json.dump(default_config, f, indent=2, ensure_ascii=False)
    print("✅ New config.json created")

# 5. เขียน log แยกไฟล์ (ถ้ามี)
if log_entries:
    log_file = f"trade_log_{datetime.now().strftime('%Y%m%d')}.json"
    with open(log_file, 'w', encoding='utf-8') as f:
        json.dump(log_entries, f, indent=2, ensure_ascii=False)
    print(f"📝 Log saved to: {log_file}")

print("\n✅ Emergency fix complete!")
print("="*50)