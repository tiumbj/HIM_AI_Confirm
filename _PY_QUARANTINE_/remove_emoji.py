"""Remove all Emoji from mentor_executor.py"""

def remove_emoji():
    """Remove emoji from logging messages"""
    
    print("Reading mentor_executor.py...")
    
    with open('mentor_executor.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Backup
    with open('mentor_executor.py.backup2', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Backup created: mentor_executor.py.backup2")
    
    # แทนที่ Emoji ทั้งหมด
    emoji_replacements = {
        '🚀': '[INIT]',
        '✅': '[OK]',
        '❌': '[ERROR]',
        '⚠️': '[WARNING]',
        '💓': '[HEARTBEAT]',
        '📊': '[ANALYSIS]',
        '🎯': '[SIGNAL]',
        '💰': '[PROFIT]',
        '📈': '[BUY]',
        '📉': '[SELL]',
        '🔄': '[RETRY]',
        '🛑': '[STOP]',
        '⏳': '[WAIT]',
        '⏰': '[TIME]',
        '🌙': '[NIGHT]',
        '🌞': '[DAY]',
        '💤': '[SLEEP]',
        '📅': '[DATE]',
        '⌨️': '[KEYBOARD]',
        '👋': '[BYE]',
        '🎉': '[SUCCESS]',
        '🔧': '[FIX]',
        '📝': '[LOG]',
        '🎊': '[CELEBRATE]',
    }
    
    fixes = 0
    for emoji, replacement in emoji_replacements.items():
        if emoji in content:
            count = content.count(emoji)
            content = content.replace(emoji, replacement)
            fixes += count
            print(f"Removed {count}x: {emoji} -> {replacement}")
    
    # เขียนกลับ
    with open('mentor_executor.py', 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"\nTotal emoji removed: {fixes}")
    print("\nNow run: python mentor_executor.py")

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║              REMOVE EMOJI FROM LOGGING                      ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """)
    
    remove_emoji()