"""Test API Keys"""
import requests
from config import AI_API_KEY, NEWS_API_KEY, AI_API_URL

def test_deepseek_api():
    """Test DeepSeek AI API"""
    print("🔍 Testing DeepSeek API...")
    
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "user", "content": "Hello, are you working?"}
        ],
        "max_tokens": 50
    }
    
    try:
        response = requests.post(AI_API_URL, headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            print("✅ DeepSeek API: SUCCESS")
            print(f"   Response: {response.json()['choices'][0]['message']['content']}")
            return True
        else:
            print(f"❌ DeepSeek API: FAILED (Status {response.status_code})")
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ DeepSeek API: ERROR - {str(e)}")
        return False


def test_marketaux_api():
    """Test MarketAux News API"""
    print("\n🔍 Testing MarketAux API...")
    
    url = f"https://api.marketaux.com/v1/news/all"
    params = {
        'api_token': NEWS_API_KEY,
        'symbols': 'XAU',
        'filter_entities': 'true',
        'limit': 3
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ MarketAux API: SUCCESS")
            print(f"   News found: {len(data.get('data', []))}")
            if data.get('data'):
                print(f"   Latest headline: {data['data'][0]['title'][:60]}...")
            return True
        else:
            print(f"❌ MarketAux API: FAILED (Status {response.status_code})")
            print(f"   Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ MarketAux API: ERROR - {str(e)}")
        return False


if __name__ == "__main__":
    print("="*60)
    print("API KEY TESTING")
    print("="*60)
    
    deepseek_ok = test_deepseek_api()
    marketaux_ok = test_marketaux_api()
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"DeepSeek AI: {'✅ READY' if deepseek_ok else '❌ FAILED'}")
    print(f"MarketAux News: {'✅ READY' if marketaux_ok else '❌ FAILED'}")
    
    if deepseek_ok and marketaux_ok:
        print("\n🎉 All APIs are working! System is ready to run.")
    else:
        print("\n⚠️ Some APIs failed. Please check your API keys.")