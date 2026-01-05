import upstox_client
from upstox_client.rest import ApiException
import pandas as pd

# --- CONFIGURATION ---
# PASTE YOUR ACCESS TOKEN HERE FOR TESTING
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN_HERE" 

def run_diagnostic():
    print("🔵 Starting Upstox Diagnostic...")
    
    # 1. Setup
    configuration = upstox_client.Configuration()
    configuration.access_token = ACCESS_TOKEN
    client = upstox_client.ApiClient(configuration)
    quote_api = upstox_client.MarketQuoteApi(client)
    
    # 2. Test Keys (We test Reliance because it NEVER fails if API is working)
    test_keys = {
        "RELIANCE": "NSE_EQ|INE002A01018",  # Standard Stock
        "NIFTY 50": "NSE_INDEX|Nifty 50",    # Index
        "BANK NIFTY": "NSE_INDEX|Nifty Bank" # Index
    }
    
    keys_str = ",".join(test_keys.values())
    print(f"🔎 Requesting Data for: {keys_str}")
    
    try:
        # 3. Call API
        api_response = quote_api.get_full_market_quote(keys_str, '2.0')
        
        # 4. Print RAW Result
        print("\n🟢 API RESPONSE RECEIVED:")
        if hasattr(api_response, 'data'):
            data = api_response.data
            
            # Check Reliance (Stock)
            if "NSE_EQ|INE002A01018" in data:
                rel = data["NSE_EQ|INE002A01018"]
                print(f"✅ RELIANCE Price: {rel.last_price}")
            else:
                print("❌ RELIANCE Data Missing (Check Token Permissions)")
                
            # Check Nifty (Index)
            if "NSE_INDEX|Nifty 50" in data:
                nifty = data["NSE_INDEX|Nifty 50"]
                print(f"✅ NIFTY 50 Price: {nifty.last_price}")
            else:
                print("❌ NIFTY 50 Data Missing (Key might be wrong or Index Feed not active)")
                
        else:
            print("⚠️ Response object has no 'data' attribute.")
            print(api_response)

    except ApiException as e:
        print(f"\n🔴 CRITICAL API ERROR: {e.status}")
        print(e.body)
    except Exception as e:
        print(f"\n🔴 PYTHON ERROR: {e}")

if __name__ == "__main__":
    run_diagnostic()
