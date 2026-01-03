from dhanhq import dhanhq
import pandas as pd
from datetime import datetime, timedelta

# REPLACE THESE
CLIENT_ID = "1104089467"      # e.g., "10000xxxxx"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NTA4NzY0LCJpYXQiOjE3Njc0MjIzNjQsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA0MDg5NDY3In0.al1lcv-txOjq8JY7lUnqCjm2978ZgydGoPlw9n8d2aPjYv2gIDUmOTpgQjiB7Ha91X6VhDuHg1XTIg8FtfwkPA" # Long JWT string from Dhan Web

def test_dhan_connection():
    print("Testing DhanHQ connection...")
    try:
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        
        # 1. Check Holdings (Easiest way to validate Login)
        # If this works, your Client ID/Token are 100% correct.
        holdings = dhan.get_holdings()
        if holdings['status'] == 'success':
            print("✅ Login SUCCESS! Credentials are valid.")
        else:
            print("❌ Login Failed.")
            return

        # 2. Check Data Fetching (Historical)
        # Fetch Reliance (Security ID: 1333 for NSE Equity) for last 5 days
        print("Fetching historical data...")
        data = dhan.historical_daily_data(
            symbol='RELIANCE',
            exchange_segment='NSE_EQ',
            instrument_type='EQUITY',
            expiry_code=0,
            from_date=(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d'),
            to_date=datetime.now().strftime('%Y-%m-%d')
        )
        
        if data['status'] == 'success' and len(data['data']) > 0:
            print(f"✅ Data Fetch SUCCESS! Retrieved {len(data['data'])} days of data.")
        else:
            print("⚠️ Connected, but data fetch returned empty (Check Security ID or Date Range).")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")

test_dhan_connection()
