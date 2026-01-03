import streamlit as st
from dhanhq import dhanhq
import pandas as pd
from datetime import datetime, timedelta

# --- APP CONFIG ---
st.set_page_config(page_title="Dhan Connection Test")
st.title("🕵️ DhanHQ Connection Tester (Smart)")

# REPLACE THESE WITH YOUR ACTUAL KEYS
CLIENT_ID = "1104089467"      # e.g., "10000xxxxx"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NTA4NzY0LCJpYXQiOjE3Njc0MjIzNjQsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA0MDg5NDY3In0.al1lcv-txOjq8JY7lUnqCjm2978ZgydGoPlw9n8d2aPjYv2gIDUmOTpgQjiB7Ha91X6VhDuHg1XTIg8FtfwkPA" # Long JWT string from Dhan Web

def test_dhan_connection():
    st.info("Testing DhanHQ connection...")
    
    try:
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        
        # 1. CHECK LOGIN (Smart Check)
        st.subheader("1. Login Check")
        holdings = dhan.get_holdings()
        
        # Check for Success OR the specific "Empty Portfolio" error
        is_authenticated = False
        
        if holdings['status'] == 'success':
            st.success("✅ Login SUCCESS! (Holdings found)")
            is_authenticated = True
        elif holdings['remarks']['error_code'] == 'DH-1111':
            st.success("✅ Login SUCCESS! (Connected, but Portfolio is Empty)")
            st.info("The API is working. You just don't have any long-term stocks yet.")
            is_authenticated = True
        else:
            st.error(f"❌ Login Failed. Message: {holdings}")
            return 

        # 2. CHECK HISTORICAL DATA
        if is_authenticated:
            st.subheader("2. Data Fetch Check (Reliance)")
            st.write("Fetching last 5 days of data...")
            
            data = dhan.historical_daily_data(
                symbol='RELIANCE',
                exchange_segment='NSE_EQ',
                instrument_type='EQUITY',
                expiry_code=0,
                from_date=(datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d'),
                to_date=datetime.now().strftime('%Y-%m-%d')
            )
            
            if data['status'] == 'success' and 'data' in data:
                if len(data['data']) > 0:
                    st.success(f"✅ Data Fetch SUCCESS! Retrieved {len(data['data'])} days of data.")
                    st.dataframe(pd.DataFrame(data['data']))
                else:
                    st.warning("⚠️ Connected, but returned 0 rows. (Check Date Range)")
            else:
                st.error("❌ Data Fetch Failed.")
                st.write(data)

    except Exception as e:
        st.error(f"❌ CRITICAL ERROR: {e}")

if st.button("Run Smart Connection Test"):
    test_dhan_connection()
