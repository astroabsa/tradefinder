import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import time

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="Upstox Live Screener", layout="wide")
st.title("📈 Upstox Live F&O Screener")

# --- 2. CONFIGURATION & AUTHENTICATION ---
# Note: Upstox Access Tokens expire every 24 hours.
# You must generate a new one daily via their login flow.
ACCESS_TOKEN = st.sidebar.text_input("Enter Today's Access Token", type="password")
API_VERSION = '2.0'

# List of Instrument Keys (Example: Reliance and HDFC Bank)
# You need the official Instrument Keys from Upstox (e.g., NSE_EQ|INE002A01018)
# You can download the master list from Upstox Developer API docs.
WATCHLIST = {
    "RELIANCE": "NSE_EQ|INE002A01018",
    "HDFC BANK": "NSE_EQ|INE004A01026",
    "INFOSYS": "NSE_EQ|INE009A01021",
    "TCS": "NSE_EQ|INE467B01029",
    "ICICI BANK": "NSE_EQ|INE090A01021"
}

# --- 3. HELPER FUNCTION: FETCH DATA ---
def fetch_upstox_data(api_instance, instrument_key):
    """
    Fetches 1-minute interval candles for the last 5 days.
    """
    try:
        # DYNAMIC DATE CALCULATION (Fixes the previous error)
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        api_response = api_instance.get_historical_candle_data1(
            instrument_key=instrument_key,
            interval='1minute',  # Options: 1minute, 30minute, day, etc.
            to_date=to_date,
            from_date=from_date, # <--- CRITICAL PARAMETER
            api_version=API_VERSION
        )

        if api_response.status == 'success' and api_response.data.candles:
            # Upstox returns list of lists: [timestamp, open, high, low, close, volume, oi]
            cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
            df = pd.DataFrame(api_response.data.candles, columns=cols)
            
            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Ensure numeric columns are floats
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
                
            # Upstox data comes in Reverse Chronological order (Newest first)
            # We need Oldest First for Indicator Calculation
            df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
            
            return df
            
    except ApiException as e:
        # st.error(f"API Error for {instrument_key}: {e}") # Uncomment for debugging
        return None
    except Exception as e:
        return None
    return None

# --- 4. MAIN SCREENER LOGIC ---
if st.button("🚀 Run Scanner"):
    if not ACCESS_TOKEN:
        st.error("⚠️ Please enter a valid Access Token in the sidebar.")
        st.stop()

    # Configure Upstox Client
    configuration = upstox_client.Configuration()
    configuration.access_token = ACCESS_TOKEN
    api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

    results = []
    progress_bar = st.progress(0, text="Scanning Market...")

    for i, (symbol_name, instrument_key) in enumerate(WATCHLIST.items()):
        
        df = fetch_upstox_data(api_instance, instrument_key)
        
        if df is not None and not df.empty:
            # --- CALCULATE INDICATORS ---
            # RSI (14)
            df['RSI'] = ta.rsi(df['close'], length=14)
            # ADX (14)
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
            df['ADX'] = adx_df['ADX_14']
            
            # Get Latest Values (Last row is the most recent candle)
            last_row = df.iloc[-1]
            
            rsi_val = last_row['RSI']
            adx_val = last_row['ADX']
            close_price = last_row['close']
            
            # --- SIGNAL LOGIC ---
            signal = "NEUTRAL"
            color = "gray"
            
            # Bullish: RSI > 60 and ADX > 25
            if rsi_val > 60 and adx_val > 25:
                signal = "BULLISH 🟢"
                color = "green"
            # Bearish: RSI < 40 and ADX > 25
            elif rsi_val < 40 and adx_val > 25:
                signal = "BEARISH 🔴"
                color = "red"

            results.append({
                "Symbol": symbol_name,
                "Price": f"₹{close_price}",
                "RSI": round(rsi_val, 2),
                "ADX": round(adx_val, 2),
                "Signal": signal
            })
            
        # Update Progress
        progress_bar.progress((i + 1) / len(WATCHLIST))
        time.sleep(0.1) # Avoid Rate Limits

    progress_bar.empty()

    # --- 5. DISPLAY RESULTS ---
    if results:
        results_df = pd.DataFrame(results)
        
        # Display as a clean table
        st.subheader("Scanner Results")
        st.dataframe(
            results_df.style.map(
                lambda x: 'color: green; font-weight: bold' if 'BULLISH' in str(x) 
                else ('color: red; font-weight: bold' if 'BEARISH' in str(x) else ''), 
                subset=['Signal']
            ),
            use_container_width=True
        )
    else:
        st.warning("No data returned. Check your Access Token or Market Hours.")
