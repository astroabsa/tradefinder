import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta
import time

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="Upstox Pro Screener", layout="wide")
st.title("🚀 Upstox Pro Live Screener")

# --- 2. AUTHENTICATION (SECRET MANAGEMENT) ---
# This looks directly into your Streamlit Cloud Secrets
try:
    ACCESS_TOKEN = st.secrets["UPSTOX_ACCESS_TOKEN"]
except FileNotFoundError:
    st.error("❌ `secrets.toml` file not found. Please set it up in Streamlit Cloud.")
    st.stop()
except KeyError:
    st.error("❌ Key `UPSTOX_ACCESS_TOKEN` is missing in your Secrets.")
    st.info("Go to App Settings -> Secrets and add: UPSTOX_ACCESS_TOKEN = 'your_token'")
    st.stop()

# --- 3. WATCHLIST ---
# Add your F&O stocks here
WATCHLIST = {
    "RELIANCE": "NSE_EQ|INE002A01018",
    "INFOSYS": "NSE_EQ|INE009A01021",
    "TCS": "NSE_EQ|INE467B01029",
    "HDFC BANK": "NSE_EQ|INE004A01026",
    "ICICI BANK": "NSE_EQ|INE090A01021",
    "SBIN": "NSE_EQ|INE062A01020",
    "AXIS BANK": "NSE_EQ|INE238A01034",
    "KOTAK BANK": "NSE_EQ|INE237A01028"
}

# --- 4. DATA ENGINE ---
def fetch_candle_data(api_instance, instrument_key):
    try:
        # Dynamic Date Window (Last 5 Days)
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        api_response = api_instance.get_historical_candle_data1(
            instrument_key=instrument_key,
            interval='1minute',
            to_date=to_date,
            from_date=from_date,
            api_version='2.0'
        )

        if api_response.status == 'success' and api_response.data.candles:
            cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
            df = pd.DataFrame(api_response.data.candles, columns=cols)
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
                
            # Sort Oldest -> Newest
            df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
            return df
            
    except Exception:
        return None
    return None

# --- 5. MAIN EXECUTION ---
if st.button("🔄 Scan Market Now"):
    
    # Initialize Upstox with Secret Token
    configuration = upstox_client.Configuration()
    configuration.access_token = ACCESS_TOKEN
    api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

    results = []
    progress_bar = st.progress(0, text="Analyzing Market Data...")

    for i, (symbol_name, instrument_key) in enumerate(WATCHLIST.items()):
        
        df = fetch_candle_data(api_instance, instrument_key)
        
        if df is not None and not df.empty:
            # Indicator Calculations
            df['RSI'] = ta.rsi(df['close'], length=14)
            adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
            df['ADX'] = adx_df['ADX_14']
            
            # Latest Signals
            last = df.iloc[-1]
            price, rsi, adx = last['close'], last['RSI'], last['ADX']
            
            signal = "NEUTRAL"
            if rsi > 60 and adx > 25: signal = "BULLISH 🟢"
            elif rsi < 40 and adx > 25: signal = "BEARISH 🔴"
            
            results.append({
                "Symbol": symbol_name,
                "Price": f"₹{price}",
                "RSI": round(rsi, 2),
                "ADX": round(adx, 2),
                "Signal": signal
            })
            
        progress_bar.progress((i + 1) / len(WATCHLIST))
        time.sleep(0.1) # Rate limit safety

    progress_bar.empty()

    if results:
        df_res = pd.DataFrame(results)
        
        # Color coding function
        def color_signal(val):
            if 'BULLISH' in val: return 'color: #0f9d58; font-weight: bold' # Google Green
            if 'BEARISH' in val: return 'color: #d93025; font-weight: bold' # Google Red
            return 'color: #5f6368' # Google Gray

        st.dataframe(
            df_res.style.map(color_signal, subset=['Signal']),
            use_container_width=True,
            hide_index=True
        )
        
        # Timestamp Footer
        st.caption(f"Last Updated: {datetime.now().strftime('%H:%M:%S')}")
    else:
        st.error("No data received. Token might be expired.")
