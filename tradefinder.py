import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
import pandas_ta as ta

# --- CONFIGURATION ---
API_KEY = "085bad90-9701-4f88-a591-95fdfeac8c7e"
API_SECRET = "8euyjnujw6"
REDIRECT_URI = "https://tradefinder.streamlit.app" # Defined in your Upstox App settings

st.title("📈 Upstox Live Screener")

# --- STEP 1: AUTHENTICATION (The Hard Part) ---
# You generally need a separate script to generate the ACCESS_TOKEN daily.
# For this screener, we assume you already have the valid token.
ACCESS_TOKEN = st.text_input("Enter Today's Access Token", type="password")

if not ACCESS_TOKEN:
    st.warning("Please generate an access token to proceed.")
    st.markdown(f"[Login to Generate Code](https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI})")
    st.stop()

# Configure the client
configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN

# --- STEP 2: FETCHING DATA ---
def get_upstox_data(instrument_key):
    api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))
    try:
        # Fetch Intraday 1-minute candles
        api_response = api_instance.get_historical_candle_data1(
            instrument_key=instrument_key, 
            interval='1minute', 
            to_date='2024-01-01', # Current date
            api_version='2.0'
        )
        
        if api_response.status == 'success':
            candles = api_response.data.candles
            # Convert to DataFrame
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
            return df
    except ApiException as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# --- STEP 3: SCREENER LOGIC ---
if st.button("Run Upstox Scan"):
    # Upstox uses instrument keys like 'NSE_EQ|INE002A01018' (Reliance)
    # You need a master list mapper similar to what we did for Dhan.
    
    df = get_upstox_data('NSE_EQ|INE002A01018') # Example for Reliance
    
    if not df.empty:
        # Calculate RSI/ADX using pandas-ta
        df['close'] = df['close'].astype(float)
        df['RSI'] = ta.rsi(df['close'], length=14)
        
        last_rsi = df['RSI'].iloc[0] # Upstox sends data in reverse order often (check docs)
        
        st.metric("Reliance RSI", f"{last_rsi:.2f}")
