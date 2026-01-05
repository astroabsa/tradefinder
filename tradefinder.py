import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
import pandas_ta as ta
import requests
from datetime import datetime, timedelta
import time
import pytz

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="iTW Live F&O Screener Pro", layout="wide")

# --- 2. AUTHENTICATION SYSTEM ---
AUTH_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSEan21a9IVnkdmTFP2Q9O_ILI3waF52lFWQ5RTDtXDZ5MI4_yTQgFYcCXN5HxgkCxuESi5Dwe9iROB/pub?gid=0&single=true&output=csv"

def authenticate_user(user_in, pw_in):
    try:
        df = pd.read_csv(AUTH_CSV_URL)
        df['username'] = df['username'].astype(str).str.strip().str.lower()
        df['password'] = df['password'].astype(str).str.strip()
        match = df[(df['username'] == str(user_in).strip().lower()) & (df['password'] == str(pw_in).strip())]
        return not match.empty
    except: return False

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    st.title("🔐 iTW Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username"); p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p):
                st.session_state["authenticated"] = True
                st.rerun()
            else: st.error("Invalid Credentials")
    st.stop()

# --- 3. UPSTOX SETUP ---
st.title("🚀 iTW Live F&O Screener Pro")
if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False; st.rerun()

ACCESS_TOKEN = st.secrets.get("UPSTOX_ACCESS_TOKEN", "")
if not ACCESS_TOKEN:
    ACCESS_TOKEN = st.text_input("Enter Today's Access Token", type="password")
if not ACCESS_TOKEN: st.warning("⚠️ Enter Token to Start"); st.stop()

# DEBUG TOGGLE
SHOW_DEBUG = st.sidebar.checkbox("Show Raw API Response (Debug Mode)")

configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN
history_api = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))
quote_api = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))

# --- 4. HARDCODED KEYS (100% Reliable) ---
# We manually define the keys to avoid mapper errors
INDICES = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "SENSEX": "BSE_INDEX|SENSEX"
}

# Top F&O Stocks with Verified Keys
STOCKS = {
    "RELIANCE": "NSE_EQ|INE002A01018",
    "TCS": "NSE_EQ|INE467B01029",
    "HDFCBANK": "NSE_EQ|INE040A01034",
    "ICICIBANK": "NSE_EQ|INE090A01021",
    "SBIN": "NSE_EQ|INE062A01020",
    "INFY": "NSE_EQ|INE009A01021",
    "ITC": "NSE_EQ|INE154A01025",
    "BHARTIARTL": "NSE_EQ|INE397D01024",
    "LT": "NSE_EQ|INE018A01030",
    "AXISBANK": "NSE_EQ|INE238A01034",
    "KOTAKBANK": "NSE_EQ|INE237A01028",
    "ULTRACEMCO": "NSE_EQ|INE481G01011",
    "BAJFINANCE": "NSE_EQ|INE296A01024",
    "MARUTI": "NSE_EQ|INE585B01010",
    "SUNPHARMA": "NSE_EQ|INE044A01036",
    "TITAN": "NSE_EQ|INE280A01028",
    "TATAMOTORS": "NSE_EQ|INE155A01022",
    "NTPC": "NSE_EQ|INE733E01010",
    "POWERGRID": "NSE_EQ|INE752E01010",
    "ADANIENT": "NSE_EQ|INE423A01024"
}

# --- 5. FUNCTIONS ---

def fetch_live_quotes(keys_list):
    if not keys_list: return {}
    try:
        keys_str = ",".join(keys_list)
        # Using FULL market quote as it works best with NSE_EQ keys
        response = quote_api.get_full_market_quote(symbol=keys_str, api_version='2.0')
        if response.status == 'success':
            return response.data
    except Exception as e:
        if SHOW_DEBUG: st.error(f"API Error: {e}")
    return {}

def fetch_history(key):
    try:
        to_d = datetime.now().strftime("%Y-%m-%d")
        from_d = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        res = history_api.get_historical_candle_data1(
            instrument_key=key, interval='30minute', 
            to_date=to_d, from_date=from_d, api_version='2.0'
        )
        if res.status == 'success' and res.data.candles:
            df = pd.DataFrame(res.data.candles, columns=['timestamp','open','high','low','close','vol','oi'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            for c in ['open','high','low','close','vol','oi']: df[c] = df[c].astype(float)
            return df.sort_values('timestamp').reset_index(drop=True)
    except: pass
    return None

def extract_data_robust(response_data, target_key):
    """Robust extraction for both Dict and Object responses"""
    ltp, ref_price = 0.0, 0.0
    if not response_data: return 0.0, 0.0

    # 1. Fuzzy Match
    data_item = None
    if target_key in response_data:
        data_item = response_data[target_key]
    else:
        target_part = target_key.split("|")[-1]
        for k, v in response_data.items():
            if target_part in str(k):
                data_item = v; break
    
    if not data_item: return 0.0, 0.0

    # 2. Extract
    try:
        if isinstance(data_item, dict):
            ltp = float(data_item.get('last_price', 0))
            ohlc = data_item.get('ohlc', {})
            # Use OPEN for % Change reference
            ref_price = float(ohlc.get('open', 0))
            if ref_price == 0: ref_price = float(ohlc.get('close', 0))
        else:
            ltp = float(data_item.last_price)
            ref_price = float(data_item.ohlc.open)
            if ref_price == 0: ref_price = float(data_item.ohlc.close)
    except: pass

    return ltp, ref_price

# --- 6. DASHBOARD ---
@st.fragment(run_every=2)
def market_dashboard():
    # Fetch Data
    live_data = fetch_live_quotes(list(INDICES.values()))
    
    if SHOW_DEBUG:
        with st.expander("🔍 Raw Dashboard Data", expanded=False):
            st.write(live_data)

    c1, c2, c3, c4 = st.columns([1,1,1,1.5])
    
    n_ltp, n_open = extract_data_robust(live_data, INDICES["NIFTY 50"])
    b_ltp, b_open = extract_data_robust(live_data, INDICES["BANK NIFTY"])
    s_ltp, s_open = extract_data_robust(live_data, INDICES["SENSEX"])

    n_pct = ((n_ltp - n_open)/n_open)*100 if n_open > 0 else 0
    b_pct = ((b_ltp - b_open)/b_open)*100 if b_open > 0 else 0
    s_pct = ((s_ltp - s_open)/s_open)*100 if s_open > 0 else 0

    with c1: st.metric("NIFTY 50", f"{n_ltp:,.2f}", f"{n_pct:.2f}%")
    with c2: st.metric("BANK NIFTY", f"{b_ltp:,.2f}", f"{b_pct:.2f}%")
    with c3: st.metric("SENSEX", f"{s_ltp:,.2f}", f"{s_pct:.2f}%")
    
    with c4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        if n_pct > 0.25: bias, color = ("BULLISH 🚀", "green")
        elif n_pct < -0.25: bias, color = ("BEARISH 📉", "red")
        st.markdown(f"<div style='text-align:center; padding:10px; border:1px solid {color}; border-radius:10px; color:{color}'><h3>Bias: {bias}</h3></div>", unsafe_allow_html=True)

market_dashboard()
st.markdown("---")

# --- 7. SCANNER ---
@st.fragment(run_every=180) 
def scanner():
    bulls, bears = [], []
    bar = st.progress(0, "Scanning...")
    
    valid_keys = list(STOCKS.values())
    
    # 1. BATCH FETCH LIVE PRICES
    live_quotes = fetch_live_quotes(valid_keys)
    
    if SHOW_DEBUG:
        with st.expander("🔍 Raw Scanner Data", expanded=True):
            st.write(live_quotes)
    
    for i, (name, key) in enumerate(STOCKS.items()):
        try:
            # 2. LIVE PRICE (Robust)
            ltp, _ = extract_data_robust(live_quotes, key)
            
            # 3. HISTORY (Indicators)
            df = fetch_history(key)
            if df is None or len(df) < 30: continue
            
            # Fallback Check
            is_live = True
            if ltp == 0: 
                ltp = df.iloc[-1]['close']
                is_live = False
            
            # 4. CALC INDICATORS
            df['RSI'] = ta.rsi(df['close'], 14)
            df['ADX'] = ta.adx(df['high'], df['low'], df['close'], 14)['ADX_14']
            df['EMA'] = ta.ema(df['close'], 5)
            
            last = df.iloc[-1]
            mom_pct = round(((ltp - last['EMA'])/last['EMA'])*100, 2)
            
            # Display Logic
            display_price = f"₹{ltp:,.2f}"
            if not is_live: display_price += " (Delayed)"

            row = {
                "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{name}",
                "LTP": display_price, 
                "Mom %": mom_pct,
                "RSI": round(last['RSI'], 2), 
                "ADX": round(last['ADX'], 2)
            }
            
            # 5. FILTER (Relaxed slightly to ensure data visibility)
            if mom_pct > 0.1 and last['RSI'] > 55: bulls.append(row)
            elif mom_pct < -0.1 and last['RSI'] < 45: bears.append(row)
            
            bar.progress((i+1)/len(valid_keys))
        except: pass
    
    bar.empty()
    
    col_conf = {"Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"), "LTP": st.column_config.TextColumn("Price")}
    
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 TOP BULLS (Live)")
        if bulls: st.dataframe(pd.DataFrame(bulls).sort_values("Mom %", ascending=False).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bullish signals.")
        
    with c2:
        st.error("🔴 TOP BEARS (Live)")
        if bears: st.dataframe(pd.DataFrame(bears).sort_values("Mom %", ascending=True).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bearish signals.")
        
    st.markdown(f"<div style='text-align:left; color:grey; margin-top:20px;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}<br><strong>Powered by : i-Tech World</strong></div>", unsafe_allow_html=True)

scanner()
