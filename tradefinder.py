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

# --- 4. HARDCODED KEYS (Verified ISINs) ---
INDICES = {
    "NIFTY 50": "NSE_INDEX|Nifty 50",
    "BANK NIFTY": "NSE_INDEX|Nifty Bank",
    "SENSEX": "BSE_INDEX|SENSEX"
}

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

def fetch_live_batch(keys_list, mode='FULL'):
    if not keys_list: return {}
    combined_data = {}
    BATCH_SIZE = 10 
    for i in range(0, len(keys_list), BATCH_SIZE):
        batch = keys_list[i:i+BATCH_SIZE]
        keys_str = ",".join(batch)
        try:
            if mode == 'FULL':
                response = quote_api.get_full_market_quote(symbol=keys_str, api_version='2.0')
            else:
                response = quote_api.get_market_quote_ohlc(symbol=keys_str, interval='1d', api_version='2.0')
            if response.status == 'success':
                combined_data.update(response.data)
        except Exception as e:
            if SHOW_DEBUG: st.warning(f"Batch Failed: {e}")
    return combined_data

def fetch_history(key):
    try:
        to_d = datetime.now().strftime("%Y-%m-%d")
        from_d = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        # 15-Minute Candles
        res = history_api.get_historical_candle_data1(
            instrument_key=key, interval='15minute', 
            to_date=to_d, from_date=from_d, api_version='2.0'
        )
        if res.status == 'success' and res.data.candles:
            df = pd.DataFrame(res.data.candles, columns=['timestamp','open','high','low','close','vol','oi'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            for c in ['open','high','low','close','vol','oi']: df[c] = df[c].astype(float)
            return df.sort_values('timestamp').reset_index(drop=True)
    except Exception as e:
        if SHOW_DEBUG: st.write(f"History Error ({key}): {e}")
    return None

def extract_price_robust(response_data, target_key):
    """Deep Search for Instrument Token"""
    ltp, ref_price = 0.0, 0.0
    if not response_data: return 0.0, 0.0

    # 1. Direct or Deep Search
    data_item = response_data.get(target_key)
    if not data_item:
        for k, v in response_data.items():
            # Check dict
            if isinstance(v, dict) and v.get('instrument_token') == target_key:
                data_item = v; break
            # Check object
            elif hasattr(v, 'instrument_token') and v.instrument_token == target_key:
                data_item = v; break

    if not data_item: return 0.0, 0.0

    try:
        if isinstance(data_item, dict):
            ltp = float(data_item.get('last_price', 0))
            ohlc = data_item.get('ohlc', {})
            ref_price = float(ohlc.get('open', 0))
        else:
            ltp = float(data_item.last_price)
            ref_price = float(data_item.ohlc.open)
    except: pass
    return ltp, ref_price

def get_oi_analysis(price_change_pct, oi_change_pct):
    if price_change_pct > 0 and oi_change_pct > 0: return "Long Buildup 🟢"
    elif price_change_pct < 0 and oi_change_pct > 0: return "Short Buildup 🔴"
    elif price_change_pct < 0 and oi_change_pct < 0: return "Long Unwinding ⚠️"
    elif price_change_pct > 0 and oi_change_pct < 0: return "Short Covering ⚡"
    else: return "Neutral ⚪"

# --- 6. DASHBOARD (Indices) ---
@st.fragment(run_every=2)
def market_dashboard():
    live_data = fetch_live_batch(list(INDICES.values()), mode='OHLC')
    c1, c2, c3, c4 = st.columns([1,1,1,1.5])
    
    n_ltp, n_open = extract_price_robust(live_data, INDICES["NIFTY 50"])
    b_ltp, b_open = extract_price_robust(live_data, INDICES["BANK NIFTY"])
    s_ltp, s_open = extract_price_robust(live_data, INDICES["SENSEX"])

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

# --- 7. SCANNER (Stocks - 15 Min Logic) ---
@st.fragment(run_every=60)
def scanner():
    all_stocks = []
    bar = st.progress(0, "Scanning (15-Min TF)...")
    
    valid_keys = list(STOCKS.values())
    live_quotes = fetch_live_batch(valid_keys, mode='FULL')
    
    if SHOW_DEBUG:
        with st.expander("🔍 Scanner Debug", expanded=True):
            st.write(live_quotes)
    
    for i, (name, key) in enumerate(STOCKS.items()):
        
        # 1. LIVE PRICE (Robust)
        ltp, _ = extract_price_robust(live_quotes, key)
        
        # Default Values (Used if History Fails)
        curr_rsi = 0.0
        curr_adx = 0.0
        mom_pct = 0.0
        oi_change_pct = 0.0
        nature = "Waiting..."
        is_live = True

        # 2. HISTORY FETCH (With Rate Limit Protection)
        time.sleep(0.2) # FIX: Prevents API Blocking
        df = fetch_history(key)
        
        if df is not None and len(df) > 20:
            # 3. TECHNICALS
            df['RSI'] = ta.rsi(df['close'], 14)
            df['ADX'] = ta.adx(df['high'], df['low'], df['close'], 14)['ADX_14']
            df['EMA'] = ta.ema(df['close'], 5)
            
            last = df.iloc[-1]
            prev = df.iloc[-2]
            
            # If Live LTP is 0, use History
            if ltp == 0: 
                ltp = last['close']
                is_live = False
            
            curr_rsi = round(last['RSI'], 2)
            curr_adx = round(last['ADX'], 2)
            mom_pct = round(((ltp - last['EMA'])/last['EMA'])*100, 2)
            
            # 4. OI Analysis
            curr_oi = last['oi']
            prev_oi = prev['oi']
            if prev_oi > 0:
                oi_change_pct = ((curr_oi - prev_oi) / prev_oi) * 100
            
            price_change_candle = ((last['close'] - prev['close']) / prev['close']) * 100
            nature = get_oi_analysis(price_change_candle, oi_change_pct)

        # 5. PREPARE ROW (Always Add, Never Skip)
        display_price = f"₹{ltp:,.2f}"
        if not is_live: display_price += " (Old)"

        row = {
            "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{name}",
            "LTP": display_price, 
            "Mom %": mom_pct,
            "RSI": curr_rsi, 
            "ADX": curr_adx,
            "OI Chg %": round(oi_change_pct, 2),
            "Nature": nature 
        }
        all_stocks.append(row)
        
        bar.progress((i+1)/len(valid_keys))
    
    bar.empty()
    
    # --- DISPLAY ---
    if all_stocks:
        df_all = pd.DataFrame(all_stocks)
        
        col_conf = {
            "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
            "LTP": st.column_config.TextColumn("Price"),
            "OI Chg %": st.column_config.NumberColumn("OI Chg %", format="%.2f%%")
        }

        st.subheader("🟢 Market Watch: Strongest Stocks")
        st.dataframe(
            df_all.sort_values("Mom %", ascending=False).head(10), 
            use_container_width=True, hide_index=True, column_config=col_conf
        )

        st.subheader("🔴 Market Watch: Weakest Stocks")
        st.dataframe(
            df_all.sort_values("Mom %", ascending=True).head(10), 
            use_container_width=True, hide_index=True, column_config=col_conf
        )
    else:
        st.error("Critical Error: No data could be processed.")
        
    st.markdown(f"<div style='text-align:left; color:grey; margin-top:20px;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}<br><strong>Powered by : i-Tech World</strong></div>", unsafe_allow_html=True)

scanner()
