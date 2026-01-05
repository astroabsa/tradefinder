import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
import pandas_ta as ta
import requests
import gzip
import io
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

# --- 4. SMART MAPPER ---
@st.cache_data(ttl=3600*12) 
def get_upstox_master_map():
    symbol_map = {}
    index_map = {}
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
        with gzip.GzipFile(fileobj=io.BytesIO(requests.get(url).content)) as f:
            df = pd.read_json(f)
        
        # Equities
        eq_df = df[df['segment'] == 'NSE_EQ']
        symbol_map = dict(zip(eq_df['trading_symbol'], eq_df['instrument_key']))
        
        # Indices
        nifty = df[df['trading_symbol'] == 'Nifty 50']['instrument_key'].values
        bank = df[df['trading_symbol'] == 'Nifty Bank']['instrument_key'].values
        
        if len(nifty) > 0: index_map['Nifty 50'] = nifty[0]
        if len(bank) > 0: index_map['Nifty Bank'] = bank[0]
        
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return symbol_map, index_map

SYMBOL_MAP, INDEX_MAP = get_upstox_master_map()
FNO_SYMBOLS = [
    'RELIANCE.NS', 'INFY.NS', 'TCS.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'SBIN.NS', 
    'ADANIENT.NS', 'AXISBANK.NS', 'KOTAKBANK.NS', 'LT.NS', 'ITC.NS', 'BAJFINANCE.NS',
    'MARUTI.NS', 'TATAMOTORS.NS', 'SUNPHARMA.NS', 'ONGC.NS', 'TITAN.NS', 'NTPC.NS',
    'POWERGRID.NS', 'ULTRACEMCO.NS', 'WIPRO.NS', 'NESTLEIND.NS'
]

# --- 5. CORE FUNCTIONS ---
def fetch_live_quotes(keys_list):
    if not keys_list: return {}
    try:
        keys_str = ",".join(keys_list)
        # Using OHLC endpoint (1d interval)
        response = quote_api.get_market_quote_ohlc(symbol=keys_str, interval='1d', api_version='2.0')
        if response.status == 'success':
            return response.data
    except Exception as e:
        if SHOW_DEBUG: st.error(f"Live Quote Error: {e}")
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
    """
    Robustly extracts LTP and % Change.
    Used by BOTH Dashboard and Scanner for consistency.
    """
    ltp, pct = 0.0, 0.0
    if not response_data: return ltp, pct

    # 1. Fuzzy Match Key
    data_item = None
    if target_key in response_data:
        data_item = response_data[target_key]
    else:
        # Fallback: Search for matching token inside response keys
        target_part = target_key.split("|")[-1]
        for k, v in response_data.items():
            if target_part in str(k):
                data_item = v
                break
    
    if not data_item: return 0.0, 0.0

    # 2. Extract Values
    try:
        # Handle Dict
        if isinstance(data_item, dict):
            ltp = float(data_item.get('last_price', 0))
            ohlc = data_item.get('ohlc', {})
            open_price = float(ohlc.get('open', 0))
            close_price = float(ohlc.get('close', 0))
        # Handle Object
        else:
            ltp = float(data_item.last_price)
            open_price = float(data_item.ohlc.open)
            close_price = float(data_item.ohlc.close)

        # 3. Calculate % Change (Using OPEN for accuracy)
        ref_price = open_price if open_price > 0 else close_price
        
        if ref_price > 0:
            pct = ((ltp - ref_price) / ref_price) * 100
            
    except Exception: pass

    return ltp, pct

# --- 6. DASHBOARD ---
@st.fragment(run_every=2)
def market_dashboard():
    indices = {
        "NIFTY 50": INDEX_MAP.get("Nifty 50", "NSE_INDEX|Nifty 50"),
        "BANK NIFTY": INDEX_MAP.get("Nifty Bank", "NSE_INDEX|Nifty Bank"),
        "SENSEX": "BSE_INDEX|SENSEX"
    }
    
    live_data = fetch_live_quotes(list(indices.values()))
    
    if SHOW_DEBUG:
        with st.expander("🔍 Raw Dashboard Data", expanded=True):
            st.write(live_data)

    n_ltp, n_pct = extract_data_robust(live_data, indices["NIFTY 50"])
    b_ltp, b_pct = extract_data_robust(live_data, indices["BANK NIFTY"])
    s_ltp, s_pct = extract_data_robust(live_data, indices["SENSEX"])

    c1, c2, c3, c4 = st.columns([1,1,1,1.5])
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
    
    # Prepare Keys
    valid_keys = []
    key_to_name = {}
    
    for s in FNO_SYMBOLS:
        clean = s.replace('.NS','')
        if clean in SYMBOL_MAP:
            k = SYMBOL_MAP[clean]
            valid_keys.append(k)
            key_to_name[k] = clean
            
    # Batch Fetch
    live_quotes = fetch_live_quotes(valid_keys)
    
    for i, key in enumerate(valid_keys):
        try:
            # 1. LIVE PRICE (Using the same robust helper!)
            ltp, _ = extract_data_robust(live_quotes, key)
            
            # 2. HISTORY
            df = fetch_history(key)
            if df is None or len(df) < 30: continue
            
            # Fallback if live failed
            if ltp == 0: ltp = df.iloc[-1]['close']
            
            # 3. INDICATORS
            df['RSI'] = ta.rsi(df['close'], 14)
            df['ADX'] = ta.adx(df['high'], df['low'], df['close'], 14)['ADX_14']
            df['EMA'] = ta.ema(df['close'], 5)
            
            last = df.iloc[-1]
            mom_pct = round(((ltp - last['EMA'])/last['EMA'])*100, 2)
            
            row = {
                "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{key_to_name[key]}",
                "LTP": ltp, "Mom %": mom_pct,
                "RSI": round(last['RSI'], 2), "ADX": round(last['ADX'], 2)
            }
            
            if mom_pct > 0.5 and last['RSI'] > 60: bulls.append(row)
            elif mom_pct < -0.5 and last['RSI'] < 40: bears.append(row)
            
            bar.progress((i+1)/len(valid_keys))
        except: pass
    
    bar.empty()
    
    col_conf = {"Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"), "LTP": st.column_config.NumberColumn("Price", format="₹%.2f")}
    
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 TOP 10 BULLS")
        if bulls: st.dataframe(pd.DataFrame(bulls).sort_values("Mom %", ascending=False).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bullish signals.")
        
    with c2:
        st.error("🔴 TOP 10 BEARS")
        if bears: st.dataframe(pd.DataFrame(bears).sort_values("Mom %", ascending=True).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bearish signals.")
        
    st.markdown(f"<div style='text-align:left; color:grey; margin-top:20px;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}<br><strong>Powered by : i-Tech World</strong></div>", unsafe_allow_html=True)

scanner()
