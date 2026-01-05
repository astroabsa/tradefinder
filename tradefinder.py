import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import requests
import io
from datetime import datetime, timedelta
import time
import pytz

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="Absa's DhanHQ Screener", layout="wide")

# --- 2. AUTHENTICATION ---
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
    st.title("🔐 DhanHQ Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username"); p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p):
                st.session_state["authenticated"] = True
                st.rerun()
            else: st.error("Invalid Credentials")
    st.stop()

# --- 3. DHAN API CONNECTION ---
try:
    # Try fetching from secrets first
    CLIENT_ID = st.secrets["DHAN_CLIENT_ID"]
    ACCESS_TOKEN = st.secrets["DHAN_ACCESS_TOKEN"]
except:
    st.error("⚠️ Secrets not found! Please add DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN to .streamlit/secrets.toml")
    st.stop()

try:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
except Exception as e:
    st.error(f"Connection Failed: {e}")
    st.stop()

# --- 4. SMART MASTER LIST ---
@st.cache_data(ttl=3600*24)
def get_dhan_master_map():
    symbol_map = {}
    index_map = {}
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        s = requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        # Standardize Names
        df['SEM_TRADING_SYMBOL'] = df['SEM_TRADING_SYMBOL'].str.strip().str.upper()
        
        # Equities
        eq_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')]
        symbol_map = dict(zip(eq_df['SEM_TRADING_SYMBOL'], eq_df['SEM_SMST_SECURITY_ID']))
        
        # Indices (Using 'INDEX' instrument name)
        idx_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'INDEX')]
        index_map = dict(zip(idx_df['SEM_TRADING_SYMBOL'], idx_df['SEM_SMST_SECURITY_ID']))
        
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return symbol_map, index_map

with st.spinner("Syncing with Dhan Server..."):
    SYMBOL_MAP, INDEX_MAP = get_dhan_master_map()

# --- 5. CORE FUNCTIONS ---

def fetch_live_snapshot(security_ids, exchange_segment='NSE_EQ'):
    """
    Fetches REAL-TIME Snapshot (LTP, Open, OHLC) using Market Quote API.
    Fastest way to get current prices for Dashboard.
    """
    try:
        # Dhan expects securities as a Dict: {'NSE_EQ': [1333, 123]}
        # Ensure IDs are strings/ints as required. Dhan usually takes numbers for IDs.
        sec_ids = [str(x) for x in security_ids]
        
        res = dhan.ohlc_data(securities={exchange_segment: sec_ids})
        
        if res['status'] == 'success':
            return res['data'].get(exchange_segment, [])
            
    except Exception as e:
        pass
    return []

def fetch_history_data(security_id, exchange_segment='NSE_EQ'):
    """
    Fetches 15-min candles for Indicators (RSI, ADX).
    """
    try:
        to_d = datetime.now().strftime("%Y-%m-%d")
        from_d = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        res = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment=exchange_segment,
            instrument_type='EQUITY' if exchange_segment == 'NSE_EQ' else 'INDEX',
            from_date=from_d,
            to_date=to_d,
            interval=15 
        )
        
        if res['status'] == 'success':
            data = res['data']
            if not data: return None
            
            # FIX: Robust DataFrame creation (Indices might not have volume/oi)
            df = pd.DataFrame()
            df['timestamp'] = data.get('start_Time', [])
            df['open'] = data.get('open', [])
            df['high'] = data.get('high', [])
            df['low'] = data.get('low', [])
            df['close'] = data.get('close', [])
            # Handle missing volume for indices
            if 'volume' in data: df['volume'] = data['volume']
            else: df['volume'] = 0
            
            return df
    except Exception: pass
    return None

# --- 6. DASHBOARD (Uses Live Snapshot) ---
@st.fragment(run_every=10) # Fast refresh for dashboard
def market_dashboard():
    
    # 1. Prepare Request
    indices = {
        "NIFTY 50": INDEX_MAP.get("NIFTY 50"),
        "BANK NIFTY": INDEX_MAP.get("NIFTY BANK"),
        "FIN NIFTY": INDEX_MAP.get("NIFTY FIN SERVICE")
    }
    
    # Remove None values
    valid_ids = [v for k,v in indices.items() if v]
    
    # 2. Fetch Live Data
    # Indices are usually in 'IDX_I' segment
    snapshots = fetch_live_snapshot(valid_ids, exchange_segment='IDX_I')
    
    # 3. Create Map for easy access
    # Snapshot returns list of dicts. Map security_id -> data
    data_map = {str(item['security_id']): item for item in snapshots}
    
    def get_display_val(name):
        sec_id = str(indices.get(name))
        if sec_id in data_map:
            d = data_map[sec_id]
            ltp = float(d.get('last_price', 0))
            ohlc = d.get('ohlc', {})
            open_p = float(ohlc.get('open', 0))
            
            if open_p > 0:
                pct = ((ltp - open_p) / open_p) * 100
                return ltp, pct
        return 0.0, 0.0

    n_ltp, n_pct = get_display_val("NIFTY 50")
    b_ltp, b_pct = get_display_val("BANK NIFTY")
    f_ltp, f_pct = get_display_val("FIN NIFTY")

    st.markdown("### 📊 Market Dashboard")
    c1, c2, c3, c4 = st.columns([1,1,1,1.5])
    
    with c1: st.metric("NIFTY 50", f"{n_ltp:,.2f}", f"{n_pct:.2f}%")
    with c2: st.metric("BANK NIFTY", f"{b_ltp:,.2f}", f"{b_pct:.2f}%")
    with c3: st.metric("FIN NIFTY", f"{f_ltp:,.2f}", f"{f_pct:.2f}%")
    
    with c4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        if n_pct > 0.15: bias, color = ("BULLISH 🚀", "green")
        elif n_pct < -0.15: bias, color = ("BEARISH 📉", "red")
        st.markdown(f"<div style='text-align:center; padding:10px; border:1px solid {color}; border-radius:10px; color:{color}'><h3>Bias: {bias}</h3></div>", unsafe_allow_html=True)

market_dashboard()
st.markdown("---")

# --- 7. SCANNER (History + Snapshot) ---
@st.fragment(run_every=60)
def scanner():
    target_stocks = [
        'RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'INFY', 'ITC', 
        'BHARTIARTL', 'LT', 'AXISBANK', 'KOTAKBANK', 'ULTRACEMCO', 'BAJFINANCE',
        'MARUTI', 'SUNPHARMA', 'TITAN', 'TATAMOTORS', 'NTPC', 'POWERGRID', 'ADANIENT'
    ]
    
    all_stocks = []
    debug_exp = st.expander("🔍 Scanner Debug", expanded=False)
    bar = st.progress(0, "Scanning...")
    
    # 1. Prepare Batch for Live Price
    sec_ids = []
    symbol_to_id = {}
    for s in target_stocks:
        if s in SYMBOL_MAP:
            sid = SYMBOL_MAP[s]
            sec_ids.append(sid)
            symbol_to_id[s] = str(sid)
            
    # 2. Fetch Live Prices (One Shot)
    # Equities are 'NSE_EQ'
    live_data = fetch_live_snapshot(sec_ids, exchange_segment='NSE_EQ')
    live_map = {str(item['security_id']): item for item in live_data}
    
    # 3. Process Each Stock
    for i, symbol in enumerate(target_stocks):
        try:
            if symbol not in symbol_to_id: continue
            sid = symbol_to_id[symbol]
            
            # Get Live Price
            ltp = 0.0
            if sid in live_map:
                ltp = float(live_map[sid].get('last_price', 0))
            
            # Get History (For Technicals)
            df = fetch_history_data(sid)
            
            if df is not None and len(df) > 20:
                # Technicals
                df['RSI'] = ta.rsi(df['close'], 14)
                df['ADX'] = ta.adx(df['high'], df['low'], df['close'], 14)['ADX_14']
                df['EMA'] = ta.ema(df['close'], 5)
                
                curr = df.iloc[-1]
                
                # Use Live LTP if available, else history close
                final_ltp = ltp if ltp > 0 else curr['close']
                
                curr_rsi = round(curr['RSI'], 2)
                curr_adx = round(curr['ADX'], 2)
                ema_val = curr['EMA']
                
                mom_pct = round(((final_ltp - ema_val) / ema_val) * 100, 2)
                
                nature = "Neutral"
                if mom_pct > 0.1 and curr_rsi > 55: nature = "Bullish"
                elif mom_pct < -0.1 and curr_rsi < 45: nature = "Bearish"

                row = {
                    "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{symbol}",
                    "LTP": f"₹{final_ltp:,.2f}",
                    "Mom %": mom_pct,
                    "RSI": curr_rsi,
                    "ADX": curr_adx,
                    "Nature": nature
                }
                all_stocks.append(row)
            else:
                # Fallback if history fails but we have price
                if ltp > 0:
                    all_stocks.append({
                        "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{symbol}",
                        "LTP": f"₹{ltp:,.2f}",
                        "Mom %": 0.0, "RSI": 0, "ADX": 0, "Nature": "Data Lag"
                    })
                
        except Exception as e:
            debug_exp.write(f"Error {symbol}: {e}")
            
        bar.progress((i + 1) / len(target_stocks))
        time.sleep(0.1) # Polite delay
    
    bar.empty()
    
    if all_stocks:
        df_all = pd.DataFrame(all_stocks)
        
        col_conf = {
            "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
            "LTP": st.column_config.TextColumn("Price"),
        }

        st.subheader("🟢 Strongest Stocks")
        st.dataframe(
            df_all.sort_values("Mom %", ascending=False).head(10), 
            use_container_width=True, hide_index=True, column_config=col_conf
        )

        st.subheader("🔴 Weakest Stocks")
        st.dataframe(
            df_all.sort_values("Mom %", ascending=True).head(10), 
            use_container_width=True, hide_index=True, column_config=col_conf
        )
    else:
        st.info("Waiting for data... Check API permissions.")
        
    st.markdown(f"<div style='text-align:left; color:grey; margin-top:20px;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

scanner()
