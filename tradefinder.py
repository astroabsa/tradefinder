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

# --- 2. AUTHENTICATION (App Login) ---
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

# --- 3. DHAN API SETUP ---
st.sidebar.title("⚡ Dhan API Settings")
if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False; st.rerun()

# Inputs for Dhan Credentials
CLIENT_ID = st.sidebar.text_input("Client ID", value=st.secrets.get("DHAN_CLIENT_ID", ""))
ACCESS_TOKEN = st.sidebar.text_input("Access Token", type="password", value=st.secrets.get("DHAN_ACCESS_TOKEN", ""))

if not CLIENT_ID or not ACCESS_TOKEN:
    st.warning("⚠️ Please enter your Dhan Client ID and Access Token in the sidebar to start.")
    st.stop()

# Initialize Dhan Object
try:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
except Exception as e:
    st.error(f"Connection Failed: {e}")
    st.stop()

# --- 4. SMART MASTER LIST (Auto-Fetch IDs) ---
@st.cache_data(ttl=3600*24) # Cache for 24 hours
def get_dhan_master_map():
    """Downloads Dhan's Scrip Master to map Symbols -> Security IDs"""
    symbol_map = {}
    index_map = {}
    try:
        # Dhan Scrip Master URL
        url = "https://images.dhan.co/api/csv/scrip_master.csv"
        s = requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        # Filter for NSE Equity and Indices
        eq_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')]
        idx_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'INDEX')]
        
        # Create Maps: { 'RELIANCE': '2885' }
        symbol_map = dict(zip(eq_df['SEM_TRADING_SYMBOL'], eq_df['SEM_SMST_SECURITY_ID']))
        index_map = dict(zip(idx_df['SEM_TRADING_SYMBOL'], idx_df['SEM_SMST_SECURITY_ID']))
        
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return symbol_map, index_map

with st.spinner("Downloading Scrip Master..."):
    SYMBOL_MAP, INDEX_MAP = get_dhan_master_map()

# --- 5. DATA FETCHING ---
def fetch_15min_data(security_id, exchange_segment='NSE_EQ'):
    """
    Fetches 15-min candles using Dhan Historical API.
    Used for both Technicals AND 'Live' Price (last close).
    """
    try:
        # Dhan requires dates in YYYY-MM-DD
        to_d = datetime.now().strftime("%Y-%m-%d")
        from_d = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        # Fetch Data
        res = dhan.historical_minute_charts(
            symbol=str(security_id),
            exchange_segment=exchange_segment,
            instrument_type='EQUITY',
            expiry_code=0,
            from_date=from_d,
            to_date=to_d,
            interval=15 # 15 Minute Interval
        )
        
        if res['status'] == 'success':
            data = res['data']
            # Convert Dhan's list-of-lists to DataFrame
            # Dhan format: start_Time, open, high, low, close, volume, oi
            df = pd.DataFrame({
                'timestamp': data['start_Time'],
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume']
                # 'oi': data.get('oi', []) # OI might be empty for Equity
            })
            
            # Convert timestamp (Dhan uses custom integer format sometimes, standardizing it)
            # Actually Dhan returns "start_Time" as list of epochs usually.
            # Let's trust pandas to handle standard formats or just take the last one.
            return df
            
    except Exception as e:
        # st.error(f"Data Error: {e}") 
        pass
    return None

def get_oi_analysis(price_change_pct, oi_change_pct):
    if price_change_pct > 0 and oi_change_pct > 0: return "Long Buildup 🟢"
    elif price_change_pct < 0 and oi_change_pct > 0: return "Short Buildup 🔴"
    elif price_change_pct < 0 and oi_change_pct < 0: return "Long Unwinding ⚠️"
    elif price_change_pct > 0 and oi_change_pct < 0: return "Short Covering ⚡"
    else: return "Neutral ⚪"

# --- 6. DASHBOARD (Indices) ---
@st.fragment(run_every=60)
def market_dashboard():
    # Helper to get index data
    def get_index_val(name, dhan_symbol):
        if dhan_symbol in INDEX_MAP:
            sec_id = INDEX_MAP[dhan_symbol]
            # Indices are 'IDX_I' segment in Dhan usually, or NSE_INDICES
            # Using 'NSE_EQ' often works for fetching index OHLC too if mapped correctly
            # But specifically for Dhan, Index Historical is often exchange_segment='IDX_I'
            df = fetch_15min_data(sec_id, exchange_segment='IDX_I') 
            
            if df is not None and not df.empty:
                ltp = df.iloc[-1]['close']
                # Calc % Change from Day Open (Approx using first candle of today)
                # For simplicity in this snippet, using prev candle close
                prev = df.iloc[-2]['close']
                pct = ((ltp - prev)/prev)*100
                return ltp, pct
        return 0.0, 0.0

    # Nifty 50 and Bank Nifty
    n_ltp, n_pct = get_index_val("NIFTY", "Nifty 50")
    b_ltp, b_pct = get_index_val("BANKNIFTY", "Nifty Bank")
    s_ltp, s_pct = get_index_val("FINNIFTY", "Nifty Fin Service") # Example 3rd index

    c1, c2, c3, c4 = st.columns([1,1,1,1.5])
    with c1: st.metric("NIFTY 50", f"{n_ltp:,.2f}", f"{n_pct:.2f}%")
    with c2: st.metric("BANK NIFTY", f"{b_ltp:,.2f}", f"{b_pct:.2f}%")
    with c3: st.metric("FIN NIFTY", f"{s_ltp:,.2f}", f"{s_pct:.2f}%")
    
    with c4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        if n_pct > 0.15: bias, color = ("BULLISH 🚀", "green")
        elif n_pct < -0.15: bias, color = ("BEARISH 📉", "red")
        st.markdown(f"<div style='text-align:center; padding:10px; border:1px solid {color}; border-radius:10px; color:{color}'><h3>Bias: {bias}</h3></div>", unsafe_allow_html=True)

market_dashboard()
st.markdown("---")

# --- 7. SCANNER (Stocks) ---
@st.fragment(run_every=60)
def scanner():
    # Stocks List
    target_stocks = [
        'RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'INFY', 'ITC', 
        'BHARTIARTL', 'LT', 'AXISBANK', 'KOTAKBANK', 'ULTRACEMCO', 'BAJFINANCE',
        'MARUTI', 'SUNPHARMA', 'TITAN', 'TATAMOTORS', 'NTPC', 'POWERGRID', 'ADANIENT'
    ]
    
    all_stocks = []
    bar = st.progress(0, "Scanning DhanHQ (15-Min)...")
    
    for i, symbol in enumerate(target_stocks):
        try:
            # 1. Get Security ID
            if symbol not in SYMBOL_MAP:
                continue
            sec_id = SYMBOL_MAP[symbol]
            
            # 2. Fetch Data (15 Min)
            df = fetch_15min_data(sec_id)
            
            if df is not None and len(df) > 20:
                # 3. Technicals
                df['RSI'] = ta.rsi(df['close'], 14)
                df['ADX'] = ta.adx(df['high'], df['low'], df['close'], 14)['ADX_14']
                df['EMA'] = ta.ema(df['close'], 5)
                
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                
                ltp = curr['close']
                curr_rsi = round(curr['RSI'], 2)
                curr_adx = round(curr['ADX'], 2)
                
                # Momentum
                ema_val = curr['EMA']
                mom_pct = round(((ltp - ema_val) / ema_val) * 100, 2)
                
                # OI Logic (Placeholder for Equity - Equity has no OI usually)
                # Note: To get real OI, we need to query the FUTURES segment ID.
                # For this basic version, we stick to Equity Price Action.
                nature = "Waiting"
                if mom_pct > 0.1 and curr_rsi > 55: nature = "Bullish"
                elif mom_pct < -0.1 and curr_rsi < 45: nature = "Bearish"
                else: nature = "Neutral"

                row = {
                    "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{symbol}",
                    "LTP": f"₹{ltp:,.2f}",
                    "Mom %": mom_pct,
                    "RSI": curr_rsi,
                    "ADX": curr_adx,
                    "Nature": nature
                }
                all_stocks.append(row)
                
        except Exception as e:
            # st.error(e)
            pass
            
        # Update Bar
        bar.progress((i + 1) / len(target_stocks))
        time.sleep(0.1) # Tiny sleep to be polite to Dhan API
    
    bar.empty()
    
    # --- DISPLAY ---
    if all_stocks:
        df_all = pd.DataFrame(all_stocks)
        
        col_conf = {
            "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
            "LTP": st.column_config.TextColumn("Price"),
        }

        st.subheader("🟢 Strongest Stocks (15-Min)")
        st.dataframe(
            df_all.sort_values("Mom %", ascending=False).head(10), 
            use_container_width=True, hide_index=True, column_config=col_conf
        )

        st.subheader("🔴 Weakest Stocks (15-Min)")
        st.dataframe(
            df_all.sort_values("Mom %", ascending=True).head(10), 
            use_container_width=True, hide_index=True, column_config=col_conf
        )
    else:
        st.info("Waiting for data... (Check API Credentials)")
        
    st.markdown(f"<div style='text-align:left; color:grey; margin-top:20px;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

scanner()
