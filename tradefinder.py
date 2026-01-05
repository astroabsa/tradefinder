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

# --- 3. DHAN API SETUP (FROM SECRETS) ---
try:
    CLIENT_ID = st.secrets["DHAN_CLIENT_ID"]
    ACCESS_TOKEN = st.secrets["DHAN_ACCESS_TOKEN"]
except Exception:
    st.error("⚠️ Secrets Missing! Please add 'DHAN_CLIENT_ID' and 'DHAN_ACCESS_TOKEN' to your .streamlit/secrets.toml file.")
    st.stop()

# Initialize Dhan Object
try:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
except Exception as e:
    st.error(f"Dhan Connection Failed: {e}")
    st.stop()

# --- 4. SMART MASTER LIST ---
@st.cache_data(ttl=3600*24)
def get_dhan_master_map():
    """Downloads Dhan's Scrip Master to map Symbols -> Security IDs"""
    symbol_map = {}
    index_map = {}
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        s = requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        # Filter for NSE Equity and Indices
        eq_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')]
        idx_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'INDEX')]
        
        symbol_map = dict(zip(eq_df['SEM_TRADING_SYMBOL'], eq_df['SEM_SMST_SECURITY_ID']))
        index_map = dict(zip(idx_df['SEM_TRADING_SYMBOL'], idx_df['SEM_SMST_SECURITY_ID']))
        
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return symbol_map, index_map

with st.spinner("Connecting to DhanHQ..."):
    SYMBOL_MAP, INDEX_MAP = get_dhan_master_map()

# --- 5. DATA FETCHING ---
def fetch_15min_data(security_id, exchange_segment='NSE_EQ'):
    """Fetches 15-min candles using Dhan v2 API."""
    try:
        to_d = datetime.now().strftime("%Y-%m-%d")
        from_d = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        # Using correct v2 endpoint
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
            
            df = pd.DataFrame({
                'timestamp': data['start_Time'],
                'open': data['open'],
                'high': data['high'],
                'low': data['low'],
                'close': data['close'],
                'volume': data['volume']
            })
            return df
    except Exception: pass
    return None

# --- 6. DASHBOARD ---
@st.fragment(run_every=60)
def market_dashboard():
    st.markdown("## 🚀 Absa's DhanHQ Live Screener")
    
    def get_index_val(name, dhan_symbol):
        if dhan_symbol in INDEX_MAP:
            sec_id = INDEX_MAP[dhan_symbol]
            df = fetch_15min_data(sec_id, exchange_segment='IDX_I') 
            if df is not None and not df.empty:
                ltp = df.iloc[-1]['close']
                prev = df.iloc[-2]['close']
                pct = ((ltp - prev)/prev)*100
                return ltp, pct
        return 0.0, 0.0

    n_ltp, n_pct = get_index_val("NIFTY", "Nifty 50")
    b_ltp, b_pct = get_index_val("BANKNIFTY", "Nifty Bank")
    s_ltp, s_pct = get_index_val("FINNIFTY", "Nifty Fin Service") 

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

# --- 7. SCANNER ---
@st.fragment(run_every=60)
def scanner():
    target_stocks = [
        'RELIANCE', 'TCS', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'INFY', 'ITC', 
        'BHARTIARTL', 'LT', 'AXISBANK', 'KOTAKBANK', 'ULTRACEMCO', 'BAJFINANCE',
        'MARUTI', 'SUNPHARMA', 'TITAN', 'TATAMOTORS', 'NTPC', 'POWERGRID', 'ADANIENT'
    ]
    
    all_stocks = []
    debug_exp = st.expander("🔍 Scanner Debug (Check Errors)", expanded=False)
    
    bar = st.progress(0, "Scanning DhanHQ (15-Min)...")
    
    for i, symbol in enumerate(target_stocks):
        try:
            if symbol not in SYMBOL_MAP:
                debug_exp.write(f"❌ {symbol} not found in Master List")
                continue
            sec_id = SYMBOL_MAP[symbol]
            
            # 1. Fetch Data
            df = fetch_15min_data(sec_id)
            
            if df is not None and len(df) > 20:
                # 2. Technicals
                df['RSI'] = ta.rsi(df['close'], 14)
                df['ADX'] = ta.adx(df['high'], df['low'], df['close'], 14)['ADX_14']
                df['EMA'] = ta.ema(df['close'], 5)
                
                curr = df.iloc[-1]
                ltp = curr['close']
                curr_rsi = round(curr['RSI'], 2)
                curr_adx = round(curr['ADX'], 2)
                
                ema_val = curr['EMA']
                mom_pct = round(((ltp - ema_val) / ema_val) * 100, 2)
                
                # 3. Logic
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
            else:
                debug_exp.write(f"⚠️ No Data for {symbol} (Sec ID: {sec_id})")
                
        except Exception as e:
            debug_exp.write(f"❌ Error {symbol}: {e}")
            pass
            
        bar.progress((i + 1) / len(target_stocks))
        time.sleep(0.1) 
    
    bar.empty()
    
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
        st.info("Waiting for data... Check the 'Scanner Debug' section above for details.")
        
    st.markdown(f"<div style='text-align:left; color:grey; margin-top:20px;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

scanner()
