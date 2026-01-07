import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time
import os

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="Absa's Live F&O Screener Pro", layout="wide")

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

if "authenticated" not in st.session_state: st.session_state["authenticated"] = False
if not st.session_state["authenticated"]:
    st.title("🔐 Absa's F&O Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p): st.session_state["authenticated"] = True; st.rerun()
            else: st.error("Invalid Credentials")
    st.stop()

# --- 3. MAIN APP START ---
st.title("🚀 Absa's Live F&O Screener Pro")
if st.sidebar.button("Log out"): st.session_state["authenticated"] = False; st.rerun()

# --- 4. DHAN API CONNECTION ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e: st.error(f"API Error: {e}"); st.stop()

# --- 5. HARDCODED INDEX MAP ---
INDEX_MAP = {'NIFTY': '13', 'BANKNIFTY': '25', 'SENSEX': '51206'}

# --- 6. ROBUST MASTER LIST LOADER ---
@st.cache_data(ttl=3600*4)
def get_fno_stock_map():
    fno_map = {}
    
    # Check File
    if not os.path.exists("dhan_master.csv"):
        st.error("❌ 'dhan_master.csv' NOT FOUND. Please upload it to your app folder.")
        return fno_map 

    try:
        # Load CSV
        df = pd.read_csv("dhan_master.csv", on_bad_lines='skip', low_memory=False)
        df.columns = df.columns.str.strip() 
        
        # Sidebar Debug Stats
        st.sidebar.markdown("### 🛠️ CSV Debugger")
        st.sidebar.info(f"Total Rows in CSV: {len(df)}")
        
        # Standardize Columns
        col_exch = 'SEM_EXM_EXCH_ID'
        col_id = 'SEM_SMST_SECURITY_ID'
        col_name = 'SEM_TRADING_SYMBOL'
        col_inst = 'SEM_INSTRUMENT_NAME'
        col_expiry = 'SEM_EXPIRY_DATE'
        
        # Normalize Text
        if col_name in df.columns: df[col_name] = df[col_name].astype(str).str.upper().str.strip()
        if col_exch in df.columns: df[col_exch] = df[col_exch].astype(str).str.strip()
        if col_inst in df.columns: df[col_inst] = df[col_inst].astype(str).str.strip()
        
        # Filter 1: Exchange & Instrument
        if col_exch in df.columns and col_inst in df.columns:
            stk_df = df[(df[col_exch] == 'NSE') & (df[col_inst] == 'FUTSTK')].copy()
            st.sidebar.info(f"NSE Futures Found: {len(stk_df)}")
            
            # Filter 2: Dates (The Tricky Part)
            if col_expiry in stk_df.columns:
                # Force string format first to handle mixed types
                stk_df[col_expiry] = stk_df[col_expiry].astype(str)
                
                # Robust Parsing: Try Day-First, coerce errors
                stk_df['dt_parsed'] = pd.to_datetime(stk_df[col_expiry], dayfirst=True, errors='coerce')
                
                # Check for parsing failures
                valid_dates = stk_df[stk_df['dt_parsed'].notna()]
                st.sidebar.info(f"Valid Dates Parsed: {len(valid_dates)}")
                
                # Filter Future Expiries
                today = pd.Timestamp.now().normalize()
                valid_futures = valid_dates[valid_dates['dt_parsed'] >= today]
                st.sidebar.success(f"Active Futures (>= Today): {len(valid_futures)}")
                
                if len(valid_futures) == 0:
                    st.error("❌ All dates were filtered out! Check if your CSV dates are older than today.")
                
                # Sort and Deduplicate
                valid_futures = valid_futures.sort_values(by=[col_name, 'dt_parsed'])
                curr_stk = valid_futures.drop_duplicates(subset=[col_name], keep='first')
                
                # Build Map
                for _, row in curr_stk.iterrows():
                    base_sym = row[col_name].split('-')[0]
                    # Skip Test Symbols usually starting with numbers (like 011NSETEST)
                    if not base_sym[0].isdigit(): 
                        disp_name = row.get('SEM_CUSTOM_SYMBOL', row[col_name])
                        fno_map[base_sym] = {'id': str(row[col_id]), 'name': disp_name}
        
        if not fno_map:
            st.error("❌ Scanner List is Empty. Check Sidebar Debugger for details.")
            
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
    
    return fno_map

with st.spinner("Loading Stock List..."):
    FNO_MAP = get_fno_stock_map()

# --- 7. DATA FETCHING ---
def fetch_futures_data(security_id, interval=60):
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        res = dhan.intraday_minute_data(str(security_id), "NSE_FNO", "FUTSTK", from_date, to_date, interval)
        if res['status'] == 'success':
            df = pd.DataFrame(res['data'])
            if df.empty: return pd.DataFrame()
            df.rename(columns={'start_Time':'datetime', 'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume', 'oi':'OI'}, inplace=True)
            return df
    except: pass
    return pd.DataFrame()

# --- 8. OI LOGIC ---
def get_oi_analysis(price_chg, oi_chg):
    if price_chg > 0 and oi_chg > 0: return "Long Buildup 🟢"
    if price_chg < 0 and oi_chg > 0: return "Short Buildup 🔴"
    if price_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"
    if price_chg > 0 and oi_chg < 0: return "Short Covering 🚀"
    return "Neutral ⚪"

# --- 9. DASHBOARD (Fixed Lookback) ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    indices = [
        {"name": "NIFTY 50", "key": "NIFTY", "seg": "IDX_I"},
        {"name": "BANK NIFTY", "key": "BANKNIFTY", "seg": "IDX_I"},
        {"name": "SENSEX", "key": "SENSEX", "seg": "BSE_IDX"}
    ]
    data = {}
    for i in indices:
        try:
            to_d = datetime.now().strftime('%Y-%m-%d')
            # Look back 10 days to handle weekends/holidays
            from_d = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
            r = dhan.intraday_minute_data(INDEX_MAP[i['key']], i['seg'], "INDEX", from_d, to_d, 1)
            
            if r['status'] == 'success' and r['data'].get('close'):
                ltp = r['data']['close'][-1]
                # Compare vs ~1 day ago (375 mins)
                prev = r['data']['close'][max(0, len(r['data']['close']) - 375)]
                chg = ltp - prev
                pct = (chg/prev)*100 if prev > 0 else 0
                data[i['name']] = {"ltp": ltp, "chg": chg, "pct": pct}
            else: data[i['name']] = {"ltp":0.0, "chg":0.0, "pct":0.0}
        except: data[i['name']] = {"ltp":0.0, "chg":0.0, "pct":0.0}

    c1, c2, c3, c4 = st.columns([1,1,1,1.2])
    with c1: d=data.get("NIFTY 50"); st.metric("NIFTY 50", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c2: d=data.get("BANK NIFTY"); st.metric("BANK NIFTY", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c3: d=data.get("SENSEX"); st.metric("SENSEX", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        if data.get("NIFTY 50", {}).get('pct', 0) > 0.25: bias, color = ("BULLISH 🚀", "green")
        elif data.get("NIFTY 50", {}).get('pct', 0) < -0.25: bias, color = ("BEARISH 📉", "red")
        st.markdown(f"<div style='text-align:center; padding:10px; border:1px solid {color}; border-radius:10px; color:{color}'><h3>Bias: {bias}</h3></div>", unsafe_allow_html=True)

# --- 10. SCANNER ---
@st.fragment(run_every=180)
def refreshable_scanner():
    st.markdown("---")
    tab1, tab2 = st.tabs(["🚀 Signals", "📋 All Data"])
    
    targets = list(FNO_MAP.keys())
    if not targets: st.warning("Scanner paused: No symbols found."); return

    bar = st.progress(0, f"Scanning {len(targets)} Futures...")
    bull, bear, all_data = [], [], []

    for i, sym in enumerate(targets):
        try:
            sid = FNO_MAP[sym]['id']
            df = fetch_futures_data(sid)
            if not df.empty and len(df) > 20:
                df['RSI'] = ta.rsi(df['Close'], 14)
                df['ADX'] = ta.adx(df['High'], df['Low'], df['Close'], 14)['ADX_14']
                df['EMA'] = ta.ema(df['Close'], 5)
                
                curr = df.iloc[-1]; prev = df.iloc[-2]
                ltp = curr['Close']
                p_chg = round(((ltp - prev['Close'])/prev['Close'])*100, 2)
                o_chg = round(((curr['OI'] - prev['OI'])/prev['OI'])*100, 2) if prev['OI'] > 0 else 0
                sent = get_oi_analysis(p_chg, o_chg)
                mom = round(((ltp - df['EMA'].iloc[-1])/df['EMA'].iloc[-1])*100, 2)
                
                row = {
                    "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{sym}",
                    "LTP": round(ltp, 2), "Mom %": mom, "Price Chg%": p_chg,
                    "RSI": round(curr['RSI'], 1), "ADX": round(curr['ADX'], 1),
                    "OI Chg%": o_chg, "Analysis": sent
                }
                
                r_m = row.copy(); r_m['Sort'] = sym
                all_data.append(r_m)
                
                if p_chg > 0.5 and row['RSI'] > 60 and row['ADX'] > 20: bull.append(row)
                elif p_chg < -0.5 and row['RSI'] < 45 and row['ADX'] > 20: bear.append(row)
        except: pass
        bar.progress((i+1)/len(targets))
    
    bar.empty()
    cfg = {"Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"), 
           "OI Chg%": st.column_config.NumberColumn(format="%.2f%%"),
           "Price Chg%": st.column_config.NumberColumn(format="%.2f%%")}
    
    with tab1:
        c1, c2 = st.columns(2)
        with c1: 
            st.success(f"🟢 BULLS ({len(bull)})")
            if bull: st.dataframe(pd.DataFrame(bull).sort_values("Mom %", ascending=False).head(15), use_container_width=True, hide_index=True, column_config=cfg)
        with c2: 
            st.error(f"🔴 BEARS ({len(bear)})")
            if bear: st.dataframe(pd.DataFrame(bear).sort_values("Mom %", ascending=True).head(15), use_container_width=True, hide_index=True, column_config=cfg)
            
    with tab2:
        if all_data: st.dataframe(pd.DataFrame(all_data).sort_values("Sort").drop(columns=['Sort']), use_container_width=True, hide_index=True, column_config=cfg, height=600)
        else: st.warning("No data.")

    st.write(f"🕒 **Last Data Sync:** {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')} IST")
    st.markdown("<div style='text-align: center; color: grey;'>Powered by : i-Tech World</div>", unsafe_allow_html=True)

if dhan: refreshable_dashboard(); refreshable_scanner()
