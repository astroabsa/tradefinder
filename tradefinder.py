import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time
import os

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="Absa's Live F&O Screener Pro", layout="wide")

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

if "authenticated" not in st.session_state: st.session_state["authenticated"] = False
if not st.session_state["authenticated"]:
    st.title("🔐 Absa's F&O Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username"); p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p): st.session_state["authenticated"] = True; st.rerun()
            else: st.error("Invalid Credentials")
    st.stop()

# --- 3. MAIN UI ---
st.title("🚀 Absa's Live F&O Screener Pro")
if st.sidebar.button("Log out"): st.session_state["authenticated"] = False; st.rerun()

# --- 4. API CONNECTION ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e: st.error(f"API Error: {e}"); st.stop()

# --- 5. INDEX CONFIGURATION (Back to what worked) ---
INDEX_CONFIG = {
    'NIFTY': {'id': '13', 'seg': 'IDX_I', 'name': 'NIFTY 50'}, 
    'BANKNIFTY': {'id': '25', 'seg': 'IDX_I', 'name': 'BANK NIFTY'}, 
    'SENSEX': {'id': '51', 'seg': 'BSE_I', 'name': 'SENSEX'}
}

# --- 6. MASTER LIST LOADER ---
@st.cache_data(ttl=3600*4)
def get_fno_stock_map():
    fno_map = {}
    if not os.path.exists("dhan_master.csv"):
        st.error("❌ 'dhan_master.csv' NOT FOUND."); return fno_map 

    try:
        df = pd.read_csv("dhan_master.csv", on_bad_lines='skip', low_memory=False)
        df.columns = df.columns.str.strip() 
        
        col_exch = 'SEM_EXM_EXCH_ID'
        col_id = 'SEM_SMST_SECURITY_ID'
        col_name = 'SEM_TRADING_SYMBOL'
        col_inst = 'SEM_INSTRUMENT_NAME'
        col_expiry = 'SEM_EXPIRY_DATE'
        
        if col_name in df.columns: df[col_name] = df[col_name].astype(str).str.upper().str.strip()
        if col_exch in df.columns: df[col_exch] = df[col_exch].astype(str).str.strip()
        if col_inst in df.columns: df[col_inst] = df[col_inst].astype(str).str.strip()
        
        if col_exch in df.columns and col_inst in df.columns:
            stk_df = df[(df[col_exch] == 'NSE') & (df[col_inst] == 'FUTSTK')].copy()
            
            if col_expiry in stk_df.columns:
                stk_df[col_expiry] = stk_df[col_expiry].astype(str)
                stk_df['dt_parsed'] = pd.to_datetime(stk_df[col_expiry], dayfirst=True, errors='coerce')
                
                today = pd.Timestamp.now().normalize()
                valid_futures = stk_df[stk_df['dt_parsed'] >= today]
                valid_futures = valid_futures.sort_values(by=[col_name, 'dt_parsed'])
                curr_stk = valid_futures.drop_duplicates(subset=[col_name], keep='first')
                
                for _, row in curr_stk.iterrows():
                    base_sym = row[col_name].split('-')[0]
                    disp_name = row.get('SEM_CUSTOM_SYMBOL', row[col_name])
                    fno_map[base_sym] = {'id': str(row[col_id]), 'name': disp_name}
        
    except Exception as e: st.error(f"Error reading CSV: {e}")
    return fno_map

with st.spinner("Loading Stock List..."):
    FNO_MAP = get_fno_stock_map()

# --- 7. DATA FETCHING ---
def fetch_futures_data(security_id, interval=60):
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        res = dhan.intraday_minute_data(str(security_id), "NSE_FNO", "FUTSTK", from_date, to_date, interval)
        return res
    except Exception as e: return {"status": "failure", "remarks": str(e)}

# --- 8. ANALYSIS LOGIC ---
def get_trend_analysis(price_chg, vol_ratio):
    if price_chg > 0 and vol_ratio > 1.2: return "Bullish (Vol) 🟢"
    if price_chg < 0 and vol_ratio > 1.2: return "Bearish (Vol) 🔴"
    if price_chg > 0: return "Mild Bullish ↗️"
    if price_chg < 0: return "Mild Bearish ↘️"
    return "Neutral ⚪"

# --- 9. DASHBOARD (REVERTED TO CHART DATA) ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    data = {}
    
    for key, info in INDEX_CONFIG.items():
        try:
            # We use intraday data because we know it works for you
            to_d = datetime.now().strftime('%Y-%m-%d')
            # Look back 5 days to ensure we find "Previous Close"
            from_d = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
            
            # Using specific segments (IDX_I for NSE, BSE_IDX for Sensex)
            r = dhan.intraday_minute_data(info['id'], info['seg'], "INDEX", from_d, to_d, 1)
            
            if r['status'] == 'success' and r['data'].get('close'):
                closes = r['data']['close']
                ltp = closes[-1]
                
                # Estimate Change: Compare to price ~1 day (375 mins) ago
                # This is an approximation but it guarantees non-zero values
                lookback_idx = max(0, len(closes) - 375)
                prev = closes[lookback_idx]
                
                if prev == 0: prev = ltp
                
                chg = ltp - prev
                pct = (chg / prev) * 100
                data[info['name']] = {"ltp": ltp, "chg": chg, "pct": pct}
            else:
                data[info['name']] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}
        except: 
            data[info['name']] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}

    c1, c2, c3, c4 = st.columns([1,1,1,1.2])
    with c1: d=data.get("NIFTY 50"); st.metric("NIFTY 50", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c2: d=data.get("BANK NIFTY"); st.metric("BANK NIFTY", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c3: d=data.get("SENSEX"); st.metric("SENSEX", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        nifty_pct = data.get("NIFTY 50", {}).get('pct', 0)
        if nifty_pct > 0.25: bias, color = ("BULLISH 🚀", "green")
        elif nifty_pct < -0.25: bias, color = ("BEARISH 📉", "red")
        st.markdown(f"<div style='text-align:center; padding:10px; border:1px solid {color}; border-radius:10px; color:{color}'><h3>Bias: {bias}</h3></div>", unsafe_allow_html=True)

# --- 10. SCANNER (WORKING) ---
@st.fragment(run_every=180)
def refreshable_scanner():
    st.markdown("---")
    st.caption(f"Scanning {len(FNO_MAP)} symbols...")
    
    tab1, tab2 = st.tabs(["🚀 Signals", "📋 All Data"])
    targets = list(FNO_MAP.keys())
    if not targets: st.warning("Scanner paused: No symbols found."); return

    bar = st.progress(0)
    bull, bear, all_data = [], [], []

    for i, sym in enumerate(targets):
        try:
            sid = FNO_MAP[sym]['id']
            res = fetch_futures_data(sid, interval=60)
            
            if res['status'] == 'success':
                raw_data = res['data']
                if raw_data:
                    df = pd.DataFrame(raw_data)
                    rename_map = {'start_Time':'datetime', 'timestamp':'datetime', 'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume', 'oi':'OI'}
                    df.rename(columns=rename_map, inplace=True)
                    
                    if not df.empty and len(df) > 0:
                        # Indicators
                        if len(df) >= 14:
                            df['RSI'] = ta.rsi(df['Close'], 14)
                            df['ADX'] = ta.adx(df['High'], df['Low'], df['Close'], 14)['ADX_14']
                            curr_rsi = df['RSI'].iloc[-1]
                            curr_adx = df['ADX'].iloc[-1]
                        else: curr_rsi = 0.0; curr_adx = 0.0

                        if len(df) >= 5:
                            df['EMA'] = ta.ema(df['Close'], 5)
                            mom = round(((df['Close'].iloc[-1] - df['EMA'].iloc[-1])/df['EMA'].iloc[-1])*100, 2)
                        else: mom = 0.0

                        # Volume Analysis
                        curr_vol = df['Volume'].iloc[-1]
                        avg_vol = df['Volume'].rolling(10).mean().iloc[-1] if len(df) > 10 else curr_vol
                        vol_ratio = curr_vol / avg_vol if avg_vol > 0 else 1.0

                        curr = df.iloc[-1]
                        ltp = curr['Close']
                        
                        if len(df) > 1:
                            prev = df.iloc[-2]
                            p_chg = round(((ltp - prev['Close'])/prev['Close'])*100, 2)
                        else: p_chg = 0.0
                            
                        sent = get_trend_analysis(p_chg, vol_ratio)
                        
                        row = {
                            "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{sym}",
                            "LTP": round(ltp, 2), "Mom %": mom, "Price Chg%": p_chg,
                            "RSI": round(curr_rsi, 1), "ADX": round(curr_adx, 1),
                            "Vol Ratio": round(vol_ratio, 1), "Analysis": sent
                        }
                        
                        r_m = row.copy(); r_m['Sort'] = sym
                        all_data.append(r_m)
                        
                        # --- SIGNALS ---
                        if curr_rsi > 0:
                            if p_chg > 0.3 and curr_rsi > 55: bull.append(row)
                            elif p_chg < -0.3 and curr_rsi < 52: bear.append(row)
                            
            elif res.get('remarks', '') == 'Too Many Requests':
                time.sleep(1.5)
                
        except: pass
        
        time.sleep(0.12)
        bar.progress((i+1)/len(targets))
    
    bar.empty()
    cfg = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)", width="medium"),
        "LTP": st.column_config.NumberColumn("LTP", format="%.2f"),
        "Mom %": st.column_config.NumberColumn("Mom%", format="%.2f%%"),
        "Price Chg%": st.column_config.NumberColumn("Chg%", format="%.2f%%"),
        "RSI": st.column_config.NumberColumn("RSI", format="%.1f"),
        "ADX": st.column_config.NumberColumn("ADX", format="%.1f"),
        "Vol Ratio": st.column_config.NumberColumn("Vol x", format="%.1fx"),
        "Analysis": st.column_config.TextColumn("Analysis", width="medium")
    }
    
    with tab1:
        c1, c2 = st.columns(2)
        with c1: 
            st.success(f"🟢 BULLS ({len(bull)})")
            if bull: st.dataframe(pd.DataFrame(bull).sort_values("Mom %", ascending=False).head(20), use_container_width=True, hide_index=True, column_config=cfg)
            else: st.info("No Strong Bullish setups.")
        with c2: 
            st.error(f"🔴 BEARS ({len(bear)})")
            if bear: st.dataframe(pd.DataFrame(bear).sort_values("Mom %", ascending=True).head(20), use_container_width=True, hide_index=True, column_config=cfg)
            else: st.info("No Strong Bearish setups.")
            
    with tab2:
        if all_data: 
            st.dataframe(pd.DataFrame(all_data).sort_values("Sort").drop(columns=['Sort']), use_container_width=True, hide_index=True, column_config=cfg, height=600)
        else: st.warning("No data found.")

    st.write(f"🕒 **Last Data Sync:** {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')} IST")
    st.markdown("<div style='text-align: center; color: grey;'>Powered by : i-Tech World</div>", unsafe_allow_html=True)

if dhan: refreshable_dashboard(); refreshable_scanner()
