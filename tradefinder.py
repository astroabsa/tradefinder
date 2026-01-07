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
IST = pytz.timezone('Asia/Kolkata') # Force IST Timezone

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
    st.title("🔐 iTW's F&O Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username"); p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p): st.session_state["authenticated"] = True; st.rerun()
            else: st.error("Invalid Credentials")
    st.stop()

# --- 3. MAIN UI ---
st.title("🚀 iTW's Live F&O Pro")
if st.sidebar.button("Log out"): st.session_state["authenticated"] = False; st.rerun()

# --- 4. API CONNECTION ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e: st.error(f"API Error: {e}"); st.stop()

# --- 5. INDEX MAP (Standardized) ---
INDEX_MAP = {
    'NIFTY': {'id': '13', 'name': 'NIFTY 50'}, 
    'BANKNIFTY': {'id': '25', 'name': 'BANK NIFTY'}, 
    'SENSEX': {'id': '51', 'name': 'SENSEX'}
}

# --- 6. CUSTOM CSV WATCHLIST LOADER (UPDATED) ---
@st.cache_data(ttl=3600*4)
def get_fno_stock_map():
    fno_map = {}
    # File must be in the same folder as this script
    file_path = "stock_watchlist.csv" 
    
    if not os.path.exists(file_path):
        st.error(f"❌ '{file_path}' NOT FOUND. Please upload it to your repository folder.")
        return fno_map 

    try:
        # Read the CSV with the specific columns from your screenshot
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip() 
        
        # Check for the required columns shown in your image
        req_cols = ['SEM_TRADING_SYMBOL', 'SEM_SMST_SECURITY_ID']
        
        if all(col in df.columns for col in req_cols):
            st.sidebar.success(f"✅ Loaded {len(df)} Stocks from CSV")
            
            for index, row in df.iterrows():
                # Extract Symbol and ID
                sym = str(row['SEM_TRADING_SYMBOL']).strip().upper()
                sid = str(row['SEM_SMST_SECURITY_ID']).strip()
                
                # Add to map
                fno_map[sym] = {'id': sid, 'name': sym}
        else:
            st.sidebar.error(f"❌ CSV Error: Missing columns. Found: {list(df.columns)}")
            
    except Exception as e: st.error(f"Error reading CSV: {e}")
    return fno_map

# --- 7. HELPER: GET YESTERDAY'S CLOSE (The "Simple" Way) ---
def get_prev_close(security_id):
    try:
        # Fetch last 10 days of DAILY candles. The API handles the "Daily" logic.
        to_d = datetime.now(IST).strftime('%Y-%m-%d')
        from_d = (datetime.now(IST) - timedelta(days=10)).strftime('%Y-%m-%d')
        
        # IDX_I works for all indices (NSE & BSE)
        res = dhan.historical_daily_data(str(security_id), "IDX_I", "INDEX", from_d, to_d)
        
        if res['status'] == 'success' and 'data' in res:
            df = pd.DataFrame(res['data'])
            if df.empty: return 0.0
            
            # Helper to get date string
            time_col = 'start_Time' if 'start_Time' in df.columns else 'timestamp'
            df['date_str'] = df[time_col].astype(str).str[:10]
            
            # Filter out "Today" to find the last completed day
            today_str = datetime.now(IST).strftime('%Y-%m-%d')
            past_df = df[df['date_str'] != today_str]
            
            if not past_df.empty:
                return float(past_df.iloc[-1]['close'])
                
    except: pass
    return 0.0

# --- 8. HELPER: GET LIVE PRICE (The "Simple" Way) ---
def get_live_price(security_id):
    try:
        # Intraday chart for just today/yesterday gives the absolute latest price
        to_d = datetime.now(IST).strftime('%Y-%m-%d')
        from_d = (datetime.now(IST) - timedelta(days=3)).strftime('%Y-%m-%d')
        
        res = dhan.intraday_minute_data(str(security_id), "IDX_I", "INDEX", from_d, to_d, 1)
        
        if res['status'] == 'success' and 'data' in res:
            closes = res['data']['close']
            if len(closes) > 0:
                return float(closes[-1])
    except: pass
    return 0.0

# --- 9. DASHBOARD ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    data = {}
    
    for key, info in INDEX_MAP.items():
        sid = info['id']
        
        # 1. Direct API call for Yesterday
        prev = get_prev_close(sid)
        
        # 2. Direct API call for Today
        ltp = get_live_price(sid)
        
        # 3. Simple Math
        if ltp == 0: ltp = prev # Fallback if live fetch fails
        if prev == 0: prev = ltp # Fallback if history fetch fails
        
        chg = 0.0
        pct = 0.0
        
        if prev > 0:
            chg = ltp - prev
            pct = (chg / prev) * 100
            
        data[info['name']] = {"ltp": ltp, "chg": chg, "pct": pct}

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

# --- 10. SCANNER ---
def get_trend_analysis(price_chg, vol_ratio):
    if price_chg > 0 and vol_ratio > 1.2: return "Bullish (Vol) 🟢"
    if price_chg < 0 and vol_ratio > 1.2: return "Bearish (Vol) 🔴"
    if price_chg > 0: return "Mild Bullish ↗️"
    if price_chg < 0: return "Mild Bearish ↘️"
    return "Neutral ⚪"

@st.fragment(run_every=180)
def refreshable_scanner():
    st.markdown("---")
    st.caption(f"Scanning {len(FNO_MAP)} symbols... (Updates every 3 mins)")
    
    tab1, tab2 = st.tabs(["🚀 Signals", "📋 All Data"])
    targets = list(FNO_MAP.keys())
    if not targets: st.warning("Scanner paused: No symbols found."); return

    bar = st.progress(0)
    bull, bear, all_data = [], [], []

    for i, sym in enumerate(targets):
        try:
            sid = FNO_MAP[sym]['id']
            
            # Simple Intraday Fetch for Stocks
            to_d = datetime.now(IST).strftime('%Y-%m-%d')
            from_d = (datetime.now(IST) - timedelta(days=5)).strftime('%Y-%m-%d')
            res = dhan.intraday_minute_data(str(sid), "NSE_FNO", "FUTSTK", from_d, to_d, 60)
            
            if res['status'] == 'success':
                raw_data = res['data']
                if raw_data:
                    df = pd.DataFrame(raw_data)
                    rename_map = {'start_Time':'datetime', 'timestamp':'datetime', 'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume', 'oi':'OI'}
                    df.rename(columns=rename_map, inplace=True)
                    
                    if not df.empty and len(df) > 0:
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

    st.write(f"🕒 **Last Data Sync:** {datetime.now(IST).strftime('%H:%M:%S')} IST")
    st.markdown("<div style='text-align: center; color: grey;'>Powered by : i-Tech World</div>", unsafe_allow_html=True)

if dhan: refreshable_dashboard(); refreshable_scanner()
