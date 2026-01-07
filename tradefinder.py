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
IST = pytz.timezone('Asia/Kolkata')

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
    st.title("🔐 Absa's Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username"); p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p): st.session_state["authenticated"] = True; st.rerun()
            else: st.error("Invalid Credentials")
    st.stop()

# --- 3. MAIN UI ---
st.title("🚀 Absa's Custom Watchlist Scanner")
if st.sidebar.button("Log out"): st.session_state["authenticated"] = False; st.rerun()

# --- 4. API CONNECTION ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e: st.error(f"API Error: {e}"); st.stop()

# --- 5. INDEX MAP ---
INDEX_MAP = {
    'NIFTY': {'id': '13', 'name': 'NIFTY 50'}, 
    'BANKNIFTY': {'id': '25', 'name': 'BANK NIFTY'}, 
    'SENSEX': {'id': '51', 'name': 'SENSEX'}
}

# --- 6. CSV WATCHLIST LOADER ---
@st.cache_data(ttl=3600*4)
def get_fno_stock_map():
    fno_map = {}
    file_path = "stock_watchlist.csv" 
    
    if not os.path.exists(file_path):
        st.error(f"❌ '{file_path}' NOT FOUND. Please upload it to your repository.")
        return fno_map 

    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip() 
        req_cols = ['SEM_TRADING_SYMBOL', 'SEM_SMST_SECURITY_ID']
        
        if all(col in df.columns for col in req_cols):
            st.sidebar.success(f"✅ Loaded {len(df)} Stocks")
            for index, row in df.iterrows():
                try:
                    sym = str(row['SEM_TRADING_SYMBOL']).strip().upper()
                    raw_id = row['SEM_SMST_SECURITY_ID']
                    # Clean ID to ensure it's a pure string digit
                    try: sid = str(int(float(raw_id))).strip()
                    except: sid = str(raw_id).strip()
                    fno_map[sym] = {'id': sid, 'name': sym}
                except: continue
        else:
            st.sidebar.error(f"❌ CSV Missing Columns: {req_cols}")
    except Exception as e: st.error(f"Error reading CSV: {e}")
    return fno_map

with st.spinner("Loading Stock List..."):
    FNO_MAP = get_fno_stock_map()

# --- 7. HELPER: DAILY DATA FETCH (Robust for After-Hours) ---
def fetch_daily_data(security_id):
    try:
        # Fetch 60 days of daily history (Enough for EMA/RSI)
        to_d = datetime.now(IST).strftime('%Y-%m-%d')
        from_d = (datetime.now(IST) - timedelta(days=90)).strftime('%Y-%m-%d')
        
        # Using "NSE", "EQUITY" as per your CSV type
        res = dhan.historical_daily_data(str(security_id), "NSE", "EQUITY", from_d, to_d)
        
        if res['status'] == 'success':
            data = res.get('data', {})
            if data:
                df = pd.DataFrame(data)
                # Standardize column names
                if 'start_Time' in df.columns: df['datetime'] = df['start_Time']
                elif 'timestamp' in df.columns: df['datetime'] = df['timestamp']
                
                # Ensure we have numeric columns
                cols = ['open', 'high', 'low', 'close', 'volume']
                for c in cols: df[c] = pd.to_numeric(df[c])
                
                return df
    except: pass
    return pd.DataFrame()

# --- 8. DASHBOARD ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    # Simple dashboard logic (omitted for brevity, using placeholder logic if needed or keep existing)
    # Keeping it simple as requested:
    data = {}
    # (Reuse the robust dashboard logic from previous step or keep it minimal)
    # For now, let's just show a static header to focus on the scanner fix
    st.markdown("### 📊 Market Status (Daily)")

# --- 9. SCANNER LOGIC ---
def get_trend_analysis(price_chg, rsi, adx):
    if price_chg > 0 and rsi > 55 and adx > 20: return "Bullish 🟢"
    if price_chg < 0 and rsi < 45 and adx > 20: return "Bearish 🔴"
    if rsi > 70: return "Overbought ⚠️"
    if rsi < 30: return "Oversold ⚠️"
    return "Neutral ⚪"

@st.fragment(run_every=180)
def refreshable_scanner():
    st.markdown("---")
    
    targets = list(FNO_MAP.keys())
    if not targets:
        st.warning("No symbols loaded.")
        return

    st.caption(f"Scanning {len(targets)} Stocks (Daily Candles for Stability)...")
    
    tab1, tab2 = st.tabs(["🚀 Signals", "📋 All Data"])
    
    # Placeholder for data
    all_data = []
    bull = []
    bear = []
    
    # Progress Bar
    bar = st.progress(0)
    
    for i, sym in enumerate(targets):
        sid = FNO_MAP[sym]['id']
        
        # Initialize default row (So every symbol appears!)
        row = {
            "Symbol": sym, "LTP": 0.0, "Chg%": 0.0, 
            "RSI": 0.0, "ADX": 0.0, "Vol": 0, "Analysis": "No Data"
        }
        
        try:
            df = fetch_daily_data(sid)
            
            if not df.empty and len(df) > 10:
                # Calculate Indicators
                df['RSI'] = ta.rsi(df['close'], 14)
                df['ADX'] = ta.adx(df['high'], df['low'], df['close'], 14)['ADX_14']
                
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                
                ltp = float(curr['close'])
                prev_close = float(prev['close'])
                
                chg_pct = 0.0
                if prev_close > 0:
                    chg_pct = ((ltp - prev_close) / prev_close) * 100
                
                curr_rsi = float(curr['RSI']) if pd.notnull(curr['RSI']) else 0.0
                curr_adx = float(curr['ADX']) if pd.notnull(curr['ADX']) else 0.0
                curr_vol = int(curr['volume'])
                
                analysis = get_trend_analysis(chg_pct, curr_rsi, curr_adx)
                
                # Update Row
                row = {
                    "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{sym}", # Clickable Link
                    "LTP": round(ltp, 2),
                    "Chg%": round(chg_pct, 2),
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "Vol": f"{curr_vol:,}",
                    "Analysis": analysis
                }
                
                # Logic for Signals Tab
                if analysis == "Bullish 🟢": bull.append(row)
                elif analysis == "Bearish 🔴": bear.append(row)
                
        except Exception as e:
            # Keep the default row but mark error
            row["Analysis"] = "Error"
            
        # Add to Master List (Always!)
        # We store a hidden sort key to sort by name later if needed
        r_m = row.copy()
        r_m['SortKey'] = sym 
        all_data.append(r_m)
        
        # Rate Limit Sleep
        time.sleep(0.05) 
        bar.progress((i+1)/len(targets))
        
    bar.empty()
    
    # --- DISPLAY ---
    
    # Config for nice table
    cfg = {
        "Symbol": st.column_config.LinkColumn("Symbol", display_text="NSE:(.*)"),
        "LTP": st.column_config.NumberColumn("Price", format="%.2f"),
        "Chg%": st.column_config.NumberColumn("Change", format="%.2f%%"),
        "RSI": st.column_config.NumberColumn("RSI", format="%.1f"),
        "ADX": st.column_config.NumberColumn("ADX", format="%.1f"),
    }

    with tab1:
        c1, c2 = st.columns(2)
        with c1: 
            st.success(f"🟢 BULLS ({len(bull)})")
            if bull: st.dataframe(pd.DataFrame(bull), hide_index=True, column_config=cfg)
        with c2: 
            st.error(f"🔴 BEARS ({len(bear)})")
            if bear: st.dataframe(pd.DataFrame(bear), hide_index=True, column_config=cfg)

    with tab2:
        if all_data:
            df_all = pd.DataFrame(all_data)
            # Remove helper key before showing
            if 'SortKey' in df_all.columns: 
                df_all = df_all.sort_values('SortKey').drop(columns=['SortKey'])
                
            st.dataframe(
                df_all, 
                use_container_width=True, 
                hide_index=True, 
                column_config=cfg,
                height=700
            )
        else:
            st.warning("No data rows generated.")

    st.write(f"🕒 **Last Sync:** {datetime.now(IST).strftime('%H:%M:%S')} IST")

if dhan: refreshable_scanner()
