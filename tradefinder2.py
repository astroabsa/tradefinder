import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time
import os

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="iTW's Live F&O Screener Pro + OI", layout="wide")
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
    except: 
        return False

if "authenticated" not in st.session_state: 
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    st.title("🔐 iTW's F&O Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p): 
                st.session_state["authenticated"] = True
                st.rerun()
            else: 
                st.error("Invalid Credentials")
    st.stop()

# --- 3. MAIN UI ---
st.title("🚀 iTW's Live F&O Screener Pro + OI Analysis")
if st.sidebar.button("Log out"): 
    st.session_state["authenticated"] = False
    st.rerun()

# --- 4. API CONNECTION ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e: 
    st.error(f"API Error: {e}")
    st.stop()

# --- 5. INDEX MAP ---
INDEX_MAP = {
    'NIFTY': {'id': '13', 'name': 'NIFTY 50'}, 
    'BANKNIFTY': {'id': '25', 'name': 'BANK NIFTY'}, 
    'SENSEX': {'id': '51', 'name': 'SENSEX'}
}

# --- 6. MASTER LIST LOADER ---
@st.cache_data(ttl=3600*4)
def get_fno_stock_map():
    fno_map = {}
    if not os.path.exists("dhan_master.csv"):
        st.error("❌ 'dhan_master.csv' NOT FOUND.")
        return fno_map 

    try:
        df = pd.read_csv("dhan_master.csv", on_bad_lines='skip', low_memory=False)
        df.columns = df.columns.str.strip() 
        
        col_exch = 'SEM_EXM_EXCH_ID'
        col_id = 'SEM_SMST_SECURITY_ID'
        col_name = 'SEM_TRADING_SYMBOL'
        col_inst = 'SEM_INSTRUMENT_NAME'
        col_expiry = 'SEM_EXPIRY_DATE'
        
        if col_name in df.columns: 
            df[col_name] = df[col_name].astype(str).str.upper().str.strip()
        if col_exch in df.columns: 
            df[col_exch] = df[col_exch].astype(str).str.strip()
        if col_inst in df.columns: 
            df[col_inst] = df[col_inst].astype(str).str.strip()
        
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
    except Exception as e: 
        st.error(f"Error reading CSV: {e}")
    return fno_map

with st.spinner("Loading Stock List..."):
    FNO_MAP = get_fno_stock_map()

# --- 7. OI PERSISTENCE STORAGE (Session-based) ---
if "oi_history" not in st.session_state:
    st.session_state["oi_history"] = {}  # Format: {symbol: [{'time': datetime, 'oi': value, 'price': value}, ...]}

def update_oi_history(symbol, oi_value, price):
    """Track OI changes over time to detect sustained buildup"""
    now = datetime.now(IST)
    if symbol not in st.session_state["oi_history"]:
        st.session_state["oi_history"][symbol] = []
    
    # Keep only last 6 hours of data (to persist through refreshes)
    cutoff = now - timedelta(hours=6)
    st.session_state["oi_history"][symbol] = [
        rec for rec in st.session_state["oi_history"][symbol] 
        if rec['time'] > cutoff
    ]
    
    st.session_state["oi_history"][symbol].append({
        'time': now,
        'oi': oi_value,
        'price': price
    })

def get_oi_trend_metrics(symbol):
    """Calculate OI trend strength and persistence"""
    history = st.session_state.get("oi_history", {}).get(symbol, [])
    if len(history) < 2:
        return {
            'oi_change_pct': 0.0,
            'oi_trend_duration': 0,
            'avg_oi': 0.0,
            'oi_acceleration': 0.0,
            'setup_strength': 'WEAK'
        }
    
    # Sort by time
    history = sorted(history, key=lambda x: x['time'])
    
    # Calculate OI change from first to latest
    first_oi = history[0]['oi']
    latest_oi = history[-1]['oi']
    first_price = history[0]['price']
    latest_price = history[-1]['price']
    
    if first_oi == 0:
        oi_change_pct = 0.0
    else:
        oi_change_pct = ((latest_oi - first_oi) / first_oi) * 100
    
    # Duration of trend in minutes
    duration_mins = (history[-1]['time'] - history[0]['time']).total_seconds() / 60
    
    # Average OI
    avg_oi = sum([h['oi'] for h in history]) / len(history)
    
    # OI Acceleration (rate of change speeding up?)
    if len(history) >= 3:
        mid_idx = len(history) // 2
        early_oi = sum([h['oi'] for h in history[:mid_idx]]) / mid_idx
        late_oi = sum([h['oi'] for h in history[mid_idx:]]) / (len(history) - mid_idx)
        if early_oi > 0:
            oi_acceleration = ((late_oi - early_oi) / early_oi) * 100
        else:
            oi_acceleration = 0.0
    else:
        oi_acceleration = 0.0
    
    # Setup Strength Classification
    setup_strength = 'WEAK'
    if duration_mins >= 60 and abs(oi_change_pct) > 5:
        setup_strength = 'STRONG'
    elif duration_mins >= 30 and abs(oi_change_pct) > 3:
        setup_strength = 'MODERATE'
    elif duration_mins >= 15 and abs(oi_change_pct) > 1.5:
        setup_strength = 'BUILDING'
    
    return {
        'oi_change_pct': round(oi_change_pct, 2),
        'oi_trend_duration': int(duration_mins),
        'avg_oi': round(avg_oi, 0),
        'oi_acceleration': round(oi_acceleration, 2),
        'setup_strength': setup_strength,
        'first_price': first_price,
        'price_change_pct': round(((latest_price - first_price) / first_price) * 100, 2) if first_price > 0 else 0.0
    }

# --- 8. HELPER: GET YESTERDAY'S CLOSE ---
def get_prev_close(security_id):
    try:
        to_d = datetime.now(IST).strftime('%Y-%m-%d')
        from_d = (datetime.now(IST) - timedelta(days=10)).strftime('%Y-%m-%d')
        
        res = dhan.historical_daily_data(str(security_id), "IDX_I", "INDEX", from_d, to_d)
        
        if res['status'] == 'success' and 'data' in res:
            df = pd.DataFrame(res['data'])
            if df.empty: 
                return 0.0
            
            time_col = 'start_Time' if 'start_Time' in df.columns else 'timestamp'
            df['date_str'] = df[time_col].astype(str).str[:10]
            
            today_str = datetime.now(IST).strftime('%Y-%m-%d')
            past_df = df[df['date_str'] != today_str]
            
            if not past_df.empty:
                return float(past_df.iloc[-1]['close'])
    except: 
        pass
    return 0.0

# --- 9. HELPER: GET LIVE PRICE ---
def get_live_price(security_id):
    try:
        to_d = datetime.now(IST).strftime('%Y-%m-%d')
        from_d = (datetime.now(IST) - timedelta(days=3)).strftime('%Y-%m-%d')
        
        res = dhan.intraday_minute_data(str(security_id), "IDX_I", "INDEX", from_d, to_d, 1)
        
        if res['status'] == 'success' and 'data' in res:
            closes = res['data']['close']
            if len(closes) > 0:
                return float(closes[-1])
    except: 
        pass
    return 0.0

# --- 10. DASHBOARD ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    data = {}
    
    for key, info in INDEX_MAP.items():
        sid = info['id']
        prev = get_prev_close(sid)
        ltp = get_live_price(sid)
        
        if ltp == 0: 
            ltp = prev
        if prev == 0: 
            prev = ltp
        
        chg = 0.0
        pct = 0.0
        
        if prev > 0:
            chg = ltp - prev
            pct = (chg / prev) * 100
            
        data[info['name']] = {"ltp": ltp, "chg": chg, "pct": pct}

    c1, c2, c3, c4 = st.columns([1,1,1,1.2])
    with c1: 
        d = data.get("NIFTY 50")
        st.metric("NIFTY 50", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c2: 
        d = data.get("BANK NIFTY")
        st.metric("BANK NIFTY", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c3: 
        d = data.get("SENSEX")
        st.metric("SENSEX", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        nifty_pct = data.get("NIFTY 50", {}).get('pct', 0)
        if nifty_pct > 0.25: 
            bias, color = ("BULLISH 🚀", "green")
        elif nifty_pct < -0.25: 
            bias, color = ("BEARISH 📉", "red")
        st.markdown(f"<div style='text-align:center; padding:10px; border:1px solid {color}; border-radius:10px; color:{color}'><h3>Bias: {bias}</h3></div>", unsafe_allow_html=True)

# --- 11. OI-BASED SIGNAL CLASSIFICATION ---
def classify_oi_setup(oi_change_pct, price_change_pct, trend_strength, adx):
    """
    Classify OI setups for all-day performance potential:
    - Long Buildup: OI ↑ + Price ↑ (Fresh longs, strong conviction)
    - Short Buildup: OI ↑ + Price ↓ (Fresh shorts, strong conviction)
    - Long Unwinding: OI ↓ + Price ↓ (Profit booking, weak)
    - Short Covering: OI ↓ + Price ↑ (Squeeze, weak)
    """
    if oi_change_pct > 2:
        if price_change_pct > 0.5:
            if trend_strength == 'STRONG' and adx > 25:
                return "🟢 Long Buildup (STRONG)"
            return "🟢 Long Buildup"
        elif price_change_pct < -0.5:
            if trend_strength == 'STRONG' and adx > 25:
                return "🔴 Short Buildup (STRONG)"
            return "🔴 Short Buildup"
        else:
            return "⚪ OI Buildup (Neutral)"
    elif oi_change_pct < -2:
        if price_change_pct > 0.5:
            return "🟡 Short Covering"
        elif price_change_pct < -0.5:
            return "🟠 Long Unwinding"
        else:
            return "⚪ OI Reduction"
    else:
        if price_change_pct > 1:
            return "↗️ Price Up (No OI)"
        elif price_change_pct < -1:
            return "↘️ Price Down (No OI)"
        return "⚪ Neutral"

def calculate_conviction_score(oi_metrics, rsi, adx, vol_ratio, mom):
    """
    Calculate a conviction score (0-100) for all-day performance potential
    Higher score = more likely to sustain momentum throughout the day
    """
    score = 0
    
    # OI Trend Strength (max 30 points)
    if oi_metrics['setup_strength'] == 'STRONG':
        score += 30
    elif oi_metrics['setup_strength'] == 'MODERATE':
        score += 20
    elif oi_metrics['setup_strength'] == 'BUILDING':
        score += 10
    
    # OI Acceleration (max 15 points)
    if abs(oi_metrics['oi_acceleration']) > 5:
        score += 15
    elif abs(oi_metrics['oi_acceleration']) > 2:
        score += 8
    
    # OI Change Magnitude (max 15 points)
    if abs(oi_metrics['oi_change_pct']) > 8:
        score += 15
    elif abs(oi_metrics['oi_change_pct']) > 5:
        score += 10
    elif abs(oi_metrics['oi_change_pct']) > 2:
        score += 5
    
    # Duration (max 10 points) - longer trend = more conviction
    if oi_metrics['oi_trend_duration'] > 120:  # > 2 hours
        score += 10
    elif oi_metrics['oi_trend_duration'] > 60:
        score += 7
    elif oi_metrics['oi_trend_duration'] > 30:
        score += 4
    
    # Technical Confirmation (max 20 points)
    if 40 < rsi < 65:  # Sweet spot for continuation
        score += 10
    if adx > 25:  # Strong trend
        score += 10
    
    # Volume Confirmation (max 10 points)
    if vol_ratio > 1.5:
        score += 10
    elif vol_ratio > 1.2:
        score += 5
    
    return min(score, 100)

# --- 12. SCANNER WITH OI ANALYSIS ---
@st.fragment(run_every=180)
def refreshable_scanner():
    st.markdown("---")
    st.caption(f"Scanning {len(FNO_MAP)} symbols with OI Analysis... (Updates every 3 mins)")
    
    # Create tabs including new OI-focused tab
    tab1, tab2, tab3, tab4 = st.tabs(["🚀 High Conviction (OI)", "📊 OI Buildup", "📋 All Data", "📈 OI History"])
    targets = list(FNO_MAP.keys())
    if not targets: 
        st.warning("Scanner paused: No symbols found.")
        return

    bar = st.progress(0)
    high_conviction, oi_buildup, all_data = [], [], []

    for i, sym in enumerate(targets):
        try:
            sid = FNO_MAP[sym]['id']
            
            # Fetch intraday data
            to_d = datetime.now(IST).strftime('%Y-%m-%d')
            from_d = (datetime.now(IST) - timedelta(days=5)).strftime('%Y-%m-%d')
            res = dhan.intraday_min
