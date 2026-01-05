import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time
import requests
import io

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
    except Exception as e:
        st.error(f"Login Error: {e}")
        return False

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    st.title("🔐 Absa's F&O Pro Login")
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

# --- 3. MAIN APP START ---
st.title("🚀 Absa's Live F&O Screener Pro")
if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False
    st.rerun()

# --- 4. DHAN API CONNECTION ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e:
    st.error(f"⚠️ API Error: Check .streamlit/secrets.toml. Details: {e}")
    st.stop()

# --- 5. ROBUST MASTER LIST LOADER ---
@st.cache_data(ttl=3600*4)
def get_master_data_map():
    fno_map = {}
    index_map = {}
    
    # Fail-safe defaults in case CSV fails completely
    index_map['NIFTY'] = '13'       # Standard NSE ID
    index_map['BANKNIFTY'] = '25'   # Standard NSE ID
    
    try:
        # FIX 1: Add User-Agent to prevent 403 Forbidden errors
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        
        r = requests.get(url, headers=headers)
        r.raise_for_status() # Check for download errors
        
        df = pd.read_csv(io.StringIO(r.content.decode('utf-8')))
        df.columns = df.columns.str.strip() # Fix whitespace in headers
        
        # FIX 2: Smart Column Detection (Handle format changes)
        col_exch = 'SEM_EXM_EXCH_ID' if 'SEM_EXM_EXCH_ID' in df.columns else 'EXCH_ID'
        col_id = 'SEM_SMST_SECURITY_ID' if 'SEM_SMST_SECURITY_ID' in df.columns else 'SECURITY_ID'
        col_name = 'SEM_TRADING_SYMBOL' if 'SEM_TRADING_SYMBOL' in df.columns else 'TRADING_SYMBOL'
        col_inst = 'SEM_INSTRUMENT_NAME' if 'SEM_INSTRUMENT_NAME' in df.columns else 'INSTRUMENT'
        
        # Normalize Symbols
        df[col_name] = df[col_name].astype(str).str.upper().str.strip()
        
        # 1. Stocks Futures (FUTSTK)
        stk_df = df[(df[col_exch] == 'NSE') & (df[col_inst] == 'FUTSTK')]
        
        # 2. Spot Indices (INDEX)
        idx_df = df[df[col_inst] == 'INDEX']
        
        # Helper to find nearest expiry
        def get_current_futures(dataframe):
            # Try parsing expiry date
            if 'SEM_EXPIRY_DATE' in dataframe.columns:
                dataframe['SEM_EXPIRY_DATE'] = pd.to_datetime(dataframe['SEM_EXPIRY_DATE'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                today = pd.Timestamp.now().normalize()
                valid = dataframe[dataframe['SEM_EXPIRY_DATE'] >= today]
                valid = valid.sort_values(by=[col_name, 'SEM_EXPIRY_DATE'])
                return valid.drop_duplicates(subset=[col_name], keep='first')
            return dataframe

        # Process Stocks
        curr_stk = get_current_futures(stk_df)
        for _, row in curr_stk.iterrows():
            base_sym = row[col_name].split('-')[0]
            # Use Custom Symbol if available for cleaner display
            disp_name = row.get('SEM_CUSTOM_SYMBOL', row[col_name])
            fno_map[base_sym] = {'id': str(row[col_id]), 'name': disp_name}
            
        # Process Spot Indices (Robust Name Match)
        nifty = idx_df[(idx_df[col_exch] == 'NSE') & (idx_df[col_name] == 'NIFTY 50')]
        if not nifty.empty: index_map['NIFTY'] = str(nifty.iloc[0][col_id])
        
        bank = idx_df[(idx_df[col_exch] == 'NSE') & (idx_df[col_name].isin(['NIFTY BANK', 'BANKNIFTY']))]
        if not bank.empty: index_map['BANKNIFTY'] = str(bank.iloc[0][col_id])
        
        # BSE SENSEX
        sensex = idx_df[(idx_df[col_exch] == 'BSE') & (idx_df[col_name].str.contains('SENSEX'))]
        if not sensex.empty: index_map['SENSEX'] = str(sensex.iloc[0][col_id])

    except Exception as e:
        st.error(f"Master List Error: {e}. Using offline defaults for Dashboard.")
        # We rely on the hardcoded defaults for Nifty/BankNifty so dashboard doesn't break
    
    return fno_map, index_map

with st.spinner("Syncing Master List..."):
    FNO_MAP, INDEX_MAP = get_master_data_map()

# --- 6. DATA FETCHING ---
def fetch_futures_data(security_id, interval=60):
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        res = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment="NSE_FNO", 
            instrument_type="FUTSTK",
            from_date=from_date,
            to_date=to_date,
            interval=interval
        )
        
        if res['status'] == 'success':
            data = res['data']
            if not data: return pd.DataFrame()
            df = pd.DataFrame(data)
            # Rename columns to standard names
            col_map = {'start_Time':'datetime', 'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume', 'oi':'OI'}
            df.rename(columns=col_map, inplace=True)
            return df
    except: pass
    return pd.DataFrame()

# --- 7. OI LOGIC ---
def get_oi_analysis(price_chg, oi_chg):
    if price_chg > 0 and oi_chg > 0: return "Long Buildup 🟢"
    if price_chg < 0 and oi_chg > 0: return "Short Buildup 🔴"
    if price_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"
    if price_chg > 0 and oi_chg < 0: return "Short Covering 🚀"
    return "Neutral ⚪"


# --- 8. DASHBOARD (SPOT PRICES) ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    # Configuration for Spot Indices
    indices_config = [
        {"name": "NIFTY 50", "key": "NIFTY", "seg": "IDX_I"},
        {"name": "BANK NIFTY", "key": "BANKNIFTY", "seg": "IDX_I"},
        {"name": "SENSEX", "key": "SENSEX", "seg": "BSE_IDX"}
    ]
    
    data_display = {}
    
    for item in indices_config:
        key = item['key']
        # Skip if ID not found (e.g. Sensex if CSV failed)
        if key not in INDEX_MAP: 
            data_display[item['name']] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}
            continue
        
        sec_id = INDEX_MAP[key]
        segment = item['seg']
        
        try:
            to_date = datetime.now().strftime('%Y-%m-%d')
            # Look back 4 days to ensure we find "Previous Close" even on Mondays/Holidays
            from_date = (datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d')
            
            res = dhan.intraday_minute_data(
                security_id=sec_id,
                exchange_segment=segment,
                instrument_type="INDEX",
                from_date=from_date, 
                to_date=to_date,
                interval=1 
            )
            
            if res['status'] == 'success' and res['data'].get('close'):
                # Latest Price (LTP)
                ltp = res['data']['close'][-1]
                
                # Calculate Change: 
                # We need the close of the *previous* trading day.
                # Since we fetched 4 days, we can roughly grab a candle from 1 day ago (approx 375 minutes back)
                # or just use the very first candle of the fetched series if the series is short.
                data_len = len(res['data']['close'])
                prev_idx = max(0, data_len - 375) # Approx 1 day ago in minutes
                prev_price = res['data']['close'][prev_idx]
                
                chg = ltp - prev_price
                pct = (chg / prev_price) * 100
                data_display[item['name']] = {"ltp": ltp, "chg": chg, "pct": pct}
            else:
                data_display[item['name']] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}
        except:
            data_display[item['name']] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1.2])
    
    with c1:
        n = data_display.get("NIFTY 50", {"ltp":0, "chg":0, "pct":0})
        st.metric("NIFTY 50", f"{n['ltp']:,.2f}", f"{n['chg']:.2f} ({n['pct']:.2f}%)")
    
    with c2:
        b = data_display.get("BANK NIFTY", {"ltp":0, "chg":0, "pct":0})
        st.metric("BANK NIFTY", f"{b['ltp']:,.2f}", f"{b['chg']:.2f} ({b['pct']:.2f}%)")

    with c3:
        s = data_display.get("SENSEX", {"ltp":0, "chg":0, "pct":0})
        st.metric("SENSEX", f"{s['ltp']:,.2f}", f"{s['chg']:.2f} ({s['pct']:.2f}%)")
        
    with c4:
        n_pct = data_display.get("NIFTY 50", {}).get("pct", 0)
        bias = "SIDEWAYS ↔️"
        color = "gray"
        if n_pct > 0.25: bias = "BULLISH 🚀"; color = "green"
        elif n_pct < -0.25: bias = "BEARISH 📉"; color = "red"
            
        st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 10px;">
                <h3 style="margin:0; color: {color};">Bias: {bias}</h3>
            </div>
        """, unsafe_allow_html=True)


# --- 9. SCANNER (TABBED) ---
@st.fragment(run_every=180)
def refreshable_scanner():
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["🚀 Market Scanners (Signals)", "📋 All F&O Symbols (Data)"])
    
    target_symbols = list(FNO_MAP.keys()) 
    
    # If Master List failed, target_symbols might be empty.
    if not target_symbols:
        st.error("Scanner Error: No F&O symbols found. Please check API connection or Master List status.")
        return

    progress_bar = st.progress(0, f"Scanning {len(target_symbols)} Futures...")
    
    bullish_list = []
    bearish_list = []
    all_data_list = []
    
    for i, sym in enumerate(target_symbols):
        try:
            futa_data = FNO_MAP[sym]
            sec_id = futa_data['id']
            
            df = fetch_futures_data(sec_id, interval=60)
            
            if not df.empty and len(df) > 20:
                # Indicators
                df['RSI'] = ta.rsi(df['Close'], length=14)
                adx_df = ta.adx(df['High'], df['Low'], df['Close'], length=14)
                df['EMA_5'] = ta.ema(df['Close'], length=5)
                
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                
                ltp = curr['Close']
                curr_rsi = curr['RSI']
                curr_adx = adx_df['ADX_14'].iloc[-1]
                mom_pct = round(((ltp - df['EMA_5'].iloc[-1]) / df['EMA_5'].iloc[-1]) * 100, 2)
                price_chg = round(((ltp - prev['Close']) / prev['Close']) * 100, 2)
                
                oi_chg_pct = 0.0
                if prev['OI'] > 0:
                    oi_chg_pct = round(((curr['OI'] - prev['OI']) / prev['OI']) * 100, 2)
                
                sentiment = get_oi_analysis(price_chg, oi_chg_pct)
                
                row = {
                    "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{sym}",
                    "LTP": round(ltp, 2),
                    "Mom %": mom_pct,
                    "Price Chg%": price_chg,
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "OI Chg%": oi_chg_pct,
                    "Analysis": sentiment
                }
                
                # Add to Master Data (Tab 2)
                row_master = row.copy()
                row_master['CleanSym'] = sym
                all_data_list.append(row_master)
                
                # Filters (Tab 1)
                if price_chg > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish_list.append(row)
                elif price_chg < -0.5 and curr_rsi < 45 and curr_adx > 20:
                    bearish_list.append(row)
                    
        except: pass
        progress_bar.progress((i + 1) / len(target_symbols))
        # time.sleep(0.05) # Uncomment if hitting rate limits
        
    progress_bar.empty()
    
    col_config = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
        "OI Chg%": st.column_config.NumberColumn("OI Chg%", format="%.2f%%"),
        "Price Chg%": st.column_config.NumberColumn("Price Chg%", format="%.2f%%")
    }
    
    # Tab 1
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.success(f"🟢 BULLS ({len(bullish_list)})")
            if bullish_list:
                st.dataframe(pd.DataFrame(bullish_list).sort_values(by="Mom %", ascending=False).head(15), use_container_width=True, hide_index=True, column_config=col_config)
            else: st.info("No bullish setups found.")
        with c2:
            st.error(f"🔴 BEARS ({len(bearish_list)})")
            if bearish_list:
                st.dataframe(pd.DataFrame(bearish_list).sort_values(by="Mom %", ascending=True).head(15), use_container_width=True, hide_index=True, column_config=col_config)
            else: st.info("No bearish setups found.")

    # Tab 2
    with tab2:
        st.info(f"📋 Showing Data for all {len(all_data_list)} F&O Symbols")
        if all_data_list:
            df_all = pd.DataFrame(all_data_list).sort_values(by="CleanSym").drop(columns=['CleanSym'])
            st.dataframe(df_all, use_container_width=True, hide_index=True, column_config=col_config, height=600)
        else:
            st.warning("No data available.")

    # Footer
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.write(f"🕒 **Last Data Sync:** {ist_time} IST")
    st.markdown("<div style='text-align: center; color: grey; padding-top: 20px;'>Powered by : i-Tech World</div>", unsafe_allow_html=True)

# --- 10. EXECUTION ---
if dhan:
    refreshable_dashboard() 
    refreshable_scanner()
