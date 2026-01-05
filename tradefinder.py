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

# --- 5. SMART F&O MASTER LIST ---
@st.cache_data(ttl=3600*4)
def get_fno_futures_map():
    fno_map = {}
    index_map = {}
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        s = requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        # 1. Stocks Futures
        stk_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'FUTSTK')]
        # 2. Index Futures (For Dashboard)
        idx_df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'FUTIDX')]
        
        # Helper to find nearest expiry
        def get_current_futures(dataframe):
            dataframe['SEM_EXPIRY_DATE'] = pd.to_datetime(dataframe['SEM_EXPIRY_DATE'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
            today = pd.Timestamp.now().normalize()
            valid = dataframe[dataframe['SEM_EXPIRY_DATE'] >= today]
            valid = valid.sort_values(by=['SEM_TRADING_SYMBOL', 'SEM_EXPIRY_DATE'])
            return valid.drop_duplicates(subset=['SEM_TRADING_SYMBOL'], keep='first')

        # Process Stocks
        curr_stk = get_current_futures(stk_df)
        for _, row in curr_stk.iterrows():
            base_sym = row['SEM_TRADING_SYMBOL'].split('-')[0]
            fno_map[base_sym] = {'id': str(row['SEM_SMST_SECURITY_ID']), 'name': row['SEM_CUSTOM_SYMBOL']}
            
        # Process Indices
        curr_idx = get_current_futures(idx_df)
        for _, row in curr_idx.iterrows():
            base_sym = row['SEM_TRADING_SYMBOL'].split('-')[0]
            index_map[base_sym] = str(row['SEM_SMST_SECURITY_ID'])
            
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return fno_map, index_map

with st.spinner("Syncing F&O List..."):
    FNO_MAP, INDEX_MAP = get_fno_futures_map()

# --- 6. DATA FETCHING ---
def fetch_futures_data(security_id, interval=60):
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        res = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment="NSE_FNO", 
            instrument_type="FUTSTK", # Default for Stocks
            from_date=from_date,
            to_date=to_date,
            interval=interval
        )
        
        if res['status'] == 'success':
            data = res['data']
            if not data: return pd.DataFrame()
            df = pd.DataFrame(data)
            df.rename(columns={'start_Time':'datetime', 'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume', 'oi':'OI'}, inplace=True)
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


# --- 8. COMPONENT: DASHBOARD (Refreshes every 5 sec) ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    indices = {
        "NIFTY 50": INDEX_MAP.get("NIFTY", ""), 
        "BANK NIFTY": INDEX_MAP.get("BANKNIFTY", "")
    }
    
    data_display = {}
    
    # Fetch Data for Indices
    for name, sec_id in indices.items():
        if not sec_id: continue
        try:
            to_date = datetime.now().strftime('%Y-%m-%d')
            res = dhan.intraday_minute_data(
                security_id=sec_id,
                exchange_segment="NSE_FNO",
                instrument_type="FUTIDX",
                from_date=to_date, 
                to_date=to_date,
                interval=1 
            )
            
            if res['status'] == 'success' and res['data'].get('close'):
                ltp = res['data']['close'][-1]
                open_price = res['data']['open'][0]
                chg = ltp - open_price
                pct = (chg / open_price) * 100
                data_display[name] = {"ltp": ltp, "chg": chg, "pct": pct}
            else:
                data_display[name] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}
        except:
            data_display[name] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}

    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        n = data_display.get("NIFTY 50", {"ltp":0, "chg":0, "pct":0})
        st.metric(label="NIFTY 50 (Fut)", value=f"{n['ltp']:,.2f}", delta=f"{n['chg']:.2f} ({n['pct']:.2f}%)")
    
    with col2:
        b = data_display.get("BANK NIFTY", {"ltp":0, "chg":0, "pct":0})
        st.metric(label="BANK NIFTY (Fut)", value=f"{b['ltp']:,.2f}", delta=f"{b['chg']:.2f} ({b['pct']:.2f}%)")
        
    with col3:
        n_pct = data_display.get("NIFTY 50", {}).get("pct", 0)
        bias = "SIDEWAYS ↔️"
        color = "gray"
        if n_pct > 0.25: bias = "BULLISH 🚀"; color = "green"
        elif n_pct < -0.25: bias = "BEARISH 📉"; color = "red"
            
        st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 10px;">
                <h3 style="margin:0; color: {color};">Market Bias: {bias}</h3>
            </div>
        """, unsafe_allow_html=True)


# --- 9. COMPONENT: SCANNER TABLE (Refreshes every 180 sec) ---
@st.fragment(run_every=180)
def refreshable_scanner():
    st.markdown("---")
    bullish_list = []
    bearish_list = []
    
    target_symbols = list(FNO_MAP.keys()) 
    progress_bar = st.progress(0, f"Scanning {len(target_symbols)} Futures...")
    
    for i, sym in enumerate(target_symbols):
        try:
            futa_data = FNO_MAP[sym]
            sec_id = futa_data['id']
            
            # Fetch Data (1H Candles)
            df = fetch_futures_data(sec_id, interval=60)
            
            if not df.empty and len(df) > 20:
                # Technicals
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
                
                # OI Logic
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
                
                if price_chg > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish_list.append(row)
                elif price_chg < -0.5 and curr_rsi < 45 and curr_adx > 20:
                    bearish_list.append(row)
                    
        except: pass
        progress_bar.progress((i + 1) / len(target_symbols))
        time.sleep(0.05)
        
    progress_bar.empty()
    
    col_config = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
        "OI Chg%": st.column_config.NumberColumn("OI Chg%", format="%.2f%%"),
        "Price Chg%": st.column_config.NumberColumn("Price Chg%", format="%.2f%%")
    }
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.success(f"🟢 ACTIVE BULLS ({len(bullish_list)})")
        if bullish_list:
            st.dataframe(pd.DataFrame(bullish_list).sort_values(by="Mom %", ascending=False).head(15), use_container_width=True, hide_index=True, column_config=col_config)
        else: st.info("No bullish setups found.")
            
    with c2:
        st.error(f"🔴 ACTIVE BEARS ({len(bearish_list)})")
        if bearish_list:
            st.dataframe(pd.DataFrame(bearish_list).sort_values(by="Mom %", ascending=True).head(15), use_container_width=True, hide_index=True, column_config=col_config)
        else: st.info("No bearish setups found.")

    # Footer
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.write(f"🕒 **Last Data Sync:** {ist_time} IST")
    st.markdown("""
        <div style='text-align: center; color: grey; padding-top: 20px;'>
            Powered by : i-Tech World
        </div>
    """, unsafe_allow_html=True)

# --- 10. MAIN APP EXECUTION ---
if dhan:
    # These two functions now run INDEPENDENTLY at their own speeds
    refreshable_dashboard() 
    refreshable_scanner()
