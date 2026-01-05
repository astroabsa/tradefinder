import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="Absa's Live F&O Screener Pro", layout="wide")

# --- 2. DHAN API CREDENTIALS (FROM SECRETS) ---
dhan = None
try:
    # Fetching credentials securely from secrets.toml
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    
    # Initialize DhanHQ
    dhan = dhanhq(client_id, access_token)
except Exception as e:
    st.error(f"⚠️ API Error: Could not load credentials from secrets.toml. Details: {e}")
    st.stop()

# --- 3. GLOBAL SYMBOL LIST (MAPPED FOR DHAN) ---
@st.cache_data(ttl=86400) # Cache for 1 day
def get_dhan_master_list():
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        df = pd.read_csv(url)
        # Filter for NSE Equity (EQ)
        df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')]
        return df[['SEM_SMST_SECURITY_ID', 'SEM_TRADING_SYMBOL']]
    except Exception as e:
        st.error(f"Error fetching Master List: {e}")
        return pd.DataFrame()

# Helper to find Security ID
def get_security_id(symbol, master_df):
    clean_sym = symbol.replace('.NS', '')
    row = master_df[master_df['SEM_TRADING_SYMBOL'] == clean_sym]
    if not row.empty:
        return row.iloc[0]['SEM_SMST_SECURITY_ID']
    return None

# User's Symbol List
FNO_SYMBOLS_RAW = [
    'ABFRL.NS', 'ADANIENT.NS', 'ADANIPORTS.NS', 'AXISBANK.NS', 'BANDHANBNK.NS', 
    'BANKBARODA.NS', 'BHARTIARTL.NS', 'BPCL.NS', 'BRITANNIA.NS', 'CIPLA.NS', 
    'COALINDIA.NS', 'DIVISLAB.NS', 'DRREDDY.NS', 'EICHERMOT.NS', 'GRASIM.NS', 
    'HCLTECH.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS', 'HEROMOTOCO.NS', 'HINDALCO.NS', 
    'HINDUNILVR.NS', 'ICICIBANK.NS', 'INDUSINDBK.NS', 'INFY.NS', 'ITC.NS', 
    'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LT.NS', 'M&M.NS', 'MARUTI.NS', 'NESTLEIND.NS', 
    'NTPC.NS', 'ONGC.NS', 'POWERGRID.NS', 'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS', 
    'SUNPHARMA.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS', 'TCS.NS', 
    'TECHM.NS', 'TITAN.NS', 'ULTRACEMCO.NS', 'UPL.NS', 'WIPRO.NS'
]

# --- 4. AUTHENTICATION (Web CSV Method) ---
def authenticate_user(user_in, pw_in):
    try:
        csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSEan21a9IVnkdmTFP2Q9O_ILI3waF52lFWQ5RTDtXDZ5MI4_yTQgFYcCXN5HxgkCxuESi5Dwe9iROB/pub?gid=0&single=true&output=csv"
        df = pd.read_csv(csv_url)
        df['username'] = df['username'].astype(str).str.strip().str.lower()
        df['password'] = df['password'].astype(str).str.strip()
        
        match = df[(df['username'] == str(user_in).strip().lower()) & 
                   (df['password'] == str(pw_in).strip())]
        return not match.empty
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return False

# --- 5. LOGIN GATE ---
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
                st.error("Invalid credentials or Connection Failed.")
    st.stop()

# --- 6. MAIN APPLICATION ---
st.title("🚀 Absa's Live F&O Screener Pro (DhanHQ)")
if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False
    st.rerun()

# Load Master List
master_list = get_dhan_master_list()
if master_list.empty:
    st.error("Could not load Scrip Master. Check internet connection.")
    st.stop()

def get_sentiment(p_chg, oi_chg):
    if p_chg > 0 and oi_chg > 0: return "Long Buildup 🚀"
    if p_chg < 0 and oi_chg > 0: return "Short Buildup 📉"
    if p_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"
    if p_chg > 0 and oi_chg < 0: return "Short Covering 💨"
    return "Neutral"

# --- HELPER: FETCH INTRADAY DATA FROM DHAN ---
def fetch_dhan_data(security_id):
    """Fetches intraday data for the last 5 days"""
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        # Dhan Interval Map: 1 (1min), 5, 15, 25, 60
        res = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment="NSE_EQ", 
            instrument_type="EQUITY",
            from_date=from_date,
            to_date=to_date,
            interval=60  # 1 Hour interval
        )
        
        if res['status'] == 'success':
            data = res['data']
            df = pd.DataFrame(data)
            df.rename(columns={
                'start_Time': 'datetime', 
                'open': 'Open', 
                'high': 'High', 
                'low': 'Low', 
                'close': 'Close',
                'volume': 'Volume'
            }, inplace=True)
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        return pd.DataFrame()

# --- HELPER: MARKET DASHBOARD ---
def fetch_market_dashboard():
    # Nifty 50 ID: 13, Bank Nifty ID: 25
    indices = {"NIFTY 50": "13", "BANK NIFTY": "25"} 
    
    col1, col2, col3 = st.columns([1, 1, 2])
    data_display = {}
    
    for name, sec_id in indices.items():
        try:
            to_date = datetime.now().strftime('%Y-%m-%d')
            # Using interval 1 to get latest minute candle for LTP
            res = dhan.intraday_minute_data(sec_id, 'IDX_I', 'INDEX', to_date, to_date, 1)
            
            if res['status'] == 'success' and len(res['data']['close']) > 0:
                ltp = res['data']['close'][-1]
                prev = res['data']['open'][0] 
                chg = ltp - prev
                pct = (chg / prev) * 100
                data_display[name] = {"ltp": ltp, "chg": chg, "pct": pct}
            else:
                data_display[name] = {"ltp": 0, "chg": 0, "pct": 0}
        except:
            data_display[name] = {"ltp": 0, "chg": 0, "pct": 0}

    # Render Metrics
    with col1:
        nifty = data_display.get("NIFTY 50", {"ltp":0, "chg":0, "pct":0})
        st.metric(label="NIFTY 50", value=f"{nifty['ltp']:,.2f}", delta=f"{nifty['chg']:.2f} ({nifty['pct']:.2f}%)")
    
    with col2:
        bank = data_display.get("BANK NIFTY", {"ltp":0, "chg":0, "pct":0})
        st.metric(label="BANK NIFTY", value=f"{bank['ltp']:,.2f}", delta=f"{bank['chg']:.2f} ({bank['pct']:.2f}%)")
        
    with col3:
        bias, color = "SIDEWAYS ↔️", "gray"
        if nifty['pct'] > 0.25: bias, color = "BULLISH 🚀", "green"
        elif nifty['pct'] < -0.25: bias, color = "BEARISH 📉", "red"
            
        st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 10px;">
                <h3 style="margin:0; color: {color};">Market Bias: {bias}</h3>
            </div>
        """, unsafe_allow_html=True)

@st.fragment(run_every=180)
def refreshable_data_tables():
    fetch_market_dashboard()
    st.markdown("---")
    
    bullish, bearish = [], []
    progress_bar = st.progress(0, text="Fetching Live Data from Dhan...")
    
    for i, sym in enumerate(FNO_SYMBOLS_RAW):
        try:
            sec_id = get_security_id(sym, master_list)
            if not sec_id: continue

            df = fetch_dhan_data(sec_id)
            
            if not df.empty and len(df) > 20:
                # Indicators
                df['RSI'] = ta.rsi(df['Close'], length=14)
                adx_df = ta.adx(df['High'], df['Low'], df['Close'], length=14)
                df['EMA_5'] = ta.ema(df['Close'], length=5)
                
                ltp = df['Close'].iloc[-1]
                ema_5 = df['EMA_5'].iloc[-1]
                curr_rsi = df['RSI'].iloc[-1]
                curr_adx = adx_df['ADX_14'].iloc[-1]
                
                momentum_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                prev_close = df['Close'].iloc[-2] 
                p_change = round(((ltp - prev_close) / prev_close) * 100, 2)
                
                clean_sym = sym.replace(".NS", "")
                tv_url = f"https://in.tradingview.com/chart/?symbol=NSE:{clean_sym}"
                
                oi_chg = 1 
                sentiment = get_sentiment(p_change, oi_chg)
                
                row = {
                    "Symbol": tv_url,
                    "LTP": round(ltp, 2),
                    "Mom %": momentum_pct,
                    "Chg %": p_change,
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "Sentiment": sentiment
                }

                if p_change > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish.append(row)
                elif p_change < -0.5 and curr_rsi < 45 and curr_adx > 20:
                    bearish.append(row)
            
            progress_bar.progress((i + 1) / len(FNO_SYMBOLS_RAW))
            time.sleep(0.1) 
            
        except Exception:
            continue
            
    progress_bar.empty()
    
    column_config = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)")
    }
    
    col1, col2 = st.columns(2)
    with col1:
        st.success("🟢 ACTIVE BULLS")
        if bullish:
            st.dataframe(
                pd.DataFrame(bullish).sort_values(by="Mom %", ascending=False).head(10), 
                use_container_width=True, hide_index=True, column_config=column_config
            )
        else:
            st.info("No bullish breakouts detected.")

    with col2:
        st.error("🔴 ACTIVE BEARS")
        if bearish:
            st.dataframe(
                pd.DataFrame(bearish).sort_values(by="Mom %", ascending=True).head(10), 
                use_container_width=True, hide_index=True, column_config=column_config
            )
        else:
            st.info("No bearish breakdowns detected.")

    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.write(f"🕒 **Last Data Sync:** {ist_time} IST (Auto-refreshing in 3 mins)")
    st.markdown("<div style='text-align: center; color: grey; padding-top: 20px;'>Powered by : i-Tech World</div>", unsafe_allow_html=True)

refreshable_data_tables()
