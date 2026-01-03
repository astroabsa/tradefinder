import streamlit as st
import upstox_client
from upstox_client.rest import ApiException
import pandas as pd
import pandas_ta as ta
import requests
import gzip
import io
from datetime import datetime, timedelta
import time
import pytz

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="Absa's Upstox Pro Screener", layout="wide")

# --- 2. AUTHENTICATION SYSTEM (LOGIN) ---
# Replace this with your specific Google Sheet URL if changed
AUTH_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSEan21a9IVnkdmTFP2Q9O_ILI3waF52lFWQ5RTDtXDZ5MI4_yTQgFYcCXN5HxgkCxuESi5Dwe9iROB/pub?gid=0&single=true&output=csv"

def authenticate_user(user_in, pw_in):
    try:
        # Load the CSV from the Web
        df = pd.read_csv(AUTH_CSV_URL)
        
        # Normalize strings for comparison
        df['username'] = df['username'].astype(str).str.strip().str.lower()
        df['password'] = df['password'].astype(str).str.strip()
        user_in = str(user_in).strip().lower()
        pw_in = str(pw_in).strip()
        
        # Check for match
        match = df[(df['username'] == user_in) & (df['password'] == pw_in)]
        return not match.empty
    except Exception as e:
        st.error(f"Login System Error: {e}")
        return False

# Session State for Login
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

# --- LOGIN GATE ---
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
                st.error("Invalid Username or Password.")
    st.stop() # Stop here if not logged in

# --- 3. MAIN APP (AFTER LOGIN) ---
st.title("🚀 Absa's Live F&O Screener Pro")

# LOGOUT BUTTON
if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False
    st.rerun()

# --- 4. UPSTOX SETUP ---
# Try to load token from secrets, else ask user (No Sidebar)
ACCESS_TOKEN = st.secrets.get("UPSTOX_ACCESS_TOKEN", "")

if not ACCESS_TOKEN:
    with st.expander("🔐 Upstox Authorization (Required)", expanded=True):
        st.info("Secrets not found. Please enter your Daily Access Token manually.")
        ACCESS_TOKEN = st.text_input("Enter Upstox Access Token", type="password")

if not ACCESS_TOKEN:
    st.warning("⚠️ Waiting for Access Token to start scanning...")
    st.stop()

# Initialize API
configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN
api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

# --- 5. SMART MAPPING ENGINE ---
@st.cache_data(ttl=3600*12) 
def get_upstox_master_map():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        response = requests.get(url)
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            df = pd.read_json(f)
        df = df[df['segment'] == 'NSE_EQ']
        symbol_map = dict(zip(df['trading_symbol'], df['instrument_key']))
        return symbol_map
    except Exception as e:
        st.error(f"Map Error: {e}")
        return {}

SYMBOL_MAP = get_upstox_master_map()

# --- 6. SYMBOL LIST ---
FNO_SYMBOLS_RAW = [
    'ABFRL.NS', 'ADANIENSOL.NS', 'ADANIENT.NS', 'ADANIGREEN.NS', 'ADANIPORTS.NS', 'ALKEM.NS', 
    'AUROPHARMA.NS', 'AXISBANK.NS', 'BANDHANBNK.NS', 'BANKBARODA.NS', 'BANKINDIA.NS', 'BDL.NS', 
    'BEL.NS', 'BEML.NS', 'BHARTIARTL.NS', 'BHEL.NS', 'BIOCON.NS', 'BPCL.NS', 'BRITANNIA.NS', 
    'BSE.NS', 'CAMS.NS', 'CANBK.NS', 'CDSL.NS', 'CGPOWER.NS', 'CHAMBLFERT.NS', 'CHOLAFIN.NS', 
    'CIPLA.NS', 'COALINDIA.NS', 'COFORGE.NS', 'COLPAL.NS', 'CONCOR.NS', 'COROMANDEL.NS', 
    'CROMPTON.NS', 'CUMMINSIND.NS', 'CYIENT.NS', 'DABUR.NS', 'DALBHARAT.NS', 'DEEPAKNTR.NS', 
    'DELHIVERY.NS', 'DIVISLAB.NS', 'DIXON.NS', 'DMART.NS', 'DRREDDY.NS', 'FSL.NS', 'GAIL.NS', 
    'GLENMARK.NS', 'GMRINFRA.NS', 'GNFC.NS', 'GODREJCP.NS', 'GODREJPROP.NS', 'GRANULES.NS', 
    'GUJGASLTD.NS', 'HAL.NS', 'HAVELLS.NS', 'HCLTECH.NS', 'HDFCAMC.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS', 
    'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDCOPPER.NS', 'HINDPETRO.NS', 'HINDUNILVR.NS', 'ICICIBANK.NS', 
    'ICICIGI.NS', 'IDFC.NS', 'IDFCFIRSTB.NS', 'IEX.NS', 'IGL.NS', 'INDHOTEL.NS', 'INDIACEM.NS', 'INDIAMART.NS', 
    'INDIGO.NS', 'INDUSINDBK.NS', 'INDUSTOWER.NS', 'INFY.NS', 'IOC.NS', 'IPCALAB.NS', 'IRCTC.NS', 'IRFC.NS', 
    'ITC.NS', 'JINDALSTEL.NS', 'JSWSTEEL.NS', 'JUBLFOOD.NS', 'KOTAKBANK.NS', 'LALPATHLAB.NS', 
    'LAURUSLABS.NS', 'LICHSGFIN.NS', 'LICI.NS', 'LT.NS', 'LTIM.NS', 'LTTS.NS', 
    'LUPIN.NS', 'M&M.NS', 'M&MFIN.NS', 'MANAPPURAM.NS', 'MARICO.NS', 'MARUTI.NS', 
    'MCDOWELL-N.NS', 'MCX.NS', 'METROPOLIS.NS', 'MGL.NS', 'MOTHERSON.NS', 'MPHASIS.NS', 
    'MRF.NS', 'MUTHOOTFIN.NS', 'NATIONALUM.NS', 'NAUKRI.NS', 'NAVINFLUOR.NS', 'NBCC.NS', 
    'NESTLEIND.NS', 'NHPC.NS', 'NMDC.NS', 'NTPC.NS', 'NYKAA.NS', 'OBEROIRLTY.NS', 
    'OFSS.NS', 'OIL.NS', 'ONGC.NS', 'PAGEIND.NS', 'PATANJALI.NS', 'PEL.NS', 
    'PERSISTENT.NS', 'PETRONET.NS', 'PFC.NS', 'PHOENIXLTD.NS', 'PIDILITIND.NS', 'PIIND.NS', 
    'PNB.NS', 'POLYCAP.NS', 'POWERTARID.NS', 'PRESTIGE.NS', 'PVRINOX.NS', 'RAMCOCEM.NS', 
    'RBLBANK.NS', 'RECLTD.NS', 'RELIANCE.NS', 'SAIL.NS', 'SBICARD.NS', 'SBILIFE.NS', 
    'SBIN.NS', 'SHREECEM.NS', 'SHRIRAMFIN.NS', 'SIEMENS.NS', 'SONACOMS.NS', 'SRF.NS', 
    'SUNPHARMA.NS', 'SUNTV.NS', 'SUPREMEIND.NS', 'SYNGENE.NS', 'TATACHEMICAL.NS', 
    'TATACOMM.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TATAPOWER.NS', 'TATASTEEL.NS', 
    'TCS.NS', 'TECHM.NS', 'TITAN.NS', 'TORNTPHARM.NS', 'TRENT.NS', 'TVSMOTOR.NS', 
    'UNIONBANK.NS', 'UNITDSPIRITS.NS', 'UPL.NS', 'VBL.NS', 'VEDL.NS', 
    'VOLTAS.NS', 'WIPRO.NS', 'YESBANK.NS', 'ZOMATO.NS', 'ZYDUSLIFE.NS'
]

# --- 7. HELPER FUNCTIONS ---
def fetch_upstox_candles(instrument_key):
    try:
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        api_response = api_instance.get_historical_candle_data1(
            instrument_key=instrument_key,
            interval='30minute', 
            to_date=to_date,
            from_date=from_date,
            api_version='2.0'
        )
        
        if api_response.status == 'success' and api_response.data.candles:
            cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
            df = pd.DataFrame(api_response.data.candles, columns=cols)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            for col in ['open', 'high', 'low', 'close', 'volume', 'oi']:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
            return df
    except: pass
    return None

def get_sentiment(p_chg, oi_chg):
    # Core F&O Logic
    if p_chg > 0 and oi_chg > 0: return "Long Buildup 🚀"     # Strong Bullish
    if p_chg < 0 and oi_chg > 0: return "Short Buildup 📉"    # Strong Bearish
    if p_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"   # Weak Bearish
    if p_chg > 0 and oi_chg < 0: return "Short Covering 💨"   # Weak Bullish
    return "Neutral"

# --- 8. SCANNER ENGINE ---
@st.fragment(run_every=180)
def scanner_engine():
    bullish, bearish = [], []
    progress_bar = st.progress(0, text="Scanning Market (Updates every 3 mins)...")
    
    total = len(FNO_SYMBOLS_RAW)
    
    for i, raw_sym in enumerate(FNO_SYMBOLS_RAW):
        try:
            clean_sym = raw_sym.replace(".NS", "")
            instrument_key = SYMBOL_MAP.get(clean_sym)
            
            if not instrument_key: continue
            
            df = fetch_upstox_candles(instrument_key)
            
            if df is not None and len(df) > 30:
                # Indicators
                df['RSI'] = ta.rsi(df['close'], length=14)
                adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
                df['ADX'] = adx_df['ADX_14']
                df['EMA_5'] = ta.ema(df['close'], length=5)
                
                # Latest Data Points
                last = df.iloc[-1]
                prev = df.iloc[-2] # Previous candle for OI comparison
                
                ltp = last['close']
                curr_rsi = last['RSI']
                curr_adx = last['ADX']
                ema_5 = last['EMA_5']
                
                # --- OI CALCULATION (The new part) ---
                curr_oi = last['oi']
                prev_oi = prev['oi']
                
                # Avoid division by zero
                if prev_oi > 0:
                    oi_chg = ((curr_oi - prev_oi) / prev_oi) * 100
                else:
                    oi_chg = 0
                
                # Price Change (Day)
                day_open = df[df['timestamp'].dt.date == datetime.now().date()]['open'].min()
                if pd.isna(day_open): day_open = df['open'].iloc[-10]
                p_change = round(((ltp - day_open) / day_open) * 100, 2)
                
                # Momentum
                momentum_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                
                # Sentiment with REAL OI
                sentiment = get_sentiment(p_change, oi_chg)
                
                # TradingView Link
                tv_url = f"https://in.tradingview.com/chart/?symbol=NSE:{clean_sym}"
                
                row = {
                    "Symbol": tv_url,
                    "LTP": ltp,
                    "Mom %": momentum_pct,
                    "Chg %": p_change,
                    "OI Chg %": round(oi_chg, 2), # Display OI Change
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "Sentiment": sentiment
                }
                
                # --- FILTER LOGIC (Considering OI) ---
                # Bulls: Price Up + Strong RSI + (Ideally Positive OI for Buildup OR Negative for Covering)
                if p_change > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish.append(row)
                
                # Bears: Price Down + Weak RSI + (Ideally Positive OI for Buildup OR Negative for Unwinding)
                elif p_change < -0.5 and curr_rsi < 40 and curr_adx > 20:
                    bearish.append(row)
            
            time.sleep(0.02)
            progress_bar.progress((i + 1) / total)
            
        except: continue

    progress_bar.empty()
    
    # --- DISPLAY TABLES ---
    column_config = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
        "LTP": st.column_config.NumberColumn("Price", format="₹%.2f"),
        "OI Chg %": st.column_config.NumberColumn("OI Chg", format="%.2f%%")
    }
    
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 TOP 10 BULLS (Long Buildup / Covering)")
        if bullish:
            # Sort: Prioritize 'Long Buildup' (OI > 0) logic if needed, but Momentum is usually best for sorting
            df_bull = pd.DataFrame(bullish).sort_values(by="Mom %", ascending=False).head(10)
            st.dataframe(df_bull, use_container_width=True, hide_index=True, column_config=column_config)
        else:
            st.info("No bullish signals.")
            
    with c2:
        st.error("🔴 TOP 10 BEARS (Short Buildup / Unwinding)")
        if bearish:
            # Sort by Momentum (Ascending for bears)
            df_bear = pd.DataFrame(bearish).sort_values(by="Mom %", ascending=True).head(10)
            st.dataframe(df_bear, use_container_width=True, hide_index=True, column_config=column_config)
        else:
            st.info("No bearish signals.")
            
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.markdown(f"<div style='text-align: center; color: grey;'>Last Updated: {ist_time} (Auto-refresh: 3 mins)</div>", unsafe_allow_html=True)

scanner_engine()
