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
st.set_page_config(page_title="iTW Live F&O Screener Pro", layout="wide")

# --- 2. AUTHENTICATION SYSTEM (LOGIN) ---
AUTH_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSEan21a9IVnkdmTFP2Q9O_ILI3waF52lFWQ5RTDtXDZ5MI4_yTQgFYcCXN5HxgkCxuESi5Dwe9iROB/pub?gid=0&single=true&output=csv"

def authenticate_user(user_in, pw_in):
    try:
        df = pd.read_csv(AUTH_CSV_URL)
        df['username'] = df['username'].astype(str).str.strip().str.lower()
        df['password'] = df['password'].astype(str).str.strip()
        user_in = str(user_in).strip().lower()
        pw_in = str(pw_in).strip()
        match = df[(df['username'] == user_in) & (df['password'] == pw_in)]
        return not match.empty
    except Exception as e:
        st.error(f"Login System Error: {e}")
        return False

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

# --- LOGIN GATE ---
if not st.session_state["authenticated"]:
    st.title("🔐 iTW Live F&O Screener Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Invalid Username or Password.")
    st.stop()

# --- 3. MAIN APP SETUP ---
st.title("🚀 iTW Live F&O Screener Pro")

if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False
    st.rerun()

# UPSTOX AUTH
ACCESS_TOKEN = st.secrets.get("UPSTOX_ACCESS_TOKEN", "")
if not ACCESS_TOKEN:
    with st.expander("🔐 Upstox Authorization", expanded=True):
        ACCESS_TOKEN = st.text_input("Enter Upstox Access Token", type="password")

if not ACCESS_TOKEN:
    st.warning("⚠️ Waiting for Access Token...")
    st.stop()

configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN
api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

# --- 4. SMART MAPPER & SYMBOLS ---
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
    except: return {}

SYMBOL_MAP = get_upstox_master_map()

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

# --- 5. CORE FUNCTIONS ---
# B. LIVE QUOTE DATA (Debug Version)
# B. LIVE QUOTE DATA (Debug Version)
def fetch_live_quotes(instrument_keys_list):
    """Fetches real-time snapshot with Error Reporting"""
    if not instrument_keys_list:
        return {}
    
    try:
        # Join keys with comma
        keys_str = ",".join(instrument_keys_list)
        
        # Call API
        response = quote_api.get_full_market_quote(keys_str, '2.0')
        
        # DEBUG: Check if response is valid
        if hasattr(response, 'status') and response.status == 'success':
            return response.data
        else:
            # If status is not success, show why
            st.error(f"❌ API returned failure: {response}")
            return {}

    except ApiException as e:
        # SHOW THE EXACT ERROR ON DASHBOARD
        st.error(f"❌ Upstox API Error: {e.body}")
        return {}
    except Exception as e:
        st.error(f"❌ Python Error: {e}")
        return {}

def get_sentiment(p_chg, oi_chg):
    if p_chg > 0 and oi_chg > 0: return "Long Buildup 🚀"
    if p_chg < 0 and oi_chg > 0: return "Short Buildup 📉"
    if p_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"
    if p_chg > 0 and oi_chg < 0: return "Short Covering 💨"
    return "Neutral"

# --- 6. DASHBOARD COMPONENT (REAL-TIME) ---
@st.fragment(run_every=5) 
def market_dashboard():
    # Use these keys - they are safer (verify them in Upstox dev portal if needed)
    indices = {
        "NIFTY 50": "NSE_INDEX|Nifty 50",  # Ensure exact casing
        "BANK NIFTY": "NSE_INDEX|Nifty Bank",
        "SENSEX": "BSE_INDEX|SENSEX"
    }
    
    # FETCH REAL-TIME QUOTES
    live_data = fetch_live_quotes(list(indices.values()))
    
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1.5])
    
    nifty_pct = 0
    
    for name, key in indices.items():
        ltp = 0.0
        pct = 0.0
        
        # Check if data exists for this specific key
        if live_data and key in live_data:
            quote = live_data[key]
            ltp = quote.last_price
            prev_close = quote.ohlc.close
            if prev_close > 0:
                pct = ((ltp - prev_close) / prev_close) * 100
        else:
            # If 0.00, it means the key was rejected or data is missing
            pass 
        
        if name == "NIFTY 50": nifty_pct = pct

        # Display Metrics
        label = f"{name}"
        if name == "NIFTY 50":
            with col1: st.metric(label, f"{ltp:,.2f}", f"{pct:.2f}%")
        elif name == "BANK NIFTY":
            with col2: st.metric(label, f"{ltp:,.2f}", f"{pct:.2f}%")
        elif name == "SENSEX":
            with col3: st.metric(label, f"{ltp:,.2f}", f"{pct:.2f}%")

    with col4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        if nifty_pct > 0.25: bias, color = ("BULLISH 🚀", "green")
        elif nifty_pct < -0.25: bias, color = ("BEARISH 📉", "red")
        
        st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 10px;">
                <h3 style="margin:0; color: {color};">Bias: {bias}</h3>
            </div>
        """, unsafe_allow_html=True)
# --- 7. SCANNER ENGINE (AUTO-REFRESH: 3 MINUTES) ---
@st.fragment(run_every=180) # <--- KEPT AT 3 MINUTES
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
                df['RSI'] = ta.rsi(df['close'], length=14)
                adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
                df['ADX'] = adx_df['ADX_14']
                df['EMA_5'] = ta.ema(df['close'], length=5)
                
                last, prev = df.iloc[-1], df.iloc[-2]
                ltp = last['close']
                curr_rsi, curr_adx, ema_5 = last['RSI'], last['ADX'], last['EMA_5']
                
                curr_oi, prev_oi = last['oi'], prev['oi']
                oi_chg = ((curr_oi - prev_oi) / prev_oi * 100) if prev_oi > 0 else 0
                
                day_open = df[df['timestamp'].dt.date == datetime.now().date()]['open'].min()
                if pd.isna(day_open): day_open = df['open'].iloc[-10]
                p_change = round(((ltp - day_open) / day_open) * 100, 2)
                mom_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                
                tv_url = f"https://in.tradingview.com/chart/?symbol=NSE:{clean_sym}"
                
                row = {
                    "Symbol": tv_url, "LTP": ltp, "Mom %": mom_pct,
                    "Chg %": p_change, "OI Chg %": round(oi_chg, 2),
                    "RSI": round(curr_rsi, 1), "ADX": round(curr_adx, 1),
                    "Sentiment": get_sentiment(p_change, oi_chg)
                }
                
                if p_change > 0.5 and curr_rsi > 60 and curr_adx > 20: bullish.append(row)
                elif p_change < -0.5 and curr_rsi < 40 and curr_adx > 20: bearish.append(row)
            
            time.sleep(0.02)
            progress_bar.progress((i + 1) / total)
        except: continue

    progress_bar.empty()
    
    col_conf = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
        "LTP": st.column_config.NumberColumn("Price", format="₹%.2f"),
        "OI Chg %": st.column_config.NumberColumn("OI Chg", format="%.2f%%")
    }
    
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 TOP 10 BULLS")
        if bullish: st.dataframe(pd.DataFrame(bullish).sort_values(by="Mom %", ascending=False).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bullish signals.")
    with c2:
        st.error("🔴 TOP 10 BEARS")
        if bearish: st.dataframe(pd.DataFrame(bearish).sort_values(by="Mom %", ascending=True).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bearish signals.")
    
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.markdown(f"""
        <div style='text-align:left; color:grey; margin-top:20px;'>
            Last Updated: {ist_time}<br>
        </div>
        <div style='text-align:center; color:grey; margin-top:20px;'>
            <strong>Powered by : i-Tech World</strong>
        </div>
    """, unsafe_allow_html=True)

scanner_engine()
