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
    st.stop()

# --- 3. MAIN APP SETUP ---
st.title("🚀 Absa's Live F&O Screener Pro")

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

# INIT CLIENTS
configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN
api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))
# NEW: Market Quote API for Real-Time Data
quote_api = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration)) 

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

# A. HISTORICAL DATA (For Indicators like RSI/ADX) - Has Lag
def fetch_upstox_history(instrument_key):
    try:
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        api_response = api_instance.get_historical_candle_data1(
            instrument_key=instrument_key,
            interval='30minute', to_date=to_date, from_date=from_date, api_version='2.0'
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

# B. LIVE QUOTE DATA (For Price & % Change) - No Lag
def fetch_live_quotes(instrument_keys_list):
    """Fetches real-time snapshot of LTP and OHLC for a list of keys"""
    try:
        # Join keys with comma (Upstox accepts up to 500 keys)
        keys_str = ",".join(instrument_keys_list)
        response = quote_api.get_full_market_quote(keys_str, '2.0')
        
        if response.status == 'success':
            return response.data # Returns a dictionary keyed by instrument_key
    except Exception as e:
        # print(f"Quote Error: {e}") 
        pass
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
    indices = {
        "NIFTY 50": "NSE_INDEX|Nifty 50",
        "BANK NIFTY": "NSE_INDEX|Nifty Bank",
        "SENSEX": "BSE_INDEX|SENSEX"
    }
    
    # FETCH REAL-TIME QUOTES FOR INDICES
    live_data = fetch_live_quotes(list(indices.values()))
    
    col1, col2, col3, col4 = st.columns([1, 1, 1, 1.5])
    
    # Process NIFTY for Bias
    nifty_key = indices["NIFTY 50"]
    nifty_pct = 0
    
    for name, key in indices.items():
        ltp = 0.0
        pct = 0.0
        
        if live_data and key in live_data:
            quote = live_data[key]
            ltp = quote.last_price
            # Calculate % Change from Previous Close
            prev_close = quote.ohlc.close
            if prev_close > 0:
                pct = ((ltp - prev_close) / prev_close) * 100
        
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

market_dashboard()
st.markdown("---")

# --- 7. SCANNER ENGINE (HYBRID: LIVE PRICE + HISTORICAL INDICATORS) ---
@st.fragment(run_every=180) 
def scanner_engine():
    bullish, bearish = [], []
    progress_bar = st.progress(0, text="Scanning Market...")
    
    # 1. PREPARE KEYS
    valid_symbols = []
    key_map = {} # Map key -> symbol name
    
    for raw_sym in FNO_SYMBOLS_RAW:
        clean_sym = raw_sym.replace(".NS", "")
        key = SYMBOL_MAP.get(clean_sym)
        if key:
            valid_symbols.append(key)
            key_map[key] = clean_sym
            
    # 2. BATCH FETCH LIVE PRICES (FAST)
    # We fetch all LTPs in one go to ensure displayed price is FRESH
    # Upstox allows multiple keys in one call
    chunk_size = 400 # Upstox limit is 500
    live_quotes_cache = {}
    
    for i in range(0, len(valid_symbols), chunk_size):
        chunk = valid_symbols[i:i+chunk_size]
        quotes = fetch_live_quotes(chunk)
        if quotes:
            live_quotes_cache.update(quotes)

    # 3. ANALYZE EACH SYMBOL
    total = len(valid_symbols)
    
    for i, key in enumerate(valid_symbols):
        try:
            clean_sym = key_map[key]
            
            # A. Get Live Data (LTP & Day Change)
            if key not in live_quotes_cache: continue
            
            quote = live_quotes_cache[key]
            ltp = quote.last_price
            prev_close = quote.ohlc.close
            
            # Calculate Live % Change
            p_change = 0.0
            if prev_close > 0:
                p_change = round(((ltp - prev_close) / prev_close) * 100, 2)
                
            # B. Get History (For RSI/ADX/OI)
            # We still need history for indicators, even if it's 1-min delayed
            df = fetch_upstox_history(key)
            
            if df is not None and len(df) > 30:
                # Indicators
                df['RSI'] = ta.rsi(df['close'], length=14)
                adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
                df['ADX'] = adx_df['ADX_14']
                df['EMA_5'] = ta.ema(df['close'], length=5)
                
                # Get Indicator Values (from closed candles)
                last_candle = df.iloc[-1]
                curr_rsi = last_candle['RSI']
                curr_adx = last_candle['ADX']
                ema_5 = last_candle['EMA_5'] # This might be slightly old
                
                # C. Live Momentum (Live Price vs EMA)
                # Using LIVE LTP vs EMA gives much better "Active" signals
                mom_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                
                # D. OI Logic (From History)
                curr_oi = last_candle['oi']
                prev_oi = df.iloc[-2]['oi']
                oi_chg = ((curr_oi - prev_oi) / prev_oi * 100) if prev_oi > 0 else 0
                
                # URL
                tv_url = f"https://in.tradingview.com/chart/?symbol=NSE:{clean_sym}"
                
                row = {
                    "Symbol": tv_url, 
                    "LTP": ltp,           # LIVE PRICE
                    "Mom %": mom_pct,     # LIVE MOMENTUM
                    "Chg %": p_change,    # LIVE CHANGE
                    "OI Chg %": round(oi_chg, 2),
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "Sentiment": get_sentiment(p_change, oi_chg)
                }
                
                if p_change > 0.5 and curr_rsi > 60 and curr_adx > 20: bullish.append(row)
                elif p_change < -0.5 and curr_rsi < 40 and curr_adx > 20: bearish.append(row)
            
            # Small sleep to prevent rate limiting on History API
            time.sleep(0.05)
            progress_bar.progress((i + 1) / total)
            
        except Exception: continue

    progress_bar.empty()
    
    col_conf = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
        "LTP": st.column_config.NumberColumn("Price", format="₹%.2f"),
        "OI Chg %": st.column_config.NumberColumn("OI Chg", format="%.2f%%")
    }
    
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 TOP 10 BULLS (Live)")
        if bullish: st.dataframe(pd.DataFrame(bullish).sort_values(by="Mom %", ascending=False).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bullish signals.")
    with c2:
        st.error("🔴 TOP 10 BEARS (Live)")
        if bearish: st.dataframe(pd.DataFrame(bearish).sort_values(by="Mom %", ascending=True).head(10), use_container_width=True, hide_index=True, column_config=col_conf)
        else: st.info("No bearish signals.")
    
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.markdown(f"""
        <div style='text-align:left; color:grey; margin-top:20px;'>
            Last Updated: {ist_time}<br>
            <strong>Powered by : i-Tech World</strong>
        </div>
    """, unsafe_allow_html=True)

scanner_engine()
