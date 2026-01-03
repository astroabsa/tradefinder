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
st.title("🚀 Absa's Live F&O Screener (Upstox Powered)")

# --- 2. AUTHENTICATION (Sidebar) ---
st.sidebar.header("🔐 Upstox Login")
# Try to load from secrets, otherwise ask user
default_token = st.secrets.get("UPSTOX_ACCESS_TOKEN", "")
ACCESS_TOKEN = st.sidebar.text_input("Enter Today's Access Token", value=default_token, type="password")

if not ACCESS_TOKEN:
    st.warning("⚠️ Waiting for Access Token...")
    st.stop()

# Configure Upstox Client
configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN
api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))
market_quote_api = upstox_client.MarketQuoteApi(upstox_client.ApiClient(configuration))

# --- 3. SMART MAPPING ENGINE (The Magic Part) ---
@st.cache_data(ttl=3600*12) # Cache for 12 hours
def get_upstox_master_map():
    """
    Downloads the massive Upstox Instrument list and creates a 
    dictionary mapping 'RELIANCE' -> 'NSE_EQ|INE002A01018'
    """
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
    try:
        response = requests.get(url)
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as f:
            df = pd.read_json(f)
        
        # Filter for Equity and Indices to keep it fast
        df = df[df['segment'] == 'NSE_EQ']
        
        # Create Map: {'RELIANCE': 'NSE_EQ|INE...', 'TCS': 'NSE_EQ|INE...'}
        symbol_map = dict(zip(df['trading_symbol'], df['instrument_key']))
        return symbol_map
    except Exception as e:
        st.error(f"Failed to load Upstox Master List: {e}")
        return {}

# Load the map immediately
st.sidebar.info("Loading Instrument Keys...")
SYMBOL_MAP = get_upstox_master_map()
st.sidebar.success(f"Loaded {len(SYMBOL_MAP)} Instruments!")

# --- 4. GLOBAL SYMBOL LIST (Your Original List) ---
# We keep the .NS format but strip it during processing
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

# --- 5. HELPER FUNCTIONS ---
def get_sentiment(p_chg, oi_chg):
    if p_chg > 0 and oi_chg > 0: return "Long Buildup 🚀"
    if p_chg < 0 and oi_chg > 0: return "Short Buildup 📉"
    if p_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"
    if p_chg > 0 and oi_chg < 0: return "Short Covering 💨"
    return "Neutral"

def fetch_upstox_candles(instrument_key):
    try:
        # Dynamic Dates (Last 5 days for robust RSI calculation)
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        api_response = api_instance.get_historical_candle_data1(
            instrument_key=instrument_key,
            interval='30minute', # Standard Intraday
            to_date=to_date,
            from_date=from_date,
            api_version='2.0'
        )
        
        if api_response.status == 'success' and api_response.data.candles:
            cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
            df = pd.DataFrame(api_response.data.candles, columns=cols)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            
            # Sort: Oldest to Newest
            df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
            return df
    except:
        pass
    return None

# --- 6. FRAGMENT: MARKET DASHBOARD ---
@st.fragment(run_every=60)
def market_dashboard():
    # Hardcoded Upstox Keys for Indices
    indices = {
        "NIFTY 50": "NSE_INDEX|Nifty 50",
        "BANK NIFTY": "NSE_INDEX|Nifty Bank"
    }
    
    col1, col2, col3 = st.columns([1, 1, 2])
    data = {}

    for name, key in indices.items():
        try:
            # We use the Candle API to get today's move
            df = fetch_upstox_candles(key)
            if df is not None and not df.empty:
                ltp = df['close'].iloc[-1]
                # Calculate Change from Yesterday's Close (Approx via first candle of today)
                # Ideally, use Previous Close API, but this is faster for single call
                open_today = df['open'].iloc[0] 
                chg = ltp - open_today
                pct = (chg / open_today) * 100
                data[name] = {"ltp": ltp, "chg": chg, "pct": pct}
            else:
                data[name] = {"ltp": 0, "chg": 0, "pct": 0}
        except:
            data[name] = {"ltp": 0, "chg": 0, "pct": 0}

    # Render Metrics
    with col1:
        n = data["NIFTY 50"]
        st.metric("NIFTY 50", f"{n['ltp']:,.2f}", f"{n['pct']:.2f}%")
    with col2:
        b = data["BANK NIFTY"]
        st.metric("BANK NIFTY", f"{b['ltp']:,.2f}", f"{b['pct']:.2f}%")
    with col3:
        # Bias Logic
        bias, color = ("SIDEWAYS ↔️", "gray")
        if data["NIFTY 50"]['pct'] > 0.25: bias, color = ("BULLISH 🚀", "green")
        elif data["NIFTY 50"]['pct'] < -0.25: bias, color = ("BEARISH 📉", "red")
        
        st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 10px;">
                <h3 style="margin:0; color: {color};">Market Bias: {bias}</h3>
            </div>
        """, unsafe_allow_html=True)

market_dashboard()
st.markdown("---")

# --- 7. FRAGMENT: SCANNER ENGINE ---
@st.fragment(run_every=180)
def scanner_engine():
    bullish, bearish = [], []
    progress_bar = st.progress(0, text="Fetching Upstox Data...")
    
    for i, raw_sym in enumerate(FNO_SYMBOLS_RAW):
        try:
            # 1. CLEAN SYMBOL & GET KEY
            clean_sym = raw_sym.replace(".NS", "")
            instrument_key = SYMBOL_MAP.get(clean_sym)
            
            if not instrument_key:
                continue # Skip if mapping failed
            
            # 2. FETCH DATA
            df = fetch_upstox_candles(instrument_key)
            
            if df is not None and len(df) > 30:
                # 3. INDICATORS
                df['RSI'] = ta.rsi(df['close'], length=14)
                adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
                df['ADX'] = adx_df['ADX_14']
                df['EMA_5'] = ta.ema(df['close'], length=5)
                
                # 4. CURRENT VALUES
                last = df.iloc[-1]
                ltp = last['close']
                curr_rsi = last['RSI']
                curr_adx = last['ADX']
                ema_5 = last['EMA_5']
                
                momentum_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                
                # Calculate Day Change
                day_open = df[df['timestamp'].dt.date == datetime.now().date()]['open'].min()
                if pd.isna(day_open): day_open = df['open'].iloc[-10] # Fallback
                p_change = round(((ltp - day_open) / day_open) * 100, 2)
                
                sentiment = get_sentiment(p_change, 1) # Dummy OI (Upstox Free tier limits OI data sometimes)
                
                # Link to TradingView (Generic) or Upstox Pro
                tv_url = f"https://tv.upstox.com/charts/NSE_EQ|{instrument_key.split('|')[1]}"
                
                row = {
                    "Symbol": clean_sym, # Just text for now
                    "LTP": ltp,
                    "Mom %": momentum_pct,
                    "Chg %": p_change,
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "Sentiment": sentiment
                }
                
                # 5. FILTER LOGIC
                if p_change > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish.append(row)
                elif p_change < -0.5 and curr_rsi < 40 and curr_adx > 20:
                    bearish.append(row)
            
            # Rate Limit Protection
            time.sleep(0.05) 
            progress_bar.progress((i + 1) / len(FNO_SYMBOLS_RAW))
            
        except Exception as e:
            # print(f"Error {raw_sym}: {e}")
            continue

    progress_bar.empty()
    
    # DISPLAY
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 ACTIVE BULLS")
        if bullish:
            st.dataframe(pd.DataFrame(bullish).sort_values(by="Mom %", ascending=False), use_container_width=True, hide_index=True)
        else:
            st.info("No bullish signals.")
            
    with c2:
        st.error("🔴 ACTIVE BEARS")
        if bearish:
            st.dataframe(pd.DataFrame(bearish).sort_values(by="Mom %", ascending=True), use_container_width=True, hide_index=True)
        else:
            st.info("No bearish signals.")
            
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.caption(f"Last Scan: {ist_time}")

scanner_engine()
