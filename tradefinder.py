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
st.title("🚀 Absa's Live F&O Screener Pro")

# --- 2. AUTHENTICATION (Main Page, No Sidebar) ---
# Try to load from secrets first to keep UI clean
ACCESS_TOKEN = st.secrets.get("UPSTOX_ACCESS_TOKEN", "")

# If no secret is found, show a simple input box at the top
if not ACCESS_TOKEN:
    with st.expander("🔐 Login (Enter Access Token)", expanded=True):
        ACCESS_TOKEN = st.text_input("Upstox Access Token", type="password")

if not ACCESS_TOKEN:
    st.warning("⚠️ Please enter your Access Token to start the scanner.")
    st.stop()

# Configure Upstox Client
configuration = upstox_client.Configuration()
configuration.access_token = ACCESS_TOKEN
api_instance = upstox_client.HistoryApi(upstox_client.ApiClient(configuration))

# --- 3. SMART MAPPING ENGINE ---
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

# --- 4. SYMBOL LIST (Hidden for brevity, same list as before) ---
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

# --- 5. DATA ENGINE ---
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
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
            return df
    except: pass
    return None

def get_sentiment(p_chg):
    if p_chg > 0: return "Bullish 🚀"
    if p_chg < 0: return "Bearish 📉"
    return "Neutral"

# --- 6. SCANNER ENGINE (3-Minute Auto Refresh) ---
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
                
                last = df.iloc[-1]
                ltp = last['close']
                curr_rsi = last['RSI']
                curr_adx = last['ADX']
                ema_5 = last['EMA_5']
                
                momentum_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                
                # Day Change Logic
                day_open = df[df['timestamp'].dt.date == datetime.now().date()]['open'].min()
                if pd.isna(day_open): day_open = df['open'].iloc[-10]
                p_change = round(((ltp - day_open) / day_open) * 100, 2)
                
                # TradingView India Link
                tv_url = f"https://in.tradingview.com/chart/?symbol=NSE:{clean_sym}"
                
                row = {
                    "Symbol": tv_url, # The Link URL
                    "LTP": ltp,
                    "Mom %": momentum_pct,
                    "Chg %": p_change,
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "Sentiment": get_sentiment(p_change)
                }
                
                # Filters
                if p_change > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish.append(row)
                elif p_change < -0.5 and curr_rsi < 40 and curr_adx > 20:
                    bearish.append(row)
            
            # Tiny sleep to respect rate limits
            time.sleep(0.02)
            progress_bar.progress((i + 1) / total)
            
        except: continue

    progress_bar.empty()
    
    # --- DISPLAY LOGIC ---
    # Config to make "Symbol" clickable and show only the name (e.g. "RELIANCE")
    column_config = {
        "Symbol": st.column_config.LinkColumn(
            "Script (Click to Chart)", 
            display_text="symbol=NSE:(.*)" # Regex to extract name from URL
        ),
        "LTP": st.column_config.NumberColumn("Price", format="₹%.2f")
    }
    
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 ACTIVE BULLS")
        if bullish:
            # Sort by Momentum -> Take Top 10 -> Show
            df_bull = pd.DataFrame(bullish).sort_values(by="Mom %", ascending=False).head(10)
            st.dataframe(df_bull, use_container_width=True, hide_index=True, column_config=column_config)
        else:
            st.info("No bullish signals.")
            
    with c2:
        st.error("🔴 ACTIVE BEARS")
        if bearish:
            # Sort by Momentum (Ascending for bears) -> Take Top 10 -> Show
            df_bear = pd.DataFrame(bearish).sort_values(by="Mom %", ascending=True).head(10)
            st.dataframe(df_bear, use_container_width=True, hide_index=True, column_config=column_config)
        else:
            st.info("No bearish signals.")
            
    ist_time = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')
    st.markdown(f"<div style='text-align: center; color: grey;'>Last Updated: {ist_time} (Next update in 3 mins)</div>", unsafe_allow_html=True)

# Run the engine
scanner_engine()
