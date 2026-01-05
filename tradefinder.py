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

# --- 2. DHAN API CREDENTIALS (FROM SECRETS) ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e:
    st.error(f"⚠️ API Error: Check .streamlit/secrets.toml. Details: {e}")
    st.stop()

# --- 3. SMART F&O MASTER LIST (Dynamic Filtering) ---
@st.cache_data(ttl=3600*4) # Cache for 4 hours
def get_fno_futures_map():
    """
    Downloads Dhan Master List and filters for CURRENT MONTH STOCK FUTURES only.
    This ensures we ONLY scan valid F&O scripts.
    """
    fno_map = {}
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        s = requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        # 1. Filter for NSE Stock Futures (FUTSTK)
        # We exclude Indices (FUTIDX) to focus on Stocks as per request
        df = df[
            (df['SEM_EXM_EXCH_ID'] == 'NSE') & 
            (df['SEM_INSTRUMENT_NAME'] == 'FUTSTK')
        ]
        
        # 2. Find Nearest Expiry
        # Convert expiry to datetime objects
        df['SEM_EXPIRY_DATE'] = pd.to_datetime(df['SEM_EXPIRY_DATE'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        today = pd.Timestamp.now().normalize()
        
        # Keep only future expiries
        df = df[df['SEM_EXPIRY_DATE'] >= today]
        
        # Sort by Symbol and Date, then pick the first one (Nearest Month)
        df = df.sort_values(by=['SEM_TRADING_SYMBOL', 'SEM_EXPIRY_DATE'])
        current_futures = df.drop_duplicates(subset=['SEM_TRADING_SYMBOL'], keep='first')
        
        # 3. Create Map: { 'RELIANCE': 'Security_ID' }
        # We strip the "-JAN-202X" part to get the base symbol key if needed, 
        # but Dhan's TRADING_SYMBOL for futures is usually like 'RELIANCE-JAN2026-FUT'
        # We need a clean key for display.
        for _, row in current_futures.iterrows():
            # Extract base symbol (e.g., 'RELIANCE' from 'RELIANCE-JAN-2025-FUT')
            # The format in CSV is usually 'SYMBOL-EXPIRY'. Let's use the Custom Symbol or parse Trading Symbol.
            # Robust way: Use the first part of the trading symbol string.
            base_sym = row['SEM_TRADING_SYMBOL'].split('-')[0]
            fno_map[base_sym] = {
                'id': str(row['SEM_SMST_SECURITY_ID']),
                'name': row['SEM_CUSTOM_SYMBOL']
            }
            
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return fno_map

# Load Map
with st.spinner("Syncing F&O List & Expiries..."):
    FNO_MAP = get_fno_futures_map()
    
# --- 4. OI ANALYSIS LOGIC ---
def get_oi_analysis(price_chg, oi_chg):
    """
    Classifies the move based on Price vs OI correlation.
    """
    if price_chg > 0 and oi_chg > 0:
        return "Long Buildup 🟢" # Strong Bullish
    elif price_chg < 0 and oi_chg > 0:
        return "Short Buildup 🔴" # Strong Bearish
    elif price_chg < 0 and oi_chg < 0:
        return "Long Unwinding ⚠️" # Weak Bullish -> Bearish
    elif price_chg > 0 and oi_chg < 0:
        return "Short Covering 🚀" # Explosive Bullish
    return "Neutral ⚪"

# --- 5. DATA FETCHING ---
def fetch_futures_data(security_id):
    """Fetches intraday data for the Futures Contract (Includes Volume & OI)"""
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        # Fetch 60-minute candles for the FUTURE contract to match your original timeframe
        res = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment="NSE_FNO",  # Critical: Use FNO segment for Futures
            instrument_type="FUTSTK",
            from_date=from_date,
            to_date=to_date,
            interval=60  # 1 Hour Interval
        )
        
        if res['status'] == 'success':
            data = res['data']
            if not data: return pd.DataFrame()
            
            df = pd.DataFrame(data)
            df.rename(columns={
                'start_Time': 'datetime', 
                'open': 'Open', 
                'high': 'High', 
                'low': 'Low', 
                'close': 'Close',
                'volume': 'Volume',
                'oi': 'OI' # Capture Open Interest
            }, inplace=True)
            return df
            
    except Exception: pass
    return pd.DataFrame()

# --- 6. MAIN SCANNER ---
@st.fragment(run_every=180)
def refreshable_data_tables():
    # 1. MARKET DASHBOARD
    # We can fetch Nifty Future for dashboard or keep Spot. Keeping Spot for simplicity.
    col1, col2, col3 = st.columns([1, 1, 2])
    with col1: st.metric("NIFTY 50", "Live", "Dashboard") # Placeholder or implement fetch
    
    st.markdown("---")
    st.subheader("📊 F&O Momentum Scanner (Technicals + OI)")
    
    bullish_list = []
    bearish_list = []
    
    # We loop through the keys of our dynamic F&O Map
    # Limit to top 50-100 liquid stocks if the full list is too slow, 
    # or scan all. For demo, we use the map directly.
    target_symbols = list(FNO_MAP.keys()) 
    
    progress_bar = st.progress(0, f"Scanning {len(target_symbols)} F&O Futures...")
    
    for i, sym in enumerate(target_symbols):
        try:
            futa_data = FNO_MAP[sym]
            sec_id = futa_data['id']
            contract_name = futa_data['name']
            
            # 1. Fetch Futures Data
            df = fetch_futures_data(sec_id)
            
            if not df.empty and len(df) > 20:
                # 2. Calculate Technicals
                df['RSI'] = ta.rsi(df['Close'], length=14)
                adx_df = ta.adx(df['High'], df['Low'], df['Close'], length=14)
                df['EMA_5'] = ta.ema(df['Close'], length=5)
                
                # Get Latest Candle
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                
                ltp = curr['Close']
                ema_5 = df['EMA_5'].iloc[-1]
                curr_rsi = curr['RSI']
                curr_adx = adx_df['ADX_14'].iloc[-1]
                
                # Momentum & Change
                momentum_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                
                # Price Change % (Close vs Prev Close of timeframe)
                price_chg = round(((ltp - prev['Close']) / prev['Close']) * 100, 2)
                
                # 3. CALCULATE OI CHANGE
                curr_oi = float(curr['OI'])
                prev_oi = float(prev['OI'])
                
                oi_chg_pct = 0.0
                if prev_oi > 0:
                    oi_chg_pct = round(((curr_oi - prev_oi) / prev_oi) * 100, 2)
                
                # 4. INITIAL FILTER: Technicals (Bulls vs Bears)
                # Note: ADX logic kept (Trend Strength)
                is_bull = False
                is_bear = False
                
                # Bullish Criteria
                if price_chg > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    is_bull = True
                # Bearish Criteria
                elif price_chg < -0.5 and curr_rsi < 45 and curr_adx > 20:
                    is_bear = True
                
                # 5. SECONDARY CHECK: OI Integration (Only if Technicals Pass)
                if is_bull or is_bear:
                    sentiment = get_oi_analysis(price_chg, oi_chg_pct)
                    
                    row = {
                        "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{sym}",
                        "Contract": contract_name,
                        "LTP": round(ltp, 2),
                        "Mom %": momentum_pct,
                        "RSI": round(curr_rsi, 1),
                        "ADX": round(curr_adx, 1),
                        "OI Chg%": oi_chg_pct,
                        "Sentiment": sentiment # This helps find "Strong" momentum
                    }
                    
                    if is_bull: bullish_list.append(row)
                    if is_bear: bearish_list.append(row)
            
        except Exception: pass
        
        # Progress & Rate Limit
        progress_bar.progress((i + 1) / len(target_symbols))
        time.sleep(0.05) # Tiny sleep to be polite
        
    progress_bar.empty()
    
    # --- DISPLAY LOGIC ---
    col_config = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
        "OI Chg%": st.column_config.NumberColumn("OI Chg%", format="%.2f%%")
    }
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.success(f"🟢 BULLS ({len(bullish_list)})")
        if bullish_list:
            df_bull = pd.DataFrame(bullish_list)
            # Prioritize "Short Covering" as it's the strongest intraday move
            # Sort by Mom% usually, but users can see Sentiment
            st.dataframe(
                df_bull.sort_values(by="Mom %", ascending=False).head(15),
                use_container_width=True, hide_index=True, column_config=col_config
            )
        else:
            st.info("No bullish setups found.")
            
    with c2:
        st.error(f"🔴 BEARS ({len(bearish_list)})")
        if bearish_list:
            df_bear = pd.DataFrame(bearish_list)
            # Prioritize "Long Unwinding" or "Short Buildup"
            st.dataframe(
                df_bear.sort_values(by="Mom %", ascending=True).head(15),
                use_container_width=True, hide_index=True, column_config=col_config
            )
        else:
            st.info("No bearish setups found.")

    st.markdown(f"<div style='text-align:right; color:grey;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

if dhan:
    refreshable_data_tables()
