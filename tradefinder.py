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
st.set_page_config(page_title="Absa's F&O OI Scanner", layout="wide")

# --- 2. CREDENTIALS ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]
    dhan = dhanhq(client_id, access_token)
except Exception as e:
    st.error(f"⚠️ API Error: Check .streamlit/secrets.toml. Details: {e}")
    st.stop()

# --- 3. SMART MASTER LIST (F&O FUTURES ONLY) ---
@st.cache_data(ttl=3600*4) # Cache for 4 hours
def get_fno_master_map():
    """
    Downloads Dhan Master List and filters for CURRENT MONTH FUTURES only.
    This automatically ensures we only scan F&O stocks.
    """
    symbol_map = {}
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        s = requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        
        # 1. Filter for NSE Futures (FUTSTK)
        # We ignore Indices (FUTIDX) here to focus on Stocks, or include if needed
        fno_df = df[
            (df['SEM_EXM_EXCH_ID'] == 'NSE') & 
            (df['SEM_INSTRUMENT_NAME'] == 'FUTSTK')
        ].copy() # Copy to avoid SettingWithCopyWarning
        
        # 2. Find Current Expiry (Nearest Future Date)
        # Convert expiry to datetime
        fno_df['SEM_EXPIRY_DATE'] = pd.to_datetime(fno_df['SEM_EXPIRY_DATE'], errors='coerce')
        today = pd.Timestamp.now().normalize()
        
        # Filter only future expiries
        valid_futures = fno_df[fno_df['SEM_EXPIRY_DATE'] >= today]
        
        # Sort by Symbol and Date, then pick the first one (Nearest Expiry)
        valid_futures = valid_futures.sort_values(by=['SEM_TRADING_SYMBOL', 'SEM_EXPIRY_DATE'])
        current_futures = valid_futures.drop_duplicates(subset=['SEM_TRADING_SYMBOL'], keep='first')
        
        # 3. Create Map: { 'RELIANCE': {'id': '12345', 'name': 'RELIANCE-JAN-2026-FUT'} }
        for _, row in current_futures.iterrows():
            # Clean symbol name just in case
            base_sym = row['SEM_TRADING_SYMBOL'].split('-')[0] 
            symbol_map[base_sym] = {
                'id': str(row['SEM_SMST_SECURITY_ID']),
                'name': row['SEM_CUSTOM_SYMBOL']
            }
            
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return symbol_map

# Load Map
with st.spinner("Syncing F&O Futures List..."):
    FNO_MAP = get_fno_master_map()

# --- 4. OI ANALYSIS LOGIC ---
def get_oi_interpretation(price_chg_pct, oi_chg_pct):
    if price_chg_pct > 0 and oi_chg_pct > 0:
        return "Long Buildup 🟢" # Bullish
    elif price_chg_pct < 0 and oi_chg_pct > 0:
        return "Short Buildup 🔴" # Bearish
    elif price_chg_pct < 0 and oi_chg_pct < 0:
        return "Long Unwinding ⚠️" # Weakness
    elif price_chg_pct > 0 and oi_chg_pct < 0:
        return "Short Covering 🚀" # Explosive Up
    return "Neutral ⚪"

# --- 5. DATA FETCHING ---
def fetch_futures_data(security_id):
    """Fetches intraday data for the Futures Contract (Includes OI)"""
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        # Fetch 15-minute candles for the FUTURE contract
        # Dhan returns OI in the 'oi' field for Futures
        res = dhan.intraday_minute_data(
            security_id=str(security_id),
            exchange_segment="NSE_FNO",  # Critical: Use FNO segment
            instrument_type="FUTSTK",
            from_date=from_date,
            to_date=to_date,
            interval=15  # 15 Minute Interval
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
def scanner():
    st.subheader("📊 Live F&O Sentiment Scanner (Price + OI)")
    
    # List of Symbols to Scan (You can expand this list)
    # The scanner will AUTO-FILTER this list to only valid F&O stocks found in FNO_MAP
    RAW_SYMBOLS = [
        'RELIANCE', 'HDFCBANK', 'INFY', 'ICICIBANK', 'TCS', 'SBIN', 'KOTAKBANK', 'LICI', 'AXISBANK', 
        'ITC', 'BHARTIARTL', 'LT', 'BAJFINANCE', 'MARUTI', 'HCLTECH', 'TITAN', 'SUNPHARMA', 
        'ULTRACEMCO', 'TATAMOTORS', 'NTPC', 'POWERGRID', 'M&M', 'TATASTEEL', 'JSWSTEEL', 
        'ADANIENT', 'HINDUNILVR', 'COALINDIA', 'GRASIM', 'ONGC', 'ADANIPORTS', 'BAJAJFINSV', 
        'WIPRO', 'TECHM', 'HINDALCO', 'CIPLA', 'APOLLOHOSP', 'DRREDDY', 'EICHERMOT', 'DIVISLAB', 
        'BRITANNIA', 'HEROMOTOCO', 'SBILIFE', 'TATACONSUM', 'BPCL', 'ASIANPAINT', 'NESTLEIND', 'INDUSINDBK'
    ]
    
    results = []
    progress_bar = st.progress(0, "Analyzing Futures Data...")
    
    for i, sym in enumerate(RAW_SYMBOLS):
        try:
            # 1. F&O FILTER: Check if symbol exists in our Futures Map
            if sym not in FNO_MAP:
                # This automatically skips Non-F&O stocks
                continue
                
            futa_data = FNO_MAP[sym]
            sec_id = futa_data['id']
            contract_name = futa_data['name']
            
            # 2. Fetch Futures Data (Contains OI)
            df = fetch_futures_data(sec_id)
            
            if not df.empty and len(df) > 20:
                # 3. Technicals
                df['RSI'] = ta.rsi(df['Close'], length=14)
                df['EMA'] = ta.ema(df['Close'], length=5)
                
                curr = df.iloc[-1]
                prev = df.iloc[-2]
                
                ltp = curr['Close']
                curr_oi = curr['OI']
                prev_oi = prev['OI']
                
                # 4. Calculations
                # Price Change % (Candle to Candle)
                price_chg_pct = round(((curr['Close'] - prev['Close']) / prev['Close']) * 100, 2)
                
                # OI Change %
                oi_chg_pct = 0.0
                if prev_oi > 0:
                    oi_chg_pct = round(((curr_oi - prev_oi) / prev_oi) * 100, 2)
                
                # Momentum (Distance from EMA)
                ema_val = curr['EMA']
                mom_pct = round(((ltp - ema_val) / ema_val) * 100, 2)
                
                # 5. Interpretation
                nature = get_oi_interpretation(price_chg_pct, oi_chg_pct)
                
                results.append({
                    "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{sym}",
                    "Contract": contract_name,
                    "LTP": ltp,
                    "Price Chg%": price_chg_pct,
                    "OI Chg%": oi_chg_pct,
                    "RSI": round(curr['RSI'], 1),
                    "Analysis": nature,
                    "Mom %": mom_pct # Hidden sort key
                })
                
        except Exception: pass
        
        # Polite delay
        time.sleep(0.1)
        progress_bar.progress((i + 1) / len(RAW_SYMBOLS))
        
    progress_bar.empty()
    
    if results:
        df_res = pd.DataFrame(results)
        
        # Configuration for nicer table
        col_config = {
            "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
            "LTP": st.column_config.NumberColumn("Price", format="₹%.2f"),
            "OI Chg%": st.column_config.NumberColumn("OI Chg%", format="%.2f%%"),
            "Price Chg%": st.column_config.NumberColumn("Price Chg%", format="%.2f%%"),
        }
        
        # Split into Bullish / Bearish based on Analysis
        st.write("### 🚀 Movers & Shakers (Futures)")
        
        # Show specific "Short Covering" (High conviction upside)
        short_cov = df_res[df_res['Analysis'].str.contains("Short Covering")]
        if not short_cov.empty:
            st.success(f"🔥 **Short Covering Detected** ({len(short_cov)} stocks)")
            st.dataframe(short_cov, use_container_width=True, hide_index=True, column_config=col_config)

        # Show specific "Long Buildup"
        long_build = df_res[df_res['Analysis'].str.contains("Long Buildup")]
        if not long_build.empty:
            st.info(f"🟢 **Long Buildup** ({len(long_build)} stocks)")
            st.dataframe(long_build, use_container_width=True, hide_index=True, column_config=col_config)

        # Show Bearish
        bearish_df = df_res[df_res['Analysis'].str.contains("Short Buildup|Long Unwinding")]
        if not bearish_df.empty:
            st.error(f"🔴 **Bearish Pressure** ({len(bearish_df)} stocks)")
            st.dataframe(bearish_df, use_container_width=True, hide_index=True, column_config=col_config)
            
    else:
        st.warning("No data returned. Market might be closed or API connection failed.")
        
    st.markdown(f"<div style='text-align:right; color:grey;'>Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

scanner()
