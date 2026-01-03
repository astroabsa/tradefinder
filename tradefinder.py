import streamlit as st
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time
from dhanhq import dhanhq

# --- 1. APP CONFIGURATION ---
st.set_page_config(page_title="Absa's DhanHQ Live Pro", layout="wide")

# --- 2. USER CREDENTIALS (REPLACE THESE) ---
# For security, ideally use st.secrets in Streamlit Cloud
CLIENT_ID = "1104089467"      # e.g., "10000xxxxx"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3NTA4NzY0LCJpYXQiOjE3Njc0MjIzNjQsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA0MDg5NDY3In0.al1lcv-txOjq8JY7lUnqCjm2978ZgydGoPlw9n8d2aPjYv2gIDUmOTpgQjiB7Ha91X6VhDuHg1XTIg8FtfwkPA" # Long JWT string from Dhan Web

# --- 3. GLOBAL SYMBOL LIST (Example Subset) ---
# Keep your full list here. I'm using a small one for the demo.
FNO_SYMBOLS = [
    'RELIANCE', 'HDFCBANK', 'INFY', 'TCS', 'ICICIBANK', 'SBIN',
    'TATAMOTORS', 'ADANIENT', 'AXISBANK', 'BAJFINANCE'
]

# --- 4. INITIALIZE DHAN CLIENT ---
@st.cache_resource
def init_dhan():
    try:
        dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)
        return dhan
    except Exception as e:
        st.error(f"Dhan Login Failed: {e}")
        return None

dhan = init_dhan()

# --- 5. SMART MAPPING: SYMBOL -> SECURITY ID ---
@st.cache_data(ttl=86400) # Cache for 24 hours
def get_fno_master_map():
    """
    Downloads Dhan's master scrip list to map 'RELIANCE' -> '1333'
    """
    try:
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        df = pd.read_csv(url)
        
        # Filter for NSE Equity & Indices to keep it small
        mask = (df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'].isin(['EQUITY', 'INDEX']))
        df = df[mask]
        
        # Create a dictionary: {'RELIANCE': '1333', 'NIFTY 50': '13'}
        # We clean the symbol name to match your list
        symbol_map = dict(zip(df['SEM_TRADING_SYMBOL'], df['SEM_SMST_SECURITY_ID']))
        return symbol_map
    except Exception as e:
        st.error(f"Failed to fetch Scrip Master: {e}")
        return {}

symbol_map = get_fno_master_map()

# --- 6. HELPER FUNCTIONS ---
def get_sentiment(p_chg, oi_chg):
    # Dhan provides live OI in their quote API, unlike yfinance!
    if p_chg > 0 and oi_chg > 0: return "Long Buildup 🚀"
    if p_chg < 0 and oi_chg > 0: return "Short Buildup 📉"
    if p_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"
    if p_chg > 0 and oi_chg < 0: return "Short Covering 💨"
    return "Neutral"

# --- 7. MARKET DASHBOARD (REALTIME) ---
def fetch_market_dashboard():
    # IDs for Indices (From Dhan Scrip Master)
    # NIFTY 50 = '13', BANK NIFTY = '25' (Check master csv to be sure, usually these are standard)
    indices = [
        {"name": "NIFTY 50", "id": "13"},
        {"name": "BANK NIFTY", "id": "25"}
    ]
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    for idx, index_info in enumerate(indices):
        try:
            # Dhan Intraday API for Index
            data = dhan.intraday_minute_data(
                security_id=index_info['id'],
                exchange_segment='NSE_IND', # NSE Index Segment
                instrument_type='INDEX'
            )
            
            if data['status'] == 'success':
                df = pd.DataFrame(data['data'])
                if not df.empty:
                    ltp = df['close'].iloc[-1]
                    prev = df['close'].iloc[0] # Open of the day (approx) or prev close
                    # Better to fetch OHLC quote for exact prev close, but this works for calculation
                    
                    chg = ltp - prev
                    pct = (chg / prev) * 100
                    
                    if idx == 0:
                        with col1: st.metric(index_info['name'], f"{ltp:,.2f}", f"{pct:.2f}%")
                        # Bias Logic based on Nifty
                        bias, color = ("SIDEWAYS ↔️", "gray")
                        if pct > 0.25: bias, color = ("BULLISH 🚀", "green")
                        elif pct < -0.25: bias, color = ("BEARISH 📉", "red")
                    else:
                        with col2: st.metric(index_info['name'], f"{ltp:,.2f}", f"{pct:.2f}%")
        except:
            pass

    with col3:
        # Display Bias (calculated from Nifty loop above)
        if 'color' in locals():
            st.markdown(f"""
                <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 10px;">
                    <h3 style="margin:0; color: {color};">Market Bias: {bias}</h3>
                </div>
            """, unsafe_allow_html=True)

# --- 8. SCANNER ENGINE (OPTIMIZED LOOP) ---
@st.fragment(run_every=180) # Auto-refresh every 3 mins
def run_scanner():
    fetch_market_dashboard()
    st.markdown("---")
    
    bullish, bearish = [], []
    progress_bar = st.progress(0, text="Fetching Realtime Data from DhanHQ...")
    
    for i, sym in enumerate(FNO_SYMBOLS):
        try:
            # 1. Get Security ID
            sec_id = str(symbol_map.get(sym))
            if not sec_id or sec_id == "None":
                continue # Skip if ID not found
                
            # 2. Fetch Intraday Data (Hourly)
            # Dhan allows fetching last few days of minute data. 
            # We fetch '60' minute interval.
            response = dhan.intraday_minute_data(
                security_id=sec_id,
                exchange_segment='NSE_EQ', # Use NSE_FNO if you want Futures data specifically
                instrument_type='EQUITY'
            )
            
            if response['status'] == 'failure':
                continue

            df = pd.DataFrame(response['data'])
            
            # 3. Calculate Indicators
            if len(df) > 15:
                # Rename columns to match pandas_ta expectations if needed
                # Dhan returns: start_Time, open, high, low, close, volume
                # Ensure they are floats
                df['close'] = df['close'].astype(float)
                df['high'] = df['high'].astype(float)
                df['low'] = df['low'].astype(float)
                
                # RSI & ADX
                df['RSI'] = ta.rsi(df['close'], length=14)
                adx_df = ta.adx(df['high'], df['low'], df['close'], length=14)
                df['EMA_5'] = ta.ema(df['close'], length=5)
                
                # Get Latest Values
                ltp = df['close'].iloc[-1]
                curr_rsi = df['RSI'].iloc[-1]
                curr_adx = adx_df['ADX_14'].iloc[-1]
                ema_5 = df['EMA_5'].iloc[-1]
                momentum_pct = round(((ltp - ema_5) / ema_5) * 100, 2)
                
                # Price Change (Intraday)
                open_price = df['open'].iloc[0] # Or fetch prev_close via quote API for accuracy
                p_change = round(((ltp - open_price) / open_price) * 100, 2)
                
                # Dhan Sentiment Logic
                # Note: To get REAL OI, you'd query the 'NSE_FNO' segment, not 'NSE_EQ'
                sentiment = get_sentiment(p_change, 1) # Placeholder OI change
                
                # Build Row
                tv_url = f"https://tv.dhan.co/?symbol={sym}" # Deep link to Dhan TV
                
                row = {
                    "Symbol": tv_url,
                    "LTP": ltp,
                    "Mom %": momentum_pct,
                    "Chg %": p_change,
                    "RSI": round(curr_rsi, 1),
                    "ADX": round(curr_adx, 1),
                    "Sentiment": sentiment
                }
                
                # 4. Filter Logic
                if p_change > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish.append(row)
                elif p_change < -0.5 and curr_rsi < 45 and curr_adx > 20:
                    bearish.append(row)
            
            time.sleep(0.05) # Tiny sleep to respect rate limits (Dhan is generous though)
            progress_bar.progress((i + 1) / len(FNO_SYMBOLS))
            
        except Exception as e:
            # print(f"Error {sym}: {e}")
            continue

    progress_bar.empty()
    
    # --- RENDER TABLES ---
    column_config = {
        "Symbol": st.column_config.LinkColumn("Scrip (Dhan TV)", display_text="symbol=(.*)")
    }
    
    c1, c2 = st.columns(2)
    with c1:
        st.success("🟢 ACTIVE BULLS")
        if bullish:
            st.dataframe(pd.DataFrame(bullish).sort_values(by="Mom %", ascending=False), 
                         use_container_width=True, hide_index=True, column_config=column_config)
        else:
            st.info("No signals.")
            
    with c2:
        st.error("🔴 ACTIVE BEARS")
        if bearish:
            st.dataframe(pd.DataFrame(bearish).sort_values(by="Mom %", ascending=True), 
                         use_container_width=True, hide_index=True, column_config=column_config)
        else:
            st.info("No signals.")

    st.write(f"🕒 Last Update: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')}")

run_scanner()
