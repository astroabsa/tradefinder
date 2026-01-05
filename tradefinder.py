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

# --- 3. SMART F&O MASTER LIST ---
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
            
        # Process Indices (NIFTY/BANKNIFTY)
        curr_idx = get_current_futures(idx_df)
        for _, row in curr_idx.iterrows():
            # Index symbols are often 'NIFTY-JAN...' or 'BANKNIFTY-JAN...'
            base_sym = row['SEM_TRADING_SYMBOL'].split('-')[0]
            index_map[base_sym] = str(row['SEM_SMST_SECURITY_ID'])
            
    except Exception as e:
        st.error(f"Master List Error: {e}")
    
    return fno_map, index_map

with st.spinner("Syncing F&O List..."):
    FNO_MAP, INDEX_MAP = get_fno_futures_map()

# --- 4. DATA FETCHING ---
def fetch_futures_data(security_id, interval=60):
    try:
        to_date = datetime.now().strftime('%Y-%m-%d')
        from_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        # Determine Instrument Type based on ID (Simple heuristic or pass as arg)
        # For simplicity, we use the Master List context, but here we assume FUTSTK/FUTIDX logic
        # For Dashboard (Indices), we need 'FUTIDX'. For Stocks, 'FUTSTK'.
        # Dhan API requires correct type. We will try FUTSTK first (majority), 
        # but for dashboard specific function we use FUTIDX.
        
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

# --- 5. DASHBOARD FUNCTION (Matches Original Layout) ---
def fetch_market_dashboard():
    # Map for Display Name -> Dhan Symbol Key (from INDEX_MAP)
    # Ensure keys match what is in INDEX_MAP (e.g., 'NIFTY', 'BANKNIFTY')
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
            # Fetch Index Futures Data (FUTIDX)
            res = dhan.intraday_minute_data(
                security_id=sec_id,
                exchange_segment="NSE_FNO",
                instrument_type="FUTIDX",
                from_date=to_date, # Only today for speed
                to_date=to_date,
                interval=1 # 1 Minute for latest price
            )
            
            if res['status'] == 'success' and res['data'].get('close'):
                ltp = res['data']['close'][-1]
                # Calculate Change from Day Open
                open_price = res['data']['open'][0]
                chg = ltp - open_price
                pct = (chg / open_price) * 100
                data_display[name] = {"ltp": ltp, "chg": chg, "pct": pct}
            else:
                data_display[name] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}
        except:
            data_display[name] = {"ltp": 0.0, "chg": 0.0, "pct": 0.0}

    # --- RENDER DASHBOARD (Exact Original Layout) ---
    col1, col2, col3 = st.columns([1, 1, 2])
    
    # Nifty Metric
    with col1:
        n = data_display.get("NIFTY 50", {"ltp":0, "chg":0, "pct":0})
        st.metric(
            label="NIFTY 50 (Fut)", 
            value=f"{n['ltp']:,.2f}", 
            delta=f"{n['chg']:.2f} ({n['pct']:.2f}%)"
        )
    
    # Bank Nifty Metric
    with col2:
        b = data_display.get("BANK NIFTY", {"ltp":0, "chg":0, "pct":0})
        st.metric(
            label="BANK NIFTY (Fut)", 
            value=f"{b['ltp']:,.2f}", 
            delta=f"{b['chg']:.2f} ({b['pct']:.2f}%)"
        )
        
    # Sentiment Box
    with col3:
        n_pct = data_display.get("NIFTY 50", {}).get("pct", 0)
        bias = "SIDEWAYS ↔️"
        color = "gray"
        
        if n_pct > 0.25: 
            bias = "BULLISH 🚀"
            color = "green"
        elif n_pct < -0.25: 
            bias = "BEARISH 📉"
            color = "red"
            
        st.markdown(f"""
            <div style="text-align: center; padding: 10px; border: 1px solid {color}; border-radius: 10px;">
                <h3 style="margin:0; color: {color};">Market Bias: {bias}</h3>
            </div>
        """, unsafe_allow_html=True)

# --- 6. OI LOGIC ---
def get_oi_analysis(price_chg, oi_chg):
    if price_chg > 0 and oi_chg > 0: return "Long Buildup 🟢"
    if price_chg < 0 and oi_chg > 0: return "Short Buildup 🔴"
    if price_chg < 0 and oi_chg < 0: return "Long Unwinding ⚠️"
    if price_chg > 0 and oi_chg < 0: return "Short Covering 🚀"
    return "Neutral ⚪"

# --- 7. MAIN SCANNER ---
@st.fragment(run_every=180)
def refreshable_data_tables():
    # 1. Render Dashboard
    fetch_market_dashboard()
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
                
                # Filters
                if price_chg > 0.5 and curr_rsi > 60 and curr_adx > 20:
                    bullish_list.append(row)
                elif price_chg < -0.5 and curr_rsi < 45 and curr_adx > 20:
                    bearish_list.append(row)
                    
        except: pass
        progress_bar.progress((i + 1) / len(target_symbols))
        time.sleep(0.05)
        
    progress_bar.empty()
    
    # --- DISPLAY TABLES ---
    col_config = {
        "Symbol": st.column_config.LinkColumn("Script", display_text="symbol=NSE:(.*)"),
        "OI Chg%": st.column_config.NumberColumn("OI Chg%", format="%.2f%%"),
        "Price Chg%": st.column_config.NumberColumn("Price Chg%", format="%.2f%%")
    }
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.success(f"🟢 ACTIVE BULLS ({len(bullish_list)})")
        if bullish_list:
            st.dataframe(
                pd.DataFrame(bullish_list).sort_values(by="Mom %", ascending=False).head(15),
                use_container_width=True, hide_index=True, column_config=col_config
            )
        else: st.info("No bullish setups found.")
            
    with c2:
        st.error(f"🔴 ACTIVE BEARS ({len(bearish_list)})")
        if bearish_list:
            st.dataframe(
                pd.DataFrame(bearish_list).sort_values(by="Mom %", ascending=True).head(15),
                use_container_width=True, hide_index=True, column_config=col_config
            )
        else: st.info("No bearish setups found.")

    st.markdown(f"<div style='text-align:center; color:grey; margin-top:20px;'>Last Updated: {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%H:%M:%S')} | Powered by: i-Tech World</div>", unsafe_allow_html=True)

if dhan:
    refreshable_data_tables()
