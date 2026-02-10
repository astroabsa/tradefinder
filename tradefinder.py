import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time
import os
import requests  # NEW: for direct v2 HTTP calls
import json

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="iTW's Live F&O Screener Pro + OI (v2)", layout="wide")
IST = pytz.timezone('Asia/Kolkata')  # Force IST Timezone

# --- 2. AUTHENTICATION ---
AUTH_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSEan21a9IVnkdmTFP2Q9O_ILI3waF52lFWQ5RTDtXDZ5MI4_yTQgFYcCXN5HxgkCxuESi5Dwe9iROB/"
    "pub?gid=0&single=true&output=csv"
)

def authenticate_user(user_in, pw_in):
    try:
        df = pd.read_csv(AUTH_CSV_URL)
        df['username'] = df['username'].astype(str).str.strip().str.lower()
        df['password'] = df['password'].astype(str).str.strip()
        match = df[
            (df['username'] == str(user_in).strip().lower())
            & (df['password'] == str(pw_in).strip())
        ]
        return not match.empty
    except Exception:
        return False

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if not st.session_state["authenticated"]:
    st.title("🔐 iTW's F&O Pro Login")
    with st.form("login_form"):
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")
        if st.form_submit_button("Log In"):
            if authenticate_user(u, p):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Invalid Credentials")
    st.stop()

# --- 3. MAIN UI ---
st.title("🚀 iTW's Live F&O Screener Pro + OI (Dhan v2)")
if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False
    st.rerun()

# --- 4. API CONNECTION (v1 client still used for some calls) ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]  # used for v1 & v2
    dhan = dhanhq(client_id, access_token)
except Exception as e:
    st.error(f"API Error: {e}")
    st.stop()

DHAN_V2_BASE = "https://api.dhan.co/v2"  # v2 REST base URL [web:42]

# --- 5. INDEX MAP ---
INDEX_MAP = {
    'NIFTY': {'id': '13', 'name': 'NIFTY 50'},
    'BANKNIFTY': {'id': '25', 'name': 'BANK NIFTY'},
    'SENSEX': {'id': '51', 'name': 'SENSEX'},
}

# --- 6. MASTER LIST LOADER ---
@st.cache_data(ttl=3600 * 4)
def get_fno_stock_map():
    fno_map = {}
    if not os.path.exists("dhan_master.csv"):
        st.error("❌ 'dhan_master.csv' NOT FOUND.")
        return fno_map

    try:
        df = pd.read_csv("dhan_master.csv", on_bad_lines='skip', low_memory=False)
        df.columns = df.columns.str.strip()

        col_exch = 'SEM_EXM_EXCH_ID'
        col_id = 'SEM_SMST_SECURITY_ID'
        col_name = 'SEM_TRADING_SYMBOL'
        col_inst = 'SEM_INSTRUMENT_NAME'
        col_expiry = 'SEM_EXPIRY_DATE'

        if col_name in df.columns:
            df[col_name] = df[col_name].astype(str).str.upper().str.strip()
        if col_exch in df.columns:
            df[col_exch] = df[col_exch].astype(str).str.strip()
        if col_inst in df.columns:
            df[col_inst] = df[col_inst].astype(str).str.strip()

        if col_exch in df.columns and col_inst in df.columns:
            stk_df = df[(df[col_exch] == 'NSE') & (df[col_inst] == 'FUTSTK')].copy()

            if col_expiry in stk_df.columns:
                stk_df[col_expiry] = stk_df[col_expiry].astype(str)
                stk_df['dt_parsed'] = pd.to_datetime(
                    stk_df[col_expiry], dayfirst=True, errors='coerce'
                )

                today = pd.Timestamp.now().normalize()
                valid_futures = stk_df[stk_df['dt_parsed'] >= today]
                valid_futures = valid_futures.sort_values(by=[col_name, 'dt_parsed'])
                curr_stk = valid_futures.drop_duplicates(subset=[col_name], keep='first')

                for _, row in curr_stk.iterrows():
                    base_sym = row[col_name].split('-')[0]
                    disp_name = row.get('SEM_CUSTOM_SYMBOL', row[col_name])
                    fno_map[base_sym] = {'id': str(row[col_id]), 'name': disp_name}
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
    return fno_map

with st.spinner("Loading Stock List..."):
    FNO_MAP = get_fno_stock_map()

# --- 7. DAILY HELPERS (still v1, index-only) ---
def get_prev_close(security_id):
    try:
        to_d = datetime.now(IST).strftime('%Y-%m-%d')
        from_d = (datetime.now(IST) - timedelta(days=10)).strftime('%Y-%m-%d')

        res = dhan.historical_daily_data(str(security_id), "IDX_I", "INDEX", from_d, to_d)

        if res.get('status') == 'success' and 'data' in res:
            df = pd.DataFrame(res['data'])
            if df.empty:
                return 0.0

            time_col = 'start_Time' if 'start_Time' in df.columns else 'timestamp'
            df['date_str'] = df[time_col].astype(str).str[:10]

            today_str = datetime.now(IST).strftime('%Y-%m-%d')
            past_df = df[df['date_str'] != today_str]

            if not past_df.empty:
                return float(past_df.iloc[-1]['close'])
    except Exception:
        pass
    return 0.0

def get_live_price(security_id):
    try:
        to_d = datetime.now(IST).strftime('%Y-%m-%d')
        from_d = (datetime.now(IST) - timedelta(days=3)).strftime('%Y-%m-%d')

        res = dhan.intraday_minute_data(str(security_id), "IDX_I", "INDEX", from_d, to_d, 1)
        if res.get('status') == 'success' and 'data' in res:
            closes = res['data']['close']
            if len(closes) > 0:
                return float(closes[-1])
    except Exception:
        pass
    return 0.0

# --- 8. DASHBOARD ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    data = {}

    for key, info in INDEX_MAP.items():
        sid = info['id']

        prev = get_prev_close(sid)
        ltp = get_live_price(sid)

        if ltp == 0:
            ltp = prev
        if prev == 0:
            prev = ltp

        chg = 0.0
        pct = 0.0

        if prev > 0:
            chg = ltp - prev
            pct = (chg / prev) * 100

        data[info['name']] = {"ltp": ltp, "chg": chg, "pct": pct}

    c1, c2, c3, c4 = st.columns([1, 1, 1, 1.2])
    with c1:
        d = data.get("NIFTY 50", {"ltp": 0, "chg": 0, "pct": 0})
        st.metric("NIFTY 50", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c2:
        d = data.get("BANK NIFTY", {"ltp": 0, "chg": 0, "pct": 0})
        st.metric("BANK NIFTY", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c3:
        d = data.get("SENSEX", {"ltp": 0, "chg": 0, "pct": 0})
        st.metric("SENSEX", f"{d['ltp']:,.2f}", f"{d['chg']:.2f} ({d['pct']:.2f}%)")
    with c4:
        bias, color = ("SIDEWAYS ↔️", "gray")
        nifty_pct = data.get("NIFTY 50", {}).get("pct", 0)
        if nifty_pct > 0.25:
            bias, color = ("BULLISH 🚀", "green")
        elif nifty_pct < -0.25:
            bias, color = ("BEARISH 📉", "red")
        st.markdown(
            f"<div style='text-align:center; padding:10px; border:1px solid {color}; "
            f"border-radius:10px; color:{color}'><h3>Bias: {bias}</h3></div>",
            unsafe_allow_html=True,
        )

# --- 9. SIMPLE SENTIMENT ---
def get_trend_analysis(price_chg, vol_ratio):
    if price_chg > 0 and vol_ratio > 1.2:
        return "Bullish (Vol) 🟢"
    if price_chg < 0 and vol_ratio > 1.2:
        return "Bearish (Vol) 🔴"
    if price_chg > 0:
        return "Mild Bullish ↗️"
    if price_chg < 0:
        return "Mild Bearish ↘️"
    return "Neutral ⚪"

# --- 10. OI SIGNAL ---
def get_oi_signal(oi_chg, day_price_chg):
    if oi_chg > 2 and day_price_chg > 0.5:
        return "Long Buildup 🟢"
    if oi_chg > 2 and day_price_chg < -0.5:
        return "Short Buildup 🔴"
    if oi_chg < -2 and day_price_chg > 0.5:
        return "Short Covering 🟡"
    if oi_chg < -2 and day_price_chg < -0.5:
        return "Long Unwinding 🟠"
    return "No Clear OI ⚪"

# --- 11. STRENGTH STORAGE ---
def init_signal_history():
    if "signal_history" not in st.session_state:
        st.session_state["signal_history"] = {"bull": {}, "bear": {}}

def update_signal_history(side, symbol, now):
    h = st.session_state["signal_history"][side]
    if symbol not in h:
        h[symbol] = {"first_seen": now, "last_seen": now}
    else:
        h[symbol]["last_seen"] = now

def get_strength_minutes(side, symbol, now):
    h = st.session_state["signal_history"][side]
    rec = h.get(symbol)
    if not rec:
        return 0.0
    delta = now - rec["first_seen"]
    return round(delta.total_seconds() / 60.0, 1)

# --- 12. v2 INTRADAY FETCH WITH OI ---
def fetch_intraday_v2_futstk(security_id, from_d, to_d, interval_min=60):
    """
    Uses Dhan v2 /charts/intraday to get OHLC + Volume + OI for FUTSTK [web:42].
    Returns a DataFrame with datetime (IST), Open, High, Low, Close, Volume, OI.
    """
    url = f"{DHAN_V2_BASE}/charts/intraday"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": access_token,
    }
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "FUTSTK",
        "expiryCode": 0,          # 0 works when securityId already points to a specific contract
        "oi": True,               # request OI data [web:42]
        "fromDate": from_d,
        "toDate": to_d,
        "interval": int(interval_min),
    }

    resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=5)
    resp.raise_for_status()
    data = resp.json()

    # v2 returns arrays: open, high, low, close, volume, timestamp, open_interest [web:42]
    closes = data.get("close", [])
    if not closes:
        return pd.DataFrame()

    opens = data.get("open", [])
    highs = data.get("high", [])
    lows = data.get("low", [])
    vols = data.get("volume", [])
    ts = data.get("timestamp", [])
    oi = data.get("open_interest", [])

    n = len(closes)
    # Ensure all lists are same length
    def safe_list(lst):
        return lst if len(lst) == n else (lst + [lst[-1]] * (n - len(lst)) if lst else [0] * n)

    opens = safe_list(opens)
    highs = safe_list(highs)
    lows = safe_list(lows)
    vols = safe_list(vols)
    ts = safe_list(ts)
    oi = safe_list(oi)

    dt_index = [
        datetime.fromtimestamp(t, tz=IST) if isinstance(t, (int, float)) else
        datetime.fromtimestamp(float(t), tz=IST)
        for t in ts
    ]

    df = pd.DataFrame(
        {
            "datetime": dt_index,
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": vols,
            "OI": oi,
        }
    )
    return df

# --- 13. SCANNER (uses v2 + strength) ---
@st.fragment(run_every=180)
def refreshable_scanner():
    init_signal_history()
    now_scan = datetime.now(IST)

    st.markdown("---")
    st.caption(
        f"Scanning {len(FNO_MAP)} symbols using Dhan v2 intraday (with OI where available)... "
        f"(Updates every 3 mins)"
    )

    tab1, tab2 = st.tabs(["🚀 Signals", "📋 All Data"])
    targets = list(FNO_MAP.keys())
    if not targets:
        st.warning("Scanner paused: No symbols found.")
        return

    bar = st.progress(0)
    bull, bear, all_data = [], [], []

    today = now_scan.date()

    for i, sym in enumerate(targets):
        try:
            sid = FNO_MAP[sym]['id']

            to_d = now_scan.strftime('%Y-%m-%d')
            from_d = (now_scan - timedelta(days=5)).strftime('%Y-%m-%d')

            try:
                df = fetch_intraday_v2_futstk(sid, from_d, to_d, interval_min=60)
            except Exception:
                df = pd.DataFrame()

            if df.empty:
                continue

            # --- TECH INDICATORS ---
            if len(df) >= 14:
                df['RSI'] = ta.rsi(df['Close'], 14)
                adx_df = ta.adx(df['High'], df['Low'], df['Close'], 14)
                df['ADX'] = adx_df['ADX_14']
                curr_rsi = float(df['RSI'].iloc[-1])
                curr_adx = float(df['ADX'].iloc[-1])
            else:
                curr_rsi = 0.0
                curr_adx = 0.0

            if len(df) >= 5:
                df['EMA'] = ta.ema(df['Close'], 5)
                mom = round(
                    ((df['Close'].iloc[-1] - df['EMA'].iloc[-1]) / df['EMA'].iloc[-1]) * 100,
                    2,
                )
            else:
                mom = 0.0

            curr_vol = float(df['Volume'].iloc[-1])
            avg_vol = (
                df['Volume'].rolling(10).mean().iloc[-1]
                if len(df) > 10
                else curr_vol
            )
            vol_ratio = (curr_vol / avg_vol) if avg_vol > 0 else 1.0

            curr = df.iloc[-1]
            ltp = float(curr['Close'])

            if len(df) > 1:
                prev = df.iloc[-2]
                p_chg = round(
                    ((ltp - prev['Close']) / prev['Close']) * 100,
                    2,
                )
            else:
                p_chg = 0.0

            # --- INTRADAY OI & PRICE CHANGE FOR TODAY ---
            oi_available = not (df['OI'].max() == 0 and df['OI'].min() == 0)

            if oi_available:
                day_df = df[df['datetime'].dt.date == today]
                if len(day_df) >= 2:
                    day_first = day_df.iloc[0]
                    day_last = day_df.iloc[-1]

                    oi_start = float(day_first.get('OI', 0) or 0)
                    oi_end = float(day_last.get('OI', 0) or 0)
                    if oi_start > 0:
                        oi_chg = round(((oi_end - oi_start) / oi_start) * 100, 2)
                    else:
                        oi_chg = 0.0

                    price_start = float(day_first['Close'])
                    if price_start > 0:
                        day_price_chg = round(
                            ((ltp - price_start) / price_start) * 100,
                            2,
                        )
                    else:
                        day_price_chg = 0.0
                else:
                    oi_chg = 0.0
                    day_price_chg = 0.0

                oi_signal = get_oi_signal(oi_chg, day_price_chg)
            else:
                oi_chg = 0.0
                day_price_chg = 0.0
                oi_signal = "No OI Data ❔"

            intraday_sent = get_trend_analysis(p_chg, vol_ratio)

            row = {
                "Sym": sym,  # internal
                "Symbol": f"https://in.tradingview.com/chart/?symbol=NSE:{sym}",
                "LTP": round(ltp, 2),
                "Mom %": mom,
                "Price Chg%": p_chg,
                "Day Price%": day_price_chg,
                "RSI": round(curr_rsi, 1),
                "ADX": round(curr_adx, 1),
                "Vol Ratio": round(vol_ratio, 1),
                "OI Chg%": oi_chg,
                "OI Signal": oi_signal,
                "Analysis": intraday_sent,
            }

            r_m = row.copy()
            r_m["Sort"] = sym
            all_data.append(r_m)

            # --- SIGNAL LOGIC + STRENGTH ---
            if oi_available and "Buildup" in oi_signal:
                if day_price_chg > 0 and p_chg > 0:
                    update_signal_history("bull", sym, now_scan)
                    bull_row = row.copy()
                    bull_row["Strength (min)"] = get_strength_minutes(
                        "bull", sym, now_scan
                    )
                    bull.append(bull_row)

                if day_price_chg < 0 and p_chg < 0:
                    update_signal_history("bear", sym, now_scan)
                    bear_row = row.copy()
                    bear_row["Strength (min)"] = get_strength_minutes(
                        "bear", sym, now_scan
                    )
                    bear.append(bear_row)
            else:
                # fallback: no OI yet, use technical‑only
                if curr_rsi > 0:
                    if p_chg > 0.3 and curr_rsi > 55 and vol_ratio > 1.1:
                        update_signal_history("bull", sym, now_scan)
                        bull_row = row.copy()
                        bull_row["Strength (min)"] = get_strength_minutes(
                            "bull", sym, now_scan
                        )
                        bull.append(bull_row)
                    elif p_chg < -0.3 and curr_rsi < 52 and vol_ratio > 1.1:
                        update_signal_history("bear", sym, now_scan)
                        bear_row = row.copy()
                        bear_row["Strength (min)"] = get_strength_minutes(
                            "bear", sym, now_scan
                        )
                        bear.append(bear_row)

        except Exception:
            pass

        time.sleep(0.12)
        bar.progress((i + 1) / len(targets))

    bar.empty()

    cfg = {
        "Symbol": st.column_config.LinkColumn(
            "Script", display_text="symbol=NSE:(.*)", width="medium"
        ),
        "LTP": st.column_config.NumberColumn("LTP", format="%.2f"),
        "Mom %": st.column_config.NumberColumn("Mom%", format="%.2f%%"),
        "Price Chg%": st.column_config.NumberColumn(
            "Chg% (Last bar)", format="%.2f%%"
        ),
        "Day Price%": st.column_config.NumberColumn("Chg% (Today)", format="%.2f%%"),
        "RSI": st.column_config.NumberColumn("RSI", format="%.1f"),
        "ADX": st.column_config.NumberColumn("ADX", format="%.1f"),
        "Vol Ratio": st.column_config.NumberColumn("Vol x", format="%.1fx"),
        "OI Chg%": st.column_config.NumberColumn("OI Chg%", format="%.2f%%"),
        "Strength (min)": st.column_config.NumberColumn(
            "Strength (min)", format="%.1f"
        ),
        "OI Signal": st.column_config.TextColumn("OI Signal", width="medium"),
        "Analysis": st.column_config.TextColumn("Analysis", width="medium"),
    }

    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.success(f"🟢 BULLS ({len(bull)}) – OI/Tech-backed")
            if bull:
                df_bull = pd.DataFrame(bull).drop(columns=["Sym"], errors="ignore")
                st.dataframe(
                    df_bull.sort_values("Strength (min)", ascending=False).head(20),
                    use_container_width=True,
                    hide_index=True,
                    column_config=cfg,
                )
            else:
                st.info("No bullish setups as per current criteria.")
        with c2:
            st.error(f"🔴 BEARS ({len(bear)}) – OI/Tech-backed")
            if bear:
                df_bear = pd.DataFrame(bear).drop(columns=["Sym"], errors="ignore")
                st.dataframe(
                    df_bear.sort_values("Strength (min)", ascending=False).head(20),
                    use_container_width=True,
                    hide_index=True,
                    column_config=cfg,
                )
            else:
                st.info("No bearish setups as per current criteria.")

    with tab2:
        if all_data:
            df_all = pd.DataFrame(all_data).sort_values("Sort")
            df_all = df_all.drop(columns=["Sort", "Sym"], errors="ignore")
            st.dataframe(
                df_all,
                use_container_width=True,
                hide_index=True,
                column_config=cfg,
                height=600,
            )
        else:
            st.warning("No data found.")

    st.write(f"🕒 **Last Data Sync:** {now_scan.strftime('%H:%M:%S')} IST")
    st.markdown(
        "<div style='text-align: center; color: grey;'>Powered by : i-Tech World</div>",
        unsafe_allow_html=True,
    )

# --- 14. RUN APP ---
if dhan:
    refreshable_dashboard()
    refreshable_scanner()
