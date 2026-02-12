import streamlit as st
from dhanhq import dhanhq
import pandas as pd
import pandas_ta as ta
import pytz
from datetime import datetime, timedelta
import time
import os
import requests
import json

# --- 1. CONFIGURATION ---
st.set_page_config(page_title="iTW's Live F&O Screener Pro ", layout="wide")
IST = pytz.timezone('Asia/Kolkata')  # Force IST Timezone

MIN_SCAN_GAP_SECONDS = 30  # wait at least 30s between full scans

# --- 2. AUTHENTICATION ---
AUTH_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vSEan21a9IVnkdmTFP2Q9O_ILI3waF52lFWQ5RTDtXDZ5MI4_yTQgFYcCXN5HxgkCxuESi5Dwe9iROB/"
    "pub?gid=0&single=true&output=csv"
)

def authenticate_user(user_in, pw_in):
    try:
        df = pd.read_csv(AUTH_CSV_URL)
        df["username"] = df["username"].astype(str).str.strip().str.lower()
        df["password"] = df["password"].astype(str).str.strip()
        match = df[
            (df["username"] == str(user_in).strip().lower())
            & (df["password"] == str(pw_in).strip())
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
st.title("🚀 iTW's Live F&O Screener Pro")
if st.sidebar.button("Log out"):
    st.session_state["authenticated"] = False
    st.rerun()

DEBUG_SHOW_ERRORS = st.sidebar.checkbox("Show API / OI debug info", value=False)

# --- 4. API CONNECTION ---
dhan = None
try:
    client_id = st.secrets["DHAN_CLIENT_ID"]
    access_token = st.secrets["DHAN_ACCESS_TOKEN"]  # used for v1 & v2
    dhan = dhanhq(client_id, access_token)
except Exception as e:
    st.error(f"API Error: {e}")
    st.stop()

DHAN_V2_BASE = "https://api.dhan.co/v2"  # v2 REST base URL

# --- 5. INDEX MAP (spot indices) ---
INDEX_MAP = {
    "NIFTY": {"id": "13", "name": "NIFTY 50"},
    "BANKNIFTY": {"id": "25", "name": "BANK NIFTY"},
    "SENSEX": {"id": "51", "name": "SENSEX"},
}

# --- 6. MASTER LIST LOADERS ---
@st.cache_data(ttl=3600 * 4)
def get_fno_stock_map():
    fno_map = {}
    if not os.path.exists("dhan_master.csv"):
        st.error("❌ 'dhan_master.csv' NOT FOUND.")
        return fno_map

    try:
        df = pd.read_csv("dhan_master.csv", on_bad_lines="skip", low_memory=False)
        df.columns = df.columns.str.strip()

        col_exch = "SEM_EXM_EXCH_ID"
        col_id = "SEM_SMST_SECURITY_ID"
        col_name = "SEM_TRADING_SYMBOL"
        col_inst = "SEM_INSTRUMENT_NAME"
        col_expiry = "SEM_EXPIRY_DATE"

        if col_name in df.columns:
            df[col_name] = df[col_name].astype(str).str.upper().str.strip()
        if col_exch in df.columns:
            df[col_exch] = df[col_exch].astype(str).str.strip()
        if col_inst in df.columns:
            df[col_inst] = df[col_inst].astype(str).str.strip()

        if col_exch in df.columns and col_inst in df.columns:
            stk_df = df[(df[col_exch] == "NSE") & (df[col_inst] == "FUTSTK")].copy()

            if col_expiry in stk_df.columns:
                stk_df[col_expiry] = stk_df[col_expiry].astype(str)
                stk_df["dt_parsed"] = pd.to_datetime(
                    stk_df[col_expiry], dayfirst=True, errors="coerce"
                )

                today = pd.Timestamp.now().normalize()
                valid_futures = stk_df[stk_df["dt_parsed"] >= today]
                valid_futures = valid_futures.sort_values(by=[col_name, "dt_parsed"])
                curr_stk = valid_futures.drop_duplicates(subset=[col_name], keep="first")

                for _, row in curr_stk.iterrows():
                    base_sym = row[col_name].split("-")[0]
                    disp_name = row.get("SEM_CUSTOM_SYMBOL", row[col_name])
                    fno_map[base_sym] = {"id": str(row[col_id]), "name": disp_name}
    except Exception as e:
        st.error(f"Error reading CSV: {e}")
    return fno_map


@st.cache_data(ttl=900)
def get_index_fut_ids():
    """
    Auto-detect current (nearest non‑expired) index futures IDs (FUTIDX)
    for NIFTY, BANKNIFTY, SENSEX from dhan_master.csv. [web:47][web:49]
    """
    ids = {"NIFTY": None, "BANKNIFTY": None, "SENSEX": None}

    if not os.path.exists("dhan_master.csv"):
        return ids

    df = pd.read_csv("dhan_master.csv", on_bad_lines="skip", low_memory=False)
    df.columns = df.columns.str.strip()

    needed = {
        "SEM_EXM_EXCH_ID",
        "SEM_INSTRUMENT_NAME",
        "SEM_TRADING_SYMBOL",
        "SEM_EXPIRY_DATE",
        "SEM_SMST_SECURITY_ID",
    }
    if not needed.issubset(set(df.columns)):
        return ids

    for col in needed:
        df[col] = df[col].astype(str).str.strip().str.upper()

    futidx_df = df[
        (df["SEM_EXM_EXCH_ID"] == "NSE")
        & (df["SEM_INSTRUMENT_NAME"] == "FUTIDX")
    ].copy()

    if futidx_df.empty:
        return ids

    futidx_df["dt_parsed"] = pd.to_datetime(
        futidx_df["SEM_EXPIRY_DATE"], dayfirst=True, errors="coerce"
    )
    today = pd.Timestamp.now().normalize()
    futidx_df = futidx_df[futidx_df["dt_parsed"] >= today]

    def pick_nearest(base):
        sub = futidx_df[futidx_df["SEM_TRADING_SYMBOL"].str.startswith(base)]
        if sub.empty:
            return None
        sub = sub.sort_values("dt_parsed")
        return str(sub.iloc[0]["SEM_SMST_SECURITY_ID"])

    ids["NIFTY"] = pick_nearest("NIFTY")
    ids["BANKNIFTY"] = pick_nearest("BANKNIFTY")
    ids["SENSEX"] = pick_nearest("SENSEX")

    return ids


with st.spinner("Loading Stock List..."):
    FNO_MAP = get_fno_stock_map()
    INDEX_FUT_MAP = get_index_fut_ids()

# --- 7. DAILY HELPERS (indices & FUTSTK) ---
def get_prev_close_index(security_id):
    """
    Previous day's close for any index using v2 daily historical charts. [web:42]
    """
    try:
        today = datetime.now(IST).date()
        to_d = datetime.now(IST).strftime("%Y-%m-%d")
        from_d = (datetime.now(IST) - timedelta(days=20)).strftime("%Y-%m-%d")

        url = f"{DHAN_V2_BASE}/charts/historical"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": access_token,
        }
        payload = {
            "securityId": str(security_id),
            "exchangeSegment": "IDX_I",
            "instrument": "INDEX",
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_d,
            "toDate": to_d,
        }

        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=5)
        if DEBUG_SHOW_ERRORS:
            st.caption(f"v2 daily status {resp.status_code} for index {security_id}")
        resp.raise_for_status()
        data = resp.json()

        closes = data.get("close", [])
        ts = data.get("timestamp", [])
        if not closes or not ts:
            return 0.0

        dates = [
            datetime.fromtimestamp(t, tz=IST).date()
            if isinstance(t, (int, float))
            else datetime.fromtimestamp(float(t), tz=IST).date()
            for t in ts
        ]

        past = [(d, c) for d, c in zip(dates, closes) if d < today]
        if not past:
            return 0.0

        prev_close = float(past[-1][1])
        return prev_close
    except Exception as e:
        if DEBUG_SHOW_ERRORS:
            st.error(f"Index daily v2 error ({security_id}): {e}")
        return 0.0


def get_live_price(security_id):
    try:
        to_d = datetime.now(IST).strftime("%Y-%m-%d")
        from_d = (datetime.now(IST) - timedelta(days=3)).strftime("%Y-%m-%d")

        res = dhan.intraday_minute_data(str(security_id), "IDX_I", "INDEX", from_d, to_d, 1)
        if res.get("status") == "success" and "data" in res:
            closes = res["data"]["close"]
            if len(closes) > 0:
                return float(closes[-1])
    except Exception:
        pass
    return 0.0


def get_prev_close_futstk(security_id):
    try:
        to_d = datetime.now(IST).strftime("%Y-%m-%d")
        from_d = (datetime.now(IST) - timedelta(days=10)).strftime("%Y-%m-%d")

        res = dhan.historical_daily_data(str(security_id), "NSE_FNO", "FUTSTK", from_d, to_d)
        if res.get("status") == "success" and "data" in res:
            df = pd.DataFrame(res["data"])
            if df.empty:
                return 0.0

            time_col = "start_Time" if "start_Time" in df.columns else "timestamp"
            df["date_str"] = df[time_col].astype(str).str[:10]

            today_str = datetime.now(IST).strftime("%Y-%m-%d")
            past_df = df[df["date_str"] != today_str]

            if not past_df.empty:
                return float(past_df.iloc[-1]["close"])
    except Exception:
        pass
    return 0.0

# --- 8. DASHBOARD ---
@st.fragment(run_every=5)
def refreshable_dashboard():
    data = {}

    for key, info in INDEX_MAP.items():
        sid = info["id"]

        prev = get_prev_close_index(sid)
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

        data[info["name"]] = {"ltp": ltp, "chg": chg, "pct": pct}

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

# --- 9. SIMPLE SENTIMENT & OI SIGNAL ---
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

# --- 10. STRENGTH STORAGE ---
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

# --- 11. CONVICTION SCORING HELPERS ---
def get_trend_score(side, rsi, adx, mom):
    score = 0
    if adx > 30:
        score += 18
    elif adx > 25:
        score += 14
    elif adx > 20:
        score += 8

    if side == "bull":
        if 50 <= rsi <= 65:
            score += 15
        elif 45 <= rsi < 50 or 65 < rsi <= 70:
            score += 8
        if mom > 0.7:
            score += 7
        elif mom > 0.3:
            score += 4
    else:
        if 35 <= rsi <= 50:
            score += 15
        elif 30 <= rsi < 35 or 50 < rsi <= 60:
            score += 8
        if mom < -0.7:
            score += 7
        elif mom < -0.3:
            score += 4

    if rsi > 75 or rsi < 25:
        score -= 5
    return max(0, min(score, 40))

def get_participation_score(vol_ratio, oi_chg, oi_signal):
    score = 0
    if vol_ratio >= 2.0:
        score += 15
    elif vol_ratio >= 1.5:
        score += 10
    elif vol_ratio >= 1.2:
        score += 5

    if "Buildup" in oi_signal:
        if abs(oi_chg) >= 8:
            score += 15
        elif abs(oi_chg) >= 5:
            score += 10
        elif abs(oi_chg) >= 2:
            score += 5
    elif "Unwinding" in oi_signal or "Covering" in oi_signal:
        score -= 5

    return max(0, min(score, 30))

def get_persistence_score(strength_min, day_price_chg, p_chg):
    score = 0
    if strength_min >= 90:
        score += 15
    elif strength_min >= 60:
        score += 11
    elif strength_min >= 30:
        score += 7
    elif strength_min >= 15:
        score += 4

    if abs(day_price_chg) >= 2 and (
        (day_price_chg > 0 and p_chg > 0)
        or (day_price_chg < 0 and p_chg < 0)
    ):
        score += 10
    elif abs(day_price_chg) >= 1:
        score += 6

    if abs(p_chg) < 0.1:
        score -= 3

    return max(0, min(score, 30))

def compute_conviction(
    side,
    rsi,
    adx,
    mom,
    vol_ratio,
    oi_chg,
    oi_signal,
    strength_min,
    day_price_chg,
    p_chg,
):
    t_score = get_trend_score(side, rsi, adx, mom)
    p_score = get_participation_score(vol_ratio, oi_chg, oi_signal)
    s_score = get_persistence_score(strength_min, day_price_chg, p_chg)
    total = t_score + p_score + s_score
    return min(100, total), t_score, p_score, s_score

# --- 12. v2 INTRADAY FETCH WITH OI ---
def _fetch_intraday_v2(security_id, instrument, from_d, to_d, interval_min=60):
    url = f"{DHAN_V2_BASE}/charts/intraday"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": access_token,
    }
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": instrument,  # FUTSTK or FUTIDX
        "expiryCode": 0,
        "oi": True,
        "fromDate": from_d,
        "toDate": to_d,
        "interval": int(interval_min),
    }

    try:
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=5)
        if DEBUG_SHOW_ERRORS:
            st.caption(f"v2 status {resp.status_code} for {instrument} {security_id}")
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        if DEBUG_SHOW_ERRORS and "dhan_v2_error_once" not in st.session_state:
            st.session_state["dhan_v2_error_once"] = True
            st.error(f"Dhan v2 intraday error ({instrument} {security_id}): {e}")
        return pd.DataFrame()

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

def fetch_intraday_v2_futstk(security_id, from_d, to_d, interval_min=60):
    return _fetch_intraday_v2(security_id, "FUTSTK", from_d, to_d, interval_min)

def fetch_intraday_v2_futidx(security_id, from_d, to_d, interval_min=60):
    if not security_id:
        return pd.DataFrame()
    return _fetch_intraday_v2(security_id, "FUTIDX", from_d, to_d, interval_min)

# --- 13. SCANNER WITH THROTTLING & INDEX TECH/OI ---
@st.fragment(run_every=5)
def refreshable_scanner():
    init_signal_history()
    now_scan = datetime.now(IST)

    if "last_scan" not in st.session_state:
        st.session_state["last_scan"] = None
    if "scan_in_progress" not in st.session_state:
        st.session_state["scan_in_progress"] = False

    last = st.session_state["last_scan"]

    # watchdog to avoid stuck state
    if st.session_state["scan_in_progress"] and last is not None:
        stuck_elapsed = (now_scan - last["time"]).total_seconds()
        if stuck_elapsed > MIN_SCAN_GAP_SECONDS * 3:
            st.session_state["scan_in_progress"] = False

    st.markdown("---")
    st.caption(
        f"Scanning {len(FNO_MAP)} symbols using Dhan v2 intraday (with OI where available)... "
        f"(Min gap {MIN_SCAN_GAP_SECONDS}s between scans)"
    )

    tab1, tab2 = st.tabs(["🚀 Signals", "📋 All Data"])
    targets = list(FNO_MAP.keys())

    if not targets:
        with tab1:
            st.warning("Scanner paused: No symbols found.")
        return

    # throttling
    last = st.session_state["last_scan"]
    can_start_new = True
    if last is not None:
        elapsed = (now_scan - last["time"]).total_seconds()
        if elapsed < MIN_SCAN_GAP_SECONDS:
            can_start_new = False

    do_scan = (
        (last is None)
        or (can_start_new and not st.session_state["scan_in_progress"])
    )

    # --- PERFORM SCAN ONLY WHEN ALLOWED ---
    if do_scan:
        st.session_state["scan_in_progress"] = True
        try:
            scan_to = now_scan.strftime("%Y-%m-%d")
            scan_from = (now_scan - timedelta(days=5)).strftime("%Y-%m-%d")
            today = now_scan.date()

            # --- INDEX SUMMARY (Spot + FUTIDX tech + OI) ---
            index_rows = []
            for key, info in INDEX_MAP.items():
                spot_id = info["id"]
                name = info["name"]

                prev_close_idx = get_prev_close_index(spot_id)
                ltp_idx = get_live_price(spot_id)
                if prev_close_idx > 0 and ltp_idx > 0:
                    day_pct = round(((ltp_idx - prev_close_idx) / prev_close_idx) * 100, 2)
                else:
                    day_pct = 0.0

                fut_id = INDEX_FUT_MAP.get(key)

                fut_ltp = 0.0
                mom = 0.0
                p_chg = 0.0
                rsi_val = 0.0
                adx_val = 0.0
                vol_ratio = 1.0
                oi_chg = 0.0
                oi_signal = "No OI Data ❔"
                analysis = "Neutral ⚪"
                bias = "Neutral"

                if fut_id:
                    df_idx = fetch_intraday_v2_futidx(
                        fut_id, scan_from, scan_to, interval_min=60
                    )
                    if not df_idx.empty:
                        if len(df_idx) >= 14:
                            df_idx["RSI"] = ta.rsi(df_idx["Close"], 14)
                            adx_df = ta.adx(
                                df_idx["High"], df_idx["Low"], df_idx["Close"], 14
                            )
                            df_idx["ADX"] = adx_df["ADX_14"]
                            rsi_val = float(df_idx["RSI"].iloc[-1])
                            adx_val = float(df_idx["ADX"].iloc[-1])

                        if len(df_idx) >= 5:
                            df_idx["EMA"] = ta.ema(df_idx["Close"], 5)
                            mom = round(
                                (
                                    (df_idx["Close"].iloc[-1] - df_idx["EMA"].iloc[-1])
                                    / df_idx["EMA"].iloc[-1]
                                )
                                * 100,
                                2,
                            )

                        curr_vol = float(df_idx["Volume"].iloc[-1])
                        avg_vol = (
                            df_idx["Volume"].rolling(10).mean().iloc[-1]
                            if len(df_idx) > 10
                            else curr_vol
                        )
                        vol_ratio = (curr_vol / avg_vol) if avg_vol > 0 else 1.0

                        curr = df_idx.iloc[-1]
                        fut_ltp = float(curr["Close"])

                        if len(df_idx) > 1:
                            prev_bar = df_idx.iloc[-2]
                            p_chg = round(
                                ((fut_ltp - prev_bar["Close"]) / prev_bar["Close"]) * 100,
                                2,
                            )

                        oi_available = not (
                            df_idx["OI"].max() == 0 and df_idx["OI"].min() == 0
                        )
                        if oi_available:
                            day_df = df_idx[df_idx["datetime"].dt.date == today]
                            if len(day_df) >= 2:
                                d_first = day_df.iloc[0]
                                d_last = day_df.iloc[-1]
                                oi_start = float(d_first.get("OI", 0) or 0)
                                oi_end = float(d_last.get("OI", 0) or 0)
                                if oi_start > 0:
                                    oi_chg = round(
                                        ((oi_end - oi_start) / oi_start) * 100, 2
                                    )
                                else:
                                    oi_chg = 0.0

                            oi_signal = get_oi_signal(oi_chg, day_pct)
                        else:
                            oi_signal = "No OI Data ❔"

                        analysis = get_trend_analysis(p_chg, vol_ratio)

                        if "Buildup" in oi_signal:
                            if day_pct > 0.3:
                                bias = "Bull"
                            elif day_pct < -0.3:
                                bias = "Bear"

                index_rows.append(
                    {
                        "Index": name,
                        "LTP": round(fut_ltp or ltp_idx, 2),
                        "Mom %": mom,
                        "Price Chg%": p_chg,
                        "Day Price%": day_pct,
                        "RSI": round(rsi_val, 1),
                        "ADX": round(adx_val, 1),
                        "Vol Ratio": round(vol_ratio, 1),
                        "OI Chg%": oi_chg,
                        "OI Signal": oi_signal,
                        "Analysis": analysis,
                        "Bias": bias,
                    }
                )

            # optional OI debug
            if DEBUG_SHOW_ERRORS:
                try:
                    nfut_id = INDEX_FUT_MAP.get("NIFTY")
                    if nfut_id:
                        df_n = fetch_intraday_v2_futidx(
                            nfut_id, scan_from, scan_to, interval_min=60
                        )
                        if not df_n.empty:
                            st.write(
                                "NIFTY FUT OI (last 10 bars):",
                                df_n[["datetime", "OI"]].tail(10),
                            )

                    sample_sym = next(iter(FNO_MAP.keys()))
                    sfut_id = FNO_MAP[sample_sym]["id"]
                    df_s = fetch_intraday_v2_futstk(
                        sfut_id, scan_from, scan_to, interval_min=60
                    )
                    if not df_s.empty:
                        st.write(
                            f"{sample_sym} FUT OI (last 10 bars):",
                            df_s[["datetime", "OI"]].tail(10),
                        )
                except Exception as e:
                    st.error(f"Debug OI check failed: {e}")

            bar = st.progress(0)
            bull, bear, all_data = [], [], []

            for i, sym in enumerate(targets):
                try:
                    sid = FNO_MAP[sym]["id"]

                    df = fetch_intraday_v2_futstk(
                        sid, scan_from, scan_to, interval_min=60
                    )
                    if df.empty:
                        bar.progress((i + 1) / len(targets))
                        continue

                    if len(df) >= 14:
                        df["RSI"] = ta.rsi(df["Close"], 14)
                        adx_df = ta.adx(df["High"], df["Low"], df["Close"], 14)
                        df["ADX"] = adx_df["ADX_14"]
                        curr_rsi = float(df["RSI"].iloc[-1])
                        curr_adx = float(df["ADX"].iloc[-1])
                    else:
                        curr_rsi = 0.0
                        curr_adx = 0.0

                    if len(df) >= 5:
                        df["EMA"] = ta.ema(df["Close"], 5)
                        mom = round(
                            ((df["Close"].iloc[-1] - df["EMA"].iloc[-1])
                             / df["EMA"].iloc[-1]) * 100,
                            2,
                        )
                    else:
                        mom = 0.0

                    curr_vol = float(df["Volume"].iloc[-1])
                    avg_vol = (
                        df["Volume"].rolling(10).mean().iloc[-1]
                        if len(df) > 10
                        else curr_vol
                    )
                    vol_ratio = (curr_vol / avg_vol) if avg_vol > 0 else 1.0

                    curr = df.iloc[-1]
                    ltp = float(curr["Close"])

                    if len(df) > 1:
                        prev = df.iloc[-2]
                        p_chg = round(
                            ((ltp - prev["Close"]) / prev["Close"]) * 100,
                            2,
                        )
                    else:
                        p_chg = 0.0

                    prev_close = get_prev_close_futstk(sid)
                    if prev_close > 0:
                        day_price_chg = round(((ltp - prev_close) / prev_close) * 100, 2)
                    else:
                        day_price_chg = 0.0

                    oi_available = not (df["OI"].max() == 0 and df["OI"].min() == 0)

                    if oi_available:
                        day_df = df[df["datetime"].dt.date == today]
                        if len(day_df) >= 2:
                            day_first = day_df.iloc[0]
                            day_last = day_df.iloc[-1]
                            oi_start = float(day_first.get("OI", 0) or 0)
                            oi_end = float(day_last.get("OI", 0) or 0)
                            if oi_start > 0:
                                oi_chg = round(
                                    ((oi_end - oi_start) / oi_start) * 100, 2
                                )
                            else:
                                oi_chg = 0.0
                        else:
                            oi_chg = 0.0

                        oi_signal = get_oi_signal(oi_chg, day_price_chg)
                    else:
                        oi_chg = 0.0
                        oi_signal = "No OI Data ❔"

                    intraday_sent = get_trend_analysis(p_chg, vol_ratio)

                    row = {
                        "Sym": sym,
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

                    if oi_available and "Buildup" in oi_signal:
                        if day_price_chg > 0 and p_chg > 0:
                            side = "bull"
                            update_signal_history(side, sym, now_scan)
                            strength_min = get_strength_minutes(side, sym, now_scan)
                            conv, t_s, p_s, s_s = compute_conviction(
                                side,
                                curr_rsi,
                                curr_adx,
                                mom,
                                vol_ratio,
                                oi_chg,
                                oi_signal,
                                strength_min,
                                day_price_chg,
                                p_chg,
                            )
                            bull_row = row.copy()
                            bull_row["Strength (min)"] = strength_min
                            bull_row["TrendScore"] = t_s
                            bull_row["PartScore"] = p_s
                            bull_row["PersistScore"] = s_s
                            bull_row["Conviction"] = conv
                            bull.append(bull_row)

                        if day_price_chg < 0 and p_chg < 0:
                            side = "bear"
                            update_signal_history(side, sym, now_scan)
                            strength_min = get_strength_minutes(side, sym, now_scan)
                            conv, t_s, p_s, s_s = compute_conviction(
                                side,
                                curr_rsi,
                                curr_adx,
                                mom,
                                vol_ratio,
                                oi_chg,
                                oi_signal,
                                strength_min,
                                day_price_chg,
                                p_chg,
                            )
                            bear_row = row.copy()
                            bear_row["Strength (min)"] = strength_min
                            bear_row["TrendScore"] = t_s
                            bear_row["PartScore"] = p_s
                            bear_row["PersistScore"] = s_s
                            bear_row["Conviction"] = conv
                            bear.append(bear_row)

                    else:
                        if curr_rsi > 0:
                            if p_chg > 0.3 and curr_rsi > 55 and vol_ratio > 1.1:
                                side = "bull"
                                update_signal_history(side, sym, now_scan)
                                strength_min = get_strength_minutes(side, sym, now_scan)
                                conv, t_s, p_s, s_s = compute_conviction(
                                    side,
                                    curr_rsi,
                                    curr_adx,
                                    mom,
                                    vol_ratio,
                                    oi_chg,
                                    oi_signal,
                                    strength_min,
                                    day_price_chg,
                                    p_chg,
                                )
                                bull_row = row.copy()
                                bull_row["Strength (min)"] = strength_min
                                bull_row["TrendScore"] = t_s
                                bull_row["PartScore"] = p_s
                                bull_row["PersistScore"] = s_s
                                bull_row["Conviction"] = conv
                                bull.append(bull_row)

                            elif p_chg < -0.3 and curr_rsi < 52 and vol_ratio > 1.1:
                                side = "bear"
                                update_signal_history(side, sym, now_scan)
                                strength_min = get_strength_minutes(side, sym, now_scan)
                                conv, t_s, p_s, s_s = compute_conviction(
                                    side,
                                    curr_rsi,
                                    curr_adx,
                                    mom,
                                    vol_ratio,
                                    oi_chg,
                                    oi_signal,
                                    strength_min,
                                    day_price_chg,
                                    p_chg,
                                )
                                bear_row = row.copy()
                                bear_row["Strength (min)"] = strength_min
                                bear_row["TrendScore"] = t_s
                                bear_row["PartScore"] = p_s
                                bear_row["PersistScore"] = s_s
                                bear_row["Conviction"] = conv
                                bear.append(bear_row)

                except Exception as e:
                    if DEBUG_SHOW_ERRORS and "scan_error_shown" not in st.session_state:
                        st.session_state["scan_error_shown"] = True
                        st.error(f"Error while scanning {sym}: {e}")

                time.sleep(0.12)
                bar.progress((i + 1) / len(targets))

            bar.empty()

            st.session_state["last_scan"] = {
                "time": now_scan,
                "index_rows": index_rows,
                "bull": bull,
                "bear": bear,
                "all_data": all_data,
            }
        finally:
            st.session_state["scan_in_progress"] = False

    # --- RENDER FROM CACHED RESULTS ---
    last = st.session_state["last_scan"]
    if last is None:
        with tab1:
            st.info("Initial scan is running... please wait.")
        return

    index_rows = last["index_rows"]
    bull = last["bull"]
    bear = last["bear"]
    all_data = last["all_data"]
    last_time = last["time"]

    elapsed = (now_scan - last_time).total_seconds()
    remaining = max(0, MIN_SCAN_GAP_SECONDS - int(elapsed))
    if remaining > 0:
        st.caption(
            f"Next scan in ~{remaining}s (last scan at {last_time.strftime('%H:%M:%S')} IST)"
        )

    cfg = {
        "Symbol": st.column_config.LinkColumn(
            "Script", display_text="symbol=NSE:(.*)", width="medium"
        ),
        "LTP": st.column_config.NumberColumn("LTP", format="%.2f"),
        "Mom %": st.column_config.NumberColumn("Mom%", format="%.2f%%"),
        "Price Chg%": st.column_config.NumberColumn(
            "Chg% (Last bar)", format="%.2f%%"
        ),
        "Day Price%": st.column_config.NumberColumn(
            "Chg% (vs Prev Close)", format="%.2f%%"
        ),
        "RSI": st.column_config.NumberColumn("RSI", format="%.1f"),
        "ADX": st.column_config.NumberColumn("ADX", format="%.1f"),
        "Vol Ratio": st.column_config.NumberColumn("Vol x", format="%.1fx"),
        "OI Chg%": st.column_config.NumberColumn("OI Chg%", format="%.2f%%"),
        "Strength (min)": st.column_config.NumberColumn(
            "Strength (min)", format="%.1f"
        ),
        "TrendScore": st.column_config.NumberColumn("Trend", format="%.0f"),
        "PartScore": st.column_config.NumberColumn("Part", format="%.0f"),
        "PersistScore": st.column_config.NumberColumn("Persist", format="%.0f"),
        "Conviction": st.column_config.NumberColumn("Conviction", format="%.0f"),
        "OI Signal": st.column_config.TextColumn("OI Signal", width="medium"),
        "Analysis": st.column_config.TextColumn("Analysis", width="medium"),
    }

    with tab1:
        st.subheader("Indices")
        if index_rows:
            df_idx = pd.DataFrame(index_rows)
            cols = [
                "Index", "LTP", "Mom %", "Price Chg%", "Day Price%",
                "RSI", "ADX", "Vol Ratio", "OI Chg%", "OI Signal",
                "Analysis", "Bias",
            ]
            df_idx = df_idx[[c for c in cols if c in df_idx.columns]]
            st.dataframe(
                df_idx,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No index data available.")

        st.markdown("---")

        st.success(f"🟢 BULLS ({len(bull)}) – Ranked by Conviction")
        if bull:
            df_bull = pd.DataFrame(bull).drop(columns=["Sym"], errors="ignore")
            if "Conviction" in df_bull.columns:
                df_bull = df_bull.sort_values("Conviction", ascending=False)
            st.dataframe(
                df_bull.head(20),
                use_container_width=True,
                hide_index=True,
                column_config=cfg,
            )
        else:
            st.info("No bullish setups as per current criteria.")

        st.markdown("---")

        st.error(f"🔴 BEARS ({len(bear)}) – Ranked by Conviction")
        if bear:
            df_bear = pd.DataFrame(bear).drop(columns=["Sym"], errors="ignore")
            if "Conviction" in df_bear.columns:
                df_bear = df_bear.sort_values("Conviction", ascending=False)
            st.dataframe(
                df_bear.head(20),
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
            st.warning("No data found (likely no intraday candles returned by v2 API).")

    st.write(f"🕒 **Last Data Sync:** {last_time.strftime('%H:%M:%S')} IST")
    st.markdown(
        "<div style='text-align: center; color: grey;'>Powered by : i-Tech World</div>",
        unsafe_allow_html=True,
    )

# --- 14. RUN APP ---
if dhan:
    refreshable_dashboard()
    refreshable_scanner()
