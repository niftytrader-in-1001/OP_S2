import os
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
import pyotp
import sys
import requests
import zipfile
import io
import traceback
import logging
import concurrent.futures
from threading import Lock
import numpy as np
import tempfile
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
# LOGGING (reduced noise for GitHub Actions)
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# =========================================================
# ENVIRONMENT VARIABLES (GitHub Secrets)
# =========================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PIN = os.getenv("ANGEL_PIN")
ANGEL_TOTP = os.getenv("ANGEL_TOTP")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Replace lines 40-52 with:
logger.info(
    "Angel ENV present: %s",
    {
        "API_KEY": bool(ANGEL_API_KEY),
        "CLIENT_ID": bool(ANGEL_CLIENT_ID),
        "PIN": bool(ANGEL_PIN),
        "TOTP": bool(ANGEL_TOTP),
        "TG_TOKEN": bool(TELEGRAM_BOT_TOKEN),
        "TG_CHAT": bool(TELEGRAM_CHAT_ID),
    }
)

# Allow script to continue even if TOTP is missing for debugging
if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN]):
    raise RuntimeError("❌ Missing critical Angel One credentials")

# Only warn about TOTP, don't exit
if not ANGEL_TOTP:
    logger.warning("⚠️ ANGEL_TOTP is missing - login may fail")
# =========================================================
# SMART API IMPORT (GitHub compatible)
# =========================================================
sys.path.append(os.getcwd())   # <-- CRITICAL FIX
from SmartApi.smartConnect import SmartConnect

# =========================================================
# CONFIG
# =========================================================
API_SLEEP = 1.0
MAX_RETRIES = 3
MAX_WORKERS = 3
IST = timezone(timedelta(hours=5, minutes=30))

WEEKS_FOR_RANGE = 4
SENSEX_TOKEN = "99919000"
SENSEX_STRIKE_MULTIPLE = 100

# =========================================================
# THREAD SAFE GLOBALS
# =========================================================
success_list = []
failed_list = []
failed_details = []
zip_lock = Lock()
counter_lock = Lock()
processed_counter = 0
total_symbols = 0

# =========================================================
# UTILS
# =========================================================
def round_to_nearest_100(price):
    return round(price / 100) * 100


# =========================================================
# SENSEX DATA
# =========================================================
def get_SENSEX_historical_data(smart_api, weeks=4):
    try:
        to_date = datetime.now(IST)
        from_date = to_date - timedelta(weeks=weeks)

        params = {
            "exchange": "BSE",
            "symboltoken": SENSEX_TOKEN,
            "interval": "ONE_DAY",
            "fromdate": from_date.strftime("%Y-%m-%d 09:15"),
            "todate": to_date.strftime("%Y-%m-%d %H:%M"),
        }

        resp = smart_api.getCandleData(params)

        if resp and resp.get("status") and resp.get("data"):
            df = pd.DataFrame(
                resp["data"],
                columns=["Date", "Open", "High", "Low", "Close", "Volume"]
            )
            df["Date"] = pd.to_datetime(df["Date"])
            return {
                "min_low": df["Low"].min(),
                "max_high": df["High"].max(),
                "current_close": df["Close"].iloc[-1]
            }

    except Exception as e:
        logger.error(f"SENSEX historical error: {e}")

    return None


def get_SENSEX_ltp(smart_api):
    try:
        params = {
            "exchange": "BSE",
            "symboltoken": SENSEX_TOKEN,
            "interval": "ONE_MINUTE",
            "fromdate": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 09:15"),
            "todate": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        resp = smart_api.getCandleData(params)
        if resp and resp.get("status") and resp.get("data"):
            return resp["data"][-1][4]
    except:
        pass
    return None


def calculate_strike_range(smart_api, buffer=1000):
    hist = get_SENSEX_historical_data(smart_api, WEEKS_FOR_RANGE)
    if not hist:
        logger.error("❌ Historical data unavailable, aborting safely")
        sys.exit(1)

    start = round_to_nearest_100(hist["min_low"] - buffer)
    end = round_to_nearest_100(hist["max_high"] + buffer)
    return max(0, start), end


# =========================================================
# SYMBOL MASTER
# =========================================================
def load_symbol_master():
    url = "https://api.shoonya.com/BFO_symbols.txt.zip"
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open(z.namelist()[0]) as f:
            content = "\n".join(
                line.rstrip(",") for line in f.read().decode().splitlines()
            )
            return pd.read_csv(io.StringIO(content))


def is_today_SENSEX_expiry(df):
    today = datetime.now(IST).date()
    df = df[(df["Symbol"] == "BSXOPT") & (df["Instrument"] == "OPTIDX")].copy()
    df["ExpiryDate"] = pd.to_datetime(df["Expiry"], format="%d-%b-%Y").dt.date
    return (today in df["ExpiryDate"].values), today


def get_option_symbols(df, expiry_date, start, end):
    expiry = expiry_date.strftime("%d-%b-%Y").upper()
    df = df[
        (df["Symbol"] == "BSXOPT") &
        (df["Instrument"] == "OPTIDX") &
        (df["Expiry"] == expiry)
    ].copy()

    df["StrikePrice"] = pd.to_numeric(df["StrikePrice"], errors="coerce")

    return df[
        (df["StrikePrice"] >= start) &
        (df["StrikePrice"] <= end) &
        (df["StrikePrice"] % 100 == 0)
    ]


# =========================================================
# TELEGRAM UPLOAD (UNCHANGED LOGIC)
# =========================================================
def send_zip_to_telegram(zip_bytes, name):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(zip_bytes)
        path = tmp.name

    try:
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429,500,502,503])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        with open(path, "rb") as f:
            r = session.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID},
                files={"document": (name, f)},
                timeout=(30, 600),
            )
        r.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Telegram error: {e}")
        return False
    finally:
        os.remove(path)


# =========================================================
# CANDLE DOWNLOAD
# =========================================================
def get_candles_with_retry(smart, params):
    for i in range(MAX_RETRIES):
        try:
            r = smart.getCandleData(params)
            if r and r.get("status"):
                return r
            time.sleep((i + 1) * 5)
        except:
            time.sleep((i + 1) * 5)
    return None


def download_symbol(args):
    smart, row, FROM, TO = args
    symbol = row["TradingSymbol"]
    token = str(row["Token"])

    params = {
        "exchange": "BFO",
        "symboltoken": token,
        "interval": "ONE_MINUTE",
        "fromdate": FROM,
        "todate": TO,
    }

    r = get_candles_with_retry(smart, params)
    if r and r.get("data"):
        df = pd.DataFrame(
            r["data"],
            columns=["Date","Open","High","Low","Close","Volume"]
        )
        df["Date"] = pd.to_datetime(df["Date"])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        return symbol, buf.getvalue(), None

    return symbol, None, "No data"


# =========================================================
# MAIN
# =========================================================
def main():
    smart = SmartConnect(api_key=ANGEL_API_KEY)
    totp = pyotp.TOTP(ANGEL_TOTP).now()
    login = smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PIN, totp)
    if not login or not login.get("status"):
        raise RuntimeError("Login failed")

    df_master = load_symbol_master()
    is_expiry, expiry = is_today_SENSEX_expiry(df_master)
    if not is_expiry:
        logger.info("Not SENSEX expiry day. Exiting.")
        sys.exit(0)

    start, end = calculate_strike_range(smart)
    df = get_option_symbols(df_master, expiry, start, end)

    FROM = (expiry - timedelta(days=90)).strftime("%Y-%m-%d 09:15")
    TO = expiry.strftime("%Y-%m-%d 15:30")

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        pass

    args = [(smart, r, FROM, TO) for _, r in df.iterrows()]

    with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as ex:
        for symbol, data, err in ex.map(download_symbol, args):
            if data:
                with zipfile.ZipFile(zip_buf, "a") as zf:
                    zf.writestr(f"{symbol}.xlsx", data)
                success_list.append(symbol)
            else:
                failed_list.append(symbol)
                failed_details.append((symbol, err))

    if success_list:
        zip_buf.seek(0)
        send_zip_to_telegram(
            zip_buf.read(),
            f"SENSEX_expiry_{expiry.strftime('%d%m%y')}_1min.zip"
        )

    logger.info("Script completed cleanly")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)
