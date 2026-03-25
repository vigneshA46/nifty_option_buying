import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta, time
from io import StringIO
from dotenv import load_dotenv
import os

# =========================
# CONFIG
# =========================

load_dotenv()

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

HEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN
}

IDX_INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
FNO_MASTER_URL   = "https://api.dhan.co/v2/instrument/NSE_FNO"

IST = pytz.timezone("Asia/Kolkata")

TRADE_DATE = datetime.now(IST).strftime("%Y-%m-%d")
TRADE_START = time(9, 14)
TRADE_END   = time(15, 20)

INTERVAL = "1"
NIFTY_INDEX_SECURITY_ID = "13"
TARGET_SYMBOL = "NIFTY"

# =========================
# HELPERS
# =========================

def fetch_single_candle(security_id, segment, instrument, from_dt, to_dt):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": segment,
        "instrument": instrument,
        "interval": INTERVAL,
        "fromDate": from_dt,
        "toDate": to_dt
    }

    r = requests.post(IDX_INTRADAY_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    if not data.get("open"):
        return None

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)

    return df.iloc[-1]


def calculate_atm(price, step=50):
    return round(price / step) * step


def load_fno_master():
    r = requests.get(FNO_MASTER_URL, headers={"access-token": ACCESS_TOKEN})
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text), header=None, low_memory=False)

    df.columns = [
        "EXCH_ID","SEGMENT","SECURITY_ID","ISIN","INSTRUMENT",
        "UNDERLYING_SECURITY_ID","UNDERLYING_SYMBOL","SYMBOL_NAME",
        "DISPLAY_NAME","INSTRUMENT_TYPE","SERIES","LOT_SIZE",
        "SM_EXPIRY_DATE","STRIKE_PRICE","OPTION_TYPE","TICK_SIZE",
        "EXPIRY_FLAG","BRACKET_FLAG","COVER_FLAG","ASM_GSM_FLAG",
        "ASM_GSM_CATEGORY","BUY_SELL_INDICATOR",
        "BUY_CO_MIN_MARGIN_PER","BUY_CO_SL_RANGE_MAX_PERC",
        "BUY_CO_SL_RANGE_MIN_PERC","BUY_BO_MIN_MARGIN_PER",
        "BUY_BO_PROFIT_RANGE_MAX_PERC","BUY_BO_PROFIT_RANGE_MIN_PERC",
        "MTF_LEVERAGE","RESERVED"
    ]

    df["STRIKE_PRICE"] = pd.to_numeric(df["STRIKE_PRICE"], errors="coerce")
    df["SM_EXPIRY_DATE"] = pd.to_datetime(df["SM_EXPIRY_DATE"], errors="coerce")

    return df


def find_option_security(df, strike, option_type):
    trade_date = pd.to_datetime(TRADE_DATE)

    opt = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == TARGET_SYMBOL) &
        (df["STRIKE_PRICE"] == strike) &
        (df["OPTION_TYPE"] == option_type) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]


def wait_for_start():
    print("⏳ Waiting for 09:16:00 ...")
    while True:
        now = datetime.now(IST).time()
        if now >= time(9, 16):
            print("✅ Market Start Triggered")
            break
        time.sleep(1)


# =========================
# MAIN
# =========================

wait_for_start()

print("\n🚀 PAPER TRADE STARTED\n")

# ---- INDEX FIRST CANDLE ----

start_dt = f"{TRADE_DATE} 09:14:00"
end_dt   = f"{TRADE_DATE} 09:16:00"

print("index call")

idx_candle = fetch_single_candle(
    NIFTY_INDEX_SECURITY_ID,
    "IDX_I",
    "INDEX",
    start_dt,
    end_dt
)

print("ATM call")
atm = calculate_atm(idx_candle["close"])
print("📌 ATM :", atm)

# ---- OPTION SELECTION ----
print("security call")
fno_df = load_fno_master()

ce = find_option_security(fno_df, atm, "CE")
pe = find_option_security(fno_df, atm, "PE")

CE_ID = ce["SECURITY_ID"]
PE_ID = pe["SECURITY_ID"]

print("📌 CE ID :", CE_ID)
print("📌 PE ID :", PE_ID)

# ---- STATE ----

def init_state():
    return {
        "marked": None,
        "position": False,
        "pending_entry": False,
        "trading_disabled": False,   # 👈 ADD
        "entry_price": None,
        "entry_time": None,
        "lot": 1,
        "pnl": 0,
        "trades": []
    }

ce_state = init_state()
pe_state = init_state()

combined_pnl = 0

# ---- LOOP ----

current_dt = IST.localize(datetime.combine(datetime.strptime(TRADE_DATE, "%Y-%m-%d"), datetime.now(IST).time()))

def force_squareoff(state, candle, side):
    if state["position"]:
        exit_price = candle.close
        pnl = (exit_price - state["entry_price"]) * state["lot"]
        state["pnl"] += pnl

        trade = {
            "side": side,
            "entry_time": state["entry_time"],
            "entry_price": state["entry_price"],
            "exit_time": candle.datetime,
            "exit_price": exit_price,
            "lot": state["lot"],
            "pnl": round(pnl, 2),
            "reason": "TIME_EXIT"
        }

        state["trades"].append(trade)

        print(f"⏰ {side} TIME EXIT {state['lot']} @ {exit_price} PNL {round(pnl,2)}")

        state["position"] = False
        state["trading_disabled"] = True


while current_dt.time() <= TRADE_END:

    next_dt = current_dt + timedelta(minutes=2)

    from_dt = current_dt.strftime("%Y-%m-%d %H:%M:%S")
    to_dt   = next_dt.strftime("%Y-%m-%d %H:%M:%S")

    ce_candle = fetch_single_candle(CE_ID, "NSE_FNO", "OPTIDX", from_dt, to_dt)
    pe_candle = fetch_single_candle(PE_ID, "NSE_FNO", "OPTIDX", from_dt, to_dt)

    print(ce_candle)
    for candle, state, side in [
        (ce_candle, ce_state, "CE"),
        (pe_candle, pe_state, "PE")
    ]:

        if candle is None:
            continue

        avg = (candle.open + candle.high + candle.low + candle.close) / 4

        # MARK FIRST
        if state["marked"] is None:
            state["marked"] = candle.close
            print(f"{side} MARKED @ {state['marked']}")
            continue
        if current_dt.time() >= TRADE_END:
            if ce_candle is not None:
                force_squareoff(ce_state, ce_candle, "CE")
            if pe_candle is not None:
                force_squareoff(pe_state, pe_candle, "PE")
            break

        # EXECUTE PENDING ENTRY
        if state["pending_entry"] and not state["trading_disabled"]:
            state["entry_price"] = candle.open
            state["entry_time"] = candle.datetime
            state["position"] = True
            state["pending_entry"] = False

            print(f"🟢 {side} BUY {state['lot']} @ {state['entry_price']} {candle.datetime}")

        # EXIT
        if state["position"] and candle.close < state["marked"]:
            exit_price = candle.close
            pnl = (exit_price - state["entry_price"]) * state["lot"]
            state["pnl"] += pnl

            trade = {
                "side": side,
                "entry_time": state["entry_time"],
                "entry_price": state["entry_price"],
                "exit_time": candle.datetime,
                "exit_price": exit_price,
                "lot": state["lot"],
                "pnl": round(pnl, 2)
            }

            state["trades"].append(trade)

            print(f"🔴 {side} EXIT {state['lot']} @ {exit_price} PNL {round(pnl,2)}")

            state["lot"] += 1
            state["position"] = False
            state["trading_disabled"] = True     # 👈 ADD
            state["pending_entry"] = False      # 👈 SAFETY

        # ENTRY SIGNAL
        if (
            not state["position"]
            and not state["pending_entry"]
            and not state["trading_disabled"]   
            and candle.close > state["marked"]
            and avg > state["marked"]
            and avg < candle.close
        ):
            state["pending_entry"] = True

    combined_pnl = ce_state["pnl"] + pe_state["pnl"]

    if combined_pnl >= 50:
        print("\n🎯 COMBINED TARGET HIT\n")
        if ce_candle is not None:
            force_squareoff(ce_state, ce_candle, "CE")
        if pe_candle is not None:
            force_squareoff(pe_state, pe_candle, "PE")
        break

    current_dt = next_dt


# ---- SUMMARY ----

print("\n========== SUMMARY ==========")
print("CE PNL :", round(ce_state["pnl"], 2))
print("PE PNL :", round(pe_state["pnl"], 2))
print("TOTAL  :", round(combined_pnl, 2))
print("============================\n")
 