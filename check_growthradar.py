import os, requests, random, re, json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import redis

# =========================
# CONFIG
# =========================
REDIS_URL = os.environ.get("REDIS_URL")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

r = redis.from_url(REDIS_URL, decode_responses=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = set()

    try:
        url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
        res = requests.get(url, timeout=10)
        lines = res.text.splitlines()[1:]

        for l in lines:
            s = l.split(",")[0].strip().upper()
            if re.match(r"^[A-Z]{1,6}$", s):
                symbols.add(s)
    except:
        pass

    fallback = [
        "AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL","TSLA",
        "INTC","QCOM","AVGO","TSM","ASML","MU","PLTR","SNOW","CRWD"
    ]

    symbols.update(fallback)
    symbols = list(symbols)
    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]

# =========================
# REDIS STATE
# =========================
def get_state(ticker):
    raw = r.get(f"gr:{ticker}:state")
    return json.loads(raw) if raw else {}

def set_state(ticker, state):
    r.set(f"gr:{ticker}:state", json.dumps(state))

# =========================
# FEATURE ENGINE
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
        res = session.get(url, timeout=5)
        if res.status_code != 200:
            return None

        data = res.json()["chart"]["result"][0]
        close = data["indicators"]["quote"][0]["close"]
        volume = data["indicators"]["quote"][0]["volume"]

        close = [x for x in close if x]
        volume = [x for x in volume if x]

        if len(close) < 60:
            return None

        price = close[-1]
        if price < MIN_PRICE:
            return None

        vol_base = np.mean(volume[-20:-5])
        if vol_base < MIN_VOL:
            return None

        def ret(a,b): return (a/b - 1) if b else 0

        m1 = ret(close[-1], close[-21])
        m3 = ret(close[-1], close[-63])

        vol_ratio = volume[-1] / (vol_base + 1e-9)

        return {
            "ticker": ticker,
            "price": price,
            "m1": m1,
            "m3": m3,
            "vol_ratio": vol_ratio,
            "close": close[-1],
            "volume": volume[-1]
        }

    except:
        return None

# =========================
# STATE MACHINE
# =========================
def transition_engine(t, feat):
    state = get_state(t)

    prev_close = state.get("last_close")
    prev_vol = state.get("last_volume")
    prev_state = state.get("last_state", "NONE")

    breakout_flag = False

    # =========================
    # BREAKOUT (EVENT ONLY)
    # =========================
    if prev_close and prev_vol:
        price_jump = abs(feat["close"] - prev_close) / prev_close
        vol_spike = feat["volume"] / (prev_vol + 1e-9)

        breakout_flag = (price_jump > 0.03 and vol_spike > 2.2)

    # =========================
    # STATE TRANSITIONS
    # =========================
    new_state = prev_state

    if breakout_flag:
        new_state = "BREAKOUT"

    elif prev_state == "BREAKOUT":
        new_state = "EARLY"

    elif feat["m1"] > 0.45 and feat["m3"] > 0.45:
        new_state = "TRANSITION"

    elif feat["m3"] > 1.0:
        new_state = "CONT"

    # =========================
    # UPDATE REDIS
    # =========================
    new_state_obj = {
        "last_close": feat["close"],
        "last_volume": feat["volume"],
        "last_state": new_state,
        "last_update": datetime.now().isoformat()
    }

    set_state(t, new_state_obj)

    return {
        "ticker": t,
        "state": new_state,
        "m1": feat["m1"],
        "m3": feat["m3"],
        "vol_ratio": feat["vol_ratio"],
        "breakout": breakout_flag
    }

# =========================
# DIAMOND SELECTION
# =========================
def build_diamond(df):
    trans = [x for x in df if x["state"] == "TRANSITION"]

    trans = sorted(trans, key=lambda x: x["m1"], reverse=True)

    return trans[:5]

# =========================
# RUN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    universe = load_universe()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futs):
            r = f.result()
            if r:
                out = transition_engine(r["ticker"], r)
                results.append(out)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    diamond = build_diamond(results)

    msg = [
        "🚀 GrowthRadar v38 (STATE TRANSITION ENGINE)",
        f"Scan:{len(universe)} Valid:{len(results)}",
        f"Time:{now}",
        "",
        "💎 BUY SIGNAL"
    ]

    if not diamond:
        msg.append("None")
    else:
        for d in diamond:
            msg.append(f"**{d['ticker']}** M1:{d['m1']:.2f}")

    early = [x for x in results if x["state"] == "EARLY"][:4]
    msg.append("\n🔥 EARLY")
    msg += [f"{x['ticker']}" for x in early] or ["None"]

    trans = [x for x in results if x["state"] == "TRANSITION"][:4]
    msg.append("\n⚡ TRANSITION")
    msg += [f"{x['ticker']}" for x in trans] or ["None"]

    cont = [x for x in results if x["state"] == "CONT"][:4]
    msg.append("\n🔁 CONT")
    msg += [f"{x['ticker']}" for x in cont] or ["None"]

    breakout = [x for x in results if x["breakout"]][:4]
    msg.append("\n🧨 BREAKOUT (event log)")
    msg += [f"{x['ticker']}" for x in breakout] or ["None"]

    # ★必ず空行
    msg.append("")

    text = "\n".join(msg)

    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
