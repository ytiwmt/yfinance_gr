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

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# REDIS INIT
# =========================
r = None
REDIS_OK = False

def redis_healthcheck():
    global REDIS_OK
    try:
        r.ping()
        print("🟢 Redis: CONNECTED")
        REDIS_OK = True
    except Exception as e:
        print(f"🔴 Redis: FAILED ({e})")
        REDIS_OK = False

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
    if not REDIS_OK:
        return {}
    try:
        raw = r.get(f"gr:{ticker}:state")
        return json.loads(raw) if raw else {}
    except Exception as e:
        print(f"⚠️ Redis GET error {ticker}: {e}")
        return {}

def set_state(ticker, state):
    if not REDIS_OK:
        return
    try:
        r.set(f"gr:{ticker}:state", json.dumps(state))
    except Exception as e:
        print(f"⚠️ Redis SET error {ticker}: {e}")

# =========================
# FETCH
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

        close = [x for x in close if x is not None]
        volume = [x for x in volume if x is not None]

        if len(close) < 60 or len(volume) < 60:
            return None

        price = close[-1]
        if price < MIN_PRICE:
            return None

        vol_base = np.mean(volume[-20:-5])
        if np.isnan(vol_base) or vol_base <= 0:
            return None
        if vol_base < MIN_VOL:
            return None

        if volume[-1] == 0:
            return None

        def ret(a,b): return (a/b - 1) if b else 0

        m1 = ret(close[-1], close[-21])
        m3 = ret(close[-1], close[-63])

        vol_ratio = volume[-1] / (vol_base + 1e-9)

        # =========================
        # PHASE
        # =========================
        state = get_state(ticker)
        prev_state = state.get("last_state", "NONE")

        phase = "NONE"

        if (0.25 < m1 < 0.7 and m3 < 0.6):
            phase = "EARLY"
        elif (m1 > 0.45 and m3 > 0.45):
            phase = "TRANSITION"
        elif (m3 > 1.0):
            phase = "CONT"

        # =========================
        # BREAKOUT EVENT
        # =========================
        price_jump_1 = abs(close[-1] - close[-2]) / close[-2]
        price_jump_2 = abs(close[-2] - close[-3]) / close[-3]

        vol_spike_1 = volume[-1] / (vol_base + 1e-9)
        vol_spike_2 = volume[-2] / (vol_base + 1e-9)

        breakout_event = (
            price_jump_1 > 0.03 and
            price_jump_2 > 0.02 and
            vol_spike_1 > 2.0 and
            vol_spike_2 > 1.5
        )

        score = (
            m1 * 0.6 +
            m3 * 0.3 +
            vol_ratio * 0.1
        )

        # =========================
        # SAVE STATE (Redis)
        # =========================
        new_state = {
            "last_price": close[-1],
            "last_volume": volume[-1],
            "last_state": phase,
            "timestamp": datetime.now().isoformat()
        }

        set_state(ticker, new_state)

        return {
            "ticker": ticker,
            "phase": phase,
            "score": score,
            "m1": m1,
            "m3": m3,
            "vol_ratio": vol_ratio,
            "breakout": breakout_event
        }

    except Exception as e:
        print(f"fetch error {ticker}: {e}")
        return None

# =========================
# DIAMOND
# =========================
def build_diamond(df):
    trans = df[df.phase == "TRANSITION"].copy()

    if len(trans) == 0:
        return pd.DataFrame()

    trans = trans.sort_values("score", ascending=False)

    diamond = []
    prev = None

    for _, r in trans.iterrows():
        gap = 0.0 if prev is None else prev.score - r.score

        if prev is None or gap >= 0.15 or r.score > trans.score.quantile(0.85):
            diamond.append({
                "ticker": r.ticker,
                "score": r.score,
                "gap": gap
            })

        prev = r
        if len(diamond) >= 5:
            break

    return pd.DataFrame(diamond)

# =========================
# RUN
# =========================
def run():
    global r
    r = redis.from_url(REDIS_URL, decode_responses=True)

    redis_healthcheck()

    session = requests.Session()
    session.headers.update(HEADERS)

    universe = load_universe()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)
    diamond = build_diamond(df)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v37.9 (REDIS:{'ON' if REDIS_OK else 'OFF'})",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        "",
        "💎 BUY SIGNAL"
    ]

    if len(diamond) == 0:
        msg.append("None")
    else:
        for _, r in diamond.iterrows():
            msg.append(f"**{r.ticker}** S:{r.score:.2f} GAP:{r.gap:.2f}")

    early = df[df.phase=="EARLY"].head(4)
    msg.append("\n🔥 EARLY")
    msg += [f"{r.ticker}" for _, r in early.iterrows()] or ["None"]

    trans = df[df.phase=="TRANSITION"].head(4)
    msg.append("\n⚡ TRANSITION")
    msg += [f"{r.ticker}" for _, r in trans.iterrows()] or ["None"]

    cont = df[df.phase=="CONT"].head(4)
    msg.append("\n🔁 CONT")
    msg += [f"{r.ticker}" for _, r in cont.iterrows()] or ["None"]

    brk = df[df.breakout].head(4)
    msg.append("\n🧨 BREAKOUT (event)")
    msg += [f"{r.ticker}" for _, r in brk.iterrows()] or ["None"]

    msg.append("")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
