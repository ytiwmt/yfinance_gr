import os, requests, random, re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import redis

WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
REDIS_URL = os.environ.get("REDIS_URL")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# REDIS
# =========================
r = None
if REDIS_URL:
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print("🟢 Redis: CONNECTED")
    except:
        print("🔴 Redis: FAILED")
        r = None
else:
    print("⚠️ Redis: NOT SET")

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = set()

    try:
        url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
        data = requests.get(url, timeout=10).text.splitlines()[1:]

        for l in data:
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
# FETCH
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
        res = session.get(url, timeout=5)

        if res.status_code != 200:
            return None

        data = res.json()["chart"]["result"][0]

        close = [x for x in data["indicators"]["quote"][0]["close"] if x is not None]
        volume = [x for x in data["indicators"]["quote"][0]["volume"] if x is not None]

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

        # =========================
        # PHASE
        # =========================
        phase = "NONE"

        if (0.25 < m1 < 0.7 and m3 < 0.6):
            phase = "EARLY"
        elif (m1 > 0.45 and m3 > 0.45):
            phase = "TRANSITION"
        elif (m3 > 1.0):
            phase = "CONT"

        # =========================
        # BREAKOUT
        # =========================
        price_jump = abs(close[-1] - close[-2]) / close[-2]
        vol_spike = volume[-1] / (vol_base + 1e-9)

        trend_ok = close[-1] > close[-2] > close[-3]

        breakout = (
            (price_jump > 0.02 and vol_spike > 1.8) or
            (price_jump > 0.015 and vol_spike > 2.3)
        ) and trend_ok

        # =========================
        # SCORE BASE
        # =========================
        raw_score = (
            m1 * 0.6 +
            m3 * 0.3 +
            vol_ratio * 0.1 +
            breakout * 0.4
        )

        score = 8.5 * (1 - np.exp(-raw_score / 8.5))

        # =========================
        # REDIS DELTA (③)
        # =========================
        delta = 0.0
        if r:
            prev = r.get(f"score:{ticker}")
            if prev is not None:
                delta = score - float(prev)

            r.set(f"score:{ticker}", score, ex=3600)

            # =========================
            # PHASE MEMORY (②)
            # =========================
            r.set(f"phase:{ticker}", phase, ex=86400)

        return {
            "ticker": ticker,
            "phase": phase,
            "score": float(score),
            "delta": float(delta),
            "m1": float(m1),
            "m3": float(m3),
            "vol_ratio": float(vol_ratio),
            "breakout": bool(breakout)
        }

    except:
        return None

# =========================
# DIAMOND
# =========================
def build_diamond(df):
    t = df[df.phase == "TRANSITION"].copy()
    if len(t) == 0:
        return []

    t = t.sort_values("score", ascending=False)

    out = []
    prev = None

    for _, r in t.iterrows():
        gap = 0 if prev is None else prev.score - r.score

        if prev is None or gap >= 0.15 or r.score > t.score.quantile(0.85):
            out.append({
                "ticker": r.ticker,
                "score": r.score,
                "gap": gap
            })

        prev = r
        if len(out) >= 5:
            break

    return out

# =========================
# BUY (delta integrated)
# =========================
def build_buy(df):
    d = df.copy()

    structure = (
        (d["phase"] == "TRANSITION") * 0.9 +
        (d["phase"] == "CONT") * 0.6 +
        (d["phase"] == "EARLY") * 0.15
    )

    structure = np.minimum(structure, 1.1)

    # ★ ③ deltaをここで活用
    momentum_bonus = np.clip(d["delta"], 0, 2.0)

    score = (
        d["score"] +
        structure * 0.6 +
        momentum_bonus * 0.8
    )

    d["buy_score"] = score

    return d.sort_values("buy_score", ascending=False).head(5).to_dict("records")

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
                results.append(r)

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    buy = build_buy(df)
    diamond = build_diamond(df)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    print(f"🚀 GrowthRadar v37.17 (REDIS DELTA MODEL)")
    print(f"Scan:{len(universe)} Valid:{len(df)}")
    print(f"Time:{now}\n")

    print("💎 BUY SIGNAL")
    for b in buy:
        print(f"**{b['ticker']}** S:{b['buy_score']:.2f}")

    print("\n🔥 EARLY")
    early = df[df.phase=="EARLY"].head(4)
    print("\n".join([f"{r.ticker} S:{r.score:.2f}" for _, r in early.iterrows()]) or "None")

    print("\n⚡ TRANSITION")
    trans = df[df.phase=="TRANSITION"].head(4)
    print("\n".join([f"{r.ticker} S:{r.score:.2f}" for _, r in trans.iterrows()]) or "None")

    print("\n🔁 CONT")
    cont = df[df.phase=="CONT"].head(4)
    print("\n".join([f"{r.ticker} S:{r.score:.2f}" for _, r in cont.iterrows()]) or "None")

    print("\n🧨 BREAKOUT (event)")
    brk = df[df.breakout].head(4)
    print("\n".join([f"{r.ticker}" for _, r in brk.iterrows()]) or "None")

if __name__ == "__main__":
    run()
