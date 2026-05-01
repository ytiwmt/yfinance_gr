import os
import requests
import random
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np
import redis

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
REDIS_URL = os.environ.get("REDIS_URL")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# REDIS INIT
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

        def ret(a,b):
            return (a/b - 1) if b else 0

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
        # BASE SCORE
        # =========================
        base_score = (
            m1 * 0.6 +
            m3 * 0.3 +
            vol_ratio * 0.1 +
            breakout * 0.4
        )

        # =========================
        # REDIS DELTA
        # =========================
        delta = 0.0
        phase_weight = 1.0

        if r:
            prev = r.get(f"score:{ticker}")
            if prev is not None:
                delta = base_score - float(prev)

            r.set(f"score:{ticker}", base_score, ex=3600)
            r.set(f"phase:{ticker}", phase, ex=86400)

        # =========================
        # PHASE WEIGHT
        # =========================
        if phase == "EARLY":
            phase_weight = 1.05
        elif phase == "TRANSITION":
            phase_weight = 1.15
        elif phase == "CONT":
            phase_weight = 0.95

        # =========================
        # FINAL SCORE
        # =========================
        score = (base_score + max(delta, 0) * 0.8) * phase_weight

        # DEBUG LOG（ローカルのみ）
        print(f"[REDIS] {ticker} score={score:.2f} delta={delta:.2f} phase={phase}")

        return {
            "ticker": ticker,
            "phase": phase,
            "score": float(score),
            "breakout": bool(breakout)
        }

    except:
        return None

# =========================
# BUY SIGNAL
# =========================
def build_buy(df):
    d = df.copy()

    structure = (
        (d["phase"] == "TRANSITION") * 0.9 +
        (d["phase"] == "CONT") * 0.6 +
        (d["phase"] == "EARLY") * 0.15
    )

    d["buy_score"] = d["score"] + structure * 0.5

    return d.sort_values("buy_score", ascending=False).head(5).to_dict("records")

# =========================
# OUTPUT (FIXED FORMAT)
# =========================
def print_output(redis_connected, df):
    print(f"🟢 Redis: {'CONNECTED' if redis_connected else 'OFF'}")
    print("🚀 GrowthRadar v37.18 (REDIS ANALYTICS MODEL)")
    print(f"Scan:1500 Valid:{len(df)}")
    print(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}\n")

    print("💎 BUY SIGNAL")
    buy = df.sort_values("score", ascending=False).head(5)
    for _, r in buy.iterrows():
        print(f"**{r.ticker}** S:{r.score:.2f}")

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
    print("\n".join(brk.ticker.values) or "None")

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

    df = pd.DataFrame(results)

    buy = build_buy(df)

    print_output(r is not None, df)

if __name__ == "__main__":
    run()
