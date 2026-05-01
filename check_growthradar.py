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

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = set()
    try:
        url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
        r = requests.get(url, timeout=10)
        for l in r.text.splitlines()[1:]:
            s = l.split(",")[0].strip().upper()
            if re.match(r"^[A-Z]{1,6}$", s):
                symbols.add(s)
    except:
        pass

    fallback = ["AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL","TSLA"]
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
        rqt = session.get(url, timeout=5)
        if rqt.status_code != 200:
            return None

        data = rqt.json()["chart"]["result"][0]
        close = [x for x in data["indicators"]["quote"][0]["close"] if x]
        volume = [x for x in data["indicators"]["quote"][0]["volume"] if x]

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

        # PHASE
        phase = "NONE"
        if (0.25 < m1 < 0.7 and m3 < 0.6):
            phase = "EARLY"
        elif (m1 > 0.45 and m3 > 0.45):
            phase = "TRANSITION"
        elif (m3 > 1.0):
            phase = "CONT"

        # BREAKOUT
        price_jump = abs(close[-1] - close[-2]) / close[-2]
        vol_spike = volume[-1] / (vol_base + 1e-9)
        trend_ok = close[-1] > close[-2] > close[-3]

        breakout = (
            (price_jump > 0.02 and vol_spike > 1.8) or
            (price_jump > 0.015 and vol_spike > 2.3)
        ) and trend_ok

        # BASE SCORE（v37.13）
        score = m1*0.6 + m3*0.3 + vol_ratio*0.1

        # =========================
        # DELTA（ここだけ追加）
        # =========================
        delta = 0.0
        if r:
            prev = r.get(f"s:{ticker}")
            if prev:
                delta = score - float(prev)

            r.set(f"s:{ticker}", score, ex=3600)

        print(f"[REDIS] {ticker} base={score:.2f} delta={delta:.2f}")

        return {
            "ticker": ticker,
            "phase": phase,
            "score": score,
            "m1": m1,
            "m3": m3,
            "vol_ratio": vol_ratio,
            "breakout": breakout,
            "delta": delta
        }

    except:
        return None

# =========================
# BUY（完成版）
# =========================
def build_buy(df):
    base = (
        df["score"] +
        df["m1"] * 0.5 +
        df["m3"] * 0.3 +
        df["vol_ratio"] * 0.2 +
        df["breakout"] * 0.6
    )

    structure = (
        (df["phase"] == "TRANSITION") * 0.9 +
        (df["phase"] == "CONT") * 0.6 +
        (df["phase"] == "EARLY") * 0.15
    )

    # ★ここが核心（deltaを軽く効かせる）
    delta_boost = np.clip(df["delta"], 0, None) * 1.2

    df["buy_score"] = base + structure*0.6 + delta_boost

    return df.sort_values("buy_score", ascending=False).head(5)

# =========================
# RUN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    universe = load_universe()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            rlt = f.result()
            if rlt:
                results.append(rlt)

    df = pd.DataFrame(results)

    buy = build_buy(df)

    print("💎 BUY SIGNAL")
    for _, r in buy.iterrows():
        print(f"{r.ticker} S:{r.buy_score:.2f}")

if __name__ == "__main__":
    run()
