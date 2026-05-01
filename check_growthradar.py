import os
import requests
import random
import re
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
    except:
        r = None

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

    fallback = ["AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL","TSLA","QCOM"]
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

        # ===== PHASE =====
        phase = "NONE"
        if (0.25 < m1 < 0.7 and m3 < 0.6):
            phase = "EARLY"
        elif (m1 > 0.45 and m3 > 0.45):
            phase = "TRANSITION"
        elif (m3 > 1.0):
            phase = "CONT"

        # ===== BREAKOUT =====
        price_jump = abs(close[-1] - close[-2]) / close[-2]
        trend_ok = close[-1] > close[-2] > close[-3]

        breakout = (
            (price_jump > 0.02 and vol_ratio > 1.8) or
            (price_jump > 0.015 and vol_ratio > 2.3)
        ) and trend_ok

        # ===== STATE =====
        raw = m1*0.6 + m3*0.3 + vol_ratio*0.1 + breakout*0.4
        state = 8.5 * (1 - np.exp(-raw / 8.5))

        # ===== REDIS =====
        delta1 = 0
        delta2 = 0
        prev_phase = None

        if r:
            p1 = r.get(f"s:{ticker}")
            p2 = r.get(f"s2:{ticker}")
            prev_phase = r.get(f"p:{ticker}")

            if p1 and p2:
                p1 = float(p1)
                p2 = float(p2)
                delta1 = state - p1
                delta2 = p1 - p2

            if p1:
                r.set(f"s2:{ticker}", p1, ex=7200)

            r.set(f"s:{ticker}", state, ex=7200)
            r.set(f"p:{ticker}", phase, ex=86400)

        # ===== PRICE CHANGE =====
        valid_delta = delta1 > 0.05
        up = max(delta1, 0) if valid_delta else 0
        accel = max(delta1 - delta2, 0) if valid_delta else 0
        price_change = up*0.6 + accel*0.8

        # ===== VOLUME =====
        vol_trend = volume[-1] > volume[-2] > volume[-3]
        vol_accel = volume[-1] / (volume[-3] + 1e-9)

        vol_change = 0
        if vol_trend:
            vol_change += 0.3
        if vol_accel > 1.5:
            vol_change += min((vol_accel - 1.5)*0.2, 0.6)

        # ===== COMPRESSION（38.3追加）=====
        range_5 = max(close[-5:]) - min(close[-5:])
        range_20 = max(close[-20:]) - min(close[-20:])
        compression_ratio = range_5 / (range_20 + 1e-9)

        compression_score = 0
        if compression_ratio < 0.35:
            compression_score = 0.5
        elif compression_ratio < 0.5:
            compression_score = 0.25

        # ===== TOTAL CHANGE =====
        change = price_change + vol_change + compression_score
        change = min(change, 1.5)

        # ===== PHASE BOOST =====
        phase_boost = 1.0
        if prev_phase == "EARLY" and phase == "TRANSITION":
            phase_boost = 1.25
        elif phase == "TRANSITION":
            phase_boost = 1.10
        elif phase == "EARLY":
            phase_boost = 1.05
        elif phase == "CONT":
            phase_boost = 0.95

        # ===== LIQUIDITY =====
        liquidity = min(1.0, vol_base / 1_000_000)

        # ===== FINAL =====
        final = state * (1 + change) * phase_boost * liquidity

        return {
            "ticker": ticker,
            "phase": phase,
            "score": float(final),
            "breakout": breakout
        }

    except:
        return None

# =========================
# DISCORD
# =========================
def build_msg(df):
    msg = []

    msg.append("🚀 GrowthRadar v38.3 (STATE × CHANGE × VOLUME × COMPRESSION)")
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg.append(f"🟢 Redis: {'ON' if r else 'OFF'}")

    msg.append("\n💎 BUY SIGNAL")
    for _, r0 in df.sort_values("score", ascending=False).head(5).iterrows():
        msg.append(f"{r0.ticker} S:{r0.score:.2f}")

    msg.append("\n🔥 EARLY")
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in df[df.phase=="EARLY"].head(4).iterrows()] or ["None"]

    msg.append("\n⚡ TRANSITION")
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in df[df.phase=="TRANSITION"].head(4).iterrows()] or ["None"]

    msg.append("\n🔁 CONT")
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in df[df.phase=="CONT"].head(4).iterrows()] or ["None"]

    msg.append("\n🧨 BREAKOUT (event)")
    msg += [r.ticker for _, r in df[df.breakout].head(4).iterrows()] or ["None"]

    return "\n".join(msg)

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

    text = build_msg(df)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
