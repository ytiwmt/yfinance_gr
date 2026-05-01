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
# FETCH (v38.0 CORE)
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

        # ===== STATE SCORE (v37.16) =====
        raw_score = (
            m1 * 0.6 +
            m3 * 0.3 +
            vol_ratio * 0.1 +
            breakout * 0.4
        )

        state = 8.5 * (1 - np.exp(-raw_score / 8.5))

        # ===== REDIS（履歴） =====
        delta1 = 0.0
        delta2 = 0.0
        prev_phase = None

        if r:
            prev1 = r.get(f"score:{ticker}")
            prev2 = r.get(f"score_prev:{ticker}")
            prev_phase = r.get(f"phase:{ticker}")

            if prev1 and prev2:
                prev1 = float(prev1)
                prev2 = float(prev2)
                delta1 = state - prev1
                delta2 = prev1 - prev2

            if prev1:
                r.set(f"score_prev:{ticker}", prev1, ex=7200)

            r.set(f"score:{ticker}", state, ex=7200)
            r.set(f"phase:{ticker}", phase, ex=86400)

        # ===== CHANGE =====
        up = max(delta1, 0)
        accel = max(delta1 - delta2, 0)

        change = (
            up * 0.6 +
            accel * 0.8 +
            (delta1 > 0 and delta2 > 0) * 0.5
        )

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

        # ===== EVENT =====
        event_boost = 1.05 if breakout else 1.0

        # ===== FINAL SCORE =====
        final = state * (1 + change) * phase_boost * event_boost

        print(f"[v38.0] {ticker} S:{final:.2f} Δ1:{delta1:.3f} Δ2:{delta2:.3f} PH:{phase}")

        return {
            "ticker": ticker,
            "phase": phase,
            "score": float(final),
            "state": float(state),
            "breakout": breakout
        }

    except:
        return None

# =========================
# DISCORD FORMAT（修正版）
# =========================
def build_discord(df):
    msg = []

    msg.append("🚀 GrowthRadar v38.0 (STATE × CHANGE)")
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg.append("")

    # BUY
    msg.append("💎 BUY SIGNAL")
    buy = df.sort_values("score", ascending=False).head(5)
    for _, r in buy.iterrows():
        msg.append(f"**{r.ticker}** S:{r.score:.2f}")

    # EARLY
    msg.append("")
    msg.append("🔥 EARLY")
    early = df[df.phase=="EARLY"].sort_values("score", ascending=False).head(4)
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in early.iterrows()] or ["None"]

    # TRANSITION
    msg.append("")
    msg.append("⚡ TRANSITION")
    trans = df[df.phase=="TRANSITION"].sort_values("score", ascending=False).head(4)
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in trans.iterrows()] or ["None"]

    # CONT
    msg.append("")
    msg.append("🔁 CONT")
    cont = df[df.phase=="CONT"].sort_values("score", ascending=False).head(4)
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in cont.iterrows()] or ["None"]

    # BREAKOUT
    msg.append("")
    msg.append("🧨 BREAKOUT (event)")
    brk = df[df.breakout].head(4)
    msg += [r.ticker for _, r in brk.iterrows()] or ["None"]

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

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    # ログ出力
    print(build_discord(df))

    # Discord送信
    if WEBHOOK_URL:
        text = build_discord(df)
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
