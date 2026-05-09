import os
import requests
import random
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import redis

# =====================================
# CONFIG
# =====================================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
REDIS_URL = os.environ.get("REDIS_URL")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =====================================
# REDIS
# =====================================
r = None

if REDIS_URL:
    try:
        r = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True
        )
        r.ping()
    except:
        r = None

# =====================================
# UNIVERSE
# =====================================
def load_universe():
    symbols = set()

    try:
        url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"

        data = requests.get(url, timeout=10).text.splitlines()[1:]

        for line in data:
            s = line.split(",")[0].strip().upper()

            if re.match(r"^[A-Z]{1,6}$", s):
                symbols.add(s)

    except:
        pass

    fallback = [
        "AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL",
        "TSLA","PLTR","MU","AVGO","TSM","ASML","CRWD",
        "SNOW","QCOM","ARM","SMCI"
    ]

    symbols.update(fallback)

    symbols = list(symbols)
    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]

# =====================================
# FETCH
# =====================================
def fetch(session, ticker):

    try:
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/"
            f"{ticker}?range=6mo&interval=1d"
        )

        res = session.get(url, timeout=5)

        if res.status_code != 200:
            return None

        data = res.json()["chart"]["result"][0]

        close = data["indicators"]["quote"][0]["close"]
        volume = data["indicators"]["quote"][0]["volume"]

        close = [x for x in close if x is not None]
        volume = [x for x in volume if x is not None]

        if len(close) < 80:
            return None

        price = close[-1]

        if price < MIN_PRICE:
            return None

        vol_base = np.mean(volume[-20:-5])

        if np.isnan(vol_base) or vol_base <= 0:
            return None

        if vol_base < MIN_VOL:
            return None

        # =====================================
        # RETURNS
        # =====================================
        def ret(a, b):
            return (a / b - 1) if b else 0

        m1 = ret(close[-1], close[-21])
        m3 = ret(close[-1], close[-63])

        vol_ratio = volume[-1] / (vol_base + 1e-9)

        # =====================================
        # EXTENSION
        # =====================================
        ext_60 = close[-1] / (min(close[-60:]) + 1e-9)

        # extension penalty
        ext_penalty = 0.0

        if ext_60 > 4.0:
            ext_penalty = 1.4

        elif ext_60 > 3.0:
            ext_penalty = 0.9

        elif ext_60 > 2.3:
            ext_penalty = 0.4

        # =====================================
        # PHASE
        # =====================================
        phase = "NONE"

        if (
            0.25 < m1 < 0.7 and
            m3 < 0.6
        ):
            phase = "EARLY"

        elif (
            m1 > 0.45 and
            m3 > 0.45
        ):
            phase = "TRANSITION"

        elif (
            m3 > 1.0
        ):
            phase = "CONT"

        # =====================================
        # BREAKOUT
        # =====================================
        price_jump = abs(close[-1] - close[-2]) / close[-2]

        trend_ok = (
            close[-1] > close[-2] >
            close[-3]
        )

        breakout = (
            (
                price_jump > 0.02 and
                vol_ratio > 1.8
            )
            or
            (
                price_jump > 0.015 and
                vol_ratio > 2.3
            )
        ) and trend_ok

        # =====================================
        # BASE SCORE
        # =====================================
        raw_score = (
            m1 * 0.55 +
            m3 * 0.25 +
            vol_ratio * 0.10 +
            breakout * 0.35
        )

        # compression
        base_score = (
            7.5 * (1 - np.exp(-raw_score / 7.5))
        )

        # =====================================
        # REDIS DELTA
        # =====================================
        delta = 0.0
        streak = 0

        if r:

            prev_score = r.get(f"score:{ticker}")

            if prev_score is not None:
                delta = base_score - float(prev_score)

            prev_phase = r.get(f"phase:{ticker}")

            if prev_phase == phase and base_score > 1.0:
                streak = int(r.get(f"streak:{ticker}") or 0) + 1
            else:
                streak = 0

            r.set(f"score:{ticker}", base_score, ex=86400)
            r.set(f"phase:{ticker}", phase, ex=86400)
            r.set(f"streak:{ticker}", streak, ex=86400)

        # =====================================
        # PHASE BONUS
        # =====================================
        phase_bonus = 0.0

        if phase == "EARLY":
            phase_bonus = 0.15

        elif phase == "TRANSITION":
            phase_bonus = 0.55

        elif phase == "CONT":
            phase_bonus = 0.30

        # =====================================
        # STREAK BONUS
        # =====================================
        streak_bonus = min(streak * 0.18, 1.2)

        # =====================================
        # DELTA BONUS
        # =====================================
        delta_bonus = max(delta, 0) * 2.0

        # =====================================
        # FINAL SCORE
        # =====================================
        score = (
            base_score +
            phase_bonus +
            streak_bonus +
            delta_bonus -
            ext_penalty
        )

        score = max(score, 0)

        return {
            "ticker": ticker,
            "phase": phase,
            "score": round(float(score), 2),
            "streak": int(streak),
            "breakout": bool(breakout),
            "ext": round(float(ext_60), 2)
        }

    except:
        return None

# =====================================
# OUTPUT
# =====================================
def build_message(df):

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    lines = []

    lines.append(
        "🚀 GrowthRadar v40.0 "
        "(EXTENSION FILTER MODEL)"
    )

    lines.append(
        f"Scan:{SCAN_SIZE} Valid:{len(df)}"
    )

    lines.append(f"Time:{now}")

    lines.append(
        f"🟢 Redis: {'ON' if r else 'OFF'}"
    )

    lines.append("")

    # =====================================
    # BUY
    # =====================================
    lines.append("💎 BUY SIGNAL")

    buy = (
        df.sort_values(
            "score",
            ascending=False
        )
        .head(5)
    )

    for _, row in buy.iterrows():

        lines.append(
            f"{row.ticker} "
            f"S:{row.score:.2f} "
            f"Streak:{row.streak} "
            f"Ext:{row.ext}"
        )

    # =====================================
    # EARLY
    # =====================================
    lines.append("")
    lines.append("🔥 EARLY")

    early = (
        df[df.phase == "EARLY"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(early):
        for _, row in early.iterrows():
            lines.append(
                f"{row.ticker} "
                f"S:{row.score:.2f}"
            )
    else:
        lines.append("None")

    # =====================================
    # TRANSITION
    # =====================================
    lines.append("")
    lines.append("⚡ TRANSITION")

    trans = (
        df[df.phase == "TRANSITION"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(trans):
        for _, row in trans.iterrows():
            lines.append(
                f"{row.ticker} "
                f"S:{row.score:.2f}"
            )
    else:
        lines.append("None")

    # =====================================
    # CONT
    # =====================================
    lines.append("")
    lines.append("🔁 CONT")

    cont = (
        df[df.phase == "CONT"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(cont):
        for _, row in cont.iterrows():
            lines.append(
                f"{row.ticker} "
                f"S:{row.score:.2f}"
            )
    else:
        lines.append("None")

    # =====================================
    # BREAKOUT
    # =====================================
    lines.append("")
    lines.append("🧨 BREAKOUT (event)")

    brk = df[df.breakout].head(4)

    if len(brk):
        for _, row in brk.iterrows():
            lines.append(row.ticker)
    else:
        lines.append("None")

    return "\n".join(lines)

# =====================================
# RUN
# =====================================
def run():

    session = requests.Session()
    session.headers.update(HEADERS)

    universe = load_universe()

    results = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as ex:

        futures = {
            ex.submit(fetch, session, t): t
            for t in universe
        }

        for f in as_completed(futures):

            rlt = f.result()

            if rlt:
                results.append(rlt)

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    text = build_message(df)

    print(text)

    if WEBHOOK_URL:
        requests.post(
            WEBHOOK_URL,
            json={"content": text[:1900]}
        )

# =====================================
# MAIN
# =====================================
if __name__ == "__main__":
    run()
