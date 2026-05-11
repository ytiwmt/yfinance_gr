```python
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
# REDIS
# =========================
r = None

if REDIS_URL:
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print("🟢 Redis Connected")
    except:
        print("🔴 Redis Failed")
        r = None

# =========================
# UNIVERSE
# =========================
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
        "AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL","TSLA",
        "PLTR","SMCI","AVGO","TSM","ASML","MU","CRWD","SNOW",
        "QCOM","MRVL","ARM","INTC"
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

        close = data["indicators"]["quote"][0]["close"]
        volume = data["indicators"]["quote"][0]["volume"]

        close = [x for x in close if x is not None]
        volume = [x for x in volume if x is not None]

        if len(close) < 70:
            return None

        price = close[-1]

        if price < MIN_PRICE:
            return None

        vol_base = np.mean(volume[-20:-5])

        if np.isnan(vol_base) or vol_base <= 0:
            return None

        if vol_base < MIN_VOL:
            return None

        # =========================
        # RETURNS
        # =========================
        def ret(a, b):
            return (a / b - 1) if b else 0

        m1 = ret(close[-1], close[-21])
        m3 = ret(close[-1], close[-63])

        vol_ratio = volume[-1] / (vol_base + 1e-9)

        # =========================
        # PHASE
        # =========================
        phase = "NONE"

        if 0.25 < m1 < 0.7 and m3 < 0.6:
            phase = "EARLY"

        elif m1 > 0.45 and m3 > 0.45:
            phase = "TRANSITION"

        elif m3 > 1.0:
            phase = "CONT"

        # =========================
        # BREAKOUT
        # =========================
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

        # =========================
        # EXTENSION
        # =========================
        ma20 = np.mean(close[-20:])

        extension = (
            close[-1] / (ma20 + 1e-9)
        )

        # =========================
        # BASE SCORE
        # =========================
        base_score = (
            m1 * 0.55 +
            m3 * 0.25 +
            vol_ratio * 0.10 +
            breakout * 0.35
        )

        # =========================
        # OVERHEAT CONTROL
        # =========================
        if extension > 3.5:
            base_score *= 0.55

        elif extension > 3.0:
            base_score *= 0.70

        elif extension > 2.5:
            base_score *= 0.82

        # =========================
        # REDIS
        # =========================
        delta = 0.0
        streak = 0

        today = datetime.utcnow().strftime("%Y-%m-%d")

        if r:

            prev_score = r.get(f"score:{ticker}")
            prev_streak = r.get(f"streak:{ticker}")
            prev_day = r.get(f"day:{ticker}")

            if prev_score is not None:
                delta = base_score - float(prev_score)

            if prev_streak is not None:
                streak = int(prev_streak)

            # =========================
            # STREAK FIX
            # =========================
            # 同日中は維持
            # 営業日更新時のみ判定
            # =========================
            if prev_day != today:

                keep_signal = (
                    phase in ["TRANSITION", "CONT"]
                    and score_safe(base_score)
                    and delta >= -0.15
                )

                if keep_signal:
                    streak += 1
                else:
                    streak = 0

            # 保存
            r.set(
                f"score:{ticker}",
                base_score,
                ex=86400
            )

            r.set(
                f"streak:{ticker}",
                streak,
                ex=86400 * 7
            )

            r.set(
                f"day:{ticker}",
                today,
                ex=86400 * 7
            )

        # =========================
        # STREAK BONUS
        # =========================
        streak_bonus = min(
            streak * 0.18,
            0.9
        )

        # =========================
        # FINAL SCORE
        # =========================
        score = (
            base_score +
            max(delta, 0) * 0.45 +
            streak_bonus
        )

        return {
            "ticker": ticker,
            "phase": phase,
            "score": round(float(score), 2),
            "streak": int(streak),
            "breakout": bool(breakout),
            "ext": round(float(extension), 2)
        }

    except:
        return None

# =========================
# SAFE SCORE CHECK
# =========================
def score_safe(x):
    return x >= 1.0

# =========================
# DISCORD
# =========================
def build_message(df):

    msg = []

    msg.append("🚀 GrowthRadar v40.3 (ACTIVE STREAK FIX)")
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg.append(f"🟢 Redis: {'ON' if r else 'OFF'}")

    msg.append("")

    # =========================
    # BUY
    # =========================
    msg.append("💎 BUY SIGNAL")

    buy = (
        df.sort_values(
            "score",
            ascending=False
        )
        .head(5)
    )

    for _, row in buy.iterrows():

        msg.append(
            f"{row.ticker} "
            f"S:{row.score:.2f} "
            f"Streak:{row.streak} "
            f"Ext:{row.ext:.2f}"
        )

    # =========================
    # EARLY
    # =========================
    msg.append("")
    msg.append("🔥 EARLY")

    early = (
        df[df.phase=="EARLY"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(early) > 0:
        for _, row in early.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f}"
            )
    else:
        msg.append("None")

    # =========================
    # TRANSITION
    # =========================
    msg.append("")
    msg.append("⚡ TRANSITION")

    trans = (
        df[df.phase=="TRANSITION"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(trans) > 0:
        for _, row in trans.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f}"
            )
    else:
        msg.append("None")

    # =========================
    # CONT
    # =========================
    msg.append("")
    msg.append("🔁 CONT")

    cont = (
        df[df.phase=="CONT"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(cont) > 0:
        for _, row in cont.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f}"
            )
    else:
        msg.append("None")

    # =========================
    # BREAKOUT
    # =========================
    msg.append("")
    msg.append("🧨 BREAKOUT (event)")

    brk = (
        df[df.breakout]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(brk) > 0:
        for _, row in brk.iterrows():
            msg.append(row.ticker)
    else:
        msg.append("None")

    return "\n".join(msg)

# =========================
# RUN
# =========================
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

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    run()
```
