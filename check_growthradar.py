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
    except:
        r = None

# =========================
# THEME MAP
# =========================
THEME_MAP = {
    "semi": {
        "NVDA","AMD","QCOM","AVGO","TSM","MU","ASML","COHU",
        "AMKR","DIOD","LSCC","NVTS","RMBS","MRAM","AOSL",
        "AXTI","MXL","LWLG","TTMI"
    },
    "ai": {
        "PLTR","SNOW","AI","BBAI","SOUN"
    },
    "network": {
        "LITE","VIAV","AAOI","HLIT","CIEN","FSLY"
    },
    "energy": {
        "XOM","CVX","SLB","HAL","WULF"
    },
    "biotech": {
        "MRNA","NVAX","BNTX","REGN","VRTX","ALNY",
        "HCAI","KALV","CUE","AVTX"
    },
    "leveraged": {
        "TQQQ","GGLL","AMZU","AMDL","NVDL"
    }
}

def get_theme(ticker):
    for k, vals in THEME_MAP.items():
        if ticker in vals:
            return k
    return "other"

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = set()

    try:
        url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
        rows = requests.get(url, timeout=10).text.splitlines()[1:]

        for row in rows:
            s = row.split(",")[0].strip().upper()
            if re.match(r"^[A-Z]{1,6}$", s):
                symbols.add(s)

    except:
        pass

    fallback = [
        "AAPL","MSFT","NVDA","AMD","AMZN",
        "META","GOOGL","TSLA","QCOM"
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

        close = [
            x for x in
            data["indicators"]["quote"][0]["close"]
            if x is not None
        ]

        volume = [
            x for x in
            data["indicators"]["quote"][0]["volume"]
            if x is not None
        ]

        if len(close) < 70:
            return None

        price = close[-1]

        if price < MIN_PRICE:
            return None

        vol_base = np.mean(volume[-20:-5])

        if vol_base < MIN_VOL:
            return None

        def ret(a,b):
            return (a/b - 1) if b else 0

        # =========================
        # STATE
        # =========================
        m1 = ret(close[-1], close[-21])
        m3 = ret(close[-1], close[-63])

        vol_ratio = volume[-1] / (vol_base + 1e-9)

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
        day_change = ret(close[-1], close[-2])

        trend_ok = (
            close[-1] > close[-2] >
            close[-3]
        )

        breakout = (
            (
                day_change > 0.025 and
                vol_ratio > 1.8
            )
            or
            (
                day_change > 0.018 and
                vol_ratio > 2.3
            )
        ) and trend_ok

        # =========================
        # STATE SCORE
        # =========================
        raw_state = (
            m1 * 0.6 +
            m3 * 0.3 +
            vol_ratio * 0.1 +
            breakout * 0.4
        )

        state = 8.5 * (
            1 - np.exp(-raw_state / 8.5)
        )

        # =========================
        # REAL CHANGE
        # =========================
        d1 = ret(close[-1], close[-2])   # 1d
        d3 = ret(close[-1], close[-4])   # 3d
        d5 = ret(close[-1], close[-6])   # 5d

        vol_change = (
            volume[-1] /
            (volume[-2] + 1e-9)
        )

        short_change = (
            d1 * 0.5 +
            d3 * 0.3 +
            d5 * 0.2
        )

        volume_accel = max(
            vol_change - 1.0,
            0
        )

        # =========================
        # REDIS
        # =========================
        streak = 0
        prev_streak = 0

        if r:
            prev_short = r.get(f"short:{ticker}")
            ps = r.get(f"streak:{ticker}")

            if ps:
                prev_streak = int(ps)

            delta = 0

            if prev_short:
                delta = short_change - float(prev_short)

                # =========================
                # STREAK
                # =========================
                if short_change > 0.015:
                    if delta > -0.01:
                        streak = prev_streak + 1
                    else:
                        streak = prev_streak
                else:
                    streak = 0

            r.set(
                f"short:{ticker}",
                short_change,
                ex=86400
            )

            r.set(
                f"streak:{ticker}",
                streak,
                ex=86400
            )

        # =========================
        # CHANGE SCORE
        # =========================
        change_score = (
            short_change * 8 +
            volume_accel * 0.35
        )

        change_score = np.clip(
            change_score,
            0,
            2.0
        )

        # =========================
        # PHASE BOOST
        # =========================
        phase_boost = 1.0

        if phase == "TRANSITION":
            phase_boost = 1.18

        elif phase == "EARLY":
            phase_boost = 1.05

        elif phase == "CONT":
            phase_boost = 1.02

        # =========================
        # STREAK BOOST
        # =========================
        streak_boost = 1.0

        if streak >= 1:
            streak_boost += min(
                streak * 0.12,
                0.8
            )

        # =========================
        # FINAL SCORE
        # =========================
        score = (
            state *
            (1 + change_score) *
            phase_boost *
            streak_boost
        )

        # 暴走防止
        score = min(score, 8.0)

        return {
            "ticker": ticker,
            "phase": phase,
            "score": float(score),
            "breakout": bool(breakout),
            "theme": get_theme(ticker),
            "streak": int(streak)
        }

    except:
        return None

# =========================
# THEME CONTROL
# =========================
def apply_theme_control(df):

    if len(df) == 0:
        return df

    top = (
        df.sort_values(
            "score",
            ascending=False
        )
        .head(60)
    )

    theme_strength = (
        top[top.theme != "other"]
        .groupby("theme")["score"]
        .sum()
    )

    if len(theme_strength) == 0:
        df["final_score"] = df["score"]
        return df

    top_themes = (
        theme_strength
        .sort_values(ascending=False)
        .head(3)
        .index
    )

    def adjust(row):

        t = row["theme"]

        if t == "leveraged":
            return row["score"] * 0.45

        if t in top_themes:
            return row["score"] * 1.22

        if t != "other":
            return row["score"] * 0.72

        return row["score"]

    df["final_score"] = df.apply(
        adjust,
        axis=1
    )

    return df

# =========================
# BUY
# =========================
def build_buy(df):

    df = df.sort_values(
        "final_score",
        ascending=False
    )

    result = []
    theme_used = {}

    for _, row in df.iterrows():

        t = row["theme"]

        if (
            t != "other" and
            theme_used.get(t,0) >= 2
        ):
            continue

        if t == "leveraged":
            continue

        result.append(row)

        theme_used[t] = (
            theme_used.get(t,0) + 1
        )

        if len(result) >= 5:
            break

    return result

# =========================
# MESSAGE
# =========================
def build_msg(df, buy):

    msg = []

    msg.append(
        "🚀 GrowthRadar v39.3 (REAL CHANGE MODEL)"
    )

    msg.append(
        f"Scan:{SCAN_SIZE} Valid:{len(df)}"
    )

    msg.append(
        f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )

    msg.append(
        f"🟢 Redis: {'ON' if r else 'OFF'}"
    )

    msg.append("\n💎 BUY SIGNAL")

    if buy:
        for x in buy:
            msg.append(
                f"{x.ticker} "
                f"S:{x.final_score:.2f} "
                f"Streak:{x.streak}"
            )
    else:
        msg.append("None")

    msg.append("\n🔥 EARLY")

    early = (
        df[df.phase=="EARLY"]
        .sort_values(
            "final_score",
            ascending=False
        )
        .head(4)
    )

    msg += [
        f"{r.ticker} S:{r.final_score:.2f}"
        for _, r in early.iterrows()
    ] or ["None"]

    msg.append("\n⚡ TRANSITION")

    trans = (
        df[df.phase=="TRANSITION"]
        .sort_values(
            "final_score",
            ascending=False
        )
        .head(4)
    )

    msg += [
        f"{r.ticker} S:{r.final_score:.2f}"
        for _, r in trans.iterrows()
    ] or ["None"]

    msg.append("\n🔁 CONT")

    cont = (
        df[df.phase=="CONT"]
        .sort_values(
            "final_score",
            ascending=False
        )
        .head(4)
    )

    msg += [
        f"{r.ticker} S:{r.final_score:.2f}"
        for _, r in cont.iterrows()
    ] or ["None"]

    msg.append("\n🧨 BREAKOUT (event)")

    brk = df[df.breakout].head(4)

    msg += [
        r.ticker
        for _, r in brk.iterrows()
    ] or ["None"]

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

    df = apply_theme_control(df)

    buy = build_buy(df)

    text = build_msg(df, buy)

    print(text)

    if WEBHOOK_URL:
        requests.post(
            WEBHOOK_URL,
            json={"content": text[:1900]}
        )

if __name__ == "__main__":
    run()
