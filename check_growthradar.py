import os
import requests
import random
import re
import redis
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
REDIS_URL = os.environ.get("REDIS_URL")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ETF BLACKLIST
ETF_BLACKLIST = {
    "QQQ", "ARKK", "SOXX", "XLF",
    "XLK", "XBI", "IWM", "SPY",
    "WGMI", "QCML", "RKLX",
    "TQQQ", "SQQQ",
    "SOXL", "SOXS",
    "ARKW", "ARKG",
    "SMH", "IGV", "BOTZ", "TAN"
}

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
    print("⚠️ Redis: OFF")

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
        "INTC","QCOM","AVGO","TSM","ASML","MU","PLTR","SNOW","CRWD"
    ]

    symbols.update(fallback)

    # UNIVERSE FILTER (SINGLE RESPONSIBILITY)
    symbols = [s for s in symbols if s not in ETF_BLACKLIST]

    symbols = list(symbols)
    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    # fetch側のETFチェック（二重防御）は削除

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

        trend_ok = (
            close[-1] > close[-2] > close[-3]
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
            (close[-1] / (ma20 + 1e-9)) - 1
        ) * 10

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
        # REDIS TIME MODEL
        # =========================
        delta = 0.0
        streak = 0

        today = datetime.utcnow().strftime("%Y-%m-%d")

        if r:
            prev_score = r.get(f"score:{ticker}")
            prev_streak = r.get(f"streak:{ticker}")
            prev_day = r.get(f"day:{ticker}")

            # NEW
            recent_high_streak = r.get(f"highstreak:{ticker}")
            recent_high_streak = int(recent_high_streak) if recent_high_streak else 0

            if prev_score is not None:
                delta = base_score - float(prev_score)

            if prev_streak is not None:
                streak = int(prev_streak)

            # =========================
            # STREAK FIX
            # =========================
            if prev_day != today:

                keep_signal = (
                    phase in ["TRANSITION", "CONT"] and
                    base_score > 0.9 and
                    delta >= -0.15
                )

                if keep_signal:
                    streak += 1
                else:
                    streak = 0

            # =========================
            # SAVE HIGHEST STREAK
            # =========================
            recent_high_streak = max(
                recent_high_streak,
                streak
            )

            # SAVE
            r.set(f"score:{ticker}", base_score, ex=86400)
            r.set(f"streak:{ticker}", streak, ex=86400 * 7)
            r.set(f"day:{ticker}", today, ex=86400 * 7)

            # NEW
            r.set(
                f"highstreak:{ticker}",
                recent_high_streak,
                ex=86400 * 14
            )

        else:
            recent_high_streak = 0

        # =========================
        # STREAK BONUS
        # =========================
        streak_bonus = min(streak * 0.18, 0.9)

        # =========================
        # EXTENSION PENALTY
        # =========================
        ext_penalty = 0.0

        if extension > 3.5:
            ext_penalty = 1.0

        elif extension > 2.5:
            ext_penalty = 0.5

        # =========================
        # SECOND WIND
        # =========================
        second_wind_watch = (
            recent_high_streak >= 5 and
            streak <= 2 and
            phase in ["TRANSITION", "CONT"] and
            extension < 5.0 and
            delta > -0.3
        )

        # SWS (Second Wind Setup)
        second_wind_setup = (
            second_wind_watch and
            extension < 3 and
            delta > -0.15
        )

        # SWT (Second Wind Trigger)
        second_wind_trigger = (
            second_wind_setup and
            breakout
        )

        second_wind_bonus = 0.0
        if second_wind_setup:
            second_wind_bonus = 0.75

        # =========================
        # FINAL SCORE
        # =========================
        score = (
            base_score +
            max(delta, 0) * 0.45 +
            streak_bonus +
            second_wind_bonus -
            ext_penalty
        )

        score = round(float(score), 2)

        return {
            "ticker": ticker,
            "phase": phase,
            "score": score,
            "streak": int(streak),
            "breakout": bool(breakout),
            "ext": round(float(extension), 2),

            # NEW
            "second_wind_watch": bool(second_wind_watch),
            "second_wind_setup": bool(second_wind_setup),
            "second_wind_trigger": bool(second_wind_trigger)
        }

    except:
        return None

# =========================
# BUY
# =========================
def build_buy(df):
    buy = df.copy()

    structure_bonus = (
        (buy["phase"] == "TRANSITION") * 0.7 +
        (buy["phase"] == "CONT") * 0.45 +
        (buy["phase"] == "EARLY") * 0.15
    )

    streak_bonus = np.minimum(
        buy["streak"] * 0.12,
        0.8
    )

    ext_penalty = np.maximum(
        buy["ext"] - 2.5,
        0
    ) * 0.35

    second_wind_bonus = (
        buy["second_wind_setup"] * 0.9
    )

    buy["buy_score"] = (
        buy["score"] +
        structure_bonus +
        streak_bonus +
        second_wind_bonus -
        ext_penalty
    )

    buy = buy.sort_values(
        "buy_score",
        ascending=False
    )

    return buy.head(5)

# =========================
# MESSAGE
# =========================
def build_message(df):
    buy = build_buy(df)

    msg = []

    msg.append("🚀 GrowthRadar v40.11 (SWS THRESHOLD TUNING MODEL)") 
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg.append("🟢 Redis: ON" if r else "🔴 Redis: OFF")

    msg.append("")
    msg.append("💎 BUY SIGNAL")

    for _, row in buy.iterrows():

        tag = ""

        if row.second_wind_trigger:
            tag = " SW🔥"
        elif row.second_wind_setup:
            tag = " SW🧩"
        elif row.second_wind_watch:
            tag = " SW👀"

        msg.append(
            f"{row.ticker} "
            f"S:{row.buy_score:.2f} "
            f"Streak:{row.streak} "
            f"Ext:{row.ext:.2f}"
            f"{tag}"
        )

    msg.append("")
    msg.append("🔥 EARLY")

    early = (
        df[df.phase == "EARLY"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(early):
        for _, row in early.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("⚡ TRANSITION")

    trans = (
        df[df.phase == "TRANSITION"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(trans):
        for _, row in trans.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("🔁 CONT")

    cont = (
        df[df.phase == "CONT"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(cont):
        for _, row in cont.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    # =========================
    # FIRST WAVE
    # =========================
    msg.append("")
    msg.append("🌊 FIRST WAVE")

    brk = df[df.breakout].head(4)

    if len(brk):
        for _, row in brk.iterrows():
            msg.append(row.ticker)
    else:
        msg.append("None")

    # =========================
    # SECOND WIND WATCH
    # =========================
    msg.append("")
    msg.append("🌊👀 SECOND WIND WATCH")

    sw_watch = (
        df[df.second_wind_watch]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(sw_watch):
        for _, row in sw_watch.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f} "
                f"Ext:{row.ext:.2f}"
            )
    else:
        msg.append("None")

    # =========================
    # SECOND WIND SETUP
    # =========================
    msg.append("")
    msg.append("🌊🧩 SECOND WIND SETUP")

    sw_setup = (
        df[df.second_wind_setup]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(sw_setup):
        for _, row in sw_setup.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f} "
                f"Ext:{row.ext:.2f}"
            )
    else:
        msg.append("None")

    # =========================
    # SECOND WIND TRIGGER
    # =========================
    msg.append("")
    msg.append("🌊🔥 SECOND WIND TRIGGER")

    sw_trigger = (
        df[df.second_wind_trigger]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(sw_trigger):
        for _, row in sw_trigger.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f} "
                f"Ext:{row.ext:.2f}"
            )
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

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
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
