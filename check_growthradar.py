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
# THEME MAP
# =========================
THEME_MAP = {
    "semi": {"NVDA","AMD","QCOM","AVGO","TSM","MU","ASML","COHU","AMKR","DIOD","LSCC","NVTS","RMBS","MRAM"},
    "ai": {"PLTR","SNOW","AI","BBAI","SOUN"},
    "network": {"LITE","VIAV","AAOI","HLIT","CIEN","FSLY"},
    "energy": {"XOM","CVX","SLB","HAL","WULF"},
    "biotech": {"MRNA","NVAX","BNTX","REGN","VRTX","ALNY","HCAI","KALV"},
    "leveraged": {"TQQQ","GGLL","AMZU","AMDL","NVDL"},
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
        trend_ok = close[-1] > close[-2] > close[-3]
        breakout = (
            ((price_jump > 0.02 and vol_ratio > 1.8) or
             (price_jump > 0.015 and vol_ratio > 2.3))
            and trend_ok
        )

        # STATE
        raw = m1*0.6 + m3*0.3 + vol_ratio*0.1 + breakout*0.4
        state = 8.5 * (1 - np.exp(-raw / 8.5))

        # REDIS
        delta1, delta2 = 0, 0
        prev_phase = None
        prev_streak = 0

        if r:
            p1 = r.get(f"s:{ticker}")
            p2 = r.get(f"s2:{ticker}")
            prev_phase = r.get(f"p:{ticker}")
            ps = r.get(f"streak:{ticker}")

            if ps:
                prev_streak = int(ps)

            if p1 and p2:
                p1 = float(p1)
                p2 = float(p2)
                delta1 = state - p1
                delta2 = p1 - p2

            if p1:
                r.set(f"s2:{ticker}", p1, ex=7200)

            r.set(f"s:{ticker}", state, ex=7200)
            r.set(f"p:{ticker}", phase, ex=86400)

        # STREAK
        streak = prev_streak + 1 if delta1 > 0 else 0
        if r:
            r.set(f"streak:{ticker}", streak, ex=86400)

        # CHANGE
        up = max(delta1, 0)
        accel = max(delta1 - delta2, 0)

        price_change = up*0.6 + accel*0.8
        vol_trend = volume[-1] > volume[-2] > volume[-3]
        vol_change = 0.25 if vol_trend else 0

        change = min(price_change + vol_change, 1.5)
        effective_change = change * (0.3 + state * 0.4)

        state_factor = 0.4 + state * 0.6

        phase_boost = 1.0
        if prev_phase == "EARLY" and phase == "TRANSITION":
            phase_boost = 1.20
        elif phase == "TRANSITION":
            phase_boost = 1.08
        elif phase == "EARLY":
            phase_boost = 1.03
        elif phase == "CONT":
            phase_boost = 1.05

        base_score = state_factor * (1 + effective_change) * phase_boost

        # ===== STREAK BOOST
        streak_boost = 1 + min(streak * 0.08, 0.6)

        # ===== スパイク完全抑制
        if streak <= 1 and delta1 > 1.0:
            streak_boost *= 0.5

        score = base_score * streak_boost

        return {
            "ticker": ticker,
            "phase": phase,
            "score": float(score),
            "breakout": breakout,
            "theme": get_theme(ticker),
            "streak": streak
        }

    except:
        return None

# =========================
# THEME強制
# =========================
def apply_theme_control(df):
    if len(df) == 0:
        return df

    top = df.sort_values("score", ascending=False).head(60)

    theme_strength = (
        top[top["theme"] != "other"]
        .groupby("theme")["score"]
        .sum()
    )

    if len(theme_strength) == 0:
        df["final_score"] = df["score"]
        return df

    top_themes = theme_strength.sort_values(ascending=False).head(2).index

    def adjust(row):
        t = row["theme"]

        if t == "leveraged":
            return row["score"] * 0.5  # ETF減点

        if t not in top_themes and t != "other":
            return row["score"] * 0.7  # テーマ外減点

        if t in top_themes:
            return row["score"] * 1.25  # テーマ強制

        return row["score"]

    df["final_score"] = df.apply(adjust, axis=1)
    return df

# =========================
# BUY
# =========================
def build_buy(df):
    df = df.sort_values("final_score", ascending=False)

    result = []
    theme_used = {}

    for _, row in df.iterrows():
        t = row["theme"]

        if t != "other" and theme_used.get(t, 0) >= 2:
            continue

        if t == "leveraged":
            continue  # 完全除外

        result.append(row)
        theme_used[t] = theme_used.get(t, 0) + 1

        if len(result) >= 5:
            break

    return result

# =========================
# MESSAGE
# =========================
def build_msg(df, buy):
    msg = []
    msg.append("🚀 GrowthRadar v39.0 (TRADE MODEL)")
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg.append(f"🟢 Redis: {'ON' if r else 'OFF'}")

    msg.append("\n💎 BUY SIGNAL")
    msg += [f"{x.ticker} S:{x.final_score:.2f} Streak:{x.streak}" for x in buy] or ["None"]

    msg.append("\n🔥 EARLY")
    msg += [f"{r.ticker} S:{r.final_score:.2f}" for _, r in df[df.phase=="EARLY"].head(4).iterrows()] or ["None"]

    msg.append("\n⚡ TRANSITION")
    msg += [f"{r.ticker} S:{r.final_score:.2f}" for _, r in df[df.phase=="TRANSITION"].head(4).iterrows()] or ["None"]

    msg.append("\n🔁 CONT")
    msg += [f"{r.ticker} S:{r.final_score:.2f}" for _, r in df[df.phase=="CONT"].head(4).iterrows()] or ["None"]

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
            x = f.result()
            if x:
                results.append(x)

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    df = apply_theme_control(df)
    buy = build_buy(df)

    text = build_msg(df, buy)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
