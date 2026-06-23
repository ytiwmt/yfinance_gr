import os
import requests
import random
import re
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np


# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

HEADERS = {"User-Agent": "Mozilla/5.0"}

ETF_BLACKLIST = {
    "QQQ","ARKK","SOXX","XLF","XLK","XBI","IWM","SPY",
    "WGMI","QCML","RKLX","INTW","TQQQ","SQQQ","RGTX",
    "IONX","SOXL","SOXS","ORCX","DLLL","ARKW","ARKG",
    "QCMU","NBIL","AMDL","IONL","NBIG","MVLL","SMH",
    "IGV","BOTZ","TAN"
}


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
    symbols = [s for s in symbols if s not in ETF_BLACKLIST]

    symbols = sorted(symbols)

    random.seed(datetime.utcnow().strftime("%Y-%m-%d"))
    symbols = list(symbols)
    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]


# =========================
# STREAK (CURRENT ONLY)
# =========================
def calc_current_streak(close):
    r = np.diff(close) / close[:-1]
    streak = 0

    for x in reversed(r):
        if x > 0:
            streak += 1
        else:
            break

    return streak


# =========================
# FETCH
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        res = session.get(url, timeout=5)

        if res.status_code != 200:
            return None

        data = res.json().get("chart", {}).get("result", [None])[0]
        if not data:
            return None

        ind = data.get("indicators", {}).get("quote", [{}])[0]

        close = [x for x in ind.get("close", []) if x is not None]
        volume = [x for x in ind.get("volume", []) if x is not None]

        if len(close) < 200 or len(volume) < 200:
            return None

        price = close[-1]

        if price < MIN_PRICE:
            return None

        vol_base = np.mean(volume[-20:-5])
        if np.isnan(vol_base) or vol_base <= 0:
            return None
        if vol_base < MIN_VOL:
            return None

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
        trend_ok = close[-1] > close[-2] > close[-3]

        breakout = (
            ((price_jump > 0.02 and vol_ratio > 1.8) or
             (price_jump > 0.015 and vol_ratio > 2.3))
            and trend_ok
        )

        # =========================
        # EXTENSION
        # =========================
        ma20 = np.mean(close[-20:])
        extension = (price / (ma20 + 1e-9) - 1) * 10

        # =========================
        # LONG TERM
        # =========================
        ma120 = np.mean(close[-120:]) if len(close) >= 120 else np.mean(close)
        ma200 = np.mean(close[-200:]) if len(close) >= 200 else np.mean(close)

        long_term_bonus = 0.0
        if price > ma200:
            long_term_bonus += 0.25
        if len(close) >= 200 and ma120 > ma200:
            long_term_bonus += 0.25

        # =========================
        # CORE SCORE
        # =========================
        base_score = (
            m1 * 0.55 +
            m3 * 0.25 +
            vol_ratio * 0.10 +
            breakout * 0.35
        )

        # =========================
        # STATELESS STREAK
        # =========================
        streak = calc_current_streak(close)
        streak_bonus = np.log1p(streak) * 0.05

        # =========================
        # DELTA (STRUCTURE)
        # =========================
        delta = np.mean(close[-5:]) / np.mean(close[-20:-5]) - 1
        if np.isnan(delta) or np.isinf(delta):
            delta = 0.0

        # =========================
        # EXT PENALTY
        # =========================
        ext_penalty = 0.0
        if extension > 3.5:
            ext_penalty = 1.0
        elif extension > 2.5:
            ext_penalty = 0.5

        # =========================
        # YEARLY TREND
        # =========================
        idx = max(-len(close), -252)
        base = close[idx] if len(close) >= 252 else close[0]

        yearly_return = (price / base) - 1 if base else 0
        high_52w = max(close)
        high_distance = (price / high_52w) - 1

        if yearly_return > 0.3 and high_distance > -0.25:
            yearly_trend_factor = 1.0
        elif yearly_return > -0.2:
            yearly_trend_factor = 0.5
        else:
            yearly_trend_factor = 0.0

        # =========================
        # SW (STRUCTURAL REVIVAL)
        # =========================
        peak_20 = max(close[-20:])
        drawdown = price / peak_20

        second_wind_setup = (
            drawdown < 0.9 and
            delta > -0.15 and
            extension < 3
        )

        second_wind_trigger = second_wind_setup and breakout

        second_wind_bonus = 0.0
        if second_wind_setup:
            second_wind_bonus = 0.75 * (0.6 + 0.4 * yearly_trend_factor)

        # =========================
        # FINAL SCORE
        # =========================
        score = (
            base_score +
            max(delta, 0) * 0.45 +
            streak_bonus +
            second_wind_bonus +
            long_term_bonus -
            ext_penalty
        )

        score = round(float(score), 2)

        return {
            "ticker": ticker,
            "phase": phase,
            "score": score,
            "streak": int(streak),
            "breakout": bool(breakout),
            "ext": round(extension, 2),

            "second_wind_setup": second_wind_setup,
            "second_wind_trigger": second_wind_trigger,

            "long_term_bonus": round(long_term_bonus, 2),
            "yearly_trend_factor": yearly_trend_factor
        }

    except:
        return None


# =========================
# BUY SCORE
# =========================
def build_buy(df):
    buy = df.copy()

    structure_bonus = (
        (buy["phase"] == "TRANSITION") * 0.7 +
        (buy["phase"] == "CONT") * 0.45 +
        (buy["phase"] == "EARLY") * 0.15
    )

    ext_penalty = np.maximum(buy["ext"] - 2.5, 0) * 0.35
    second_wind_bonus = buy["second_wind_setup"] * 0.9

    buy["buy_score"] = (
        buy["score"] +
        structure_bonus +
        second_wind_bonus -
        ext_penalty
    )

    # freshness penalty
    buy["buy_score"] *= (1 - buy["ext"].clip(lower=0, upper=5) / 50)

    return buy.sort_values("buy_score", ascending=False).head(5)


# =========================
# MESSAGE
# =========================
def build_message(df):
    buy = build_buy(df)

    msg = []
    msg.append("🚀 GrowthRadar v42.0 (Structural Hybrid Model)")
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")

    msg.append("")
    msg.append("💎 BUY SIGNAL")

    for _, r in buy.iterrows():
        tag = "SW🧩" if r.second_wind_setup else ""
        msg.append(f"{r.ticker} S:{r.buy_score:.2f} LT:{r.long_term_bonus:.2f} Ext:{r.ext:.2f} {tag}")

    msg.append("")
    msg.append("🔥 EARLY")

    early = df[df.phase == "EARLY"].sort_values("score", ascending=False).head(4)
    msg += [f"{x.ticker} S:{x.score:.2f}" for _, x in early.iterrows()] or ["None"]

    msg.append("")
    msg.append("⚡ TRANSITION")

    trans = df[df.phase == "TRANSITION"].sort_values("score", ascending=False).head(4)
    msg += [f"{x.ticker} S:{x.score:.2f}" for _, x in trans.iterrows()] or ["None"]

    msg.append("")
    msg.append("🔁 CONT")

    cont = df[df.phase == "CONT"].sort_values("score", ascending=False).head(4)
    msg += [f"{x.ticker} S:{x.score:.2f}" for _, x in cont.iterrows()] or ["None"]

    msg.append("")
    msg.append("🌊 SECOND WIND")

    sw = df[df.second_wind_setup].sort_values("score", ascending=False).head(4)
    msg += [f"{x.ticker} S:{x.score:.2f}" for _, x in sw.iterrows()] or ["None"]

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
            r = f.result()
            if r:
                results.append(r)

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    text = build_message(df)

    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})


if __name__ == "__main__":
    run()
