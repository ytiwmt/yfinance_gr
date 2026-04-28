import os, requests, random, re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np

WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# Universe
# =========================
def load_universe():
    symbols = set()

    try:
        url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
        r = requests.get(url, timeout=10)
        lines = r.text.splitlines()[1:]

        for l in lines:
            sym = l.split(",")[0].strip().upper()
            if re.match(r"^[A-Z]{1,6}$", sym):
                symbols.add(sym)
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
# Fetch
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
        r = session.get(url, timeout=5)
        if r.status_code != 200:
            return None

        data = r.json()
        result = data["chart"]["result"][0]

        close = result["indicators"]["quote"][0]["close"]
        volume = result["indicators"]["quote"][0]["volume"]

        close = [x for x in close if x is not None]
        volume = [x for x in volume if x is not None]

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

        # =========================
        # CORE PHASES
        # =========================

        phase = "NONE"

        if (0.25 < m1 < 0.7 and m3 < 0.6):
            phase = "EARLY"

        elif (m1 > 0.45 and m3 > 0.45):
            phase = "TRANSITION"

        elif (m3 > 1.0):
            phase = "CONT"

        # =========================
        # BREAKOUT (PURE EVENT ONLY)
        # =========================
        # ★完全非スコア化

        price_jump = abs(close[-1] - close[-2]) / close[-2]
        volume_spike = volume[-1] / (vol_base + 1e-9)

        breakout_event = (
            price_jump > 0.03 and
            volume_spike > 2.2
        )

        # スコアはもう使わない（分析用に残すだけ）
        score = (
            m1 * 0.6 +
            m3 * 0.3 +
            vol_ratio * 0.1
        )

        return {
            "ticker": ticker,
            "phase": phase,
            "score": score,
            "m1": m1,
            "m3": m3,
            "vol_ratio": vol_ratio,
            "breakout": breakout_event
        }

    except:
        return None

# =========================
# DIAMOND
# =========================
def build_diamond(df):
    trans = df[df.phase == "TRANSITION"].copy()

    if len(trans) == 0:
        return pd.DataFrame()

    trans = trans.sort_values("score", ascending=False)

    diamond = []
    prev = None

    for _, r in trans.iterrows():
        gap = 0.0 if prev is None else prev.score - r.score

        if prev is None or gap >= 0.15 or r.score > trans.score.quantile(0.85):
            diamond.append({
                "ticker": r.ticker,
                "score": r.score,
                "gap": gap
            })

        prev = r

        if len(diamond) >= 5:
            break

    return pd.DataFrame(diamond)

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

    diamond = build_diamond(df)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        "🚀 GrowthRadar v37.8 (PURE EVENT BREAKOUT)",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        "",
        "💎 BUY SIGNAL"
    ]

    if len(diamond) == 0:
        msg.append("None")
    else:
        for _, r in diamond.iterrows():
            msg.append(f"**{r.ticker}** S:{r.score:.2f} GAP:{r.gap:.2f}")

    early = df[df.phase=="EARLY"].sort_values("score", ascending=False).head(4)
    msg.append("\n🔥 EARLY")
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in early.iterrows()] or ["None"]

    trans = df[df.phase=="TRANSITION"].sort_values("score", ascending=False).head(4)
    msg.append("\n⚡ TRANSITION")
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in trans.iterrows()] or ["None"]

    cont = df[df.phase=="CONT"].sort_values("score", ascending=False).head(4)
    msg.append("\n🔁 CONT")
    msg += [f"{r.ticker} S:{r.score:.2f}" for _, r in cont.iterrows()] or ["None"]

    # BREAKOUT = pure event log only
    brk = df[df.breakout].head(4)
    msg.append("\n🧨 BREAKOUT (event-only)")
    msg += [f"{r.ticker}" for _, r in brk.iterrows()] or ["None"]

    # ★重要：強制空行（確実に入れる）
    msg.append("")

    text = "\n".join(msg)

    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
