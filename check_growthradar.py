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
# ユニバース（外部依存維持）
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
# データ取得
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

        accel = m1 - (m3 / 3)
        trend = (np.mean(close[-10:]) / np.mean(close[-30:])) - 1
        vol_ratio = volume[-1] / (vol_base + 1e-9)

        # =========================
        # フェーズ定義
        # =========================
        is_early = (0.25 < m1 < 0.9 and m3 < 0.8 and accel > 0.05)

        is_transition = (
            m1 > 0.45 and m3 > 0.45 and abs(m1 - m3) < 0.6
        )

        is_cont = (m3 > 1.0 and trend > 0.02)

        # =========================
        # BREAKOUT（新規）
        # =========================
        breakout = (
            m1 > 0.7 and vol_ratio > 1.8 and abs(m1 - m3) > 0.4
        )

        spike_entry = (
            m1 > 0.5 and m3 < 0.6 and vol_ratio > 2.0
        )

        if not (is_early or is_transition or is_cont or breakout or spike_entry):
            return None

        if breakout or spike_entry:
            phase = "BREAKOUT"
        elif is_early:
            phase = "EARLY"
        elif is_transition:
            phase = "TRANSITION"
        else:
            phase = "CONT"

        return {
            "ticker": ticker,
            "phase": phase,
            "m1": m1,
            "m3": m3,
            "accel": accel,
            "vol_ratio": vol_ratio
        }

    except:
        return None

# =========================
# スコア
# =========================
def score(row):
    return (
        row.get("m1", 0) * 0.6 +
        row.get("m3", 0) * 0.3 +
        row.get("vol_ratio", 1) * 0.1
    )

# =========================
# 実行
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
    df["score"] = df.apply(score, axis=1)

    def top_n(d, n=4):
        if len(d) == 0:
            return d
        return d.sort_values("score", ascending=False).head(n)

    breakout = top_n(df[df.phase == "BREAKOUT"])
    early = top_n(df[df.phase == "EARLY"])
    trans = top_n(df[df.phase == "TRANSITION"])
    cont = top_n(df[df.phase == "CONT"])

    final = pd.concat([breakout, early, trans, cont]).sort_values("score", ascending=False).head(5)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        "🚀 GrowthRadar v37.0",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        "",
        "💎 BUY SIGNAL"
    ]

    # =========================
    # 💎（意思決定層）
    # =========================
    for _, r in final.iterrows():
        msg.append(f"**{r.ticker}**")

    # =========================
    # 分類（Top4制限）
    # =========================
    msg.append("\n🚀 BREAKOUT (Top4)")
    for _, r in breakout.iterrows():
        msg.append(f"{r.ticker} S:{r.score:.2f}")

    msg.append("\n🔥 EARLY (Top4)")
    for _, r in early.iterrows():
        msg.append(f"{r.ticker} S:{r.score:.2f}")

    msg.append("\n⚡ TRANSITION (Top4)")
    for _, r in trans.iterrows():
        msg.append(f"{r.ticker} S:{r.score:.2f}")

    msg.append("\n🔁 CONT (Top4)")
    for _, r in cont.iterrows():
        msg.append(f"{r.ticker} S:{r.score:.2f}")

    text = "\n".join(msg)

    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
