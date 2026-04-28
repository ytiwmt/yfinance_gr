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
# v36.4構造維持（外部ユニバース）
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

    # フォールバック（最低限）
    fallback = [
        "AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL","TSLA",
        "INTC","QCOM","AVGO","TSM","ASML","MU","PLTR","SNOW","CRWD"
    ]

    symbols.update(fallback)

    # シャッフル（重要：偏り防止）
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

        # =========================
        # v36.4-P 改良ポイント
        # =========================

        # AEHR型を拾うための「歪み検出」
        structural_shift = abs(m1 - m3) > 0.35 and m1 > 0.4

        is_early = (
            0.25 < m1 < 0.9 and
            m3 < 0.8 and
            accel > 0.05
        )

        # ★ここが改善ポイント
        is_transition = (
            (m1 > 0.45 and m3 > 0.45) and
            (abs(m1 - m3) < 0.6 or structural_shift)
        )

        is_cont = (
            m3 > 1.0 and trend > 0.02
        )

        if not (is_early or is_transition or is_cont):
            return None

        phase = "EARLY" if is_early else "TRANSITION" if is_transition else "CONT"

        return {
            "ticker": ticker,
            "phase": phase,
            "m1": m1,
            "m3": m3,
            "accel": accel
        }

    except:
        return None

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

    early = df[df.phase == "EARLY"].sort_values("accel", ascending=False)
    trans = df[df.phase == "TRANSITION"].sort_values("m1", ascending=False)
    cont = df[df.phase == "CONT"].sort_values("m3", ascending=False)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        "🚀 GrowthRadar v36.4-P",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        "",
        "💎 BUY SIGNAL"
    ]

    for _, r in early.head(3).iterrows():
        msg.append(f"💎 {r.ticker}")

    msg.append("\n🔥 EARLY")
    for _, r in early.head(10).iterrows():
        msg.append(f"{r.ticker} A:{r.accel:.2f} M1:{r.m1:.2f}")

    msg.append("\n⚡ TRANSITION")
    for _, r in trans.head(10).iterrows():
        msg.append(f"{r.ticker} M1:{r.m1:.2f} M3:{r.m3:.2f}")

    msg.append("\n🔁 CONT")
    for _, r in cont.head(10).iterrows():
        msg.append(f"{r.ticker} M3:{r.m3:.2f}")

    text = "\n".join(msg)

    print(text)

    if WEBHOOK_URL:
        if len(text) > 1900:
            text = text[:1900]
        requests.post(WEBHOOK_URL, json={"content": text})

if __name__ == "__main__":
    run()
