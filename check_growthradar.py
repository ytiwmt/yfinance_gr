import os, requests, pandas as pd, numpy as np, random, re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_SIZE = 1500
MAX_WORKERS = 14

MIN_PRICE = 5.0
MIN_BASE_VOLUME = 300000

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = []

    try:
        r = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        )
        if r.status_code == 200:
            symbols += r.text.splitlines()
    except:
        pass

    symbols = [
        s.strip().upper()
        for s in symbols
        if isinstance(s, str) and re.match(r"^[A-Z0-9\.\-]{1,10}$", s)
    ]

    symbols = list(set(symbols))

    if len(symbols) < 500:
        symbols += ["AAPL","NVDA","MSFT","AMD","AMZN","META"] * 200

    random.shuffle(symbols)
    return symbols[:SCAN_SIZE]

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
        r = session.get(url, timeout=5)
        if r.status_code != 200:
            return None

        data = r.json()
        res = data["chart"]["result"][0]
        q = res["indicators"]["quote"][0]

        c = [x for x in q["close"] if x is not None]
        v = [x for x in q["volume"] if x is not None]

        if len(c) < 60:
            return None

        price = c[-1]
        if price < MIN_PRICE:
            return None

        vol_base = np.mean(v[-20:-5])
        if vol_base < MIN_BASE_VOLUME:
            return None

        def ret(a,b): return (a/b - 1) if b > 0 else 0

        m1 = ret(price, c[-21])
        m3 = ret(price, c[-63])

        accel = m1 - (m3 / 3)

        ma10 = np.mean(c[-10:])
        ma30 = np.mean(c[-30:])
        trend = ret(ma10, ma30)

        # =========================
        # フェーズ設計（妥当版）
        # =========================

        # EARLY：純初動
        is_early = (
            m1 > 0.25 and m1 < 0.9 and
            m3 < 0.8 and
            accel > 0.06
        )

        # TRANSITION：核心（AEHR/BBGI型含む）
        # → 重要：乖離＋未完成トレンド
        is_transition = (
            m1 > 0.5 and
            m3 > 0.5 and
            m3 < 1.2 and
            m1 > m3 * 1.05 and   # 先行性
            abs(m1 - m3) <= 0.5 and
            trend > 0.01
        )

        # CONT：トレンド確定
        is_cont = (
            m3 > 1.0 and
            m1 < m3 and
            trend > 0.02
        )

        if not (is_early or is_transition or is_cont):
            return None

        phase = "EARLY" if is_early else "TRANSITION" if is_transition else "CONT"

        return {
            "ticker": ticker,
            "phase": phase,
            "m1": m1,
            "m3": m3,
            "accel": accel,
            "trend": trend
        }

    except:
        return None

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

    early_df = df[df["phase"] == "EARLY"].sort_values("accel", ascending=False)
    trans_df = df[df["phase"] == "TRANSITION"].sort_values("m1", ascending=False)
    cont_df  = df[df["phase"] == "CONT"].sort_values("m3", ascending=False)

    # =========================
    # BUY LOGIC
    # =========================
    buy = []

    for _, r in early_df.head(2).iterrows():
        buy.append(r["ticker"])

    for _, r in trans_df.iterrows():
        if r["ticker"] not in buy:
            buy.append(r["ticker"])
        if len(buy) >= 5:
            break

    for _, r in cont_df.iterrows():
        if r["ticker"] not in buy:
            buy.append(r["ticker"])
        if len(buy) >= 6:
            break

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        "🚀 GrowthRadar v36.5",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        "",
        "💎 BUY SIGNAL"
    ]

    for t in buy:
        msg.append(f"**{t}**")

    msg.append("\n🔥 EARLY")
    for _, r in early_df.head(10).iterrows():
        msg.append(f"{r['ticker']} A:{r['accel']:.2f} M1:{r['m1']:.2f}")

    msg.append("\n⚡ TRANSITION")
    for _, r in trans_df.head(10).iterrows():
        msg.append(f"{r['ticker']} M1:{r['m1']:.2f} M3:{r['m3']:.2f}")

    msg.append("\n🔁 CONT")
    for _, r in cont_df.head(10).iterrows():
        msg.append(f"{r['ticker']} M3:{r['m3']:.2f}")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        if len(text) > 1900:
            text = text[:1900] + "\n…"
        requests.post(WEBHOOK_URL, json={"content": text})

if __name__ == "__main__":
    run()
