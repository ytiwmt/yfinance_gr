import os, requests, pandas as pd, numpy as np, random, re, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
SCAN_SIZE = 1500
MAX_WORKERS = 10
MIN_PRICE = 5.0
MIN_VOLUME_USD = 2000000
HEADERS = {"User-Agent": "Mozilla/5.0"}

TOP = 5

# =========================
# UTILS
# =========================
def get_ret(a, b):
    return (a / b - 1) if b > 0 else 0

def send_webhook(text):
    if not WEBHOOK_URL:
        return
    try:
        requests.post(WEBHOOK_URL, json={"content": text})
        time.sleep(1.2)
    except:
        pass

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        r = session.get(url, timeout=5).json()
        res = r["chart"]["result"][0]

        q = res["indicators"]["quote"][0]
        c = [x for x in q["close"] if x is not None]
        v = [x for x in q["volume"] if x is not None]
        h = [x for x in q["high"] if x is not None]
        l = [x for x in q["low"] if x is not None]

        if len(c) < 150:
            return None

        price = c[-1]
        if price < MIN_PRICE:
            return None

        # ===== 流動性 =====
        avg_vol10 = np.mean(v[-10:])
        if avg_vol10 * price < MIN_VOLUME_USD:
            return None

        vol_mom = avg_vol10 / np.mean(v[-50:-10])

        # ===== モメンタム =====
        m1 = get_ret(price, c[-21])
        m3 = get_ret(price, c[-63])
        m6 = get_ret(price, c[-126])

        # ===== トレンド構造 =====
        ma50 = np.mean(c[-50:])
        ma150 = np.mean(c[-150:])
        ma200 = np.mean(c[-200:]) if len(c) >= 200 else ma150

        if not (price > ma50 > ma150 > ma200):
            return None

        # ★ 乖離チェック（重要）
        if price < ma50 * 1.05:
            return None

        # ===== VCP（改良版） =====
        range_recent = np.mean([hi - lo for hi, lo in zip(h[-5:], l[-5:])])
        range_past = np.mean([hi - lo for hi, lo in zip(h[-20:], l[-20:])])
        vcp = range_recent / range_past if range_past > 0 else 1

        # ★ VCP + 資金流入
        if not (vcp < 0.8 and vol_mom > 1.2):
            return None

        # ===== 崩れにくさ =====
        recent_min = min(c[-10:])
        if (price / recent_min - 1) < 0.05:
            return None

        # ===== スコア（中期主導） =====
        score = (
            (m3 * 0.4) +
            (m6 * 0.3) +
            (m1 * 0.1) +
            (vol_mom * 0.2)
        )

        return {
            "ticker": ticker,
            "score": round(score, 3),
            "m1": round(m1, 2),
            "m3": round(m3, 2),
            "m6": round(m6, 2),
            "vol": round(vol_mom, 2),
            "vcp": round(vcp, 2)
        }

    except:
        return None

# =========================
# MAIN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    print("Fetching tickers...")
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        tickers = requests.get(url).text.splitlines()
        tickers = [t for t in tickers if re.match(r"^[A-Z]{1,5}$", t)]
        random.shuffle(tickers)
        targets = tickers[:SCAN_SIZE]
    except:
        targets = ["AAPL","NVDA","TSLA"]

    print(f"Scanning {len(targets)}...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in targets}
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)

    if not results:
        print("NO HITS")
        return

    df = pd.DataFrame(results)
    df = df.sort_values("score", ascending=False)

    print(f"\n--- RESULT ({datetime.now().strftime('%H:%M')}) ---")
    print(df.head(10).to_string(index=False))

    # webhook
    msg = "\n".join([f"{r['ticker']} S:{r['score']}" for _, r in df.head(TOP).iterrows()])
    send_webhook(msg)


if __name__ == "__main__":
    run()
