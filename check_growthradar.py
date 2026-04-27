import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v33_6.json"

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOLUME = 500000

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# STATE
# =========================
class State:
    def __init__(self):
        self.data = self.load()

    def load(self):
        if os.path.exists(STATE_FILE):
            try:
                return json.load(open(STATE_FILE))
            except:
                return {}
        return {}

    def save(self):
        try:
            json.dump(self.data, open(STATE_FILE, "w"))
        except:
            pass

    def update(self, ticker, score):
        if not np.isfinite(score):
            return
        hist = self.data.get(ticker, [])
        hist.append({"t": time.time(), "s": float(score)})
        self.data[ticker] = hist[-30:]

    def get_velocity(self, ticker):
        hist = self.data.get(ticker, [])
        if len(hist) < 3:
            return 0.0
        return hist[-1]["s"] - hist[-3]["s"]

# =========================
# UNIVERSE（完全修正版）
# =========================
def load_universe():
    symbols = []

    try:
        res = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        )

        if res.status_code == 200:
            txt = res.text.splitlines()

            for s in txt:
                if not isinstance(s, str):
                    continue

                s = s.strip().upper()

                # ★ 修正ポイント：柔軟化
                if len(s) < 1 or len(s) > 10:
                    continue

                # . / - / 数字許容（BRK.B / S&P系対応）
                if not re.match(r"^[A-Z0-9\.\-]+$", s):
                    continue

                symbols.append(s)

    except:
        pass

    symbols = list(set(symbols))

    # ★ 絶対死なない保証
    if len(symbols) < 200:
        symbols += ["AAPL", "NVDA", "TSLA", "AMD", "META", "MSFT", "AMZN", "GOOGL"]

    random.shuffle(symbols)
    return symbols[:SCAN_SIZE]

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        r = session.get(url, timeout=5)

        if r.status_code != 200:
            return None

        data = r.json()
        res = data["chart"]["result"][0]
        q = res["indicators"]["quote"][0]

        c = [x for x in q["close"] if x is not None]
        v = [x for x in q["volume"] if x is not None]

        if len(c) < 120:
            return None

        price = c[-1]
        if price < MIN_PRICE:
            return None

        if np.mean(v[-10:]) < MIN_VOLUME:
            return None

        def ret(a, b):
            return (a / b - 1) if b > 0 else 0

        m6 = ret(price, c[-120])
        m3 = ret(price, c[-63])
        m1 = ret(price, c[-21])

        vol_recent = np.mean(v[-5:])
        vol_past = np.mean(v[-20:-5])

        if not (vol_recent > vol_past * 1.5 or m3 > 0.4):
            return None

        if (price / min(c[-10:]) - 1) < 0.03:
            return None

        def log_ret(x):
            return np.log1p(max(min(x, 3.0), 0))

        m6, m3, m1 = log_ret(m6), log_ret(m3), log_ret(m1)

        trend = ret(np.mean(c[-10:]), np.mean(c[-30:]))
        accel = m1 - (m3 / 3)

        score = (0.3 * m6) + (0.3 * trend) + (0.4 * accel)

        return {
            "ticker": ticker,
            "score": score,
            "m1": m1,
            "m3": m3,
            "m6": m6,
            "accel": accel
        }

    except:
        return None

# =========================
# MAIN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    state = State()

    universe = load_universe()

    print(f"Scanning {len(universe)} stocks...")

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}

        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)
                state.update(res["ticker"], res["score"])

    state.save()

    if len(results) == 0:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    early = df[df["score"] > 0.10].sort_values("score", ascending=False)
    exp = df[df["score"] > 0.20].sort_values("score", ascending=False)
    strong = df[df["score"] > 0.30].sort_values("score", ascending=False)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v33.6",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}\n",
        "🔥 EARLY"
    ]

    for _, r in early.head(5).iterrows():
        msg.append(f"{r['ticker']} S:{r['score']:.2f}")

    msg.append("\n🚀 EXP")
    for _, r in exp.head(5).iterrows():
        msg.append(f"{r['ticker']} S:{r['score']:.2f}")

    msg.append("\n💎 STRONG")
    for _, r in strong.head(5).iterrows():
        msg.append(f"{r['ticker']} S:{r['score']:.2f}")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
