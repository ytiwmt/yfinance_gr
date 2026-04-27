import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v33.json"

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOLUME = 500000

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ユニバース安定比率（ここが重要）
STABLE_RATIO = 0.8   # 80%固定
RANDOM_RATIO = 0.2   # 20%探索

# =========================
# STATE
# =========================
class State:
    def __init__(self):
        self.data = self.load()

    def load(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def save(self):
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(self.data, f)
        except:
            pass

    def update(self, ticker, score):
        hist = self.data.get(ticker, [])
        hist.append({"t": time.time(), "s": float(score)})
        self.data[ticker] = hist[-30:]

    def get_velocity(self, ticker):
        hist = self.data.get(ticker, [])
        if len(hist) < 3:
            return 0.0
        return hist[-1]["s"] - hist[-3]["s"]

# =========================
# UNIVERSE（重要改善）
# =========================
def load_universe():
    symbols = []

    # NASDAQ
    try:
        df = pd.read_csv(
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        )
        symbols += df["Symbol"].tolist()
    except:
        pass

    # GitHub
    try:
        txt = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        ).text.splitlines()
        symbols += txt
    except:
        pass

    clean = list(set([
        s.strip().upper()
        for s in symbols
        if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", s)
    ]))

    clean.sort()  # ★重要：固定ベース

    # =========================
    # ハイブリッドサンプリング
    # =========================
    stable_size = int(len(clean) * STABLE_RATIO)
    stable = clean[:stable_size]

    random_pool = clean[stable_size:]
    random_sample = random.sample(random_pool, min(len(random_pool), SCAN_SIZE - stable_size))

    universe = stable[:SCAN_SIZE - len(random_sample)] + random_sample

    return universe[:SCAN_SIZE]

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
        if "chart" not in data or not data["chart"]["result"]:
            return None

        res = data["chart"]["result"][0]
        q = res["indicators"]["quote"][0]

        c = [x for x in q["close"] if x is not None]
        v = [x for x in q["volume"] if x is not None]

        if len(c) < 120:
            return None

        price = c[-1]
        if price < MIN_PRICE:
            return None

        avg_vol = np.mean(v[-10:])
        if avg_vol < MIN_VOLUME:
            return None

        def ret(a, b):
            return (a / b - 1) if b > 0 else 0

        m6 = ret(price, c[-120])
        m3 = ret(price, c[-63])
        m1 = ret(price, c[-21])

        vol_recent = np.mean(v[-5:])
        vol_past = np.mean(v[-20:-5])

        # 初動 or トレンド継続
        if not (vol_recent > vol_past * 1.5 or m3 > 0.4):
            return None

        # 押し目制御
        if (price / min(c[-10:]) - 1) < 0.03:
            return None

        def log(x):
            return np.log1p(max(min(x, 3), 0))

        m6, m3, m1 = log(m6), log(m3), log(m1)

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
# RUN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    state = State()
    universe = load_universe()

    print(f"Scanning {len(universe)} tickers...")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)
                state.update(r["ticker"], r["score"])

    state.save()

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    early = df[df["score"] > 0.10].sort_values("score", ascending=False)
    exp = df[df["score"] > 0.20].sort_values("score", ascending=False)
    strong = df[df["score"] > 0.30].sort_values("score", ascending=False)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v33",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}\n"
    ]

    for name, d in [("EARLY", early), ("EXP", exp), ("STRONG", strong)]:
        msg.append(f"🔥 {name}")
        for _, r in d.head(5).iterrows():
            v = state.get_velocity(r["ticker"])
            status = "NEW" if v > 0.2 else "KEEP" if v > 0 else "SLOW"
            msg.append(f"{r['ticker']} S:{r['score']:.2f} [{status}]")
        msg.append("")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})


if __name__ == "__main__":
    run()
