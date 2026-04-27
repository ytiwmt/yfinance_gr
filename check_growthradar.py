import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v32_24.json"
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
        if not np.isfinite(score):
            return
        hist = self.data.get(ticker, [])
        hist.append({"t": time.time(), "s": float(score)})
        self.data[ticker] = hist[-30:]

    def velocity(self, ticker):
        hist = self.data.get(ticker, [])
        if len(hist) < 3:
            return 0.0
        prev = hist[-3]["s"]
        curr = hist[-1]["s"]
        if abs(prev) < 1e-6:
            return 0.0
        return (curr - prev) / abs(prev)

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    for _ in range(2):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = session.get(url, timeout=5)

            if r.status_code == 429:
                time.sleep(1)
                continue

            data = r.json()
            res = data["chart"]["result"][0]
            q = res["indicators"]["quote"][0]

            c = [x for x in q["close"] if x is not None]
            v = [x for x in q["volume"] if x is not None]

            if len(c) < 120 or len(v) < 120:
                return None

            price = c[-1]
            if price < MIN_PRICE:
                return None

            avg_vol = np.mean(v[-10:])
            if avg_vol < MIN_VOLUME:
                return None

            def ret(a, b):
                return (a / b - 1) if b > 0 else 0

            m6_raw = ret(price, c[-120])
            m3_raw = ret(price, c[-63])
            m1_raw = ret(price, c[-21])

            # ===== フィルタ緩和（重要） =====
            vol_recent = np.mean(v[-5:])
            vol_past = np.mean(v[-20:-5])

            if not (vol_recent > vol_past * 1.2 or m3_raw > 0.4):
                return None

            # 押し目（かなり緩和）
            if (price / min(c[-10:]) - 1) < 0.01:
                return None

            # 過熱緩和
            ema20 = pd.Series(c).ewm(span=20).mean().iloc[-1]
            if (price / ema20 - 1) > 0.35:
                return None

            # ===== スコア =====
            def log_ret(x):
                return np.log1p(max(min(x, 3.0), 0))

            m6 = log_ret(m6_raw)
            m3 = log_ret(m3_raw)
            m1 = log_ret(m1_raw)

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
            time.sleep(0.5)

    return None

# =========================
# CLASSIFY
# =========================
def classify(df):
    # 初動だけに限定（重要）
    early = df[
        (df['m1'] > 0.10) &
        (df['accel'] > 0.02) &
        (df['score'] < 0.75)
    ].sort_values("score", ascending=False)

    expansion = df[
        (df['score'] > 0.20) &
        (df['m3'] > 0.2) &
        (df['accel'] > 0)
    ].sort_values("score", ascending=False)

    strong = df[
        (df['score'] > 0.30) &
        (df['m3'] > 0.4) &
        (df['m6'] > 0.5)
    ].sort_values("score", ascending=False)

    return early, expansion, strong

# =========================
# STATUS
# =========================
def status(v):
    if v > 0.005:
        return "NEW!!"
    elif v > 0.001:
        return "RISING"
    elif v > -0.002:
        return "KEEP"
    else:
        return "DROP"

# =========================
# MAIN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)
    state = State()

    # universe
    symbols = []
    try:
        txt = requests.get("https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt").text.split("\n")
        symbols += txt
    except:
        pass

    try:
        df = pd.read_csv("https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv")
        symbols += df["Symbol"].tolist()
    except:
        pass

    clean = list(set([
        s.strip().upper()
        for s in symbols
        if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", s)
    ]))

    random.shuffle(clean)
    universe = clean[:SCAN_SIZE]

    print(f"🚀 GrowthRadar v32.24 | Scanning {len(universe)}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)
                state.update(res["ticker"], res["score"])

    state.save()

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    early, exp, strong = classify(df)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v32.24",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}\n"
    ]

    for name, data, icon in [
        ("EARLY", early, "🔥"),
        ("EXP", exp, "🚀"),
        ("STRONG", strong, "💎")
    ]:
        msg.append(f"{icon} {name}:{len(data)}")
        for _, r in data.head(5).iterrows():
            v = state.velocity(r['ticker'])
            msg.append(f"{r['ticker']} S:{r['score']:.2f} [{status(v)}]")
        msg.append("")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})


if __name__ == "__main__":
    run()
