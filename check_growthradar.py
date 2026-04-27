import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v33_4.json"

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
        hist = self.data.get(ticker, [])
        hist.append({"t": time.time(), "s": float(score)})
        self.data[ticker] = hist[-40:]

    def velocity(self, ticker):
        h = self.data.get(ticker, [])
        if len(h) < 5:
            return 0
        return h[-1]["s"] - h[-5]["s"]

# =========================
# UNIVERSE
# =========================
def load_universe():
    try:
        txt = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        ).text.splitlines()
    except:
        txt = []

    clean = list(set([
        s.strip().upper()
        for s in txt
        if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", s)
    ]))

    random.shuffle(clean)
    return clean[:SCAN_SIZE]

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

        avg_vol = np.mean(v[-10:])
        if avg_vol < MIN_VOLUME:
            return None

        def ret(a, b):
            return (a / b - 1) if b > 0 else 0

        m1 = ret(price, c[-21])
        m3 = ret(price, c[-63])
        m6 = ret(price, c[-120])

        vol_ratio = np.mean(v[-5:]) / (np.mean(v[-20:-5]) + 1e-9)

        # =========================
        # ① トレンド評価（長期軸）
        # =========================
        trend_score = (
            np.tanh(m6) * 0.5 +
            np.tanh(m3) * 0.3 +
            np.tanh(m1) * 0.2
        )

        # =========================
        # ② 初動評価（完全別枠）
        # =========================
        breakout = price / max(c[-20:]) - 1
        flow = np.tanh(vol_ratio - 1)

        early_score = breakout * 0.6 + flow * 0.4

        # フィルタ（初動だけは弱めに残す）
        if trend_score < 0.05 and early_score < 0.05:
            return None

        return {
            "ticker": ticker,
            "trend": float(trend_score),
            "early": float(early_score),
            "m1": m1,
            "m3": m3,
            "m6": m6
        }

    except:
        return None

# =========================
# CLASSIFY
# =========================
def classify(df):
    # EARLY = 初動だけ
    early = df[df["early"] > 0.15].sort_values("early", ascending=False)

    # EXP = トレンド + 初動混合
    exp = df[(df["trend"] > 0.25) & (df["early"] > 0.10)]

    # STRONG = トレンド支配
    strong = df[df["trend"] > 0.45]

    return early, exp, strong

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
                state.update(r["ticker"], r["trend"] + r["early"])

    state.save()

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    early, exp, strong = classify(df)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        "🚀 GrowthRadar v33.4",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}\n"
    ]

    for name, d in [("EARLY", early), ("EXP", exp), ("STRONG", strong)]:
        msg.append(f"🔥 {name}:{len(d)}")

        for _, r in d.head(5).iterrows():
            msg.append(
                f"{r['ticker']} "
                f"TR:{r['trend']:.2f} "
                f"ER:{r['early']:.2f}"
            )

        msg.append("")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})


if __name__ == "__main__":
    run()
