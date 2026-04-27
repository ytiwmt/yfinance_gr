import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v32_25.json"
SCAN_SIZE = 1500
MAX_WORKERS = 12
MIN_PRICE = 5.0
MIN_VOLUME = 500000
HEADERS = {"User-Agent": "Mozilla/5.0"}

TOP_N = 50  # ランク計算用上位母数

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

    def update(self, ticker, score, rank):
        if not np.isfinite(score):
            return

        hist = self.data.get(ticker, [])
        hist.append({
            "t": time.time(),
            "s": float(score),
            "r": int(rank)
        })
        self.data[ticker] = hist[-30:]

    def rank_velocity(self, ticker, current_rank):
        hist = self.data.get(ticker, [])
        if len(hist) < 3:
            return 0.0

        prev_rank = hist[-3]["r"]
        return prev_rank - current_rank  # 上昇ならプラス

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

        m6 = ret(price, c[-120])
        m3 = ret(price, c[-63])
        m1 = ret(price, c[-21])

        vol_recent = np.mean(v[-5:])
        vol_past = np.mean(v[-20:-5])

        if not (vol_recent > vol_past * 1.2 or m3 > 0.4):
            return None

        if (price / min(c[-10:]) - 1) < 0.01:
            return None

        ema20 = pd.Series(c).ewm(span=20).mean().iloc[-1]
        if (price / ema20 - 1) > 0.35:
            return None

        def log(x):
            return np.log1p(max(min(x, 3.0), 0))

        score = (
            0.3 * log(m6) +
            0.3 * ret(np.mean(c[-10:]), np.mean(c[-30:])) +
            0.4 * (log(m1) - log(m3 / 3))
        )

        return {
            "ticker": ticker,
            "score": score
        }

    except:
        return None

# =========================
# CLASSIFY
# =========================
def classify(df):
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1

    early = df[df["score"] > 0.15]
    exp = df[df["score"] > 0.25]
    strong = df[df["score"] > 0.35]

    return early, exp, strong

# =========================
# STATUS
# =========================
def status(rank_change):
    if rank_change >= 15:
        return "NEW!!"
    elif rank_change >= 5:
        return "RISING"
    elif rank_change >= -5:
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
    try:
        txt = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        ).text.split("\n")
    except:
        txt = []

    symbols = list(set([
        s.strip().upper()
        for s in txt
        if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", s)
    ]))

    random.shuffle(symbols)
    universe = symbols[:SCAN_SIZE]

    print(f"🚀 GrowthRadar v32.25 | Scanning {len(universe)}")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            res = f.result()
            if res:
                results.append(res)

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    early, exp, strong = classify(df)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v32.25",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}\n"
    ]

    for name, data, icon in [
        ("EARLY", early, "🔥"),
        ("EXP", exp, "🚀"),
        ("STRONG", strong, "💎")
    ]:
        msg.append(f"{icon} {name}:{len(data)}")

        top = data.head(5)

        for _, r in top.iterrows():
            rank = int(r["rank"])
            ticker = r["ticker"]

            prev_rank = state.data.get(ticker, [{"r": rank}])[-3]["r"] if ticker in state.data else rank
            rank_change = prev_rank - rank

            state.update(ticker, r["score"], rank)

            msg.append(f"{ticker} S:{r['score']:.2f} [{status(rank_change)}]")

        msg.append("")

    state.save()

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})


if __name__ == "__main__":
    run()
