import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v32_8.json"
SCAN_SIZE = 1500
MAX_WORKERS = 10
MIN_PRICE = 5.0   # ← ペニー排除
MIN_VOLUME = 500000  # ← 流動性フィルタ
HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# STATE（将来用）
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

# =========================
# UTILS
# =========================
def clip(x, cap=3.0):
    return min(x, cap)

def log_ret(x):
    return np.log1p(max(x, 0))

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = []

    try:
        df = pd.read_csv("https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv")
        symbols += df["Symbol"].tolist()
        print(f"[NASDAQ OK] {len(df)}")
    except:
        print("[NASDAQ FAIL]")

    try:
        txt = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        ).text.split("\n")
        symbols += txt
        print(f"[GitHub OK] {len(txt)}")
    except:
        print("[GitHub FAIL]")

    clean = list(set([
        s.strip().upper()
        for s in symbols
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
        r = session.get(url, timeout=5).json()
        res = r["chart"]["result"][0]

        quote = res["indicators"]["quote"][0]
        close = [c for c in quote["close"] if c is not None]
        volume = [v for v in quote["volume"] if v is not None]

        if len(close) < 120:
            return None

        price = close[-1]
        if price < MIN_PRICE:
            return None

        # 出来高フィルタ
        avg_vol = np.mean(volume[-10:])
        if avg_vol < MIN_VOLUME:
            return None

        def ret(a, b):
            return (a / b - 1) if b > 0 else 0

        m6 = clip(ret(price, close[-120]))
        m3 = clip(ret(price, close[-63]))
        m1 = clip(ret(price, close[-21]))

        # log圧縮
        m6 = log_ret(m6)
        m3 = log_ret(m3)
        m1 = log_ret(m1)

        ma10 = np.mean(close[-10:])
        ma30 = np.mean(close[-30:])
        trend = ret(ma10, ma30)

        accel = m1 - (m3 / 3)

        score = (0.3 * m6) + (0.3 * trend) + (0.4 * accel)

        return {
            "ticker": ticker,
            "score": score,
            "price": price,
            "m6": m6,
            "m3": m3,
            "m1": m1,
            "trend": trend,
            "accel": accel,
            "vol": avg_vol
        }

    except:
        return None

# =========================
# DETECTOR
# =========================
def detect(df):
    early = []
    expansion = []
    strong = []

    for _, r in df.iterrows():
        s = r["score"]

        if s > 0.10 and r["m1"] > 0:
            early.append(r)

        if s > 0.20 and r["m3"] > 0:
            expansion.append(r)

        if s > 0.30 and r["m6"] > 0:
            strong.append(r)

    early = sorted(early, key=lambda x: x["score"], reverse=True)
    expansion = sorted(expansion, key=lambda x: x["score"], reverse=True)
    strong = sorted(strong, key=lambda x: x["score"], reverse=True)

    return early, expansion, strong

# =========================
# REPORT
# =========================
def report(early, expansion, strong, scanned, valid):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v32.8",
        f"Scanned:{scanned} Valid:{valid}",
        f"EARLY:{len(early)} EXP:{len(expansion)} STRONG:{len(strong)}",
        f"Time:{now}",
        ""
    ]

    msg.append("🔥 EARLY")
    for c in early[:10]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f} M1:{c['m1']:.2f}")

    msg.append("\n🚀 EXPANSION")
    for c in expansion[:10]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f} M3:{c['m3']:.2f}")

    msg.append("\n💎 STRONG")
    for c in strong[:10]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f} M6:{c['m6']:.2f}")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        try:
            requests.post(WEBHOOK_URL, json={"content": text[:1900]})
        except:
            pass

# =========================
# MAIN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    state = State()
    universe = load_universe()

    print(f"Scanning {len(universe)} tickers...")

    raw = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            res = f.result()
            if res:
                raw.append(res)

    if not raw:
        print("NO DATA")
        return

    df = pd.DataFrame(raw)

    for _, r in df.iterrows():
        state.update(r["ticker"], r["score"])
    state.save()

    early, expansion, strong = detect(df)

    report(early, expansion, strong, len(universe), len(df))


if __name__ == "__main__":
    run()
