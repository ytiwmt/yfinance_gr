import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v34_3.json"

SCAN_SIZE = 1500
MAX_WORKERS = 14

MIN_PRICE = 5.0
MIN_BASE_VOLUME = 300000   # 引き上げ（マイクロ排除）

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
        hist = self.data.get(ticker, [])
        hist.append({"t": time.time(), "s": float(score)})
        self.data[ticker] = hist[-40:]

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

    try:
        df = pd.read_csv(
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        )
        symbols += df["Symbol"].tolist()
    except:
        pass

    symbols = [
        s.strip().upper()
        for s in symbols
        if isinstance(s, str) and re.match(r"^[A-Z0-9\.\-]{1,10}$", s)
    ]

    symbols = list(set(symbols))

    if len(symbols) < 500:
        symbols += ["AAPL","NVDA","MSFT","AMD","AMZN","META","GOOGL"] * 200

    random.shuffle(symbols)
    return symbols[:SCAN_SIZE]

# =========================
# FETCH（AEHR型）
# =========================
def fetch(session, ticker):
    try:
        # SPAC排除
        if ticker.endswith(("U","W","R")):
            return None

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=6mo&interval=1d"
        r = session.get(url, timeout=5)

        if r.status_code != 200:
            return None

        data = r.json()
        res = data["chart"]["result"][0]
        q = res["indicators"]["quote"][0]

        c = [x for x in q["close"] if x is not None]
        v = [x for x in q["volume"] if x is not None]

        if len(c) < 60 or len(v) < 60:
            return None

        price = c[-1]

        # ---- 基本フィルタ ----
        if price < MIN_PRICE:
            return None

        vol_now = np.mean(v[-5:])
        vol_base = np.mean(v[-20:-5])

        if vol_base < MIN_BASE_VOLUME:
            return None

        vol_spike = vol_now / (vol_base + 1e-9)
        vol_spike = min(vol_spike, 5)

        def ret(a,b):
            return (a/b - 1) if b > 0 else 0

        m1 = ret(price, c[-21])
        m3 = ret(price, c[-63])

        # ---- 初動条件（厳格化）----
        if m1 < 0.05:
            return None

        accel = m1 - (m3 / 3)
        if accel <= 0:
            return None

        # ---- ノイズ排除 ----
        if vol_spike >= 5 and m1 < 0.1:
            return None

        volatility = np.std(c[-10:]) / price
        if volatility > 0.12:
            return None

        # トレンド確認（AEHR型）
        ma10 = np.mean(c[-10:])
        ma30 = np.mean(c[-30:])
        trend = ret(ma10, ma30)

        if trend < 0:
            return None

        # ---- スコア ----
        score = (
            0.30 * vol_spike +
            0.40 * accel +
            0.30 * trend
        )

        if score <= 0 or score > 5:
            return None

        return {
            "ticker": ticker,
            "score": score,
            "vol": vol_spike,
            "m1": m1,
            "accel": accel,
            "trend": trend
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

    print(f"🚀 GrowthRadar v34.3 scanning {len(universe)}")

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
    df = df.sort_values("score", ascending=False)

    top = df.head(10)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v34.3",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        "",
        "🔥 AEHR-TYPE EARLY"
    ]

    for _, r in top.iterrows():
        msg.append(
            f"{r['ticker']} "
            f"S:{r['score']:.2f} "
            f"VOL:{r['vol']:.2f} "
            f"M1:{r['m1']:.2f} "
            f"A:{r['accel']:.2f}"
        )

    text = "\n".join(msg)

    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
