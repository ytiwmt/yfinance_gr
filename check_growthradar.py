import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v34.json"

SCAN_SIZE = 1500
MAX_WORKERS = 14

MIN_PRICE = 2.0        # 初動なのでさらに緩い
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

    def slope(self, ticker):
        h = self.data.get(ticker, [])
        if len(h) < 5:
            return 0.0
        return (h[-1]["s"] - h[-5]["s"])

# =========================
# UNIVERSE（広く拾う）
# =========================
def load_universe():
    symbols = []

    try:
        r = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        )

        if r.status_code == 200:
            for s in r.text.splitlines():
                if not isinstance(s, str):
                    continue

                s = s.strip().upper()

                if len(s) < 1 or len(s) > 10:
                    continue

                if not re.match(r"^[A-Z0-9\.\-]+$", s):
                    continue

                symbols.append(s)

    except:
        pass

    # 最低保証
    if len(symbols) < 300:
        symbols += ["AAPL","NVDA","TSLA","AMD","META","MSFT","AMZN"]

    return random.sample(list(set(symbols)), SCAN_SIZE)

# =========================
# FETCH（初動検出コア）
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

        vol_now = np.mean(v[-5:])
        vol_base = np.mean(v[-20:-5])

        def ret(a,b):
            return (a/b - 1) if b > 0 else 0

        m1 = ret(price, c[-21])
        m3 = ret(price, c[-63])

        # =========================
        # ■ 初動検出ロジック（核心）
        # =========================

        # ① 出来高“異常”だけ拾う（通常上昇は捨てる）
        vol_spike = vol_now / (vol_base + 1e-9)

        # ② まだブレイク前の圧縮状態
        recent_range = (max(c[-10:]) - min(c[-10:])) / price

        # ③ 小さな上昇 + 出来高 + 圧縮
        early_trigger = (
            vol_spike > 1.3 or
            (m1 > 0.05 and vol_spike > 1.1) or
            (m3 > 0.15 and recent_range < 0.15)
        )

        if not early_trigger:
            return None

        # ④ 「静かな上昇」だけ残す
        if m1 > 0.6:
            return None

        # =========================
        # スコア（初動寄り）
        # =========================
        volatility = np.std(c[-10:]) / price
        accel = m1 - (m3 / 3)

        score = (
            0.45 * vol_spike +
            0.30 * accel +
            0.25 * (1 - volatility)
        )

        return {
            "ticker": ticker,
            "score": score,
            "m1": m1,
            "m3": m3,
            "vol": vol_spike
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

    print(f"🚀 v34 scanning {len(universe)}")

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

    # 初動は広く
    early = df.sort_values("score", ascending=False).head(10)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v34 (EARLY MODE)",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}\n",
        "🔥 EARLY SIGNALS"
    ]

    for _, r in early.iterrows():
        msg.append(f"{r['ticker']} S:{r['score']:.2f} VOL:{r['vol']:.2f}")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
