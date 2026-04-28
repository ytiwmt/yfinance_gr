import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v34_2.json"

SCAN_SIZE = 1500
MAX_WORKERS = 14

MIN_PRICE = 3.0          # 低すぎ排除
MIN_BASE_VOLUME = 100000 # 流動性フィルタ

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
# UNIVERSE（耐障害）
# =========================
def load_universe():
    symbols = []

    # GitHub
    try:
        r = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            timeout=10
        )
        if r.status_code == 200:
            symbols += r.text.splitlines()
    except:
        print("[GitHub FAIL]")

    # NASDAQ
    try:
        df = pd.read_csv(
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        )
        symbols += df["Symbol"].tolist()
    except:
        print("[NASDAQ FAIL]")

    # clean
    symbols = [
        s.strip().upper()
        for s in symbols
        if isinstance(s, str) and re.match(r"^[A-Z0-9\.\-]{1,10}$", s)
    ]

    symbols = list(set(symbols))

    fallback = ["AAPL","NVDA","TSLA","AMD","META","MSFT","AMZN","GOOGL"]

    if len(symbols) < 300:
        print("⚠ universe不足 → fallback拡張")
        symbols += fallback * 200

    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]

# =========================
# FETCH（初動 + ノイズ除去）
# =========================
def fetch(session, ticker):
    try:
        # --- SPAC/ゴミ除去 ---
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

        # --- 価格フィルタ ---
        if price < MIN_PRICE:
            return None

        # --- 出来高 ---
        vol_now = np.mean(v[-5:])
        vol_base = np.mean(v[-20:-5])

        if vol_base < MIN_BASE_VOLUME:
            return None

        vol_spike = vol_now / (vol_base + 1e-9)

        # --- 異常クリップ ---
        vol_spike = min(vol_spike, 5)

        def ret(a,b):
            return (a/b - 1) if b > 0 else 0

        m1 = ret(price, c[-21])
        m3 = ret(price, c[-63])

        # --- 圧縮状態 ---
        recent_range = (max(c[-10:]) - min(c[-10:])) / price

        # =========================
        # 初動条件
        # =========================
        if not (
            vol_spike > 1.3 or
            (m1 > 0.05 and vol_spike > 1.1) or
            (m3 > 0.15 and recent_range < 0.15)
        ):
            return None

        # --- 過熱排除 ---
        if m1 > 0.6:
            return None

        # --- ボラ ---
        volatility = np.std(c[-10:]) / price
        if volatility > 0.15:
            return None

        accel = m1 - (m3 / 3)

        score = (
            0.45 * vol_spike +
            0.30 * accel +
            0.25 * (1 - volatility)
        )

        # --- 異常スコア排除 ---
        if score > 5 or score < 0:
            return None

        return {
            "ticker": ticker,
            "score": score,
            "vol": vol_spike,
            "m1": m1
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

    print(f"🚀 GrowthRadar v34.2 scanning {len(universe)}")

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
    top = df.sort_values("score", ascending=False).head(10)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v34.2",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        "",
        "🔥 EARLY SIGNALS"
    ]

    for _, r in top.iterrows():
        msg.append(f"{r['ticker']} S:{r['score']:.2f} VOL:{r['vol']:.2f} M1:{r['m1']:.2f}")

    text = "\n".join(msg)

    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
