import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v32_21.json"

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOLUME = 300000  # 緩和（母集団確保）

HEADERS = {"User-Agent": "Mozilla/5.0"}

TOP_N = 5

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

    # ★ 重要：ランキング変化で見る
    def get_velocity(self, ticker):
        hist = self.data.get(ticker, [])
        if len(hist) < 3:
            return 0.0
        return hist[-1]["s"] - hist[-3]["s"]

# =========================
# FETCH（緩めに通す）
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

        m6 = ret(price, c[-120])
        m3 = ret(price, c[-63])
        m1 = ret(price, c[-21])

        # 出来高変化（弱め）
        vol_recent = np.mean(v[-5:])
        vol_past = np.mean(v[-20:-5])
        vol_boost = vol_recent / vol_past if vol_past > 0 else 1

        # トレンド
        ma10 = np.mean(c[-10:])
        ma30 = np.mean(c[-30:])
        trend = ret(ma10, ma30)

        accel = m1 - (m3 / 3)

        # ★ スコア（シンプル）
        score = (0.3 * m6) + (0.3 * trend) + (0.4 * accel)

        return {
            "ticker": ticker,
            "score": score,
            "m1": m1,
            "m3": m3,
            "m6": m6,
            "accel": accel,
            "vol": vol_boost
        }

    except:
        return None

# =========================
# DETECT（ここで絞る）
# =========================
def detect(df):
    # EARLY：初動
    early = df[
        (df['m1'] > 0.05) &
        (df['vol'] > 1.2)
    ].sort_values("score", ascending=False)

    # EXP：加速
    exp = df[
        (df['m3'] > 0.15) &
        (df['accel'] > 0)
    ].sort_values("score", ascending=False)

    # STRONG：継続強者
    strong = df[
        (df['m6'] > 0.4) &
        (df['m3'] > 0.25)
    ].sort_values("score", ascending=False)

    return early, exp, strong

# =========================
# REPORT
# =========================
def report(state, early, exp, strong, scanned, valid):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v32.21",
        f"Scan:{scanned} Valid:{valid}",
        f"Time:{now}\n"
    ]

    def block(name, data, icon):
        msg.append(f"{icon} {name}:{len(data)}")
        for _, r in data.head(TOP_N).iterrows():
            v = state.get_velocity(r['ticker'])

            if v > 0.05:
                status = "NEW!!"
            elif v > 0.01:
                status = "RISING"
            elif v > -0.02:
                status = "KEEP"
            else:
                status = "DROP"

            msg.append(f"{r['ticker']} S:{r['score']:.2f} [{status}]")
        msg.append("")

    block("EARLY", early, "🔥")
    block("EXP", exp, "🚀")
    block("STRONG", strong, "💎")

    text = "\n".join(msg)
    print(text)

    if WEBHOOK_URL:
        # 分割送信
        for i in range(0, len(text), 1800):
            chunk = text[i:i+1800]
            try:
                requests.post(WEBHOOK_URL, json={"content": chunk})
                time.sleep(1)
            except:
                pass

# =========================
# MAIN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    state = State()

    # ユニバース
    symbols = []
    try:
        df = pd.read_csv("https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv")
        symbols += df["Symbol"].tolist()
    except:
        pass

    try:
        txt = requests.get(
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        ).text.split("\n")
        symbols += txt
    except:
        pass

    clean = list(set([
        s.strip().upper()
        for s in symbols
        if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", s)
    ]))

    random.shuffle(clean)
    universe = clean[:SCAN_SIZE]

    print(f"Scanning {len(universe)} stocks...")

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

    # ★ 最重要：ランキング化
    df["score"] = df["score"].rank(pct=True)

    # state更新
    for _, r in df.iterrows():
        state.update(r["ticker"], r["score"])
    state.save()

    early, exp, strong = detect(df)

    report(state, early, exp, strong, len(universe), len(df))


if __name__ == "__main__":
    run()
