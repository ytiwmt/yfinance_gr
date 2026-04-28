import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v34_7.json"

SCAN_SIZE = 1500
MAX_WORKERS = 14

MIN_PRICE = 5.0
MIN_BASE_VOLUME = 300000

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
        r = requests.get("https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt", timeout=10)
        if r.status_code == 200:
            symbols += r.text.splitlines()
    except:
        pass

    try:
        df = pd.read_csv("https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv")
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
# FETCH
# =========================
def fetch(session, ticker):
    try:
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
        if price < MIN_PRICE:
            return None

        vol_now = np.mean(v[-5:])
        vol_base = np.mean(v[-20:-5])
        if vol_base < MIN_BASE_VOLUME:
            return None

        vol_spike = min(vol_now / (vol_base + 1e-9), 5)

        def ret(a,b): return (a/b - 1) if b > 0 else 0

        m1 = ret(price, c[-21])
        m3 = ret(price, c[-63])
        accel = m1 - (m3 / 3)

        volatility = np.std(c[-10:]) / price
        if volatility > 0.15:
            return None

        ma10 = np.mean(c[-10:])
        ma30 = np.mean(c[-30:])
        trend = ret(ma10, ma30)

        is_early = (m1 > 0.05 and accel > 0 and not (vol_spike >= 5 and m1 < 0.1))
        is_cont  = (m3 > 0.4 and trend > 0.03 and m1 > -0.02)
        is_hold  = (m3 > 0.5 and trend > -0.02)

        if not (is_early or is_cont or is_hold):
            return None

        early_score = 0.35*vol_spike + 0.45*accel + 0.20*(1-volatility)
        cont_score  = 0.40*m3 + 0.30*trend + 0.30*(1-volatility)
        hold_score  = 0.50*m3 + 0.30*(1-volatility) + 0.20*trend

        return {
            "ticker": ticker,
            "early": is_early,
            "cont": is_cont,
            "hold": is_hold,
            "early_score": early_score,
            "cont_score": cont_score,
            "hold_score": hold_score,
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

    print(f"🚀 GrowthRadar v34.7 scanning {len(universe)}")

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, session, t): t for t in universe}
        for f in as_completed(futures):
            r = f.result()
            if r:
                results.append(r)
                state.update(r["ticker"], r["early_score"])

    state.save()

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    # =========================
    # BUYロジック
    # =========================
    early_buy = df[
        (df["early"]) &
        (df["m1"] > 0.2) &
        (df["vol"] > 1.5)
    ].sort_values("early_score", ascending=False).head(3)

    cont_buy = df[
        (df["cont"]) &
        (df["m1"] > -0.05) &
        (df["m1"] < 0.3)
    ].sort_values("cont_score", ascending=False).head(2)

    buy_df = pd.concat([early_buy, cont_buy])
    buy_df = buy_df.drop_duplicates(subset="ticker", keep="first")

    # =========================
    # 表示
    # =========================
    early_df = df[df["early"]].sort_values("early_score", ascending=False).head(10)
    cont_df  = df[df["cont"]].sort_values("cont_score", ascending=False).head(10)
    hold_df  = df[df["hold"]].sort_values("hold_score", ascending=False).head(10)

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    msg = [
        f"🚀 GrowthRadar v34.7",
        f"Scan:{len(universe)} Valid:{len(df)}",
        f"Time:{now}",
        ""
    ]

    # 💎 BUY（UI改善済）
    msg.append("💎 BUY SIGNAL\n")
    for _, r in buy_df.iterrows():
        msg.append(f"**{r['ticker']}**")

    # カテゴリ
    msg.append("\n🔥 EARLY")
    for _, r in early_df.iterrows():
        msg.append(f"{r['ticker']} S:{r['early_score']:.2f} M1:{r['m1']:.2f}")

    msg.append("\n🔁 CONT")
    for _, r in cont_df.iterrows():
        msg.append(f"{r['ticker']} S:{r['cont_score']:.2f} M3:{r['m3']:.2f}")

    msg.append("\n🧱 HOLD")
    for _, r in hold_df.iterrows():
        msg.append(f"{r['ticker']} S:{r['hold_score']:.2f} M3:{r['m3']:.2f}")

    text = "\n".join(msg)

    print(text)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": text[:1900]})

if __name__ == "__main__":
    run()
