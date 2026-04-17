import os, requests, pandas as pd, numpy as np, random, re, json, time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
STATE_FILE = "growth_state_v32_14.json"
SCAN_SIZE = 1500
MAX_WORKERS = 10
MIN_PRICE = 5.0
MIN_VOLUME = 500000
HEADERS = {"User-Agent": "Mozilla/5.0"}

TOP_EARLY = 5
TOP_EXP = 5
TOP_STRONG = 5

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

def send_chunks(text):
    if not WEBHOOK_URL:
        print("[WEBHOOK] SKIPPED")
        return

    chunk_size = 1800
    chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

    for i, chunk in enumerate(chunks):
        try:
            res = requests.post(WEBHOOK_URL, json={"content": chunk})
            print(f"[WEBHOOK {i+1}/{len(chunks)}] {res.status_code}")

            if res.status_code == 429:
                retry = res.json().get("retry_after", 1)
                time.sleep(retry)
            else:
                time.sleep(1.2)

        except Exception as e:
            print("[WEBHOOK ERROR]", e)

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

        avg_vol = np.mean(volume[-10:])
        if avg_vol < MIN_VOLUME:
            return None

        vol_recent = np.mean(volume[-5:])
        vol_past = np.mean(volume[-20:-5])

        def ret(a, b):
            return (a / b - 1) if b > 0 else 0

        m6_raw = ret(price, close[-120])
        m3_raw = ret(price, close[-63])
        m1_raw = ret(price, close[-21])

        # ★ 資金流入 OR 継続トレンド
        if not (
            vol_recent > vol_past * 1.5
            or m3_raw > 0.4
        ):
            return None

        # ★ ドローダウン耐性
        recent_min = min(close[-10:])
        dd = (price / recent_min - 1)
        if dd < 0.05:
            return None

        # ★ 軽量VCP（追加）
        diffs_recent = [abs(close[i] - close[i-1]) for i in range(-5, 0)]
        diffs_past = [abs(close[i] - close[i-1]) for i in range(-20, -5)]

        if len(diffs_past) > 0:
            range_recent = np.mean(diffs_recent)
            range_past = np.mean(diffs_past)

            if range_past > 0:
                vcp = range_recent / range_past
                if vcp > 0.9:
                    return None

        # スコア
        m6 = log_ret(clip(m6_raw))
        m3 = log_ret(clip(m3_raw))
        m1 = log_ret(clip(m1_raw))

        ma10 = np.mean(close[-10:])
        ma30 = np.mean(close[-30:])
        trend = ret(ma10, ma30)

        accel = m1 - (m3 / 3)

        score = (0.3 * m6) + (0.3 * trend) + (0.4 * accel)

        return {
            "ticker": ticker,
            "score": score,
            "m6": m6,
            "m3": m3,
            "m1": m1,
            "accel": accel
        }

    except:
        return None

# =========================
# DETECT
# =========================
def detect(df):
    early, expansion, strong = [], [], []

    for _, r in df.iterrows():
        s = r["score"]
        m1 = r["m1"]
        m3 = r["m3"]
        m6 = r["m6"]
        accel = r["accel"]

        if s > 0.10 and m1 > 0:
            early.append(r)

        if s > 0.20 and m3 > 0.2 and accel > 0:
            expansion.append(r)

        if (
            s > 0.30
            and m3 > 0.4
            and m6 > 0.5
            and accel > 0
            and m1 < 1.5
        ):
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
        f"🚀 GrowthRadar v32.14",
        f"Scanned:{scanned} Valid:{valid}",
        f"EARLY:{len(early)} EXP:{len(expansion)} STRONG:{len(strong)}",
        f"Time:{now}",
        ""
    ]

    msg.append("🔥 EARLY")
    for c in early[:TOP_EARLY]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f}")

    msg.append("\n🚀 EXPANSION")
    for c in expansion[:TOP_EXP]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f}")

    msg.append("\n💎 STRONG")
    for c in strong[:TOP_STRONG]:
        msg.append(f"{c['ticker']} S:{c['score']:.2f}")

    text = "\n".join(msg)
    print(text)

    send_chunks(text)

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
