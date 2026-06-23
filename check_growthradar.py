import os
import requests
import random
import re
import redis
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
REDIS_URL = os.environ.get("REDIS_URL")

SCAN_SIZE = 1500
MAX_WORKERS = 12

MIN_PRICE = 5.0
MIN_VOL = 300000

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ETF BLACKLIST
ETF_BLACKLIST = {
    "QQQ", "ARKK", "SOXX", "XLF",
    "XLK", "XBI", "IWM", "SPY",
    "WGMI", "QCML", "RKLX", "INTW",
    "TQQQ", "SQQQ", "RGTX","IONX",
    "SOXL", "SOXS", "ORCX","DLLL",
    "ARKW", "ARKG", "QCMU","NBIL",
    "AMDL", "IONL", "NBIG", "MVLL",
    "SMH", "IGV", "BOTZ", "TAN"
}

# =========================
# REDIS
# =========================
r = None

if REDIS_URL:
    try:
        r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print("🟢 Redis: CONNECTED (v42.3 Continuous Vector Mode)")
    except:
        print("🔴 Redis: FAILED")
        r = None
else:
    print("⚠️ Redis: OFF")

# =========================
# UNIVERSE
# =========================
def load_universe():
    symbols = set()

    try:
        url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"

        data = requests.get(url, timeout=10).text.splitlines()[1:]

        for line in data:
            s = line.split(",")[0].strip().upper()

            if re.match(r"^[A-Z]{1,6}$", s):
                symbols.add(s)

    except:
        pass

    fallback = [
        "AAPL","MSFT","NVDA","AMD","AMZN","META","GOOGL","TSLA",
        "INTC","QCOM","AVGO","TSM","ASML","MU","PLTR","SNOW","CRWD"
    ]

    symbols.update(fallback)

    symbols = [s for s in symbols if s not in ETF_BLACKLIST]
    symbols = sorted(symbols)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    random.seed(today)

    symbols = list(symbols)
    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]

# =========================
# UTILITIES
# =========================
def calc_current_streak(close):
    r = np.diff(close[-10:]) / close[-10:-1]
    streak = 0
    for x in reversed(r):
        if x > 0:
            streak += 1
        else:
            break
    return streak

# 💡 ② SWを“状態”ではなく“関数”にする (True/False禁止)
def calc_sw_score(close, volume):
    ma10 = np.mean(close[-10:])
    ma20 = np.mean(close[-20:])
    ma50 = np.mean(close[-50:])

    # 1. 収縮度 (MAの収斂・乖離を関数化)
    compression = 1.0 / (1.0 + abs(ma10 - ma20) / (ma20 + 1e-9) + abs(ma20 - ma50) / (ma50 + 1e-9))
    
    # 2. 再加速 (直近5日比のモメンタム強度)
    reaccel = max(0, close[-1] / (close[-5] + 1e-9) - 1.0)

    # 3. 出来高拡張度
    vol_expand = volume[-1] / (np.mean(volume[-10:]) + 1e-9)

    return float(compression * reaccel * vol_expand)

def detect_first_wave(close, volume):
    return (
        close[-1] > max(close[-20:-1]) and
        volume[-1] > np.mean(volume[-20:]) * 2.0 and
        np.std(close[-10:]) / np.mean(close[-10:]) < 0.02
    )

def growth_proxy(close, volume):
    returns_3m = close[-1] / close[-63] - 1
    returns_6m = close[-1] / close[-126] - 1

    accel = returns_3m - returns_6m

    vol_expansion = np.mean(volume[-5:]) / np.mean(volume[-60:])
    vol_exp_series = [np.mean(volume[i-5:i]) / np.mean(volume[i-60:i]) for i in range(-20, 0)]
    rolling_vol_expansion_20d = np.mean(vol_exp_series) if not np.isnan(np.mean(vol_exp_series)) else 1.0
    
    vol_accel = vol_expansion - rolling_vol_expansion_20d

    return (
        0.6 * accel +
        0.4 * returns_3m +
        0.2 * vol_accel
    )

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
        res = session.get(url, timeout=5)

        if res.status_code != 200:
            return None

        data = res.json().get("chart", {}).get("result", [None])[0]
        if not data:
            return None

        indicators = data.get("indicators", {}).get("quote", [{}])[0]

        close = indicators.get("close", [])
        volume = indicators.get("volume", [])

        close = [x for x in close if x is not None]
        volume = [x for x in volume if x is not None]

        if len(close) < 200 or len(volume) < 200:
            return None

        price = close[-1]
        if price < MIN_PRICE:
            return None

        vol_base = np.mean(volume[-20:-5])

        if np.isnan(vol_base) or vol_base <= 0:
            return None
        if vol_base < MIN_VOL:
            return None

        # =========================
        # RETURNS
        # =========================
        def ret(a, b):
            return (a / b - 1) if b else 0

        m1 = ret(close[-1], close[-21])
        m2 = ret(close[-1], close[-42])
        m3 = ret(close[-1], close[-63])

        vol_ratio = volume[-1] / (vol_base + 1e-9)

        # =========================
        # PHASE
        # =========================
        phase = "NONE"

        if (0.25 < m1 < 0.7 and m3 < 0.6):
            phase = "EARLY"
        elif (m1 > 0.45 and m3 > 0.45):
            phase = "TRANSITION"
        elif (m3 > 1.0):
            phase = "CONT"

        # =========================
        # BREAKOUT
        # =========================
        price_jump = abs(close[-1] - close[-2]) / close[-2]
        trend_ok = close[-1] > close[-2] > close[-3]

        breakout = (
            (price_jump > 0.02 and vol_ratio > 1.8) or
            (price_jump > 0.015 and vol_ratio > 2.3)
        ) and trend_ok

        # =========================
        # EXTENSION
        # =========================
        ma20 = np.mean(close[-20:])
        extension = ((close[-1] / (ma20 + 1e-9)) - 1) * 10

        # MA120 / MA200
        ma120 = np.mean(close[-120:])
        ma200 = np.mean(close[-200:])

        long_term_bonus = 0.0
        if price > ma200:
            long_term_bonus += 0.25
        if ma120 > ma200:
            long_term_bonus += 0.25

        # BASE SCORE
        base_score = (
            m1 * 0.55 +
            m3 * 0.25 +
            vol_ratio * 0.10 +
            breakout * 0.35
        )

        delta = np.mean(close[-5:]) / np.mean(close[-20:-5]) - 1
        if np.isnan(delta) or np.isinf(delta):
            delta = 0.0

        streak = calc_current_streak(close)
        streak_bonus = np.log1p(streak) * 0.05

        ext_penalty = 0.0
        if extension > 3.5:
            ext_penalty = 1.0
        elif extension > 2.5:
            ext_penalty = 0.5

        # YEARLY TREND FACTOR
        idx_252d = max(-len(close), -252)
        price_252d_ago = close[idx_252d] if len(close) >= 252 else close[0]
        yearly_return = (price / price_252d_ago) - 1 if price_252d_ago else 0

        high_52w = max(close)
        high_distance = (price / high_52w) - 1

        if yearly_return > 0.3 and high_distance > -0.25:
            yearly_trend_factor = 1.0
        elif yearly_return > -0.2 and high_distance > -0.4:
            yearly_trend_factor = 0.5
        elif yearly_return > -0.45 and high_distance > -0.65:
            yearly_trend_factor = 0.25
        else:
            yearly_trend_factor = 0.0

        # 💡 ① curvature（最重要：加速の形を2階微分的に捉える）
        curvature = m3 - 2 * m2 + m1

        # 💡 ② SWを連続的な関数値として取得
        sw_score = calc_sw_score(close, volume)
        second_wind_setup = sw_score > 0.05
        second_wind_trigger = second_wind_setup and breakout

        # 💡 ③ structure_flowをシグモイド関数による連続変化量へ修正
        # 直近30日のタイムウィンドウにおけるFW検出位置（ディスタンス）を評価
        fw_distance = 30
        for i in range(-30, 0):
            if len(close) + i - 20 >= 0:
                if detect_first_wave(close[i-20:i], volume[i-20:i]):
                    fw_distance = abs(i)
                    break
        
        # flow = sigmoid(FW → SW distance)
        # 距離が近いほど高出力を得られるよう連続化調整
        structure_flow = 1.0 / (1.0 + np.exp((fw_distance - 10) * 0.3))

        second_wind_quality = (0.6 + 0.4 * yearly_trend_factor)
        second_wind_bonus = sw_score * 0.5 * second_wind_quality

        first_wave = detect_first_wave(close, volume)
        growth = growth_proxy(close, volume)

        score = (
            base_score +
            growth * 0.6 +
            curvature * 0.5 +   # 💡 ① 曲率による加速軌道ボーナス
            max(delta, 0) * 0.2 +
            streak_bonus +
            second_wind_bonus +
            structure_flow * 0.6 +  # 💡 ③ 連続シグモイド構造フロー
            long_term_bonus -
            ext_penalty
        )

        # FRESHNESS PENALTY
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if r:
            last = r.get(f"last_seen:{ticker}")
            if last == today:
                score *= 0.95

        score = round(float(score), 2)

        # LIGHTWEIGHT STATE SAVE
        if r:
            r.set(f"peak:{ticker}", max(close), ex=86400*14)
            r.set(f"score:{ticker}", score, ex=86400)
            r.set(f"last_seen:{ticker}", today, ex=86400)
            r.set(
                f"sws_log:{ticker}:{today}",
                json.dumps({
                    "extension": extension,
                    "streak": streak,
                    "delta": delta,
                    "phase": phase,
                    "sw_score": sw_score
                }),
                ex=86400 * 14
            )

        prime_window = (
            second_wind_setup and
            first_wave and
            growth > 0.15 and
            extension < 2.0
        )

        # 💡 ④ 運動空間クラスタリング用ベクトル成分（モメンタムベクトル成分）をエクスポート
        return {
            "ticker": ticker,
            "phase": phase,
            "score": score,
            "streak": int(streak),
            "breakout": bool(breakout),
            "ext": round(float(extension), 2),

            "second_wind_setup": bool(second_wind_setup),
            "second_wind_trigger": bool(second_wind_trigger),
            "first_wave": bool(first_wave),
            
            "long_term_bonus": round(long_term_bonus, 2),
            "prime_window": bool(prime_window),
            "yearly_trend_factor": yearly_trend_factor,
            
            # 運動空間(Momentum Vector Space)を定義する3次元運動幾何パラメータ
            "v_m1": float(m1),
            "v_curv": float(curvature),
            "v_flow": float(structure_flow)
        }

    except:
        return None

# =========================
# BUY
# =========================
def build_buy(df):
    buy = df.copy()

    structure_bonus = (
        (buy["phase"] == "TRANSITION") * 0.7 +
        (buy["phase"] == "CONT") * 0.45 +
        (buy["phase"] == "EARLY") * 0.15
    )

    streak_bonus = np.minimum(
        np.log1p(buy["streak"]) * 0.05,
        0.8
    )

    ext_penalty = np.maximum(
        buy["ext"] - 2.5,
        0
    ) * 0.35

    # 連続量ベースのSW効果をBUYスコアへ反映
    buy["buy_score"] = (
        buy["score"] +
        structure_bonus +
        streak_bonus -
        ext_penalty +
        (buy["prime_window"] * 1.0)
    ) * (1 / (1 + buy["ext"]))

    # 💡 ④ clusterを“テーマ”から“運動空間（Momentum Vector Space）”へ進化
    # 価格や銘柄属性ではなく、純粋な運動ベクトル（M1速度、曲率、遷移流速）の近傍から過熱クラスターを定義してデフレペナルティを課す
    if len(buy) >= 10:
        try:
            # 運動空間上での位置ベクトル（運動エネルギーの同一性）を算出
            momentum_vector_space = buy["v_m1"] * 0.5 + buy["v_curv"] * 0.3 + buy["v_flow"] * 0.2
            buy["vector_cluster"] = pd.qcut(momentum_vector_space, 10, labels=False, duplicates="drop")
            
            # 同一の運動空間クラスターに属する銘柄群の冗長性を順位化
            buy["space_penalty"] = buy.groupby("vector_cluster")["score"].transform("rank", ascending=False)
            
            # 同一の運動パターン（塊）を形成している下位銘柄を自動制動
            buy["buy_score"] *= (1 / (1 + (buy["space_penalty"] - 1) * 0.15))
        except:
            pass

    buy = buy[
        ~(
            (buy["second_wind_setup"]) &
            (buy["yearly_trend_factor"] == 0)
        )
    ]

    buy = buy.sort_values(
        "buy_score",
        ascending=False
    )

    return buy.head(5)

# =========================
# MESSAGE
# =========================
def build_message(df):
    buy = build_buy(df)

    msg = []

    # v42.3 バージョン名変更
    msg.append("🚀 GrowthRadar v42.3 (Continuous Curvature & Vector Space Model)") 
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg.append("🟢 Redis: ON (v42.3 Lightweight State)" if r else "🔴 Redis: OFF")

    msg.append("")
    msg.append("💎 BUY SIGNAL")

    for _, row in buy.iterrows():
        tag = ""
        if row.prime_window:
            tag = " 👑PRIME"
        elif row.second_wind_trigger:
            tag = " SW🔥"
        elif row.second_wind_setup:
            tag = " SW🧩"

        msg.append(
            f"{row.ticker} "
            f"S:{row.buy_score:.2f} "
            f"LT:{row.long_term_bonus:.2f} "
            f"Streak:{row.streak} "
            f"Ext:{row.ext:.2f}"
            f"{tag}"
        )

    msg.append("")
    msg.append("🔥 EARLY")

    early = (
        df[df.phase == "EARLY"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(early):
        for _, row in early.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("⚡ TRANSITION")

    trans = (
        df[df.phase == "TRANSITION"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(trans):
        for _, row in trans.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("🔁 CONT")

    cont = (
        df[df.phase == "CONT"]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(cont):
        for _, row in cont.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("🌊 FIRST WAVE")

    brk = df[df.first_wave].head(4)

    if len(brk):
        for _, row in brk.iterrows():
            msg.append(row.ticker)
    else:
        msg.append("None")
        
    # DEBUG
    msg.append("")
    msg.append(f"DEBUG SWS RAW:{len(df[df.second_wind_setup])}")
    
    msg.append("")
    msg.append("🌊🧩 SECOND WIND SETUP")

    sw_setup_sorted = df[df.second_wind_setup].sort_values("score", ascending=False).head(4)

    if len(sw_setup_sorted):
        for _, row in sw_setup_sorted.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f} "
                f"Ext:{row.ext:.2f}"
            )
    else:
        msg.append("None")

    msg.append("")
    msg.append("🌊🔥 SECOND WIND TRIGGER")

    sw_trigger = (
        df[df.second_wind_trigger]
        .sort_values("score", ascending=False)
        .head(4)
    )

    if len(sw_trigger):
        for _, row in sw_trigger.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f} "
                f"Ext:{row.ext:.2f}"
            )
    else:
        msg.append("None")

    msg.append("")
    msg.append("👑 PRIME WINDOW")

    prime = df[df.prime_window].sort_values("score", ascending=False).head(5)

    if len(prime):
        for _, row in prime.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f} LT:{row.long_term_bonus:.2f}")
    else:
        msg.append("None")

    return "\n".join(msg)

# =========================
# RUN
# =========================
def run():
    session = requests.Session()
    session.headers.update(HEADERS)

    universe = load_universe()

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(fetch, session, t): t
            for t in universe
        }

        for f in as_completed(futures):
            rlt = f.result()

            if rlt:
                results.append(rlt)

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    text = build_message(df)

    print(text)

    if WEBHOOK_URL:
        requests.post(
            WEBHOOK_URL,
            json={"content": text[:1900]}
        )

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    run()
