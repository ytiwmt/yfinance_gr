import os
import requests
import random
import re
import redis
import json
import math
import ast
from datetime import datetime, timedelta, timezone
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

# NEW: SW PARAMS
SW_SETUP_THRESHOLD = 0.2  # second_wind_score - market_mean の閾値（調整可能）

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# JST TIMEZONE
JST = timezone(timedelta(hours=9))

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
        print("🟢 Redis: CONNECTED")
    except Exception as e:
        print(f"🔴 Redis: FAILED ({e})")
        r = None
else:
    print("⚠️ Redis: OFF")

# =========================
# UTILS
# =========================
def safe_float(x):
    try:
        return float(str(x).replace("np.float64(", "").replace(")", ""))
    except:
        return 0.0

def clean_input_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float, np.number)):
        return float(x)
    if isinstance(x, str):
        m = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", x)
        if m:
            try:
                return float(m.group())
            except:
                return None
        try:
            return float(ast.literal_eval(x))
        except:
            return None
    return None

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

    today = datetime.now(JST).strftime("%Y-%m-%d")
    random.seed(today)

    symbols = list(symbols)
    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]

# =========================
# FETCH
# =========================
def fetch(ticker):
    try:
        with requests.Session() as session:
            session.headers.update(HEADERS)
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"

            res = session.get(url, timeout=5)

            if res.status_code != 200:
                return None

            j = res.json()
            data_block = j.get("chart", {}).get("result")

            if not data_block or len(data_block) == 0:
                return None

            data = data_block[0]

            indicators = data.get("indicators", {}).get("quote", [{}])[0]
            raw_close = indicators.get("close")
            raw_volume = indicators.get("volume")
            raw_high = indicators.get("high")

            if raw_close is None or raw_volume is None or raw_high is None:
                return None

            pairs = []
            for c, v, h in zip(raw_close, raw_volume, raw_high):
                c = clean_input_float(c)
                v = clean_input_float(v)
                h = clean_input_float(h)

                if c is None or v is None or h is None:
                    continue

                if math.isnan(c) or math.isnan(v) or math.isnan(h):
                    continue

                pairs.append((c, v, h))

            if len(pairs) < 200:
                return None

            close, volume, high = zip(*pairs)
            close = list(close)
            volume = list(volume)
            high = list(high)

            if len(close) < 3:
                return None

            if len(set(close[-5:])) < 2:
                return None

            price = close[-1]

            if price < MIN_PRICE:
                return None

            vol_slice = [v for v in volume[-25:-5] if v is not None]
            vol_base = np.mean(vol_slice) if len(vol_slice) > 0 else 0

            if np.isnan(vol_base) or vol_base <= 0:
                return None

            if vol_base < (MIN_VOL * 0.5):
                return None

            # =========================
            # RETURNS
            # =========================
            def ret(a, b):
                return (a / b - 1) if b else 0

            m1 = ret(close[-1], close[-21])
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
            price_jump = (close[-1] - close[-2]) / (close[-2] + 1e-9)
            trend_ok = (close[-1] > close[-2] > close[-3])

            breakout = (
                (price_jump > 0.02 and vol_ratio > 1.8) or
                (price_jump > 0.015 and vol_ratio > 2.3)
            ) and trend_ok

            # =========================
            # EXTENSION
            # =========================
            ma20 = np.mean(close[-20:])
            extension = ((close[-1] / (ma20 + 1e-9)) - 1) * 10

            # ---------------------------------------------------------
            # MA120 / MA200 & LONG TERM TREND BONUS
            # ---------------------------------------------------------
            ma120 = np.mean(close[-120:]) if len(close) >= 120 else np.mean(close)
            ma200 = np.mean(close[-200:]) if len(close) >= 200 else np.mean(close)

            long_term_bonus = 0.0
            if price > ma200:
                long_term_bonus += 0.25
            if len(close) >= 200:
                if ma120 > ma200:
                    long_term_bonus += 0.25

            # =========================
            # BASE SCORE
            # =========================
            base_score = (
                m1 * 0.55 +
                m3 * 0.25 +
                vol_ratio * 0.10 +
                breakout * 0.35
            )

            # =========================
            # REDIS TIME MODEL
            # =========================
            delta = 0.0
            streak = 0
            today = datetime.now(JST).strftime("%Y-%m-%d")

            if r:
                prev_score = r.get(f"score:{ticker}")
                prev_streak = r.get(f"streak:{ticker}")
                prev_day = r.get(f"day:{ticker}")

                recent_high_streak = r.get(f"highstreak:{ticker}")
                recent_high_streak = int(recent_high_streak) if recent_high_streak else 1

                delta = base_score - safe_float(prev_score) if prev_score else 0.0
                
                if prev_streak is not None:
                    streak = int(prev_streak)
                else:
                    streak = 1

                if prev_day and prev_day != today and prev_score is not None:
                    keep_signal = (
                        phase in ["TRANSITION", "CONT"] and
                        base_score > 0.9 and
                        delta >= -0.15
                    )
                    if keep_signal:
                        streak += 1
                    else:
                        streak = 0

                recent_high_streak = max(recent_high_streak, streak)

                r.set(f"score:{ticker}", float(base_score), ex=86400)
                r.set(f"streak:{ticker}", int(streak), ex=86400 * 7)
                r.set(f"day:{ticker}", today, ex=86400 * 7)
                r.set(f"highstreak:{ticker}", int(recent_high_streak), ex=86400 * 14)
            else:
                recent_high_streak = 1

            streak_bonus = min(streak * 0.18, 0.9)

            ext_penalty = 0.0
            if extension > 3.5:
                ext_penalty = 1.0
            elif extension > 2.5:
                ext_penalty = 0.5

            # =========================
            # LONG TERM TREND VALIDATION MODEL
            # =========================
            idx_252d = max(-len(close), -252)
            price_252d_ago = close[idx_252d]
            yearly_return = (price / (price_252d_ago + 1e-9)) - 1

            high_52w = max(high[-252:]) if len(high) >= 252 else max(high)
            high_distance = (price / (high_52w + 1e-9)) - 1

            if yearly_return > 0.3 and high_distance > -0.25:
                yearly_trend_factor = 1.0
            elif yearly_return > -0.2 and high_distance > -0.4:
                yearly_trend_factor = 0.5
            elif yearly_return > -0.45 and high_distance > -0.65:
                yearly_trend_factor = 0.25
            else:
                yearly_trend_factor = 0.0

            # =========================
            # SECOND WIND (ファーストパス: 基本スコア算出)
            # =========================
            second_wind_score = (
                base_score +
                streak_bonus +
                long_term_bonus
            )

            # クロスセクショナル統計（平均・Zスコア）が必要なため、判定はDataFrame結合後に実施
            second_wind_watch = second_wind_score > 1.2

            # FINAL SCORE (仮)
            score = (
                base_score +
                max(delta, 0) * 0.45 +
                streak_bonus +
                long_term_bonus -
                ext_penalty
            )

            return {
                "ticker": ticker,
                "phase": phase,
                "base_score": base_score,
                "score": score,
                "streak": int(streak),
                "breakout": bool(breakout),
                "ext": round(float(extension), 2),
                "second_wind_score": second_wind_score,
                "second_wind_watch": second_wind_watch,
                "long_term_bonus": round(long_term_bonus, 2),
                "yearly_trend_factor": yearly_trend_factor,
                "delta": delta,
                "ext_penalty": ext_penalty
            }

    except Exception as e:
        print(f"[FETCH ERROR] {ticker}: {e}")
        return None

# =========================
# EVALUATE MARKET METRICS (集計・相互フィルタリング)
# =========================
def evaluate_market_signals(df):
    if df.empty:
        return df

    # 全体の統計量計算
    market_mean = df["second_wind_score"].mean()
    
    score_mean = df["score"].mean()
    score_std = df["score"].std() if df["score"].std() > 0 else 1e-9
    df["score_z"] = (df["score"] - score_mean) / score_std

    # ② SETUP修正: “差分フィルタ”
    df["second_wind_setup"] = df["second_wind_watch"] & (
        (df["second_wind_score"] - market_mean) > SW_SETUP_THRESHOLD
    )

    # ③ TRIGGER修正: “構造条件”
    df["second_wind_trigger"] = (
        df["breakout"] & 
        (df["ext"] < 3.0) & 
        (df["score_z"] > 1.5)
    )

    # SW Setup ボーナス再計算と最終スコアの確定
    second_wind_quality = (0.6 + 0.4 * df["yearly_trend_factor"])
    df["second_wind_bonus"] = np.where(df["second_wind_setup"], 0.75 * second_wind_quality, 0.0)
    
    # 最終スコア調整
    df["score"] = round(df["score"] + df["second_wind_bonus"], 2)

    # ① PRIME WINDOW修正: ANDベースに回帰 + 質フィルタ
    df["prime_window"] = (
        df["second_wind_setup"] &
        (df["base_score"] > 1.2) &
        (df["yearly_trend_factor"] >= 0.5) &
        (df["streak"] >= 1)
    )

    return df

# =========================
# BUY
# =========================
def build_buy(df):
    buy = df.copy()

    structure_bonus = (
        (buy["phase"] == "TRANSITION").astype(int) * 0.7 +
        (buy["phase"] == "CONT").astype(int) * 0.45 +
        (buy["phase"] == "EARLY").astype(int) * 0.15
    )
    streak_bonus = np.minimum(buy["streak"] * 0.12, 0.8)
    ext_penalty = np.maximum(buy["ext"] - 2.5, 0) * 0.35
    second_wind_bonus = (buy["second_wind_setup"] * 0.9)

    buy["prime_bonus"] = buy["prime_window"] * 1.0

    buy["buy_score"] = (
        buy["score"] +
        structure_bonus +
        streak_bonus +
        second_wind_bonus -
        ext_penalty +
        buy["prime_bonus"]
    )

    buy = buy[~((buy["second_wind_setup"]) & (buy["yearly_trend_factor"] == 0))]
    buy = buy.sort_values("buy_score", ascending=False)

    return buy.head(5)

# =========================
# MESSAGE
# =========================
def build_message(df):
    buy = build_buy(df)
    sw_watch = df[df.second_wind_watch]
    sw_setup = df[df.second_wind_setup]

    msg = []
    msg.append("🚀 GrowthRadar v41.17") 
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now(JST).strftime('%Y-%m-%d %H:%M')} JST")
    msg.append("🟢 Redis: ON" if r else "🔴 Redis: OFF")

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
        elif row.second_wind_watch:
            tag = " SW👀"

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
    early = df[df.phase == "EARLY"].sort_values("score", ascending=False).head(4)
    if len(early):
        for _, row in early.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("⚡ TRANSITION")
    trans = df[df.phase == "TRANSITION"].sort_values("score", ascending=False).head(4)
    if len(trans):
        for _, row in trans.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("🔁 CONT")
    cont = df[df.phase == "CONT"].sort_values("score", ascending=False).head(4)
    if len(cont):
        for _, row in cont.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("🌊 FIRST WAVE")
    brk = df[df.breakout].head(4)
    if len(brk):
        for _, row in brk.iterrows():
            msg.append(row.ticker)
    else:
        msg.append("None")
        
    msg.append("")
    msg.append(f"DEBUG SWW RAW:{len(df[df.second_wind_watch])}")
    msg.append(f"DEBUG SWS RAW:{len(df[df.second_wind_setup])}")
    
    msg.append("")
    msg.append("🌊👀 SECOND WIND WATCH")
    sw_watch_sorted = sw_watch.sort_values("score", ascending=False).head(4)
    if len(sw_watch_sorted):
        for _, row in sw_watch_sorted.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f} Ext:{row.ext:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("🌊🧩 SECOND WIND SETUP")
    sw_setup_sorted = sw_setup.sort_values("score", ascending=False).head(4)
    if len(sw_setup_sorted):
        for _, row in sw_setup_sorted.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f} Ext:{row.ext:.2f}")
    else:
        msg.append("None")

    msg.append("")
    msg.append("🌊🔥 SECOND WIND TRIGGER")
    sw_trigger = df[df.second_wind_trigger].sort_values("score", ascending=False).head(4)
    if len(sw_trigger):
        for _, row in sw_trigger.iterrows():
            msg.append(f"{row.ticker} S:{row.score:.2f} Ext:{row.ext:.2f}")
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
    universe = load_universe()
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch, t): t for t in universe}

        for f in as_completed(futures):
            rlt = f.result()
            if rlt:
                results.append(rlt)

    if not results:
        print("NO DATA")
        return

    # DataFrame化
    df = pd.DataFrame(results)
    
    # 相互フィルタリング・クロスセクション評価モデルの適用
    df = evaluate_market_signals(df)

    # Redisログ永続化 (統計確定後)
    today = datetime.now(JST).strftime("%Y-%m-%d")
    if r:
        for _, row in df.iterrows():
            try:
                r.set(
                    f"sws_log:{row.ticker}:{today}",
                    json.dumps({
                        "extension": float(row.ext),
                        "streak": int(row.streak),
                        "delta": float(row.delta),
                        "phase": row.phase,
                        "hit": bool(row.second_wind_setup)
                    }),
                    ex=86400 * 14
                )
            except:
                pass

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
