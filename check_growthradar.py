import os
import requests
import random
import re
import json  # SWSログダンプ用プレースホルダーの維持
import time  # v41.8: スロットリング用
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_SIZE = 1500
MAX_WORKERS = 5  # v41.8: 4〜6の現実ライン

MIN_PRICE = 5.0
MIN_VOL = 100000  # v41.8: 緩和値

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

    # UNIVERSE FILTER (SINGLE RESPONSIBILITY)
    symbols = [s for s in symbols if s not in ETF_BLACKLIST]

    # UPDATE v40.20: FIX SEED BY DATE FOR STABLE DAILY UNIVERSE
    symbols = sorted(symbols)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    random.seed(today)

    symbols = list(symbols)
    random.shuffle(symbols)

    return symbols[:SCAN_SIZE]

# =========================
# NEW INDEPENDENT CALCULATORS
# =========================
def calc_streak(close):
    r = pd.Series(close).pct_change()
    val = (r > 0).rolling(3).sum().iloc[-1]
    return int(val) if not np.isnan(val) else 0

def calc_high_streak(close):
    r = pd.Series(close).pct_change()
    streaks = (r > 0).astype(int)

    max_streak = 0
    current = 0

    for x in streaks:
        if x:
            current += 1
            max_streak = max(max_streak, current)
        else:
            current = 0

    return int(max_streak)

# =========================
# FETCH
# =========================
def fetch(session, ticker):
    try:
        # v41.8: スロットリング
        time.sleep(0.05)

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"

        # 最大2回のリトライ機構
        res = None
        for _ in range(2):
            res = session.get(url, timeout=5)
            if res.status_code == 200:
                break

        if res.status_code != 200:
            print(ticker, "STATUS", res.status_code)
            return None

        data = res.json()["chart"]["result"][0]

        # v41.6: Yahoo壊れ対策
        if not data.get("indicators"):
            return None

        close = data["indicators"]["quote"][0]["close"]
        volume = data["indicators"]["quote"][0]["volume"]

        close = [x for x in close if x is not None]
        volume = [x for x in volume if x is not None]

        # UPDATE v42.0: 修正①（必須） 時系列データ不足による指標計算クラッシュを完全防御
        if len(close) < 200:
            return None

        price = close[-1]
        vol_base = np.mean(volume[-20:-5])

        if price < MIN_PRICE:
            return None

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

        trend_ok = (
            close[-1] > close[-2] > close[-3]
        )

        breakout = (
            (
                price_jump > 0.02 and
                vol_ratio > 1.8
            )
            or
            (
                price_jump > 0.015 and
                vol_ratio > 2.3
            )
        ) and trend_ok

        # =========================
        # EXTENSION
        # =========================
        ma20 = np.mean(close[-20:])

        extension = (
            (close[-1] / (ma20 + 1e-9)) - 1
        ) * 10

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
        # ---------------------------------------------------------

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
        # NEW STATELESS MODEL (v42.0)
        # =========================
        delta = np.mean(close[-5:]) / np.mean(close[-20:-5]) - 1
        if np.isnan(delta):
            delta = 0.0

        streak = calc_streak(close)
        recent_high_streak = calc_high_streak(close)

        # =========================
        # STREAK BONUS
        # =========================
        streak_bonus = min(streak * 0.18, 0.9)

        # =========================
        # EXTENSION PENALTY
        # =========================
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

        # =========================
        # SECOND WIND (v42.1改)
        # =========================
        # 💡UPDATE v42.1: ② SECOND WINDの形状判定基準をさらに厳格化
        second_wind_base = (
            m3 > 0.45 and
            m1 > 0.25 and
            vol_ratio > 1.8 and
            extension < 3.5
        )

        second_wind_setup = second_wind_base
        second_wind_watch = second_wind_base and (recent_high_streak >= 3)
        second_wind_trigger = second_wind_setup and breakout

        second_wind_quality = (
            0.6 + 0.4 * yearly_trend_factor
        )

        second_wind_bonus = 0.0
        if second_wind_setup:
            second_wind_bonus = (
                0.75 *
                second_wind_quality
            )

        # =========================
        # FINAL SCORE
        # =========================
        score = (
            base_score +
            max(delta, 0) * 0.45 +
            streak_bonus +
            second_wind_bonus +
            long_term_bonus -
            ext_penalty
        )

        score = round(float(score), 2)

        return {
            "ticker": ticker,
            "phase": phase,
            "score": float(score),
            "streak": int(streak),
            "breakout": bool(breakout),
            "ext": float(round(float(extension), 2)),

            # NEW
            "second_wind_watch": bool(second_wind_watch),
            "second_wind_setup": bool(second_wind_setup),
            "second_wind_trigger": bool(second_wind_trigger),
            
            "long_term_bonus": float(round(long_term_bonus, 2)),
            
            # パーセンタイル計算用にプレースホルダーを定義
            "prime_window": False,
            "yearly_trend_factor": yearly_trend_factor
        }

    except Exception as e:
        print(ticker, "ERROR:", e)
        return None

# =========================
# BUY
# =========================
def build_buy(df):
    buy = df.copy()

    for col in [
        "second_wind_watch",
        "second_wind_setup",
        "second_wind_trigger",
        "prime_window"
    ]:
        buy[col] = buy[col].fillna(False).astype(bool)

    structure_bonus = (
        (buy["phase"] == "TRANSITION") * 0.7 +
        (buy["phase"] == "CONT") * 0.45 +
        (buy["phase"] == "EARLY") * 0.15
    )

    streak_bonus = np.minimum(
        buy["streak"] * 0.12,
        0.8
    )

    ext_penalty = np.maximum(
        buy["ext"] - 2.5,
        0
    ) * 0.35

    second_wind_bonus = (
        buy["second_wind_setup"] * 0.9
    )

    # UPDATE v41.0: PRIME PRIORITY SCORE BONUS
    buy["prime_bonus"] = buy["prime_window"] * 1.0

    buy["buy_score"] = (
        buy["score"] +
        structure_bonus +
        streak_bonus +
        second_wind_bonus -
        ext_penalty +
        buy["prime_bonus"]
    )

    # UPDATE v41.12: boolean型への統一に伴い、明示的な == True 評価へ安全化
    buy = buy[
        ~(
            (buy["second_wind_setup"] == True) &
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
    # UPDATE v41.19: build_message先頭部への型固定の追加
    df = df.copy()

    bool_cols = [
        "breakout",
        "second_wind_watch",
        "second_wind_setup",
        "second_wind_trigger",
        "prime_window"
    ]

    for col in bool_cols:
        if col in df:
            df[col] = (
                df[col]
                .fillna(False)
                .astype(bool)
            )

    buy = build_buy(df)

    # UPDATE v41.19: ターゲットリストの抽出条件を df["col"] == True に全面一括置換
    sw_watch = df[df["second_wind_watch"] == True]
    sw_setup = df[df["second_wind_setup"] == True]

    # NEW: “調整判断フラグ”の評価（スコープへの定義）
    sws_adjust_signal = (
        len(sw_watch) == 0 or
        len(sw_setup) == 0
    )

    msg = []

    # UPDATE v42.1: バージョン表示名の変更
    msg.append("🚀 GrowthRadar v42.1") 
    msg.append(f"Scan:{SCAN_SIZE} Valid:{len(df)}")
    msg.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    msg.append("⚠️ Redis: OFF (Stateless Mode)")

    msg.append("")
    msg.append("💎 BUY SIGNAL")

    for _, row in buy.iterrows():

        tag = ""

        # UPDATE v41.0: PRIME WINDOW TAG PRIORITY
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

    # =========================
    # FIRST WAVE
    # =========================
    msg.append("")
    msg.append("🌊 FIRST WAVE")

    # UPDATE v41.19: 置換箇所
    brk = df[df["breakout"] == True].head(4)

    if len(brk):
        for _, row in brk.iterrows():
            msg.append(row.ticker)
    else:
        msg.append("None")
        
    # DEBUG
    msg.append("")
    # UPDATE v41.19: 置換箇所
    msg.append(f"DEBUG SWW RAW:{len(df[df['second_wind_watch'] == True])}")
    msg.append(f"DEBUG SWS RAW:{len(df[df['second_wind_setup'] == True])}")
    
    # =========================
    # SECOND WIND WATCH
    # =========================
    msg.append("")
    msg.append("🌊👀 SECOND WIND WATCH")

    sw_watch_sorted = sw_watch.sort_values("score", ascending=False).head(4)

    if len(sw_watch_sorted):
        for _, row in sw_watch_sorted.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f} "
                f"Ext:{row.ext:.2f}"
            )
    else:
        msg.append("None")

    # =========================
    # SECOND WIND SETUP
    # =========================
    msg.append("")
    msg.append("🌊🧩 SECOND WIND SETUP")

    sw_setup_sorted = sw_setup.sort_values("score", ascending=False).head(4)

    if len(sw_setup_sorted):
        for _, row in sw_setup_sorted.iterrows():
            msg.append(
                f"{row.ticker} "
                f"S:{row.score:.2f} "
                f"Ext:{row.ext:.2f}"
            )
    else:
        msg.append("None")

    # =========================
    # SECOND WIND TRIGGER
    # =========================
    msg.append("")
    msg.append("🌊🔥 SECOND WIND TRIGGER")

    # UPDATE v41.19: 置換箇所
    sw_trigger = (
        df[df["second_wind_trigger"] == True]
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

    # =========================
    # PRIME WINDOW
    # =========================
    msg.append("")
    msg.append("👑 PRIME WINDOW")

    # UPDATE v41.19: 置換箇所
    prime = df[df["prime_window"] == True].sort_values("score", ascending=False).head(5)

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

    print("--- START DIAL-IN SINGLE TEST (AAPL) ---")
    try:
        test_url = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL?range=1y&interval=1d"
        test_res = session.get(test_url, timeout=5)
        print("AAPL STATUS CODE:", test_res.status_code)
        print("AAPL TEXT PREVIEW:", test_res.text[:200])
    except Exception as e:
        print("AAPL TEST EXCEPTION:", str(e))
    print("---------------------------------------")

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

    print(f"RESULT COUNT: {len(results)}")

    if not results:
        print("NO DATA")
        return

    df = pd.DataFrame(results)

    # UPDATE v42.0: ブール列への上書き汚染を避けるため、対象の数値列のみに限定して一括安全キャスト
    num_cols = ["score", "ext", "long_term_bonus", "streak", "yearly_trend_factor"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # 💡UPDATE v42.1: ④ 銘柄数制御を追加（ノイズ過多・無限増殖の抑止として上位300件にクリップ）
    df = df.sort_values("score", ascending=False).head(300)

    # UPDATE v42.0: 修正③（必須） ランキング算出時、NaNによるクラッシュを防ぐために明示的防衛
    # 300件クリップ後のデータフレームに対して正しく相対順位を再算出
    buy_rank = df["score"].rank(pct=True).fillna(0)

    # UPDATE v42.0: 修正②（必須） SWS関連フラグの完全な bool 固定
    df["second_wind_setup"] = df["second_wind_setup"].astype(bool)
    df["second_wind_watch"] = df["second_wind_watch"].astype(bool)
    df["second_wind_trigger"] = df["second_wind_trigger"].astype(bool)
    df["breakout"] = df["breakout"].astype(bool)

    # 💡UPDATE v42.1: ① & ③ PRIME WINDOW判定の再設計（強い + 上位のAND結合、およびbreakout/triggerの強制連動化）
    df["prime_window"] = (
        (df["second_wind_trigger"] == True) & 
        (df["breakout"] == True) & 
        (df["score"] >= 3.5) & 
        (buy_rank >= 0.85)
    )

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
