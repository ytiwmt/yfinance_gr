import os
import time
import json
import redis
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime
from collections import defaultdict

# =========================
# CONFIG
# =========================

VERSION = "v40.4 (SECOND WIND MODEL)"

WEBHOOK_URL = os.getenv("WEBHOOK_URL")
REDIS_URL = os.getenv("REDIS_URL")

SCAN_SIZE = 1500

# =========================
# REDIS
# =========================

r = None

try:
    r = redis.from_url(REDIS_URL)
    r.ping()
    redis_ok = True
except:
    redis_ok = False

# =========================
# LOAD SYMBOLS
# =========================

url = "https://raw.githubusercontent.com/datasets/nasdaq-listings/master/data/nasdaq-listed-symbols.csv"
df = pd.read_csv(url)

symbols = (
    df["Symbol"]
    .dropna()
    .astype(str)
    .tolist()
)

symbols = [
    s for s in symbols
    if len(s) <= 5
    and "^" not in s
    and "/" not in s
]

symbols = symbols[:SCAN_SIZE]

# =========================
# HELPERS
# =========================

def get_float(x, default=0):
    try:
        if pd.isna(x):
            return default
        return float(x)
    except:
        return default


def load_json(key, default):
    if not redis_ok:
        return default

    raw = r.get(key)

    if not raw:
        return default

    try:
        return json.loads(raw)
    except:
        return default


def save_json(key, value):
    if not redis_ok:
        return

    r.set(key, json.dumps(value))


# =========================
# STATE
# =========================

prev_scores = load_json("gr_prev_scores", {})
streak_map = load_json("gr_streak_map", {})
theme_map = load_json("gr_theme_map", {})
high_streak_map = load_json("gr_high_streak_map", {})

# =========================
# RESULTS
# =========================

buy_signal = []
early_signal = []
transition_signal = []
cont_signal = []
breakout_signal = []
second_wind_signal = []

valid_count = 0

# =========================
# DOWNLOAD
# =========================

data = yf.download(
    tickers=symbols,
    period="3mo",
    interval="1d",
    group_by="ticker",
    auto_adjust=True,
    progress=False,
    threads=True
)

# =========================
# MAIN LOOP
# =========================

for symbol in symbols:

    try:

        if symbol not in data:
            continue

        d = data[symbol].dropna()

        if len(d) < 30:
            continue

        close = d["Close"]
        volume = d["Volume"]

        c = get_float(close.iloc[-1])
        prev_c = get_float(close.iloc[-2])

        v = get_float(volume.iloc[-1])
        vma20 = get_float(volume.tail(20).mean())

        if c <= 1:
            continue

        if vma20 <= 100000:
            continue

        valid_count += 1

        # =========================
        # PRICE / VOLUME
        # =========================

        ret1 = (c / prev_c) - 1

        ma10 = close.tail(10).mean()
        ma20 = close.tail(20).mean()

        ext = ((c / ma20) - 1) * 10

        vol_ratio = v / vma20 if vma20 > 0 else 0

        # =========================
        # SCORE
        # =========================

        score = 0

        score += ret1 * 8
        score += max(vol_ratio - 1, 0) * 0.8

        if ma10 > ma20:
            score += 0.5

        if ext > 0:
            score += min(ext, 3) * 0.5

        # =========================
        # PREV SCORE
        # =========================

        prev_score = prev_scores.get(symbol, 0)

        delta = score - prev_score

        # =========================
        # STREAK
        # =========================

        streak = streak_map.get(symbol, 0)

        if score > 1:
            streak += 1
        else:
            streak = 0

        streak_map[symbol] = streak

        # =========================
        # HIGH STREAK MEMORY
        # =========================

        prev_high = high_streak_map.get(symbol, 0)

        if streak > prev_high:
            high_streak_map[symbol] = streak

        recent_high_streak = high_streak_map.get(symbol, 0)

        # =========================
        # THEME
        # =========================

        theme_map[symbol] = theme_map.get(symbol, 0) * 0.95

        if score > 1:
            theme_map[symbol] += 1

        theme_strength = theme_map[symbol]

        # =========================
        # BUY SIGNAL
        # =========================

        if (
            score > 0.6
            and delta > 0
        ):
            buy_signal.append(
                (
                    symbol,
                    round(score, 2),
                    streak,
                    round(ext, 2)
                )
            )

        # =========================
        # EARLY
        # =========================

        if (
            0.25 < score < 0.6
            and delta > 0
        ):
            early_signal.append(
                (
                    symbol,
                    round(score, 2)
                )
            )

        # =========================
        # TRANSITION
        # =========================

        if (
            delta > 0.5
            and score > 0.15
        ):
            transition_signal.append(
                (
                    symbol,
                    round(delta, 2)
                )
            )

        # =========================
        # CONT
        # =========================

        if (
            streak >= 2
            and score > 0.3
        ):
            cont_signal.append(
                (
                    symbol,
                    round(score, 2)
                )
            )

        # =========================
        # SECOND WIND
        # =========================

        recovering = (
            score > prev_score
            and delta > 0.25
        )

        second_wind = (
            recent_high_streak >= 4
            and streak == 0
            and ext < 1
            and recovering
            and score > 0.4
        )

        if second_wind:
            second_wind_signal.append(
                (
                    symbol,
                    round(score, 2),
                    round(ext, 2),
                    recent_high_streak
                )
            )

        # =========================
        # BREAKOUT
        # =========================

        if (
            ret1 > 0.08
            and vol_ratio > 2
        ):
            breakout_signal.append(symbol)

        # =========================
        # SAVE
        # =========================

        prev_scores[symbol] = score

    except:
        continue

# =========================
# SAVE REDIS
# =========================

save_json("gr_prev_scores", prev_scores)
save_json("gr_streak_map", streak_map)
save_json("gr_theme_map", theme_map)
save_json("gr_high_streak_map", high_streak_map)

# =========================
# SORT
# =========================

buy_signal = sorted(
    buy_signal,
    key=lambda x: x[1],
    reverse=True
)[:5]

early_signal = sorted(
    early_signal,
    key=lambda x: x[1],
    reverse=True
)[:4]

transition_signal = sorted(
    transition_signal,
    key=lambda x: x[1],
    reverse=True
)[:4]

cont_signal = sorted(
    cont_signal,
    key=lambda x: x[1],
    reverse=True
)[:4]

second_wind_signal = sorted(
    second_wind_signal,
    key=lambda x: x[1],
    reverse=True
)[:4]

breakout_signal = breakout_signal[:4]

# =========================
# FORMAT
# =========================

lines = []

lines.append(f"🚀 GrowthRadar {VERSION}")
lines.append(f"Scan:{SCAN_SIZE} Valid:{valid_count}")
lines.append(f"Time:{datetime.now().strftime('%Y-%m-%d %H:%M')}")

if redis_ok:
    lines.append("🟢 Redis: ON")
else:
    lines.append("🔴 Redis: OFF")

lines.append("")

# BUY

lines.append("💎 BUY SIGNAL")

if buy_signal:
    for s, sc, st, ex in buy_signal:
        lines.append(
            f"{s} S:{sc} Streak:{st} Ext:{ex}"
        )
else:
    lines.append("None")

lines.append("")

# EARLY

lines.append("🔥 EARLY")

if early_signal:
    for s, sc in early_signal:
        lines.append(
            f"{s} S:{sc}"
        )
else:
    lines.append("None")

lines.append("")

# TRANSITION

lines.append("⚡ TRANSITION")

if transition_signal:
    for s, sc in transition_signal:
        lines.append(
            f"{s} S:{sc}"
        )
else:
    lines.append("None")

lines.append("")

# CONT

lines.append("🔁 CONT")

if cont_signal:
    for s, sc in cont_signal:
        lines.append(
            f"{s} S:{sc}"
        )
else:
    lines.append("None")

lines.append("")

# SECOND WIND

lines.append("🌊 SECOND WIND")

if second_wind_signal:
    for s, sc, ex, hs in second_wind_signal:
        lines.append(
            f"{s} S:{sc} Ext:{ex} PrevStreak:{hs}"
        )
else:
    lines.append("None")

lines.append("")

# BREAKOUT

lines.append("🧨 BREAKOUT (event)")

if breakout_signal:
    for s in breakout_signal:
        lines.append(s)
else:
    lines.append("None")

msg = "\n".join(lines)

print(msg)

# =========================
# DISCORD
# =========================

if WEBHOOK_URL:
    try:
        requests.post(
            WEBHOOK_URL,
            json={"content": msg},
            timeout=10
        )
    except:
        pass
