def detect(df):
    early, expansion, strong = [], [], []

    for _, r in df.iterrows():
        s = r["score"]
        m1 = r["m1"]
        m3 = r["m3"]
        m6 = r["m6"]
        accel = r["accel"]

        # -----------------
        # EARLY（軽め）
        # -----------------
        if s > 0.10 and m1 > 0:
            early.append(r)

        # -----------------
        # EXPANSION（中核）
        # -----------------
        if (
            s > 0.20
            and m3 > 0.2
            and accel > 0
        ):
            expansion.append(r)

        # -----------------
        # STRONG（本命だけ）
        # -----------------
        if (
            s > 0.30          # スコア強い
            and m3 > 0.4      # 中期トレンド強い
            and m6 > 0.5      # 長期もちゃんと上
            and accel > 0     # 加速してる
            and m1 < 1.5      # バーストしすぎ除外（重要）
        ):
            strong.append(r)

    early = sorted(early, key=lambda x: x["score"], reverse=True)
    expansion = sorted(expansion, key=lambda x: x["score"], reverse=True)
    strong = sorted(strong, key=lambda x: x["score"], reverse=True)

    return early, expansion, strong
