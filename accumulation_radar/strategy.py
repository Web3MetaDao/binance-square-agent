"""
三策略评分模块

为 build_coin_data 输出的 coin_data 应用三种评分策略：
1. score_chase — 🔥 追多策略（短线轧空机会）
2. score_combined — 📊 综合评分（全面五维打分+OI递进加分）
3. score_ambush — 🎯 埋伏策略（暗流检测）

每个策略接收 coin_data: dict[str, dict]，返回排序后的 list[dict]。
"""

from math import log10


# ── 策略1: 追多（短线轧空） ──

def score_chase(coin_data: dict, prev_fr_map: dict | None = None) -> list[dict]:
    """🔥 追多策略 — 短线轧空机会。

    筛选条件：
    - px_chg > 3%（24h 涨幅 > 3%）
    - fr < 0（资金费率为负，空头燃料）
    - vol > 1_000_000（24h 成交量 > 1M）

    评分规则：
    - 涨幅越高越好：px_chg * 3
    - 费率越负越好：abs(fr) * 5000
    - 成交量越大越好：log10(vol) * 5
    - 费率趋势加分（需传入 prev_fr_map）：
      - 转负 + 20 分
      - 加速恶化 + 10 分
    - 总评分 = 上面几项之和

    Args:
        coin_data: {sym: {px_chg, fr, vol, ...}}
        prev_fr_map: 前一次资金费率快照 {sym: rate}，用于费率趋势分类。
                      为 None 或空时不分类。

    Returns:
        按 chase_score 降序排列的列表，每个元素含 chase_score 和 fr_trend 字段。
    """
    results = []
    for sym, d in coin_data.items():
        px_chg = d.get("px_chg", 0.0)
        fr = d.get("fr", 0.0)
        vol = d.get("vol", 0.0)

        # 筛选条件
        if px_chg <= 3:
            continue
        if fr >= 0:
            continue
        if vol <= 1_000_000:
            continue

        # 评分
        score_px = px_chg * 3
        score_fr = abs(fr) * 5000
        score_vol = log10(max(vol, 1)) * 5
        chase_score = round(score_px + score_fr + score_vol, 2)

        # 费率趋势分类
        fr_trend = None
        if prev_fr_map and sym in prev_fr_map:
            prev_fr = prev_fr_map.get(sym)
            if prev_fr is not None:
                if prev_fr >= 0 and fr < 0:
                    fr_trend = "转负"
                    chase_score += 20
                elif fr < prev_fr - 0.00005:
                    fr_trend = "加速恶化"
                    chase_score += 10
                elif abs(fr - prev_fr) < 0.00005:
                    fr_trend = "持平"
                elif fr > prev_fr + 0.00005:
                    fr_trend = "回升"

        record = dict(d)
        record["chase_score"] = round(chase_score, 2)
        record["fr_trend"] = fr_trend
        results.append(record)

    results.sort(key=lambda x: x["chase_score"], reverse=True)
    return results


# ── 策略2: 综合评分（五维，满分100） ──

def score_combined(coin_data: dict) -> list[dict]:
    """📊 综合评分 — 全面打分，满分100。

    五维（横盘25分拆为横盘15分+热度10分）：
    1️⃣ 费率维度 (f_sc): 25分，费率为负得高分
    2️⃣ OI维度 (o_sc): 25分，OI 6h涨幅越高越好
    3️⃣ 市值维度 (m_sc): 25分，市值越小越好
    4️⃣ 横盘维度 (s_sc): 15分，横盘越久越好
    5️⃣ 热度维度 (h_sc): 10分，cg_trending + vol_surge

    此外 OI 递进字段 oi_segments 有值时额外加分。

    Returns:
        按 total 降序排列的列表，每个元素含 f_sc, o_sc, m_sc, s_sc, h_sc, total 字段。
    """
    results = []
    for sym, d in coin_data.items():
        fr = d.get("fr", 0.0)
        d6h = d.get("d6h", 0.0)
        est_mcap = d.get("est_mcap", 0.0)
        pool_rng = d.get("pool_rng", 0.0)
        sw_days = d.get("sw_days", 0)
        cg_trending = d.get("cg_trending", False)
        vol_surge = d.get("vol_surge", False)
        oi_segments = d.get("oi_segments", [])

        # 1️⃣ 费率维度 (25分)
        if fr < 0:
            f_sc = 15 + min(abs(fr) * 500, 10)
            f_sc = min(f_sc, 25)
        elif fr < 0.0001:  # 正且 < 0.01%
            f_sc = 5
        else:
            f_sc = 0
        f_sc = round(f_sc)

        # 2️⃣ OI 维度 (25分)
        if d6h > 10:
            o_sc = 25
        elif d6h > 5:
            o_sc = 20
        elif d6h > 2:
            o_sc = 15
        elif d6h > 0:
            o_sc = 10
        elif d6h < -5:
            o_sc = 0
        else:
            o_sc = 5  # 轻微负或0
        o_sc = round(o_sc)

        # 3️⃣ 市值维度 (25分)
        if est_mcap < 10_000_000:
            m_sc = 25
        elif est_mcap < 50_000_000:
            m_sc = 20
        elif est_mcap < 200_000_000:
            m_sc = 15
        elif est_mcap < 1_000_000_000:
            m_sc = 10
        else:
            m_sc = 5
        m_sc = round(m_sc)

        # 4️⃣ 横盘维度 (15分)
        if sw_days == 0:
            s_sc = 3
        elif pool_rng < 15 and sw_days >= 30:
            s_sc = 15
        elif pool_rng < 25 and sw_days >= 20:
            s_sc = 12
        elif pool_rng < 40 and sw_days >= 10:
            s_sc = 9
        else:
            s_sc = 3
        s_sc = round(s_sc)

        # 5️⃣ 热度维度 (10分)
        h_sc = 0
        if cg_trending:
            h_sc += 5
        if vol_surge:
            # 如果已有 cg_trending 的 5 分，这里只加 5 达到上限 10
            # 如果只有 vol_surge，加 5
            h_sc += 5
        h_sc = min(h_sc, 10)
        h_sc = round(h_sc)

        # OI 递进加分（当 oi_segments 有值时）
        oi_quality_bonus = 0
        if oi_segments and len(oi_segments) >= 4:
            # 检查每段是否都比前一段高
            all_increasing = all(
                oi_segments[i] > oi_segments[i - 1] for i in range(1, len(oi_segments))
            )
            if all_increasing:
                oi_quality_bonus = 10
            else:
                oi_quality_bonus = 5
        elif oi_segments and len(oi_segments) >= 3:
            # 只有 3 段，部分递进
            increasing_count = sum(
                1 for i in range(1, len(oi_segments)) if oi_segments[i] > oi_segments[i - 1]
            )
            if increasing_count >= 2:
                oi_quality_bonus = 5

        total = f_sc + o_sc + m_sc + s_sc + h_sc + oi_quality_bonus

        record = dict(d)
        record["f_sc"] = f_sc
        record["o_sc"] = o_sc
        record["m_sc"] = m_sc
        record["s_sc"] = s_sc
        record["h_sc"] = h_sc
        record["oi_quality_bonus"] = oi_quality_bonus
        record["total"] = total
        results.append(record)

    results.sort(key=lambda x: x["total"], reverse=True)
    return results


# ── 策略3: 埋伏（暗流检测） ──

def score_ambush(coin_data: dict) -> list[dict]:
    """🎯 埋伏策略 — 暗流检测（长线布局）。

    核心逻辑：OI 涨 + 价格不动 = 资金暗中介入。

    筛选条件：
    - abs(px_chg) < 3%（价格基本不动）
    - d6h > 2%（OI 在上涨）
    - est_mcap < 500M（低市值）

    暗流评分 dc_sc：
    - d6h * 2（OI 涨幅越高越好）
    - (10 - abs(px_chg))（价格越不动越好）
    - 如果在 watchlist（sw_days > 0）：+15 分
    - 低市值加分：< 50M +10, < 200M +5

    Returns:
        按 dc_sc 降序排列的列表，每个元素含 dc_sc 字段。
    """
    results = []
    for sym, d in coin_data.items():
        px_chg = d.get("px_chg", 0.0)
        d6h = d.get("d6h", 0.0)
        est_mcap = d.get("est_mcap", 0.0)
        sw_days = d.get("sw_days", 0)
        oi = d.get("oi", 0.0)

        # 筛选条件
        if abs(px_chg) >= 3:
            continue
        if d6h <= 2:
            continue
        if est_mcap >= 500_000_000:
            continue
        if oi <= 0:  # 没有 OI 数据，忽略
            continue

        # 暗流评分
        score_oi = d6h * 2
        score_price_still = 10 - abs(px_chg)
        score_watchlist = 15 if sw_days > 0 else 0
        score_mcap = 0
        if est_mcap < 50_000_000:
            score_mcap = 10
        elif est_mcap < 200_000_000:
            score_mcap = 5

        dc_sc = round(score_oi + score_price_still + score_watchlist + score_mcap, 2)

        record = dict(d)
        record["dc_sc"] = dc_sc
        results.append(record)

    results.sort(key=lambda x: x["dc_sc"], reverse=True)
    return results
