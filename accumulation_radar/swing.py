"""异动币机会评分引擎。

不只是检测涨跌，而是对异动币做多因子评分，
筛选出真正有交易价值的信号。

双窗口扫描：
  - 5m (短线): 阈值 ±3%，捕获快速波动
  - 15m (中长线): 阈值 ±5%，捕获趋势性异动

数据源通过 sources/market_data 统一获取。
"""

from datetime import datetime, timezone, timedelta

from .config import logger
from .sources.market_data import (
    fetch_global_data,
    fetch_candle_batch,
    MIN_VOL_USDT,
)
from .sources.coinalyze_calibrate import calibrate_prices

# ── 参数 ──────────────────────────────────────────
# 双窗口阈值
SURGE_THRESHOLD_5M = 3.0     # 5m 涨幅 ≥3% 进入候选
DUMP_THRESHOLD_5M = -3.0     # 5m 跌幅 ≤-3% 进入候选
SURGE_THRESHOLD_15M = 5.0    # 15m 涨幅 ≥5% 进入候选
DUMP_THRESHOLD_15M = -5.0    # 15m 跌幅 ≤-5% 进入候选
TOP_N = 10                   # 最终输出币数

# 评分权重
W_VOL = 0.25       # 成交量（越大越好）
W_SWING = 0.20     # 涨跌幅幅度（异动信号强度）
W_CHG24H = 0.15    # 24h涨跌幅（趋势持续性）
W_FR = 0.15        # 费率（负费率有轧空潜力）
W_TRENDING = 0.10  # CoinGecko trending 加分
W_MCAP = 0.15      # 市值（过滤太小的币）

# 市值范围（美元），低于下限过滤
MCAP_MIN = 5_000_000       # 500万刀
MCAP_MAX = 50_000_000_000  # 500亿刀


def _scan_window_batch(
    scan_syms: list,
    bar: str,
    surge_thresh: float,
    dump_thresh: float,
    window_label: str,
    fr_map: dict,
    ticker_map: dict,
    mcap_map: dict,
    cg_trending: set,
    candle_cache: dict | None = None,
) -> tuple:
    """批量 K 线扫描（使用 fetch_candle_batch 并发获取）。

    Returns:
        (surge_candidates, dump_candidates)
    """
    import time as _time
    _t0 = _time.monotonic()

    # 批量获取 K 线（传入缓存或用 fetch_candle_batch）
    if candle_cache is not None:
        candle_map = candle_cache
    else:
        candle_map = fetch_candle_batch(scan_syms, bar)

    surge = []
    dump = []

    for sym in scan_syms:
        candle = candle_map.get(sym)
        if not candle or candle["close_ago"] <= 0:
            continue

        swing = (
            (candle["close_now"] - candle["close_ago"])
            / candle["close_ago"]
            * 100
        )

        if swing >= surge_thresh:
            t = ticker_map.get(sym, {})
            mcap = mcap_map.get(sym, 0)
            if mcap < MCAP_MIN or mcap > MCAP_MAX:
                continue
            surge.append({
                "sym": sym, "swing": swing, "vol": t.get("vol", 0),
                "chg24h": t.get("chg24h", 0), "fr": fr_map.get(sym, 0.0),
                "mcap": mcap, "px": t.get("px", 0),
                "cg_trending": sym in cg_trending,
                "window": window_label,
            })
        elif swing <= dump_thresh:
            t = ticker_map.get(sym, {})
            mcap = mcap_map.get(sym, 0)
            if mcap < MCAP_MIN or mcap > MCAP_MAX:
                continue
            dump.append({
                "sym": sym, "swing": swing, "vol": t.get("vol", 0),
                "chg24h": t.get("chg24h", 0), "fr": fr_map.get(sym, 0.0),
                "mcap": mcap, "px": t.get("px", 0),
                "cg_trending": sym in cg_trending,
                "window": window_label,
            })

    cost = _time.monotonic() - _t0
    logger.info(
        f"[Swing] {window_label} 扫描: surge={len(surge)}, dump={len(dump)} "
        f"({len(candle_map)}/{len(scan_syms)} K线返回, {cost:.1f}s)"
    )
    return surge, dump, candle_map


def _score_candidate(candidate: dict, is_surge: bool) -> dict:
    """单币多因子评分。"""
    swing = abs(candidate["swing"])
    vol = candidate["vol"]
    chg24h = candidate["chg24h"]
    fr = candidate["fr"]
    mcap = candidate["mcap"]
    px = candidate["px"]

    # 1. 成交量评分（对数归一化）
    vol_score = min(100, (vol / 1_000_000) ** 0.3 * 30)

    # 2. 异动幅度
    swing_score = min(100, swing * 10)

    # 3. 24h 趋势
    if is_surge:
        chg24h_score = max(0, min(100, chg24h * 3))
    else:
        chg24h_score = max(0, min(100, abs(chg24h) * 3))

    # 4. 费率评分
    if is_surge:
        fr_score = max(0, min(100, (-fr / 0.001) * 50))
    else:
        fr_score = max(0, min(100, (fr / 0.001) * 50))

    # 5. CG trending
    trending_score = 30 if candidate["cg_trending"] else 0

    # 6. 市值评分
    mcap_m = mcap / 1_000_000
    if 10 <= mcap_m <= 1000:
        mcap_score = 80
    elif 1000 < mcap_m <= 10000:
        mcap_score = 60
    elif 5 <= mcap_m < 10:
        mcap_score = 40
    else:
        mcap_score = 20

    total = (
        vol_score * W_VOL
        + swing_score * W_SWING
        + chg24h_score * W_CHG24H
        + fr_score * W_FR
        + trending_score * W_TRENDING
        + mcap_score * W_MCAP
    )

    reasons = []
    if vol_score >= 50:
        reasons.append("放量")
    if trending_score > 0:
        reasons.append("CG热度")
    if (is_surge and fr_score >= 30) or (not is_surge and fr_score >= 30):
        reasons.append(f"费率{'偏空' if is_surge else '偏多'}")

    coin_name = candidate["sym"].replace("USDT", "")

    return {
        "coin": coin_name,
        "sym": candidate["sym"],
        "score": round(total, 1),
        "swing": round(swing, 2),
        "chg24h": chg24h,
        "fr": round(fr * 100, 4),
        "vol": vol,
        "px": px,
        "mcap": mcap,
        "window": candidate.get("window", "5m"),
        "reasons": "/".join(reasons) if reasons else "",
    }


def score_opportunities(
    ticker_map: dict,
    fr_map: dict,
    mcap_map: dict,
    cg_trending: set,
) -> dict | None:
    """多窗口异动检测 + 多因子评分。

    处理流程:
      1. 候选池构建（成交量 top300 + 24h涨跌幅极端前50）
      2. 5m 窗口批量扫描（阈值 ±3%）
      3. 15m 窗口批量扫描（阈值 ±5%，复用部分5m K线）
      4. 合并候选 + 多因子评分 + 排序取 TopN
      5. 交叉窗口信号加分（同币在两个窗口都命中）

    Returns:
        {"surge": [{coin, score, sym, swing, chg24h, fr, vol, px, mcap, window, reasons}, ...],
         "dump":  [{...}, ...],
         "btc_chg24h": float,
         "candidates_scanned": int,
         "timestamp": "..."}
    """
    import time as _time

    if not ticker_map:
        return None

    # BTC 24h 涨跌幅作为大盘基准
    btc_sym = "BTCUSDT"
    btc_chg24h = ticker_map.get(btc_sym, {}).get("chg24h", 0.0)

    # ── 阶段1: 确定候选扫描范围 ──
    sorted_by_vol = sorted(
        ticker_map.keys(), key=lambda s: ticker_map[s]["vol"], reverse=True
    )
    top_300 = set(sorted_by_vol[:300])

    chg_sorted = sorted(ticker_map.items(), key=lambda x: x[1]["chg24h"])
    extreme = set()
    for sym, _ in chg_sorted[:50]:
        extreme.add(sym)
    for sym, _ in chg_sorted[-50:]:
        extreme.add(sym)

    scan_syms = list(top_300 | extreme)
    _t0 = _time.monotonic()
    logger.info(
        f"[Swing] 候选池 {len(scan_syms)} 个币 → 5m+15m 批量扫描..."
    )

    # ── 阶段2: 批量获取 K 线（先拉5m，再拉15m） ──
    # 5m K线
    s5_surge, s5_dump, c5 = _scan_window_batch(
        scan_syms, "5m",
        SURGE_THRESHOLD_5M, DUMP_THRESHOLD_5M,
        "5m", fr_map, ticker_map, mcap_map, cg_trending,
    )

    # 15m K线（部分币的 close_now 可能复用，但15m bar不同需要独立请求）
    s15_surge, s15_dump, c15 = _scan_window_batch(
        scan_syms, "15m",
        SURGE_THRESHOLD_15M, DUMP_THRESHOLD_15M,
        "15m", fr_map, ticker_map, mcap_map, cg_trending,
    )

    total_surge = len(s5_surge) + len(s15_surge)
    total_dump = len(s5_dump) + len(s15_dump)
    cost = _time.monotonic() - _t0
    logger.info(
        f"[Swing] 双窗口汇总: surge={total_surge}, dump={total_dump} "
        f"(耗时 {cost:.1f}s)"
    )

    if total_surge == 0 and total_dump == 0:
        logger.info("[Swing] 无候选币通过阈值")
        return None

    # ── 阶段3: 合并 + 交叉窗口加分 ──
    def _merge(primary_list, secondary_list):
        merged = {}
        for c in primary_list:
            merged[c["sym"]] = c
        for c in secondary_list:
            sym = c["sym"]
            if sym in merged:
                merged[sym]["window"] = "5m+15m"
            else:
                merged[sym] = c
        return list(merged.values())

    surge_candidates = _merge(s5_surge, s15_surge)
    dump_candidates = _merge(s5_dump, s15_dump)

    # ── 阶段4: 多因子评分 ──
    scored_surge = sorted(
        [_score_candidate(c, True) for c in surge_candidates],
        key=lambda x: x["score"], reverse=True,
    )[:TOP_N]

    scored_dump = sorted(
        [_score_candidate(c, False) for c in dump_candidates],
        key=lambda x: x["score"], reverse=True,
    )[:TOP_N]

    if not scored_surge and not scored_dump:
        logger.info("[Swing] 评分后无符合条件的异动机会")
        return None

    now = datetime.now(timezone(timedelta(hours=8))).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    result = {
        "surge": scored_surge,
        "dump": scored_dump,
        "btc_chg24h": btc_chg24h,
        "candidates_scanned": len(scan_syms),
        "timestamp": now,
    }

    logger.info(
        f"[Swing] ✅ 机会榜: surge={len(scored_surge)}, dump={len(scored_dump)}"
    )
    return result
