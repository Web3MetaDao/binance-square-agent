#!/usr/bin/env python3
"""
scoring.py — Pure scoring engine for multi-exchange, multi-timeframe kline data.

Exports:
  score_kline(sym, price, chg24h, kline, vol_24h=0) -> dict
  merge_multi_scores(scores) -> dict
  grade_from_score(score) -> str

Constants:
  STRAT, WEIGHTS, MAX_SCORE

The Golden 5 + RAVE + momentum/volume accel + bear + Supertrend + ADX + BB/RSI reversal.
Bull max raw = 130, normalized to 0-100.
"""

import numpy as np
import talib

# ── Strategy parameters ─────────────────────────────────
STRAT = {
    "vol_ratio_min": 2.0,          # Volume >= 2x avg of last 10 (relaxed from 3x)
    "vol_ratio_full": 3.0,         # Volume >= 3x for full points
    "rsi_min": 30,                 # RSI lower bound
    "rsi_max": 75,                 # RSI upper bound
    "atr_min": 1.2,                # ATR multiplier threshold for half points
    "atr_double_min": 1.5,         # ATR multiplier threshold for full points
    "ma88_dev_max": 0.05,          # MA88 tight deviation ±5% for full points
    "ma88_dev_loose": 0.12,        # MA88 loose deviation ±12% for half points
    "obv_lookback": 20,            # OBV lookback window
    "price_break_lookback": 10,    # Price break lookback window
}

# ── Scoring weights (bull max = 102) ────────────────────
# Adjusted based on backtest results (top-15 OKX 4H, lookahead=4 bars):
#   EMA9/26金叉: 100% win (keep),  EMA9/26多头延续: 47.5% (reduce)
#   MA10/EMA10金叉: 83.3% (keep),  MA10/EMA10多头排列: 40.5% (reduce)
#   动量加速: 71.4% (up),  量能递增: good (up)
#   OBV突破: 46.2% (down),  MA88支撑: 54.2% (keep)
#   突破前高: 54.5% (keep),  RSI: mixed (keep)
#   Volume surge 2x-3x: 60%+ with trend (keep)
WEIGHTS = {
    "trend": 10,           # EMA9/26 cross (reduced from 15 — "多头延续" too noisy)
    "entry": 10,           # MA10/EMA10 cross (83.3% win — keep)
    "volume": 10,          # Volume surge: 5 for 2x, 10 for 3x+ (60%+ — keep)
    "obv": 5,              # OBV breakout (reduced from 8 — 46.2% win, noisy)
    "rsi": 8,              # RSI in 30-75 (keep)
    "atr": 5,              # ATR: 2.5 for 1.2x, 5 for 1.5x+ (keep)
    "breakout": 8,         # Price breakout (54.5% — keep)
    "ma88": 8,             # MA88 support: full for ±5%, half for ±12% (54.2% — keep)
    "pattern": 12,         # RAVE pattern (reduced from 15 — rare, too much weight)
    "momentum_accel": 10,  # Price momentum acceleration (increased from 8 — 71.4% win)
    "volume_accel": 10,    # Volume momentum acceleration (increased from 8 — good)
    "micro": 4,            # Micro signal — reduced from 6, make room
    "micro_macro": 8,      # NEW: OFI (订单流不平衡度) — paper-validated #1 feature
    "vwap_dev": 4,         # NEW: VWAP偏离度 — reduced from 6, still effective filter
    "supertrend": 6,       # NEW: Supertrend趋势过滤 — ATR-based, filters震荡市噪音
    "adx": 6,              # NEW: ADX趋势强度 — 区分趋势/震荡市
    "bb_rsi_reversal": 6,  # NEW: 布林带+RSI超卖反转 — 捕捉低位反弹
    "candle_wick": 6,      # NEW: K线影线形态识别 — 锤子线/射击之星反转 (Binance-Futures-Bot)
    "fvg": 6,              # NEW: FVG公允价值缺口 — SMC核心 (smart-money-concepts ★1611)
    "umacd": 5,            # NEW: UniversalMACD均值回归 — EMA12/26比率 (Freqtrade community)
    "chandelier": 4,       # NEW: Chandelier Exit止盈止损位检测 (auto-injected 2026-05-02)
    "bear": 18,            # Bearish signals track separately — reduced from 20
}
MAX_SCORE = sum(WEIGHTS.values()) - WEIGHTS["bear"]  # 133


# ═══════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════

def tiered_signal(ratio: float, threshold_half: float, threshold_full: float,
                  weight_full: float) -> float:
    """
    Return points based on a ratio crossing thresholds:
    - If ratio >= threshold_full: full weight
    - If ratio >= threshold_half: half weight
    - Otherwise: 0
    """
    if ratio >= threshold_full:
        return weight_full
    elif ratio >= threshold_half:
        return weight_full * 0.5
    return 0.0


def grade_from_score(score: int) -> str:
    """Return grade letter based on normalized score (0-100)."""
    if score >= 70:
        return "A"
    elif score >= 45:
        return "B"
    elif score >= 25:
        return "C"
    return "D"


# ═══════════════════════════════════════════════════════════
# Core scoring engine
# ═══════════════════════════════════════════════════════════

def score_kline(
    sym: str,
    price: float,
    chg24h: float,
    kline: dict,
    vol_24h: int = 0,
    hours_since_listing: int = 9999,
    extras: dict = None,
) -> dict:
    """
    Score a single symbol on a single (exchange, timeframe) pair.

    Parameters
    ----------
    sym : str
        Symbol, e.g. "BTCUSDT".
    price : float
        Current price.
    chg24h : float
        24-hour price change percent.
    kline : dict
        Dict with numpy float64 arrays: open, high, low, close, volume, times.
        Each array is expected to be 130 elements long; index -1 is latest.
    vol_24h : int
        24h volume in USDT (from ticker).
    hours_since_listing : int
        Hours since the token was listed. Default 9999 means unknown/old.
    extras : dict or None
        Optional dict with extra signals: funding_rate, oi_growth_pct,
        cg_trending, large_taker, long_short_ratio.

    Returns
    -------
    dict with keys:
        sym, price, chg24h, vol_24h, score (0-100), grade (A/B/C/D),
        signals (list of passed signal names),
        signals_fail (list of failed signal names),
        signals_bear (list of bearish signal names),
        bear_score (int), has_bear (bool),
        details (list of human-readable detail strings),
        patterns (list of recognized pattern names),
        entry_advice, exit_advice,
        ema9, ema26, ma10, ema10, ma88, ma88_dev, rsi, vol_ratio, trend.
        new_coin_bonus (int) — bonus points from new-coin premium.
    """
    c = kline
    opens = c["open"]
    highs = c["high"]
    lows = c["low"]
    closes = c["close"]
    volumes = c["volume"]
    i = -1  # latest index
    n_bars = len(closes)  # expected 130
    cfg = STRAT
    isnan = np.isnan

    # ── Compute indicators ──────────────────────────────
    ema9 = talib.EMA(closes, 9)
    ema26 = talib.EMA(closes, 26)
    ema10 = talib.EMA(closes, 10)
    ma10 = talib.MA(closes, 10)
    ma88 = talib.MA(closes, 88)
    rsi14 = talib.RSI(closes, 14)
    atr14 = talib.ATR(highs, lows, closes, 14)
    macd, macds, macdh = talib.MACD(closes, 12, 26, 9)
    obv = talib.OBV(closes, volumes)

    # ── Result dict ─────────────────────────────────────
    result = {
        "sym": sym,
        "price": price,
        "chg24h": chg24h,
        "vol_24h": vol_24h,
        "score": 0,
        "grade": "D",
        "signals": [],
        "signals_fail": [],
        "signals_bear": [],
        "bear_score": 0,
        "has_bear": False,
        "details": [],
        "patterns": [],
        "entry_advice": "",
        "exit_advice": "",
        "ema9": None,
        "ema26": None,
        "ma10": None,
        "ema10": None,
        "ma88": None,
        "ma88_dev": None,
        "rsi": None,
        "vol_ratio": None,
        "trend": None,
        "bonus": 0,
    "volatility_score": "normal",
    "atr_price_ratio": 0.0,
    "ofi": 0.0,
    "vwap_dev_bps": 0.0,
    "new_coin_bonus": 0,
    "extras_score": 0,
    "supertrend_trend": None,
    "supertrend_changed": False,
    "adx": None,
    "adx_phase": None,
    "bb_lower": None,
    "bb_middle": None,
    "bb_upper": None,
    "bb_rsi_signal": False,
    "candle_wick_signal": None,   # NEW: K线影线形态: "hammer"/"shooting_star"/None
    "fvg_signal": None,           # NEW: FVG方向: 1(bull)/-1(bear)/None
    "fvg_top": 0.0,              # NEW: FVG上边界
    "fvg_bottom": 0.0,           # NEW: FVG下边界
    "umacd": 0.0,                # NEW: UniversalMACD (EMA12/EMA26 - 1)
    "umacd_signal": False,       # NEW: UniversalMACD均值回归信号
    "chandelier_long_stop": 0.0, # NEW: Chandelier Exit长止损位
    "chandelier_short_stop": 0.0,# NEW: Chandelier Exit短止损位
}

    raw_score = 0  # accumulates up to MAX_SCORE
    bear_score = 0

    # ── 1. TREND: EMA9/26 ────────────────────────────────
    if not isnan(ema9[i]) and not isnan(ema26[i]):
        trend_bull = ema9[i] > ema26[i]
        ema9_prev = ema9[i - 1] if n_bars + i >= 1 and not isnan(ema9[i - 1]) else None
        ema26_prev = ema26[i - 1] if n_bars + i >= 1 and not isnan(ema26[i - 1]) else None

        result["ema9"] = float(round(ema9[i], 4))
        result["ema26"] = float(round(ema26[i], 4))
        result["trend"] = "bull" if trend_bull else "bear"

        # Golden cross: was bearish, now bullish
        golden = (
            trend_bull
            and ema9_prev is not None and ema26_prev is not None
            and ema9_prev <= ema26_prev
        )
        # Death cross: was bullish, now bearish
        death = (
            not trend_bull
            and ema9_prev is not None and ema26_prev is not None
            and ema9_prev >= ema26_prev
        )

        # Compute EMA gap ratio to filter weak continuation (gap must be >0.1% of EMA26)
        ema_gap_ratio = abs(float(ema9[i] - ema26[i])) / float(ema26[i]) if ema26[i] > 0 else 0.0
        has_clear_gap = ema_gap_ratio > 0.001

        if golden:
            raw_score += WEIGHTS["trend"]
            result["signals"].append("EMA9/26金叉(多)")
            result["details"].append(
                f"EMA9={ema9[i]:.4f} > EMA26={ema26[i]:.4f} ✅"
            )
        elif trend_bull and has_clear_gap:
            # Bull continuation: half points (only if gap >0.1% — filters weak/noisy crosses)
            raw_score += WEIGHTS["trend"] * 0.5
            result["signals"].append("EMA9/26多头延续")
            result["details"].append(
                f"EMA9={ema9[i]:.4f} > EMA26={ema26[i]:.4f} (多头延续, gap={ema_gap_ratio:.4f})"
            )
        elif trend_bull:
            # Weak bull: gap too small, no points awarded (barely crossed noise)
            result["details"].append(
                f"EMA9={ema9[i]:.4f} > EMA26={ema26[i]:.4f} (弱多, gap={ema_gap_ratio:.4f} — 忽略)"
            )
        else:
            if death:
                result["signals_fail"].append("EMA9/26死叉(空)")
            result["details"].append(
                f"EMA9={ema9[i]:.4f} < EMA26={ema26[i]:.4f} ❌"
            )

    # ── 2. ENTRY: MA10/EMA10 ─────────────────────────────
    if not isnan(ma10[i]) and not isnan(ema10[i]):
        entry_bull = ema10[i] > ma10[i]
        ema10_prev = ema10[i - 1] if n_bars + i >= 1 and not isnan(ema10[i - 1]) else None
        ma10_prev = ma10[i - 1] if n_bars + i >= 1 and not isnan(ma10[i - 1]) else None

        result["ma10"] = float(round(ma10[i], 4))
        result["ema10"] = float(round(ema10[i], 4))

        golden10 = (
            entry_bull
            and ema10_prev is not None and ma10_prev is not None
            and ema10_prev <= ma10_prev
        )
        death10 = (
            not entry_bull
            and ema10_prev is not None and ma10_prev is not None
            and ema10_prev >= ma10_prev
        )

        if golden10:
            raw_score += WEIGHTS["entry"]
            result["signals"].append("MA10/EMA10金叉")
            result["details"].append(
                f"EMA10={ema10[i]:.4f} > MA10={ma10[i]:.4f} ✅"
            )
        elif entry_bull:
            # Bull continuation: half points
            raw_score += WEIGHTS["entry"] * 0.5
            result["signals"].append("MA10/EMA10多头排列")
            result["details"].append(
                f"EMA10={ema10[i]:.4f} > MA10={ma10[i]:.4f} (多头上行)"
            )
        else:
            if death10:
                result["signals_fail"].append("MA10/EMA10死叉(逃顶)")
            result["details"].append(
                f"EMA10={ema10[i]:.4f} < MA10={ma10[i]:.4f} ❌"
            )

    # ── 3. VOLUME: surge + OBV ──────────────────────────
    # Volume ratio: current volume / avg of last 10
    if n_bars + i >= 10:
        avg_vol_10 = float(np.mean(volumes[i - 10 : i]))
    else:
        avg_vol_10 = 0.0

    vol_ratio = float(volumes[i]) / avg_vol_10 if avg_vol_10 > 0 else 0.0
    result["vol_ratio"] = round(vol_ratio, 2)

    # Tiered volume surge: 2x → half points, 3x+ → full points
    vol_points = tiered_signal(vol_ratio, cfg["vol_ratio_min"], cfg["vol_ratio_full"], WEIGHTS["volume"])
    if vol_points > 0:
        # Vol surge is a bull signal — but also check if it's down day (bear)
        if closes[i] < closes[i - 1] if n_bars + i >= 1 else False:
            # Down day with volume surge is bearish
            bear_score += 8
            result["signals_bear"].append("放量下跌")
            result["details"].append(f"量比={vol_ratio:.1f}x (下跌放量) ⚠️")
        else:
            raw_score += vol_points
            if vol_ratio >= cfg["vol_ratio_full"]:
                result["signals"].append(f"成交量{vol_ratio:.0f}x")
            else:
                result["signals"].append(f"成交量{vol_ratio:.1f}x")
            result["details"].append(f"量比={vol_ratio:.1f}x ✅")

    # OBV breakout: current OBV > max of last N
    lb = cfg["obv_lookback"]
    if n_bars + i >= lb and not isnan(obv[i]):
        obv_peak = float(np.max(obv[i - lb : i]))
        obv_break = obv[i] > obv_peak
        if obv_break:
            raw_score += WEIGHTS["obv"]
            result["signals"].append("OBV突破")
            result["details"].append(f"OBV突破前{lb}根高点 ✅")

    # ── 4. STRENGTH: RSI + ATR ──────────────────────────
    rsi_val = float(rsi14[i]) if not isnan(rsi14[i]) else 50.0
    result["rsi"] = round(rsi_val, 1)

    if cfg["rsi_min"] <= rsi_val <= cfg["rsi_max"]:
        raw_score += WEIGHTS["rsi"]
        result["signals"].append(f"RSI={rsi_val:.0f}")
        result["details"].append(f"RSI={rsi_val:.0f} ✅")
    else:
        result["details"].append(f"RSI={rsi_val:.0f} (区间外)")

    # RSI超买 (bear warning) — always check regardless of RSI scoring
    if rsi_val > 75:
        result["signals_bear"].append("RSI超买")
        result["details"].append(f"RSI={rsi_val:.0f} 超买 ⚠️")

    # ATR: tiered — 1.2x → half, 1.5x+ → full
    atr_now = float(atr14[i]) if not isnan(atr14[i]) else 0.0
    atr_prev = float(atr14[i - 1]) if n_bars + i >= 1 and not isnan(atr14[i - 1]) else 0.0
    if atr_prev > 0:
        atr_ratio = atr_now / atr_prev
        atr_points = tiered_signal(atr_ratio, cfg["atr_min"], cfg["atr_double_min"], WEIGHTS["atr"])
        if atr_points > 0:
            raw_score += atr_points
            result["signals"].append(f"ATR放大{atr_ratio:.1f}x")
            result["details"].append(
                f"ATR={atr_now:.6f} (前={atr_prev:.6f}) ✅"
            )

    # ── 5. BREAKOUT: price breaks previous high ──────────
    lb_break = cfg["price_break_lookback"]
    if n_bars + i >= lb_break:
        prev_high = float(np.max(highs[i - lb_break : i]))
        price_break = closes[i] > prev_high
        if price_break:
            raw_score += WEIGHTS["breakout"]
            result["signals"].append("突破前高")
            result["details"].append(f"价格突破前{lb_break}根高点 ✅")

    # ── MA88 SUPPORT (tiered) ────────────────────────────
    if not isnan(ma88[i]) and ma88[i] > 0:
        ma88_dev = float((closes[i] - ma88[i]) / ma88[i])
        result["ma88"] = float(round(ma88[i], 4))
        result["ma88_dev"] = round(ma88_dev * 100, 1)  # percent

        dev_abs = abs(ma88_dev)
        if dev_abs <= cfg["ma88_dev_max"]:
            raw_score += WEIGHTS["ma88"]  # full points
            result["signals"].append("MA88支撑")
            result["details"].append(
                f"MA88={ma88[i]:.4f} (偏离{ma88_dev:+.1%}) ✅"
            )
        elif dev_abs <= cfg["ma88_dev_loose"]:
            raw_score += WEIGHTS["ma88"] * 0.5  # half points
            result["signals"].append("MA88支撑(宽松)")
            result["details"].append(
                f"MA88={ma88[i]:.4f} (偏离{ma88_dev:+.1%}) ⭕"
            )
        else:
            result["details"].append(
                f"MA88={ma88[i]:.4f} (偏离{ma88_dev:+.1%})"
            )

    # ── VOLUME MOMENTUM ACCELERATION (量能递增) ───────────
    # Compare avg vol of last 5 bars vs previous 5 bars
    if n_bars + i >= 10:
        recent_vol_5 = float(np.mean(volumes[i - 4 : i + 1]))  # last 5 incl current
        prior_vol_5 = float(np.mean(volumes[i - 9 : i - 4]))   # previous 5
        if prior_vol_5 > 0 and recent_vol_5 > prior_vol_5 * 1.3:
            raw_score += WEIGHTS["volume_accel"]
            result["signals"].append("量能递增")
            result["details"].append(
                f"近5均量/前5均量={recent_vol_5/prior_vol_5:.2f}x ✅"
            )

    # ── PRICE MOMENTUM ACCELERATION (动量加速) ────────────
    # ROC of last 3 closes vs ROC of 3 closes before that
    if n_bars + i >= 6:
        roc_recent = (closes[i] - closes[i - 2]) / closes[i - 2] if closes[i - 2] > 0 else 0.0
        roc_prior = (closes[i - 3] - closes[i - 5]) / closes[i - 5] if closes[i - 5] > 0 else 0.0
        if roc_prior > 0 and roc_recent > roc_prior * 1.2:
            raw_score += WEIGHTS["momentum_accel"]
            result["signals"].append("动量加速")
            result["details"].append(
                f"近3ROC={roc_recent:.4f} > 前3ROC={roc_prior:.4f}*1.2 ✅"
            )

    # ── MICRO SIGNAL (短线突拉 / 短线急跌) ─────────────────
    # Last 2 bars avg vol > prev 10 avg * 2.5 AND price change > 0.5%
    if n_bars + i >= 12:
        avg_vol_last2 = float(np.mean(volumes[i - 1 : i + 1]))
        avg_vol_prev10 = float(np.mean(volumes[i - 11 : i - 1]))
        if avg_vol_prev10 > 0 and avg_vol_last2 > avg_vol_prev10 * 2.5:
            # Check up move
            if closes[i] > closes[i - 2] * 1.005:
                raw_score += WEIGHTS["micro"]
                result["signals"].append("短线突拉")
                result["details"].append(
                    f"近2均量/前10均量={avg_vol_last2/avg_vol_prev10:.1f}x + 拉升 ✅"
                )
            # Check down move (bearish)
            if closes[i] < closes[i - 2] * 0.995:
                bear_score += 5
                result["signals_bear"].append("短线急跌")
                result["details"].append(
                    f"近2均量/前10均量={avg_vol_last2/avg_vol_prev10:.1f}x + 急跌 ⚠️"
                )

    # ── MICRO-MACRO: OFI (订单流不平衡度 近似版) ────────────
    # 论文 arXiv:2602.00776 验证SHAP值最高的单一特征
    # 用阳线/阴线体积累积近似 OFI
    if n_bars + i >= 20:
        # 近20根K线的成交量加权买卖压力
        bull_vol = float(np.sum(volumes[i - 19 : i + 1][closes[i - 19 : i + 1] > opens[i - 19 : i + 1]]))
        bear_vol = float(np.sum(volumes[i - 19 : i + 1][closes[i - 19 : i + 1] < opens[i - 19 : i + 1]]))
        total_vol = bull_vol + bear_vol
        if total_vol > 0:
            ofi = (bull_vol - bear_vol) / total_vol  # [-1, +1]
            result["ofi"] = round(ofi, 4)
            # OFI > 0.3 = 强买入压力
            if ofi > 0.3:
                raw_score += WEIGHTS["micro_macro"]
                result["signals"].append(f"OFI={ofi:.2f}")
                result["details"].append(f"OFI={ofi:.2f} (强买压) ✅ +{WEIGHTS['micro_macro']}")
            # OFI < -0.3 = 强卖出压力 → bear信号
            elif ofi < -0.3:
                bear_score += WEIGHTS["micro_macro"]
                result["signals_bear"].append(f"OFI={ofi:.2f}")
                result["details"].append(f"OFI={ofi:.2f} (强卖压) ⚠️")
        else:
            result["ofi"] = 0.0
    else:
        result["ofi"] = 0.0

    # ── NEW: SUPERTREND 趋势过滤 (P0: ATR-based趋势跟踪) ──────────
    # 用numpy实现Supertrend (talib可能不支持SUPERTREND)
    # Supertrend = 当价格在ATR通道上方=多头(1), 下方=空头(-1)
    if n_bars + i >= 10:
        # 计算TR
        tr = np.maximum(highs - lows, 
                        np.maximum(np.abs(highs - np.roll(closes, 1)),
                                   np.abs(lows - np.roll(closes, 1))))
        # ATR (SMA)
        atr_st = np.zeros_like(closes)
        atr_st[0] = tr[0]
        for j in range(1, n_bars):
            atr_st[j] = (atr_st[j-1] * 9 + tr[j]) / 10
        
        # 上下轨
        period = 10
        multiplier = 3.0
        hl_avg = (highs + lows) / 2
        upper_band = hl_avg + multiplier * atr_st
        lower_band = hl_avg - multiplier * atr_st
        
        # 方向跟踪
        supertrend_dir = np.ones(n_bars, dtype=np.int32)  # -1 or 1
        supertrend_main = np.zeros(n_bars)
        for j in range(1, n_bars):
            if closes[j] > upper_band[j]:
                supertrend_dir[j] = 1
            elif closes[j] < lower_band[j]:
                supertrend_dir[j] = -1
            else:
                supertrend_dir[j] = supertrend_dir[j-1]
            # main band
            if supertrend_dir[j] == 1:
                supertrend_main[j] = lower_band[j]
            else:
                supertrend_main[j] = upper_band[j]
        
        st_dir = int(supertrend_dir[i])
        st_dir_prev = int(supertrend_dir[i-1])
        result["supertrend_trend"] = st_dir
        changed = st_dir != st_dir_prev
        result["supertrend_changed"] = changed
        
        if st_dir == 1:
            if changed:
                raw_score += WEIGHTS["supertrend"]
                result["signals"].append("SuperTrend金叉(转多)")
                result["details"].append(f"SuperTrend方向切换: 空→多 ✅ +{WEIGHTS['supertrend']}")
            else:
                raw_score += WEIGHTS["supertrend"] // 2
                result["signals"].append("SuperTrend多头")
                result["details"].append(f"SuperTrend多头延续 ✅ +{WEIGHTS['supertrend']//2}")
        else:
            if changed:
                bear_score += WEIGHTS["supertrend"]
                result["signals_bear"].append("SuperTrend死叉(转空)")
                result["details"].append(f"SuperTrend方向切换: 多→空 ⚠️")
            else:
                result["details"].append(f"SuperTrend空头延续")

    # ── NEW: ADX 趋势强度 (P0: 区分趋势/震荡) ──────────────────────
    adx = talib.ADX(highs, lows, closes, timeperiod=14)
    if not isnan(adx[i]):
        adx_val = float(adx[i])
        result["adx"] = round(adx_val, 1)
        
        if adx_val > 25:
            result["adx_phase"] = "trending"
            raw_score += WEIGHTS["adx"]
            result["signals"].append(f"ADX={adx_val:.0f}(趋势市)")
            result["details"].append(f"ADX={adx_val:.0f}>25 趋势市 ✅ +{WEIGHTS['adx']}")
        elif adx_val < 20:
            result["adx_phase"] = "ranging"
            # 震荡市不给趋势分 — 但允许反转信号(OFI/BB+RSI/CandleWick/FVG)正常运作
            result["details"].append(f"ADX={adx_val:.0f}<20 震荡市 ⭕")
        else:
            result["adx_phase"] = "transition"
            raw_score += WEIGHTS["adx"] // 2
            result["signals"].append(f"ADX={adx_val:.0f}(过渡)")
            result["details"].append(f"ADX={adx_val:.0f} 趋势过渡 ✅ +{WEIGHTS['adx']//2}")

    # ── ADX STRATEGY LINKAGE: 震荡市减益趋势/突破指标 ──────────
    # 学习发现: ADX<20时趋势指标(EMA9/26, 突破前高, RAVE)误报率高
    # paper验证: 震荡市方向性信号SHAP值降低30-40%
    # 策略: 标记adx_ranging_penalty, 在最终评分中统一处减
    adx_ranging_penalty = False
    if result.get("adx_phase") == "ranging":
        adx_ranging_penalty = True

    # ── NEW: 布林带+RSI 超卖反转 (P0: 低位反弹捕捉) ────────────────
    # 条件: 价格触及布林带下轨 + RSI<30 + 当前bar收阳 + 成交量放大
    bb_upper, bb_middle, bb_lower = talib.BBANDS(closes, timeperiod=20, nbdevup=2, nbdevdn=2)
    if not isnan(bb_lower[i]) and not isnan(bb_upper[i]):
        bb_l_val = float(bb_lower[i])
        bb_m_val = float(bb_middle[i])
        bb_u_val = float(bb_upper[i])
        result["bb_lower"] = round(bb_l_val, 4)
        result["bb_middle"] = round(bb_m_val, 4)
        result["bb_upper"] = round(bb_u_val, 4)
        
        # 价格触及/跌破下轨 + RSI超卖 + 当前收阳 + 成交量放大
        if rsi_val < 30 and closes[i] <= bb_l_val * 1.01:
            # 至少需要bar收阳或低点反弹
            bb_rsi_signal = False
            if n_bars + i >= 1 and closes[i] > opens[i] and closes[i] > closes[i-1]:
                bb_rsi_signal = True
                extra_pts = 0
                # 成交量确认
                if vol_ratio > 1.5:
                    extra_pts += 2
                    result["details"].append(f"    BB放量确认(量比={vol_ratio:.1f}x) +2")
                # 从下轨下方强势反弹
                if lows[i] <= bb_l_val * 0.995:
                    extra_pts += 2
                    result["details"].append(f"    穿破下轨后反弹 +2")
                
                score_pts = WEIGHTS["bb_rsi_reversal"] + extra_pts
                raw_score += score_pts
                result["bb_rsi_signal"] = True
                result["signals"].append("BB+RSI超卖反转")
                result["details"].append(
                    f"📉 价格触BB下轨({bb_l_val:.4f})+RSI={rsi_val:.0f}<30+收阳反转 +{score_pts}"
                )

                # ── OFI+BB/RSI共振: 超卖反转+强买压=爆发力叠加 ──────────
                if "OFI" in result.get("signals", []):
                    bb_extra = 3
                    raw_score += bb_extra
                    result["details"].append(f"    OFI+BB/RSI共振: 买压确认反转 +{bb_extra}")

    # ── NEW: CANDLE WICK K线形态识别 (P0: 锤子线/射击之星) ────────────────
    # 来源: Binance-Futures-Bot (★556) — round2代码挖掘
    # 检测下影线/上影线比例，识别潜在反转形态
    if n_bars + i >= 1:
        body = abs(float(closes[i]) - float(opens[i]))
        upper_wick = float(highs[i]) - max(float(closes[i]), float(opens[i]))
        lower_wick = min(float(closes[i]), float(opens[i])) - float(lows[i])
        
        if body > 0:
            # 锤子线 (下落后的多头拒绝): 下影线 >= 2x 实体 AND 上影线 < 0.5x 实体
            if lower_wick >= body * 2.0 and upper_wick < body * 0.5:
                # 看涨锤子线 — 但需结合价格位置过滤假信号
                prev_drop = False
                if n_bars + i >= 1 and closes[i-1] < opens[i-1]:
                    prev_drop = True
                candle_wick_signal = "hammer"
                result["candle_wick_signal"] = candle_wick_signal
                
                if prev_drop:
                    raw_score += WEIGHTS["candle_wick"]
                    result["signals"].append(f"🕯️ 锤子线(下影{lower_wick/body:.1f}x)")
                    result["details"].append(
                        f"🕯️ 锤子线: 下影={lower_wick:.6f} 实体={body:.6f} (ratio={lower_wick/body:.1f}) "
                        f"前K线收阴确认 +{WEIGHTS['candle_wick']}"
                    )
                else:
                    # 无条件收阳K线出现锤子线也加分(较弱)
                    if closes[i] > opens[i]:
                        raw_score += WEIGHTS["candle_wick"] // 2
                        result["signals"].append(f"🕯️ 锤子线(轻)")
                        result["details"].append(
                            f"🕯️ 锤子线: 下影={lower_wick:.6f} (ratio={lower_wick/body:.1f}) +{WEIGHTS['candle_wick']//2}"
                        )
                    else:
                        result["details"].append(
                            f"🕯️ 锤子线(弱势,收阴忽略)"
                        )
            
            # 射击之星 (拉高后的空头拒绝): 上影线 >= 2x 实体 AND 下影线 < 0.5x 实体
            elif upper_wick >= body * 2.0 and lower_wick < body * 0.5:
                prev_rise = False
                if n_bars + i >= 1 and closes[i-1] > opens[i-1]:
                    prev_rise = True
                candle_wick_signal = "shooting_star"
                result["candle_wick_signal"] = candle_wick_signal
                
                if prev_rise:
                    bear_score += WEIGHTS["candle_wick"]
                    result["signals_bear"].append("🕯️ 射击之星(空)")
                    result["details"].append(
                        f"🕯️ 射击之星: 上影={upper_wick:.6f} 实体={body:.6f} (ratio={upper_wick/body:.1f}) "
                        f"前K线收阳确认 ⚠️"
                    )

    # ── VWAP DEVIATION (VWAP偏离度) ────────────────────────
    # 论文 arXiv:2602.00776 验证第三重要特征
    # 成交量加权均价偏离度，用于判断真假突破
    if n_bars + i >= 20:
        # 计算VWAP (近24根K线)
        vwap_n = 24
        idx_start = max(0, n_bars + i - vwap_n + 1)
        idx_end = n_bars + i + 1
        close_arr = closes[idx_start:idx_end]
        vol_arr = volumes[idx_start:idx_end]
        if float(np.sum(vol_arr)) > 0 and not isnan(close_arr[-1]) and close_arr[-1] > 0:
            vwap = float(np.sum(close_arr * vol_arr) / np.sum(vol_arr))
            vwap_dev = (float(close_arr[-1]) - vwap) / vwap * 10000  # bps
            result["vwap_dev_bps"] = round(vwap_dev, 2)
            # VWAP偏离 > 10bps = 短期追高/砸低
            abs_dev = abs(vwap_dev)
            if abs_dev > 10.0:
                if vwap_dev > 0 and closes[i] > closes[i - 1]:
                    # 价格上涨但偏离过大 — 追高信号，不是真突破
                    bear_score += WEIGHTS["vwap_dev"] // 2
                    result["signals_bear"].append(f"VWAP偏{vwap_dev:.0f}bps")
                    result["details"].append(f"VWAP偏+{vwap_dev:.0f}bps (追高风险) ⚠️")
                elif vwap_dev < 0 and closes[i] < closes[i - 1]:
                    # 价格下跌但偏离过大 — 超卖可能反弹
                    bear_score += WEIGHTS["vwap_dev"] // 2
                    result["signals_bear"].append(f"VWAP偏{vwap_dev:.0f}bps")
                    result["details"].append(f"VWAP偏{vwap_dev:.0f}bps (超卖) ⚠️")
            # VWAP偏离在合理范围内 (|dev| <= 10bps) 且方向匹配 — 真突破信号
            elif abs_dev <= 8.0:
                if vwap_dev > 0 and closes[i] > closes[i - 1]:
                    raw_score += WEIGHTS["vwap_dev"]
                    result["signals"].append(f"VWAP偏+{vwap_dev:.1f}bps")
                    result["details"].append(f"VWAP偏+{vwap_dev:.1f}bps (真突破) ✅ +{WEIGHTS['vwap_dev']}")
                elif vwap_dev < 0 and closes[i] < closes[i - 1]:
                    bear_score += WEIGHTS["vwap_dev"]
                    result["signals_bear"].append(f"VWAP偏{vwap_dev:.1f}bps")
                    result["details"].append(f"VWAP偏{vwap_dev:.1f}bps (真跌破) ⚠️")
        else:
            result["vwap_dev_bps"] = 0.0
    else:
        result["vwap_dev_bps"] = 0.0

    # ── NEW: FVG 公允价值缺口检测 (P0: SMC核心入场信号) ─────────────────────
    # 来源: smart-money-concepts (★1611) — GitHub Agent
    # FVG = 相邻K线之间价格未覆盖的gap区域
    # 看涨FVG: high[i-1] < low[i+1] AND closes[i] > opens[i] (当前收阳)
    # 看跌FVG: low[i-1] > high[i+1] AND closes[i] < opens[i] (当前收阴)
    if n_bars + i >= 2:
        # 检测FVG: 比较i-1和i+1的高低点
        h_prev = float(highs[i-1])
        l_prev = float(lows[i-1])
        h_next = float(highs[i])
        l_next = float(lows[i])
        
        # 看涨FVG: left_bar的high < right_bar的low (向上缺口)
        if h_prev < l_next and not isnan(h_prev) and not isnan(l_next) and h_prev > 0:
            result["fvg_signal"] = 1  # bullish
            result["fvg_top"] = round(l_next, 6)
            result["fvg_bottom"] = round(h_prev, 6)
            fvg_gap_pct = (l_next - h_prev) / h_prev * 100 if h_prev > 0 else 0
            
            # FVG信号强度: gap越大越强, 当前收阳确认
            if closes[i] > opens[i]:  # 当前收阳
                # 缺口 > 0.1% 为显著
                if fvg_gap_pct > 0.1:
                    raw_score += WEIGHTS["fvg"]
                    result["signals"].append(f"FVG看涨(缺口{fvg_gap_pct:.2f}%)")
                    result["details"].append(
                        f"📐 FVG看涨: [{h_prev:.6f}, {l_next:.6f}] 缺口={fvg_gap_pct:.2f}% "
                        f"收阳确认 +{WEIGHTS['fvg']}"
                    )
                else:
                    # 小gap给半额
                    raw_score += WEIGHTS["fvg"] // 2
                    result["signals"].append("FVG看涨(微)")
                    result["details"].append(
                        f"📐 FVG看涨: 缺口={fvg_gap_pct:.2f}% (小) +{WEIGHTS['fvg']//2}"
                    )
            else:
                result["details"].append(
                    f"📐 FVG看涨: 缺口={fvg_gap_pct:.2f}% (收阴,暂不确认)"
                )
        
        # 看跌FVG: left_bar的low > right_bar的high (向下缺口)
        elif l_prev > h_next and not isnan(l_prev) and not isnan(h_next) and h_next > 0:
            result["fvg_signal"] = -1  # bearish
            result["fvg_top"] = round(l_prev, 6)
            result["fvg_bottom"] = round(h_next, 6)
            fvg_gap_pct = (l_prev - h_next) / h_next * 100 if h_next > 0 else 0
            
            if closes[i] < opens[i]:  # 当前收阴
                if fvg_gap_pct > 0.1:
                    bear_score += WEIGHTS["fvg"]
                    result["signals_bear"].append(f"FVG看跌(缺口{fvg_gap_pct:.2f}%)")
                    result["details"].append(
                        f"📐 FVG看跌: [{h_next:.6f}, {l_prev:.6f}] 缺口={fvg_gap_pct:.2f}% "
                        f"收阴确认 ⚠️"
                    )
                else:
                    bear_score += WEIGHTS["fvg"] // 2
                    result["signals_bear"].append("FVG看跌(微)")
                    result["details"].append(
                        f"📐 FVG看跌: 缺口={fvg_gap_pct:.2f}% (小) ⚠️"
                    )
            else:
                result["details"].append(
                    f"📐 FVG看跌: 缺口={fvg_gap_pct:.2f}% (收阳,暂不确认)"
                )

    # ── NEW: UniversalMACD 均值回归信号 (P0: EMA12/26比率) ─────────────────
    # 来源: Freqtrade社区策略 — @mablue UniversalMACD
    # 逻辑: umacd = EMA12/EMA26 - 1. 当umacd在超卖区间[-0.014, -0.011]时=做多信号
    # 均值回归: 短期均线靠近但略低于长期均线=超卖买入机会
    if n_bars + i >= 26:
        ema12 = talib.EMA(closes, 12)
        ema26_macd = talib.EMA(closes, 26)
        if not isnan(ema12[i]) and not isnan(ema26_macd[i]) and ema26_macd[i] > 0:
            umacd_val = float(ema12[i]) / float(ema26_macd[i]) - 1.0
            result["umacd"] = round(umacd_val, 6)
            
            # 超卖区间: umacd在[-0.014, -0.011] — EMA12略低于EMA26, 均值回归机会
            if -0.014 <= umacd_val <= -0.011:
                raw_score += WEIGHTS["umacd"]
                result["umacd_signal"] = True
                result["signals"].append(f"UMACD超卖(比值={umacd_val:.5f})")
                result["details"].append(
                    f"📊 UMACD={umacd_val:.5f} (EMA12/EMA26-1) 超卖均值回归 +{WEIGHTS['umacd']}"
                )
            # 接近中性但略偏多: 温和看涨
            elif -0.005 <= umacd_val <= 0.005:
                result["details"].append(
                    f"📊 UMACD={umacd_val:.5f} (中性)"
                )
            # 超买区间: umacd > strong positive = 短期溢价
            elif umacd_val > 0.02:
                bear_score += WEIGHTS["umacd"] // 2
                result["signals_bear"].append(f"UMACD超买(比值={umacd_val:.5f})")
                result["details"].append(
                    f"📊 UMACD={umacd_val:.5f} 超买(短期溢价) ⚠️"
                )
            else:
                result["details"].append(
                    f"📊 UMACD={umacd_val:.5f}"
                )


    # ── CHANDELIER EXIT (自动止盈止损位检测) ─────────────────
    # 基于ATR的长周期趋势追踪止损位
    if n_bars + i >= 22:
        chandelier_period = 22
        chandelier_mult = 3.0
        # 确保窗口至少2根K线
        ch_idx_start = max(0, n_bars + i - chandelier_period + 1)
        ch_idx_end = n_bars + i + 1
        if ch_idx_end - ch_idx_start >= 2:
            chandelier_high = float(np.max(highs[ch_idx_start:ch_idx_end]))
            chandelier_low = float(np.min(lows[ch_idx_start:ch_idx_end]))
            chandelier_atr = float(atr14[i]) if not isnan(atr14[i]) else 0.0
            if chandelier_atr > 0:
                long_stop = chandelier_high - chandelier_mult * chandelier_atr
                short_stop = chandelier_low + chandelier_mult * chandelier_atr
                if closes[i] > long_stop and closes[i - 1] <= long_stop:
                    raw_score += WEIGHTS["chandelier"]  # 4分, 独立权重
                    result["signals"].append("Chandelier突破")
                    result["details"].append(f"Chandelier长止损突破({long_stop:.4f}) +{WEIGHTS['chandelier']}")
                elif closes[i] < short_stop and closes[i - 1] >= short_stop:
                    bear_score += WEIGHTS["chandelier"]  # 4分, 独立权重
                    result["signals_bear"].append("Chandelier跌破")
                    result["details"].append(f"Chandelier短止损跌破({short_stop:.4f}) ⚠️")

    # ── BEAR SIGNAL: 恐慌放量 (panic volume) ─────────────
    # ATR >= 1.5x prev AND close drops > 2%
    if atr_prev > 0:
        atr_ratio = atr_now / atr_prev
        if atr_ratio >= 1.5 and n_bars + i >= 1:
            if closes[i] < closes[i - 1] * 0.98:
                bear_score += 8
                result["signals_bear"].append("恐慌放量")
                result["details"].append(
                    f"ATR放大{atr_ratio:.1f}x + 跌幅>2% ⚠️"
                )

    # ── CRASH-BOUNCE DETECTION (暴跌反弹) ──────────────────
    # Detects: sharp drop (>10% in short window) followed by strong recovery (V-reversal).
    # This catches patterns like BSB 04/29: $0.80→$0.30 (-62%) then $0.30→$0.60 (+100%).
    if n_bars + i >= 10:
        # Check last 5 bars for extreme volatility
        # i = -1 (latest bar); closes[-5:] gives last 5 bars
        idx_start = max(0, n_bars + i - 4)
        idx_end = n_bars + i + 1  # exclusive
        if idx_end - idx_start >= 5:
            recent_5 = closes[idx_start:idx_end]
            max5 = float(np.max(recent_5))
            min5 = float(np.min(recent_5))
            range5_pct = (max5 - min5) / min5 * 100 if min5 > 0 else 0
            
            # Condition A: last 5-bar range > 20% (extreme volatility)
            # Condition B: current bar is >= 5% higher than 2 bars ago (recovery signal)
            if range5_pct > 20.0 and n_bars + i >= 2:
                recovery = (float(closes[i]) - float(closes[i - 2])) / float(closes[i - 2]) * 100 if closes[i - 2] > 0 else 0
                if recovery > 5.0:
                    crash_bounce_score = 10
                    result["details"].append(
                        f"🔥 暴跌反弹: 5bar波幅={range5_pct:.0f}% 恢复={recovery:+.1f}% +10"
                    )
                    result["signals"].append("暴跌反弹")
                    
                    # Check if volume confirmed (vol ratio > 1.0 on recovery)
                    if vol_ratio > 1.0:
                        crash_bounce_score += 5
                        result["details"].append(f"    放量确认(量比={vol_ratio:.1f}x) +5")
                    
                    # Check if it bounced from significant low (below MA88)
                    if not isnan(ma88[i]) and ma88[i] > 0 and closes[i - 2] < ma88[i] * 0.85:
                        crash_bounce_score += 5
                        result["details"].append("    超跌于MA88下方 +5")
                    
                    raw_score += crash_bounce_score
    
    # ── RAVE PATTERNS ────────────────────────────────────
    # Pattern 1: 横盘突破 (consolidation breakout)
    vol_surge = vol_ratio >= cfg["vol_ratio_min"]
    price_break = False
    if n_bars + i >= lb_break:
        prev_high = float(np.max(highs[i - lb_break : i]))
        price_break = closes[i] > prev_high

    if n_bars + i >= 15:
        last15_high = float(np.max(highs[i - 15 : i]))
        last15_low = float(np.min(lows[i - 15 : i]))
        amp = (last15_high - last15_low) / last15_low if last15_low > 0 else 1.0
        if amp <= 0.12 and vol_surge and price_break:
            result["patterns"].append("RAVE横盘突破")
            raw_score += 15

    # Pattern 2: 二次放量爆拉 (second volume surge)
    if n_bars + i >= 20:
        vol_ratios = []
        for j in range(i - 19, i + 1):
            if n_bars + j >= 10:
                avg_v = float(np.mean(volumes[j - 10 : j]))
            else:
                avg_v = 1.0
            vol_ratios.append(volumes[j] / avg_v if avg_v > 0 else 0.0)
        surge_count = sum(1 for vr in vol_ratios[:-1] if vr >= 2.0)
        if surge_count >= 2 and vol_ratios[-1] >= 2.0:
            result["patterns"].append("二次放量爆拉")
            raw_score += 15

    # ── EXIT DETECTION ──────────────────────────────────
    # MACD柱红转绿 -> early exit warning
    if not isnan(macdh[i]) and n_bars + i >= 1 and not isnan(macdh[i - 1]):
        macd_turn = float(macdh[i - 1]) < 0 and float(macdh[i]) > 0
        if macd_turn:
            result["exit_advice"] = "⚠️ MACD柱红转绿 — 逃顶信号，考虑减仓"

    # MA10/EMA10死叉 -> unconditional exit
    if result["ema10"] is not None and result["ma10"] is not None:
        if result["ema10"] < result["ma10"]:
            result["exit_advice"] = "🚨 MA10/EMA10死叉 — 无条件离场"

    # ── Store bear score ─────────────────────────────────
    result["bear_score"] = bear_score
    result["has_bear"] = bear_score > 0

    # ── QUALITY BONUS SYSTEM (升级版 v2) ──────────────────────
    # Bonus points for high-quality signal combinations
    # 学习蒸馏成果: 6大共振场景, 覆盖趋势/反转/动量/量能/形态
    bonus = 0
    sig_set = set(result["signals"])

    # Bonus 1: 动量加速 + 量能递增 = strong momentum confirmation
    if "动量加速" in sig_set and "量能递增" in sig_set:
        bonus += 5
        result["details"].append("🏆 动量+量能共振 +5")

    # Bonus 2: MA10/EMA10金叉 + 突破前高 = strong entry confirmation
    if "MA10/EMA10金叉" in sig_set and "突破前高" in sig_set:
        bonus += 5
        result["details"].append("🏆 金叉+突破共振 +5")

    # Bonus 3: Volume surge (>=3x) combined with momentum acceleration
    vol_surge_signals = [s for s in result["signals"] if "成交量" in s]
    if vol_surge_signals and "动量加速" in sig_set:
        bonus += 3
        result["details"].append("🏆 放量+动量加速 +3")

    # Bonus 4 (NEW): Supertrend+ADX双重确认 — 学习和论文蒸馏发现
    # Supertrend多头 + ADX>25趋势市 = 强趋势确认, 止损可靠
    if "SuperTrend多头" in sig_set and ("ADX" in str(result.get("adx_phase", "")) or "趋势市" in sig_set):
        bonus += 4
        result["details"].append("🏆 Supertrend+ADX趋势共振 +4")

    # Bonus 5 (NEW): SuperTrend金叉+EMA9/26金叉 — 两趋势系统同步翻多
    if "SuperTrend金叉(转多)" in sig_set and "EMA9/26金叉" in sig_set:
        bonus += 6
        result["details"].append("🏆 SuperTrend+EMA双金叉共振 +6")

    # Bonus 6 (NEW): OFI强买 + 突破前高 — 订单流+结构突破=真突破
    if ("OFI" in str(result.get("signals", "")) and "突破前高" in sig_set):
        bonus += 4
        result["details"].append("🏆 OFI买压+突破共振 +4")

    result["bonus"] = bonus

    # ── ADX RANGING PENALTY: 震荡市降低趋势评分 ─────────────────
    if adx_ranging_penalty:
        reduce_from_less = 0
        sig_set = set(result["signals"])
        if "EMA9/26金叉" in sig_set or "EMA9/26多头延续" in sig_set:
            raw_score = max(0, raw_score - WEIGHTS["trend"] // 2)
            reduce_from_less += WEIGHTS["trend"] // 2
        if "突破前高" in sig_set:
            raw_score = max(0, raw_score - WEIGHTS["breakout"] // 2)
            reduce_from_less += WEIGHTS["breakout"] // 2
        if reduce_from_less > 0:
            result["details"].append(f"📊 震荡市趋势减益 -{reduce_from_less}")

    # ── MA88 震荡市增强过滤: 起涨阶段远离MA88不扣分 ─────────────
    # 学习发现: 小币起涨前价格经常远高于MA88(>12%), 此时MA88分=0
    # 但如果是币正处在起涨初期(ADX>25或OFI>0.3), 应给予容忍
    ma88_far_dev = result.get("ma88_dev")
    if ma88_far_dev is not None:
        abs_dev = abs(ma88_far_dev)
        if abs_dev > 12 and abs_dev <= 50:
            # 仅当有其他趋势/动量信号支持时, 不强扣MA88分
            has_trend_support = (
                "OFI" in str(result.get("signals", ""))
                or result.get("adx_phase") == "trending"
                or "量能递增" in sig_set
                or "SuperTrend多头" in sig_set
            )
            if not has_trend_support:
                raw_score = max(0, raw_score - WEIGHTS["ma88"])
                result["details"].append(f"📊 MA88偏离{ma88_far_dev:.1f}%>12%, 无趋势支撑 = 不计分")

    # ── NEW COIN BONUS (溢价因子) ──────────────────────────
    # New coins (<72h since listing) get a base score boost so they can
    # reach a reasonable grade even when technical indicators are all NaN.
    # NOTE: new_coin_bonus only tracks listing-age-related bonuses.
    # Multi-dimensional extras (funding rate, OI, CG, etc.) go to extras_score.
    # This prevents semantic confusion and caps new-coin premium independently.
    new_coin_bonus = 0
    extras_score = 0

    if hours_since_listing < 72:
        if hours_since_listing <= 24:
            base_bonus = 30
            reason = "新币上线≤24h"
        elif hours_since_listing <= 48:
            base_bonus = 20
            reason = "新币上线≤48h"
        else:
            base_bonus = 10
            reason = "新币上线≤72h"

        new_coin_bonus += base_bonus
        result["signals"].append(reason)
        result["details"].append(f"🆕 {reason}: 基础分+{base_bonus}")

        # New coin + large volume: extra bonus
        if vol_24h >= 5_000_000:
            new_coin_bonus += 15
            result["signals"].append("新币大成交量")
            result["details"].append(f"🆕 新币+大成交量(≥500万) +15")

        # New coin + extreme price move (≥20%): extra bonus
        if abs(chg24h) >= 20:
            new_coin_bonus += 10
            result["signals"].append("新币暴涨暴跌")
            result["details"].append(f"🆕 新币+涨跌幅{chg24h:+.1f}% +10")

    result["new_coin_bonus"] = new_coin_bonus

    # ── EXTRAS (多维评分因子) ─────────────────────────────
    # These are independent of new-coin status — any coin can score from them.
    if extras is not None:
        # funding_rate
        fr = extras.get("funding_rate")
        if fr is not None:
            if abs(fr) >= 0.002:
                extras_score += 10
                result["signals"].append("资金费率极端")
                result["details"].append(f"💸 资金费率极端(fr={fr:.6f}) +10")
            elif abs(fr) >= 0.0005:
                extras_score += 5
                result["signals"].append("资金费率异常")
                result["details"].append(f"💸 资金费率异常(fr={fr:.6f}) +5")
            # Negative funding + positive price change = short squeeze potential
            if fr < 0 and chg24h > 0:
                extras_score += 5
                result["signals"].append("空头轧空潜力")
                result["details"].append(f"💸 负费率+上涨: 空头轧空潜力 +5")

        # oi_growth_pct
        oi = extras.get("oi_growth_pct")
        if oi is not None:
            if oi >= 10:
                extras_score += 8
                result["signals"].append("OI暴增")
                result["details"].append(f"📊 OI增速{oi:.1f}% +8")
            elif oi >= 5:
                extras_score += 4
                result["signals"].append("OI增长")
                result["details"].append(f"📊 OI增速{oi:.1f}% +4")

        # cg_trending
        cg = extras.get("cg_trending")
        if cg:
            extras_score += 8
            result["signals"].append("CG热榜")
            result["details"].append("🔥 CoinGecko热榜 +8")

        # large_taker
        lt = extras.get("large_taker")
        if lt:
            extras_score += 6
            result["signals"].append("大额吃单")
            result["details"].append("💹 大额吃单 +6")

        # liquidation_cascade
        liq_cas = extras.get("liquidation_cascade")
        if liq_cas and liq_cas in ("high", "medium", "low"):
            if liq_cas == "high":
                score_impact = 12
                label = "清算级联(高危)"
            elif liq_cas == "medium":
                score_impact = 8
                label = "清算级联(中危)"
            else:
                score_impact = 4
                label = "清算级联(低危)"
            extras_score += score_impact
            result["signals"].append(label)
            result["details"].append(f"⚠️ {label} +{score_impact}")

        # liquidation_score
        liq_sc = extras.get("liquidation_score", 0)
        if liq_sc and isinstance(liq_sc, (int, float)):
            if liq_sc >= 10:
                extras_score += 3
                result["signals"].append("清算压力大")
                result["details"].append(f"⚠️ 清算压力(score={liq_sc}) +3")

        # long_short_ratio
        lsr = extras.get("long_short_ratio")
        if lsr is not None:
            if lsr > 2.5:
                extras_score += 5
                result["signals"].append("多头拥挤")
                result["details"].append(f"📈 多空比{lsr:.2f}>2.5(多头拥挤) +5")
            elif lsr < 0.4:
                extras_score += 5
                result["signals"].append("空头拥挤/反弹信号")
                result["details"].append(f"📉 多空比{lsr:.2f}<0.4(空头拥挤) +5")

    result["extras_score"] = extras_score

    # ── VOLATILITY-AWARE SIGNAL ADJUSTMENT ────────────────
    # Compute ATR/price ratio to measure volatility
    atr_ratio_price = atr_now / price if price > 0 else 0.0
    volatility_score = "low" if atr_ratio_price < 0.01 else ("high" if atr_ratio_price > 0.05 else "normal")
    result["volatility_score"] = volatility_score
    result["atr_price_ratio"] = round(atr_ratio_price, 6)

    # Low volatility (stable coins, BTC-like): require 1 more signal for same grade
    # We do this by reducing raw_score by one signal weight (effectively lowering grade)
    if volatility_score == "low":
        # Low vol = less noise, but also less conviction needed — reduce score slightly
        # to compensate for easier signal generation on stable assets
        raw_score = max(0, raw_score - WEIGHTS["micro"])
        result["details"].append(f"📊 低波动率(ATR/Price={atr_ratio_price:.6f}) 信号减益 -{WEIGHTS['micro']}")

    # ── quality_score: overall quality metric ─────────────
    # A multiplier-based score: ratio of high-quality signals to total possible
    raw_score_with_bonus = raw_score + bonus + new_coin_bonus + extras_score

    # ── Normalise score and assign grade ──────────────
    normalised = min(100, int(raw_score_with_bonus / MAX_SCORE * 100))
    result["score"] = normalised
    result["grade"] = grade_from_score(normalised)

    # ── Entry advice ─────────────────────────────────────
    sig_count = len(result["signals"])
    signals_concat = " ".join(result["signals"])
    if result["grade"] == "A":
        result["entry_advice"] = (
            "5信号共振达标，量价配合+MA88确认，回调EMA10/MA10附近轻仓入场"
        )
    elif result["grade"] == "B" and "成交量" in signals_concat:
        result["entry_advice"] = "放量启动中，等MA10/EMA10金叉确认后入场"
    elif result["grade"] == "B":
        result["entry_advice"] = "部分信号达标，等待成交量放大(≥3x)确认"
    elif result["grade"] == "C":
        result["entry_advice"] = "加入自选，等待EMA9/26金叉+放量信号"
    else:
        result["entry_advice"] = "待确认，暂不参与"

    return result


# ═══════════════════════════════════════════════════════════
# Multi-score merger
# ═══════════════════════════════════════════════════════════

def merge_multi_scores(scores: list[dict]) -> dict:
    """
    Merge multiple score dicts for the SAME symbol from different
    (exchange, timeframe) pairs into a single unified score dict.

    Weighting scheme (timeframe):
        4H -> 0.5
        2H -> 0.3
        1H -> 0.2
    All exchanges are weighted equally within each timeframe group.

    Returns a dict with keys:
        avg_score, grade, signals, signals_fail, signals_bear,
        bear_score, has_bear, details, patterns,
        entry_advice, exit_advice, ema9, ema26, ma10, ema10,
        ma88, ma88_dev, rsi, vol_ratio, trend,
        cross_exchange, cross_timeframe, resonance_bonus,
        plus all other keys present in children.
    """
    if not scores:
        return {
            "sym": "",
            "price": 0.0,
            "chg24h": 0.0,
            "vol_24h": 0,
            "score": 0,
            "grade": "D",
            "signals": [],
            "signals_fail": [],
            "signals_bear": [],
            "bear_score": 0,
            "has_bear": False,
            "details": [],
            "patterns": [],
            "entry_advice": "",
            "exit_advice": "",
            "ema9": None,
            "ema26": None,
            "ma10": None,
            "ema10": None,
            "ma88": None,
            "ma88_dev": None,
            "rsi": None,
            "vol_ratio": None,
            "trend": None,
            "cross_exchange": 0,
            "cross_timeframe": 0,
            "resonance_bonus": 0,
            "bonus": 0,
            "volatility_score": "normal",
            "atr_price_ratio": 0.0,
            "new_coin_bonus": 0,
            "extras_score": 0,
        }

    # Detect unique exchanges and timeframes from the score dicts.
    # We assume each score dict may have optional 'exchange' and 'timeframe' keys.
    # If absent, we assign a generic index.
    exchanges = set()
    timeframes = set()
    tf_weight_map = {"4H": 0.5, "2H": 0.3, "1H": 0.2}

    for s in scores:
        ex = s.get("exchange", "unknown")
        tf = s.get("timeframe", "unknown")
        exchanges.add(ex)
        timeframes.add(tf)

    # Build weight per score entry
    # If we can identify timeframes, use the weight map; else equal weighting.
    weights = []
    for s in scores:
        tf = s.get("timeframe", "unknown")
        if tf in tf_weight_map:
            w = tf_weight_map[tf]
        else:
            # Unknown timeframe: distribute equally
            w = 1.0 / len(scores) if scores else 0
        weights.append(w)

    # Normalise weights to sum to 1.0
    total_w = sum(weights)
    if total_w > 0:
        weights = [w / total_w for w in weights]
    else:
        weights = [1.0 / len(scores)] * len(scores)

    # ── Weighted score average ──────────────────────────
    avg_score = sum(s["score"] * w for s, w in zip(scores, weights))
    avg_score = int(round(avg_score))

    # ── Resonance bonus (timeframe-aware) ───────────────────
    # +8 if ALL 3 timeframes (1H, 2H, 4H) agree on direction
    # +3 if 2 timeframes agree
    # 0 if only 1 timeframe (no resonance)
    resonance_bonus = 0
    # Group by timeframe
    tf_scores = {}  # timeframe -> list of scores
    for s in scores:
        tf = s.get("timeframe", "unknown")
        tf_scores.setdefault(tf, []).append(s.get("score", 0))

    # A timeframe "agrees on direction" if its average score >= 50
    agreeing_tfs = sum(
        1 for tf, scs in tf_scores.items() if np.mean(scs) >= 50
    )

    # Known timeframes: 1H, 2H, 4H — count how many of these we have data for
    known_tfs = {"1H", "2H", "4H"}
    present_known_tfs = [tf for tf in tf_scores if tf in known_tfs]

    if agreeing_tfs >= 3 and len(present_known_tfs) >= 3:
        resonance_bonus = 8  # Full 3-timeframe resonance
    elif agreeing_tfs >= 2:
        resonance_bonus = 3  # 2-timeframe agreement
    # 0 bonus for 1-timeframe (no resonance)

    final_score = min(100, avg_score + resonance_bonus)

    # ── Merge signals / signals_fail / signals_bear / details / patterns ──
    merged_signals = []
    merged_signals_fail = []
    merged_signals_bear = []
    merged_details = []
    merged_patterns = []
    seen_sig = set()
    seen_fail = set()
    seen_bear = set()
    seen_pat = set()

    # ── Merge bear_score: take max ───────────────────────
    merged_bear_score = max(s.get("bear_score", 0) for s in scores)
    merged_has_bear = any(s.get("has_bear", False) for s in scores)

    for s in scores:
        for sig in s.get("signals", []):
            if sig not in seen_sig:
                merged_signals.append(sig)
                seen_sig.add(sig)
        for sig in s.get("signals_fail", []):
            if sig not in seen_fail:
                merged_signals_fail.append(sig)
                seen_fail.add(sig)
        for sig in s.get("signals_bear", []):
            if sig not in seen_bear:
                merged_signals_bear.append(sig)
                seen_bear.add(sig)
        for det in s.get("details", []):
            merged_details.append(det)
        for pat in s.get("patterns", []):
            if pat not in seen_pat:
                merged_patterns.append(pat)
                seen_pat.add(pat)

    # ── Crash-bounce bonus ─────────────────────────────────
    crash_bounce_count = sum(1 for s in scores if "暴跌反弹" in s.get("signals", []))
    if crash_bounce_count >= 2:
        crash_bonus = 10 if crash_bounce_count >= 3 else 5
        final_score = min(100, final_score + crash_bonus)
        merged_details.append(f"🔥 暴跌反弹共识({crash_bounce_count}/{len(scores)}) +{crash_bonus}")

    # ── Extra resonance bonus: new coin or CG trending ──────────
    has_new_coin = any(s.get("new_coin_bonus", 0) > 0 for s in scores)
    has_cg_trending = any(
        "CG热榜" in s.get("signals", [])
        for s in scores
    )
    if has_new_coin or has_cg_trending:
        resonance_bonus_extra = 15
        final_score = min(100, final_score + resonance_bonus_extra)
        merged_details.append(f"🎯 新币/CG共振溢价 +{resonance_bonus_extra}")

    # ── Reduce final score if bear signals present ──────
    if merged_has_bear:
        final_score = max(0, final_score - 10)

    # ── Take latest/primary values for scalar fields ──────
    # Use the first score dict as base, then selectively override
    primary = scores[0].copy()

    # Pick best entry_advice (prefer A/B advice)
    all_advice = [s.get("entry_advice", "") for s in scores]
    # Priority: advice containing keywords
    priority_advice = None
    for adv in all_advice:
        if "5信号共振" in adv:
            priority_advice = adv
            break
    if not priority_advice:
        for adv in all_advice:
            if "放量启动" in adv:
                priority_advice = adv
                break
    if not priority_advice:
        for adv in all_advice:
            if "部分信号" in adv:
                priority_advice = adv
                break
    if not priority_advice:
        priority_advice = all_advice[0] if all_advice else ""

    # Exit advice: take the most severe
    exit_advices = [s.get("exit_advice", "") for s in scores if s.get("exit_advice")]
    exit_advice = exit_advices[0] if exit_advices else ""

    # For scalar metrics, take the one with the highest raw score as the "best" view
    best = max(scores, key=lambda s: s.get("score", 0))

    merged = {
        "sym": primary.get("sym", ""),
        "price": primary.get("price", 0.0),
        "chg24h": primary.get("chg24h", 0.0),
        "vol_24h": primary.get("vol_24h", 0),
        "score": final_score,
        "grade": grade_from_score(final_score),
        "signals": merged_signals,
        "signals_fail": merged_signals_fail,
        "signals_bear": merged_signals_bear,
        "bear_score": merged_bear_score,
        "has_bear": merged_has_bear,
        "details": merged_details,
        "patterns": merged_patterns,
        "entry_advice": priority_advice,
        "exit_advice": exit_advice,
        # Take indicator values from the best-scoring child
        "ema9": best.get("ema9"),
        "ema26": best.get("ema26"),
        "ma10": best.get("ma10"),
        "ema10": best.get("ema10"),
        "ma88": best.get("ma88"),
        "ma88_dev": best.get("ma88_dev"),
        "rsi": best.get("rsi"),
        "vol_ratio": best.get("vol_ratio"),
        "trend": best.get("trend"),
        # Cross metrics
        "cross_exchange": len(exchanges),
        "cross_timeframe": len(timeframes),
        "resonance_bonus": resonance_bonus,
        "bonus": 0,
        "volatility_score": "normal",
        "atr_price_ratio": 0.0,
        "new_coin_bonus": max(s.get("new_coin_bonus", 0) for s in scores),
        "extras_score": max(s.get("extras_score", 0) for s in scores),
    }

    return merged
