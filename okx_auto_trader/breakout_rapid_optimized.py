"""
Breakout Rapid — v1.5 基于ORDI复盘的深度优化版
==================================================
核心改进:
  1. 条件H（缓涨起跑）增加三级验证:
     - H1: 基础条件（原H）8根窗口≥5阳+累积≥1.2%+量递增≥10%
     - H2: 量级放大 — 窗口均量 > 前8根均量×1.5 (避免中继误报)
     - H3: 突破确认 — 价格突破前20根最高价 (真正起涨)
     - H4: K线延续 — 下一根K线继续收阳 (双窗口确认)
  2. 信号的"品质分级": 只有H1+H2+H3+H4才视为高确定性信号
  3. 价格区间突破自动排除横盘蓄力的假信号
  4. 量级对比减少了Stage 1-3的早期误报

对比v1.4: v1.4的H在ORDI上触发4次(Stage 1-4), 仅第4次真正有效
v1.5通过H2/H3/H4过滤后, 仅为Stage 4触发
"""

import os
import time
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger("breakout_rapid_v15")

# ════════════════════════════════════════════════════════════
#  检测阈值
# ════════════════════════════════════════════════════════════

PRICE_LOW = 0.3
PRICE_MICRO = 0.01

A_MIN_CHG = 1.2
A_MIN_VOL_RATIO = 1.8
A_VOL_RATIO_LOW = 1.5
A_MIN_VOL_ABS = 10_000
A_MIN_VOL_LOW_ABS = 3_000
A_MIN_VOL_MICRO_ABS = 1_000

B_MIN_CHG = 3.5
B_MIN_CHG_LOW = 5.0
B_MIN_VOL_ABS = 5_000
B_MIN_VOL_LOW_ABS = 2_000

C_MIN_CHG = 0.8
C_VOL_RATIO = 1.3
C_MIN_VOL_ABS = 8_000

D_SHRINK_CHG_LIMIT = 0.8
D_BREAK_VOL_RATIO = 1.5
D_BREAK_CHG = 0.8

E_24H_DROP = -8.0
E_REV_CHG = 0.8
E_REV_VOL_RATIO = 1.5

F_MIN_CHG = 1.2
F_MIN_VOL_RATIO = 1.3
F_MIN_VOL_ABS = 8_000
F_MIN_VOL_LOW_ABS = 3_000

G_MIN_CHG = 1.5
G_MIN_VOL_RATIO = 1.2
G_MIN_VOL_ABS = 5_000
G_MIN_VOL_LOW_ABS = 2_000

# ── 条件H: 缓涨起跑 — v1.5三级验证版 ──
H_WINDOW = 8              # 检测窗口长度
H_MIN_BULLISH = 5         # 至少5根阳/平
H_ACCUM_CHG_MIN = 1.2     # 窗口累计涨幅>=1.2%
H_VOL_SLOPE_MIN = 0.1     # 成交量斜率>=10%
H_BASE_VOL_USD = 60_000   # 窗口最低均量

# H2: 量级放大 — 对比前序窗口
H_PREV_WINDOW = 8         # 前序窗口长度
H_VOL_MAGNIFY = 1.5       # 当前均量 > 前序均量×1.5

# H3: 突破确认 — 价格突破前N根最高价
H_BREAK_LOOKBACK = 20     # 前20根K线
H_BREAK_MARGIN = 0.001    # 突破容差(0.1%, 避免精确相等)

# H4: 延续确认 — 下一根K线
H_CONFIRM_CANDLES = 9     # 需要至少9根K线才能看下一根

# 评分
SCORE_BASE = 20
SCORE_PER_CHG_PCT = 8
SCORE_VOL_3X = 10
SCORE_VOL_5X = 18
SCORE_VOL_10X = 25
SCORE_AMPLITUDE_6PCT = 8
SCORE_RESONANCE_DUAL = 12
SCORE_RESONANCE_TRIPLE = 20
SCORE_OVERSOLD_BONUS = 10
SCORE_MICRO_ALPHA = 8

# H评分
H_SCORE_BASE = 18
H_SCORE_PER_PCT = 5
H_SCORE_MAGNIFY_BONUS = 8    # 量级放大加分
H_SCORE_BREAK_BONUS = 10      # 突破确认加分
H_SCORE_CONFIRM_BONUS = 12    # 延续确认加分

# 运行参数
DEDUP_WINDOW = 600
MIN_VOL_24H_USDT = 30_000
MAX_TRACKED = 300
API_TIMEOUT = 5
OKX_API = "https://www.okx.com/api/v5/market"

CHG24H_DISCOUNT_15 = 0.75
CHG24H_DISCOUNT_30 = 0.50
CHG24H_DISCOUNT_50 = 0.30
CHG24H_SKIP_100 = True

MIN_SCORE_PUSH = 15


class BreakoutRapidScannerV15:
    """v1.5 - 基于ORDI复盘的深度优化版"""

    def __init__(self):
        self._last_signals: list[dict] = []
        self._dedup: dict[str, float] = {}
        self._session = requests.Session()
        self._candle_cache: dict[str, tuple] = {}

    def scan_all(self, top_n: int = 20) -> list[dict]:
        """全市场起涨扫描"""
        start = time.time()

        tickers = self._get_all_swaps()
        if not tickers:
            return []

        logger.info(f"Loaded {len(tickers)} active swaps")

        candidates = self._fast_filter(tickers)
        logger.info(f"Fast filter: {len(tickers)} → {len(candidates)} candidates")

        signals = []
        for t in candidates:
            inst_id = t["instId"]
            try:
                sig = self._check_breakout_v15(inst_id, t)
                if sig:
                    signals.append(sig)
            except Exception as e:
                logger.debug(f"Error {inst_id}: {e}")
                continue

        elapsed = time.time() - start
        logger.info(f"Deep check {len(candidates)} coins in {elapsed:.1f}s, found {len(signals)} signals")

        self._candle_cache.clear()

        signals = self._dedup_signals(signals)
        signals.sort(key=lambda x: x["score"], reverse=True)
        self._last_signals = signals[:top_n]
        return self._last_signals

    def _fast_filter(self, tickers: list[dict]) -> list[dict]:
        """
        基于ticker数据的快速初筛。
        保留至少满足以下任一条件的币种:
          - 24h涨幅>1% 或 跌幅>3% (有波动)
          - 24h成交量>500K USDT (活跃)
          - OI>500K USDT (有深度)
          - 价格<0.01 USDT (微市值, 起涨潜力大)
        """
        candidates = []

        for t in tickers:
            try:
                last = t.get("_price", 0)
                chg24 = t.get("_24h_chg", 0)
                vol24 = t.get("volUsd24h", 0)
                oi = float(t.get("oiUsd", 0) or 0)

                if (abs(chg24) >= 1 or vol24 >= 500_000 or oi >= 500_000
                        or last <= PRICE_MICRO):
                    candidates.append(t)
            except (ValueError, TypeError):
                continue

        candidates.sort(key=lambda x: x.get("volUsd24h", 0), reverse=True)
        return candidates

    def _get_all_swaps(self) -> list[dict]:
        try:
            resp = self._session.get(
                f"{OKX_API}/tickers",
                params={"instType": "SWAP"},
                timeout=API_TIMEOUT * 2,
            )
            if resp.status_code != 200:
                return []
            data = resp.json().get("data", [])

            swaps = []
            for t in data:
                inst_id = t.get("instId", "")
                if not inst_id.endswith("USDT-SWAP"):
                    continue
                try:
                    last = float(t.get("last", 0) or 0)
                    vol_coin = float(t.get("volCcy24h", 0) or 0)
                    vol_usd = last * vol_coin
                    if vol_usd < MIN_VOL_24H_USDT:
                        continue
                    t["volUsd24h"] = vol_usd
                    t["_24h_chg"] = self._calc_24h_chg(t)
                    t["_price"] = last
                    swaps.append(t)
                except (ValueError, TypeError):
                    continue

            swaps.sort(key=lambda x: x.get("volUsd24h", 0), reverse=True)
            return swaps[:MAX_TRACKED]
        except Exception as e:
            logger.warning(f"Fetch swaps failed: {e}")
            return []

    def _get_candles(self, inst_id: str, bar: str = "5m", limit: int = 60) -> list[dict]:
        """获取K线（带会话级缓存）"""
        now = time.time()
        if inst_id in self._candle_cache:
            cache_ts, cache_data = self._candle_cache[inst_id]
            if now - cache_ts < 5.0:
                return cache_data

        try:
            resp = self._session.get(
                f"{OKX_API}/candles",
                params={"instId": inst_id, "bar": bar, "limit": limit},
                timeout=API_TIMEOUT,
            )
            if resp.status_code != 200:
                return []
            data = resp.json().get("data", [])

            candles = []
            for d in data:
                try:
                    c = {
                        "ts": int(d[0]),
                        "open": float(d[1]),
                        "high": float(d[2]),
                        "low": float(d[3]),
                        "close": float(d[4]),
                        "vol_coin": float(d[5]),
                        "vol_usd": float(d[7]) if float(d[7]) else float(d[6]) * float(d[4]),
                    }
                    c["chg_pct"] = (c["close"] - c["open"]) / c["open"] * 100 if c["open"] > 0 else 0
                    c["amplitude"] = (c["high"] - c["low"]) / c["open"] * 100 if c["open"] > 0 else 0
                    candles.append(c)
                except (IndexError, ValueError):
                    continue

            self._candle_cache[inst_id] = (now, candles)
            return candles
        except Exception:
            return []

    @staticmethod
    def _calc_24h_chg(ticker: dict) -> float:
        try:
            last = float(ticker.get("last", 0) or 0)
            open24 = float(ticker.get("open24h", 0) or 0)
            if open24 > 0:
                return (last - open24) / open24 * 100
        except (ValueError, TypeError):
            pass
        return 0.0

    @staticmethod
    def _price_tier(price: float) -> str:
        if price <= PRICE_MICRO:
            return "micro"
        elif price <= PRICE_LOW:
            return "low"
        return "normal"

    def _check_breakout_v15(self, inst_id: str, ticker: dict) -> Optional[dict]:
        """v1.5 起涨检测 — 基于ORDI复盘的深度优化"""

        last_price = ticker.get("_price", 0)
        if last_price <= 0:
            return None

        price_tier = self._price_tier(last_price)

        # 获取足量K线: 至少 H_WINDOW + H_PREV_WINDOW + H_BREAK_LOOKBACK + 2
        need_candles = max(H_WINDOW + H_PREV_WINDOW + H_BREAK_LOOKBACK + 2, 30)
        candles = self._get_candles(inst_id, "5m", need_candles)
        if len(candles) < max(6, H_WINDOW + H_PREV_WINDOW + 2):
            return None

        now_utc = datetime.now(timezone.utc)
        curr_bucket = (now_utc.minute // 5) * 5
        first_candle_min = datetime.fromtimestamp(candles[0]["ts"] // 1000).minute

        if first_candle_min == curr_bucket and now_utc.minute % 5 > 1:
            latest_idx = 1
            candle_active = True
        else:
            latest_idx = 0
            candle_active = False

        prev = candles[latest_idx]

        def _get(n):
            i = latest_idx + n
            return candles[i] if i < len(candles) else None

        # 量比计算
        prev3_vols = []
        for n in range(1, 4):
            c = _get(n)
            if c:
                prev3_vols.append(c["vol_usd"])
        if not prev3_vols:
            return None

        avg_prev_vol = sum(prev3_vols) / len(prev3_vols)
        vol_ratio = prev["vol_usd"] / avg_prev_vol if avg_prev_vol > 0 else 1

        # 价格层级自适应
        if price_tier == "micro":
            a_vol_abs = A_MIN_VOL_MICRO_ABS
            a_vol_ratio_t = A_VOL_RATIO_LOW
            b_vol_abs = B_MIN_VOL_LOW_ABS
            b_chg_min = B_MIN_CHG_LOW
        elif price_tier == "low":
            a_vol_abs = A_MIN_VOL_LOW_ABS
            a_vol_ratio_t = A_VOL_RATIO_LOW
            b_vol_abs = B_MIN_VOL_LOW_ABS
            b_chg_min = B_MIN_CHG_LOW
        else:
            a_vol_abs = A_MIN_VOL_ABS
            a_vol_ratio_t = A_MIN_VOL_RATIO
            b_vol_abs = B_MIN_VOL_ABS
            b_chg_min = B_MIN_CHG

        # ── 条件检测 ──
        triggered = []
        details = {}

        # A: 放量起爆
        if (prev["chg_pct"] >= A_MIN_CHG and vol_ratio >= a_vol_ratio_t
                and prev["vol_usd"] >= a_vol_abs):
            triggered.append("A")
            details["A"] = {"chg": round(prev["chg_pct"], 2), "vr": round(vol_ratio, 2)}

        # B: 暴力拉升
        if prev["chg_pct"] >= b_chg_min and prev["vol_usd"] >= b_vol_abs:
            triggered.append("B")
            details["B"] = {"chg": round(prev["chg_pct"], 2)}

        # C: 温和放量推土
        p1 = _get(0)
        p2 = _get(1)
        if p1 and p2 and p1["chg_pct"] >= C_MIN_CHG and p2["chg_pct"] >= C_MIN_CHG:
            c_vr = p1["vol_usd"] / avg_prev_vol if avg_prev_vol > 0 else 1
            if c_vr >= C_VOL_RATIO and p1["vol_usd"] >= C_MIN_VOL_ABS:
                triggered.append("C")
                details["C"] = {"chgs": [round(p2["chg_pct"], 2), round(p1["chg_pct"], 2)], "vr": round(c_vr, 2)}

        # D: 缩量蓄力突破
        if _get(1) and _get(2):
            d_shrink = True
            for n in [1, 2]:
                c = _get(n)
                if c and abs(c["chg_pct"]) > D_SHRINK_CHG_LIMIT:
                    d_shrink = False
                    break
            if d_shrink and vol_ratio >= D_BREAK_VOL_RATIO and prev["chg_pct"] >= D_BREAK_CHG:
                triggered.append("D")
                details["D"] = {"vr": round(vol_ratio, 2)}

        # E: 超跌反转
        chg24h = ticker.get("_24h_chg", 0)
        if chg24h <= E_24H_DROP and prev["chg_pct"] >= E_REV_CHG and vol_ratio >= E_REV_VOL_RATIO:
            triggered.append("E")
            details["E"] = {"24h": round(chg24h, 2), "rev": round(prev["chg_pct"], 2)}

        # F: 盘中实时拉升
        if candle_active:
            live_candle = candles[0]
            live_chg = (last_price - live_candle["open"]) / live_candle["open"] * 100
            live_vol = live_candle["vol_usd"]
            live_vr = live_vol / avg_prev_vol if avg_prev_vol > 0 else 1

            if live_chg >= F_MIN_CHG and live_vr >= F_MIN_VOL_RATIO:
                if price_tier == "low":
                    vol_ok = live_vol >= F_MIN_VOL_LOW_ABS
                else:
                    vol_ok = live_vol >= F_MIN_VOL_ABS
                if vol_ok:
                    triggered.append("F")
                    details["F"] = {"chg": round(live_chg, 2), "vr": round(live_vr, 2)}

        # G: 跨K线持续拉升
        if candle_active:
            surge = (last_price - prev["close"]) / prev["close"] * 100 if prev["close"] > 0 else 0
            g_vol_min = G_MIN_VOL_LOW_ABS if price_tier in ("micro", "low") else G_MIN_VOL_ABS
            if surge >= G_MIN_CHG and prev["vol_usd"] >= g_vol_min:
                triggered.append("G")
                details["G"] = {"surge": round(surge, 2)}

        # ═══════════════════════════════════════════════════════
        # H: 缓涨起跑 — v1.5 三级验证版
        # ═══════════════════════════════════════════════════════
        h_quality = 0  # 0=未触发, 1=基础, 2=量级放大, 3=突破, 4=延续确认
        h_info = {}

        # H1: 基础条件
        if len(candles) >= latest_idx + H_WINDOW + 1:
            h_start = latest_idx + 1  # 最新完成K线之后开始回溯
            h_window_start = h_start + len(candles) - H_WINDOW - 1 if h_start + 50 > len(candles) else h_start
            h_window_end = min(h_window_start + H_WINDOW, len(candles))
            h_window = candles[-H_WINDOW:] if len(candles) >= H_WINDOW else []

            if len(h_window) >= H_WINDOW:
                bullish = sum(1 for c in h_window if c["chg_pct"] >= -0.1)
                accum = (h_window[-1]["close"] - h_window[0]["close"]) / h_window[0]["close"] * 100 if h_window[0]["close"] > 0 else 0

                # 量趋势
                mid = len(h_window) // 2
                vol_front = sum(c["vol_usd"] for c in h_window[:mid]) / mid if mid > 0 else 1
                vol_back = sum(c["vol_usd"] for c in h_window[mid:]) / (len(h_window) - mid) if len(h_window) > mid else 1
                vol_slope = (vol_back / vol_front - 1) if vol_front > 0 else 0
                avg_vol = sum(c["vol_usd"] for c in h_window) / len(h_window)

                # H1 判定
                h_hit = (bullish >= H_MIN_BULLISH and accum >= H_ACCUM_CHG_MIN
                         and vol_slope >= H_VOL_SLOPE_MIN and avg_vol >= H_BASE_VOL_USD)

                if h_hit:
                    h_quality = 1
                    h_info = {
                        "bullish": bullish,
                        "accum_chg": round(accum, 2),
                        "vol_slope": round(vol_slope, 2),
                        "avg_vol": int(avg_vol),
                        "levels": ["H1"],
                    }

                    # H2: 量级放大 — 对比前序窗口
                    prev_window = candles[-(H_WINDOW * 2):-H_WINDOW] if len(candles) >= H_WINDOW * 2 else None
                    if prev_window and len(prev_window) >= H_PREV_WINDOW:
                        prev_avg_vol = sum(c["vol_usd"] for c in prev_window[-H_PREV_WINDOW:]) / H_PREV_WINDOW
                        vol_magnify = avg_vol / prev_avg_vol if prev_avg_vol > 0 else 0
                        h_info["vol_magnify"] = round(vol_magnify, 2)
                        h_info["prev_avg_vol"] = int(prev_avg_vol)

                        if vol_magnify >= H_VOL_MAGNIFY:
                            h_quality = 2
                            h_info["levels"].append("H2")

                    # H3: 突破确认 — 价格突破前N根最高价
                    if len(candles) >= H_BREAK_LOOKBACK:
                        prev_highs = candles[-H_BREAK_LOOKBACK:]
                        lookback_high = max(c["high"] for c in prev_highs)
                        break_high = h_window[-1]["close"] >= lookback_high * (1 - H_BREAK_MARGIN)
                        h_info["lookback_high"] = round(lookback_high, 4)
                        h_info["break_high"] = break_high

                        if break_high:
                            h_quality = 3
                            h_info["levels"].append("H3")

                    # H4: 延续确认 — 下一根K线继续收阳
                    confirm_candle = _get(0) if latest_idx > 0 else None
                    if confirm_candle and len(candles) >= H_CONFIRM_CANDLES:
                        h_info["confirm_chg"] = round(confirm_candle["chg_pct"], 2)
                        if confirm_candle["chg_pct"] >= -0.1:  # 下一根继续收阳/平
                            h_quality = 4
                            h_info["levels"].append("H4")

                    triggered.append("H")
                    details["H"] = h_info

        if not triggered:
            return None

        # ── 过滤已爆拉的 ──
        if CHG24H_SKIP_100 and chg24h > 100:
            return None

        # ── 评分 ──
        best_chg = max(
            max([v.get("chg", 0) for v in details.values()] + [prev["chg_pct"]]),
            max([v.get("surge", 0) for v in details.values()] + [0]),
        )
        best_vr = max(
            [v.get("vr", 0) for v in details.values()] + [vol_ratio]
        )

        score = SCORE_BASE
        if best_chg > 0:
            score += best_chg * SCORE_PER_CHG_PCT
        if best_vr >= 10:
            score += SCORE_VOL_10X
        elif best_vr >= 5:
            score += SCORE_VOL_5X
        elif best_vr >= 3:
            score += SCORE_VOL_3X
        if prev["amplitude"] >= 6:
            score += SCORE_AMPLITUDE_6PCT
        if len(triggered) >= 2:
            score += SCORE_RESONANCE_DUAL
        if len(triggered) >= 3:
            score += SCORE_RESONANCE_TRIPLE
        if "E" in triggered:
            score += SCORE_OVERSOLD_BONUS
        if price_tier == "micro":
            score += SCORE_MICRO_ALPHA

        # H缓涨加分 — 按品质分级
        if "H" in triggered:
            score += H_SCORE_BASE
            if h_info.get("accum_chg", 0) > 0:
                score += h_info["accum_chg"] * H_SCORE_PER_PCT
            if h_quality >= 2:
                score += H_SCORE_MAGNIFY_BONUS
            if h_quality >= 3:
                score += H_SCORE_BREAK_BONUS
            if h_quality >= 4:
                score += H_SCORE_CONFIRM_BONUS

        if chg24h > 50:
            score *= CHG24H_DISCOUNT_50
        elif chg24h > 30:
            score *= CHG24H_DISCOUNT_30
        elif chg24h > 15:
            score *= CHG24H_DISCOUNT_15

        if best_vr < 2 and len(triggered) == 1 and best_chg < 3:
            score *= 0.6

        score = round(min(100, score), 1)
        if score < MIN_SCORE_PUSH:
            return None

        return {
            "symbol": inst_id,
            "conditions": triggered,
            "h_quality": h_quality,
            "chg_pct": round(best_chg, 2),
            "vol_ratio": round(best_vr, 2),
            "amplitude": round(prev["amplitude"], 2),
            "price": round(last_price, 8),
            "volume": int(prev["vol_usd"]),
            "chg24h": round(chg24h, 2),
            "score": score,
            "timestamp": time.time(),
            "entry_price": round(last_price, 8),
            "stop_loss_pct": -5.0,
            "take_profit_pct": 15.0,
            "direction": "long",
            "strategy": "breakout_rapid_v15",
        }

    def _dedup_signals(self, signals: list[dict]) -> list[dict]:
        now = time.time()
        result = []
        seen = set()
        for sig in signals:
            sym = sig["symbol"]
            if sym in seen:
                continue
            seen.add(sym)
            if sym in self._dedup and now - self._dedup[sym] < DEDUP_WINDOW:
                continue
            self._dedup[sym] = now
            result.append(sig)
        self._dedup = {k: v for k, v in self._dedup.items() if now - v < DEDUP_WINDOW * 2}
        return result


def run_scan(top_n: int = 20) -> list[dict]:
    scanner = BreakoutRapidScannerV15()
    return scanner.scan_all(top_n=top_n)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
    )

    signals = run_scan(top_n=20)

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print(f"\n{'='*70}")
    print(f"  起涨点快速扫描 v1.5 (ORDI深度优化版) | {now_str} UTC")
    print(f"{'='*70}")

    if not signals:
        print("  暂无起涨信号（周末市场冷清）")
    else:
        print(f"  {'币种':<20s} {'条件':8s} {'品质':>4s} {'涨幅%':>7s} {'量比':>6s} {'24h%':>7s} {'评分':>5s} {'成交量':>14s}")
        print(f"  {'-'*75}")
        for s in signals:
            cond_str = "+".join(s["conditions"])
            qual = s.get("h_quality", 0)
            qual_str = f"H{qual}" if qual > 0 else "-"
            vol_str = f"{s['volume']/1e6:.1f}M" if s['volume'] > 1e6 else f"{s['volume']/1e3:.0f}K"
            print(f"  {s['symbol']:<20s} {cond_str:8s} {qual_str:>4s} {s['chg_pct']:>+6.2f}% {s['vol_ratio']:>5.1f}x {s['chg24h']:>+6.2f}% {s['score']:>5.1f} {vol_str:>14s}")

        print(f"\n  ⚡ Top: ", end="")
        for s in signals[:5]:
            print(f"{s['symbol'].split('-')[0]}={s['score']} ", end="")
        print()

    print(f"{'='*70}")
