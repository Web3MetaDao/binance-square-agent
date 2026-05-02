"""收筹标的扫描模块。

对全市场 USDT 永续合约进行两阶段筛选：
1. 粗筛：24h 成交额 >= POOL_MIN_VOL，24h 涨跌幅 < 10%
2. 深筛：获取 90 天日 K 线（OKX API），计算横盘特征 + 评分
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from .config import logger, POOL_MIN_VOL
from .market import fetch_market_data


def scan_accumulation_pool() -> list[dict]:
    """扫描全市场 USDT 永续合约，找出疑似庄家收筹的标的。

    Returns:
        按收筹评分降序排列的列表，每个元素为 dict：
            - coin: 币种名称（无 USDT 后缀）
            - score: 收筹评分（0-100）
            - status: "firing" / "warming" / "sleeping"
            - sideways_days: 横盘分析天数
            - range_pct: 价格波动区间百分比
            - vol_breakout: 近 7 天 vs 更早 7 天均量比
            - avg_vol: 日均成交量
            - current_price: 当前价格
            - low_price: 区间最低价
            - high_price: 区间最高价
            - ma_disp: 均线离散度（MA5/MA20）
            - sideways_pct: 横盘区间内收盘占比
    """
    ticker_map, funding_map, mcap_map = fetch_market_data()
    if not ticker_map:
        logger.warning("市场数据为空，跳过收筹扫描")
        return []

    # ── 第一阶段：粗筛 ─────────────────────────────────────
    candidates = []
    for sym, data in ticker_map.items():
        if data["vol"] < POOL_MIN_VOL:
            continue
        if abs(data["chg"]) >= 10:
            continue
        candidates.append(sym)

    logger.info(f"粗筛通过 {len(candidates)} 个标的（vol>={POOL_MIN_VOL} & |chg|<10%）")

    if not candidates:
        return []

    # ── 第二阶段：K 线深度分析 ──────────────────────────────
    # 只分析粗筛结果，上限 50 个
    scope = candidates[:50]
    results = []

    def analyze(sym: str) -> dict | None:
        """分析单个标的的 K 线数据，计算收筹评分。"""
        try:
            # 转换 OKX 格式：BTCUSDT → BTC-USDT-SWAP
            okx_sym = sym.replace("USDT", "-USDT-SWAP")
            # 获取 90 天日 K 线（OKX max limit=100）
            r = requests.get(
                "https://www.okx.com/api/v5/market/history-candles",
                params={"instId": okx_sym, "bar": "1D", "limit": 90},
                timeout=10,
            )
            if r.status_code != 200:
                logger.debug(f"{sym} K线API返回 {r.status_code}")
                return None

            body = r.json()
            if body.get("code") != "0" or "data" not in body:
                logger.debug(f"{sym} K线API异常响应: {body.get('msg', 'unknown')}")
                return None
            klines = body["data"]
            if len(klines) < 20:
                logger.debug(f"{sym} K线数据不足 ({len(klines)})")
                return None

            # OKX K线格式：['ts', O, H, L, C, vol_base, ...]
            # 索引：[1]=O, [2]=H, [3]=L, [4]=C, [5]=vol
            # 用最近 30 天判断横盘
            recent = klines[-30:] if len(klines) >= 30 else klines
            n = len(recent)
            r_closes = [float(k[4]) for k in recent]
            r_highs = [float(k[2]) for k in recent]
            r_lows = [float(k[3]) for k in recent]
            r_volumes = [float(k[5]) for k in recent]

            # ── 核心指标计算 ──

            # 价格波动区间百分比
            avg_close = sum(r_closes) / n
            range_pct = (max(r_highs) - min(r_lows)) / avg_close * 100 if avg_close > 0 else 0

            # 均线离散度（MA5 / MA20 偏离）
            ma5 = (
                sum(r_closes[-5:]) / 5
                if len(r_closes) >= 5
                else sum(r_closes) / n
            )
            ma20 = (
                sum(r_closes[-20:]) / 20
                if len(r_closes) >= 20
                else sum(r_closes) / n
            )
            ma_disp = abs(ma5 - ma20) / ma20 * 100 if ma20 > 0 else 0

            # 成交量变化：近 7 天 vs 更早 7 天
            vol_recent7 = (
                sum(r_volumes[-7:]) / 7
                if len(r_volumes) >= 7
                else sum(r_volumes) / n
            )
            vol_earlier7 = (
                sum(r_volumes[-14:-7]) / 7
                if len(r_volumes) >= 14
                else vol_recent7
            )
            vol_breakout = vol_recent7 / vol_earlier7 if vol_earlier7 > 0 else 1.0

            # 横盘区间内收盘占比（中间 70% 范围）
            range_high = max(r_highs)
            range_low = min(r_lows)
            band_top = range_low + (range_high - range_low) * 0.85
            band_bot = range_low + (range_high - range_low) * 0.15
            inside_days = sum(1 for c in r_closes if band_bot <= c <= band_top)
            sideways_pct = inside_days / n * 100

            # ── 收筹评分（0-100） ──
            score = 0

            # 1. 横盘越窄分越高（0-30）
            if range_pct < 15:
                score += 30
            elif range_pct < 25:
                score += 20
            elif range_pct < 40:
                score += 10

            # 2. 均线粘合（0-20）
            if ma_disp < 3:
                score += 20
            elif ma_disp < 8:
                score += 10
            elif ma_disp < 15:
                score += 5

            # 3. 区间内天数占比（0-20）
            if sideways_pct >= 70:
                score += 20
            elif sideways_pct >= 50:
                score += 10

            # 4. 成交量萎缩（0-15）
            if vol_breakout < 0.8:
                score += 15
            elif vol_breakout < 1.0:
                score += 10
            elif vol_breakout < 1.2:
                score += 5

            # 5. 低市值弹性（0-15）
            mcap_est = ticker_map.get(sym, {}).get("vol", 0) * 4
            if mcap_est < 10_000_000:
                score += 15
            elif mcap_est < 50_000_000:
                score += 10
            elif mcap_est < 200_000_000:
                score += 5

            current_px = ticker_map.get(sym, {}).get("px", r_closes[-1])

            # ── 状态判定 ──
            if vol_breakout >= 1.5 and range_pct < 25:
                status = "firing"
            elif score >= 50:
                status = "warming"
            else:
                status = "sleeping"

            return {
                "coin": sym,
                "score": score,
                "status": status,
                "sideways_days": n,
                "range_pct": round(range_pct, 2),
                "vol_breakout": round(vol_breakout, 2),
                "avg_vol": sum(float(k[5]) for k in klines) / len(klines) if klines else 0,
                "current_price": current_px,
                "low_price": min(r_lows),
                "high_price": max(r_highs),
                "ma_disp": round(ma_disp, 2),
                "sideways_pct": round(sideways_pct, 1),
            }
        except Exception as e:
            logger.warning(f"分析 {sym} 失败: {e}")
            return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(analyze, sym): sym for sym in scope}
        for f in as_completed(futures, timeout=120):
            r = f.result()
            if r:
                results.append(r)

    # 按评分降序排列
    results.sort(key=lambda x: x["score"], reverse=True)
    logger.info(f"收筹扫描完成，找到 {len(results)} 个标的")
    return results
