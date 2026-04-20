#!/usr/bin/env python3
"""
数字人直播模块 — 行情分析引擎
功能：实时获取大盘行情、主流币分析、热点币推荐
数据源：币安 REST API（无需 Key）+ Web3 热点 API
"""
import requests
import time
import json
from datetime import datetime
from typing import Optional

# ── 币安行情 API（公开，无需 Key）──────────────────────────
BINANCE_API = "https://api.binance.com/api/v3"
WEB3_API    = "https://web3.binance.com/bapi/defi/v1/public"

# 主流币列表（直播必讲）
MAJOR_COINS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

# 热点赛道代币
TRENDING_COINS = [
    "OPUSDT", "ARBUSDT", "WUSDT", "NOTUSDT", "PEPEUSDT",
    "DOGEUSDT", "SHIBUSDT", "SUIUSDT", "APTUSDT", "AVAXUSDT"
]


def get_ticker_24h(symbol: str) -> Optional[dict]:
    """获取单个币种 24h 行情数据"""
    try:
        r = requests.get(f"{BINANCE_API}/ticker/24hr", params={"symbol": symbol}, timeout=8)
        if r.status_code == 200:
            d = r.json()
            return {
                "symbol": symbol.replace("USDT", ""),
                "price": float(d["lastPrice"]),
                "change_pct": float(d["priceChangePercent"]),
                "volume_usdt": float(d["quoteVolume"]),
                "high": float(d["highPrice"]),
                "low": float(d["lowPrice"]),
            }
    except Exception:
        pass
    return None


def get_market_overview() -> dict:
    """获取大盘整体行情（BTC 主导，恐惧贪婪指数替代方案）"""
    btc = get_ticker_24h("BTCUSDT")
    eth = get_ticker_24h("ETHUSDT")
    if not btc:
        return {"status": "error"}

    # 根据 BTC 涨跌幅判断市场情绪
    change = btc["change_pct"]
    if change >= 3:
        sentiment = "极度贪婪"
        trend = "强势上涨"
    elif change >= 1:
        sentiment = "贪婪"
        trend = "温和上涨"
    elif change >= -1:
        sentiment = "中性"
        trend = "横盘震荡"
    elif change >= -3:
        sentiment = "恐惧"
        trend = "温和下跌"
    else:
        sentiment = "极度恐惧"
        trend = "大幅下跌"

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "btc_price": btc["price"],
        "btc_change": btc["change_pct"],
        "eth_price": eth["price"] if eth else 0,
        "eth_change": eth["change_pct"] if eth else 0,
        "market_sentiment": sentiment,
        "market_trend": trend,
        "btc_volume_b": round(btc["volume_usdt"] / 1e9, 2),  # 十亿美元
    }


def get_major_coins_analysis() -> list:
    """获取主流币详细分析数据"""
    results = []
    for symbol in MAJOR_COINS:
        data = get_ticker_24h(symbol)
        if data:
            # 生成简单技术判断
            if data["change_pct"] > 2:
                signal = "强烈看多"
                action = "可考虑做多"
            elif data["change_pct"] > 0.5:
                signal = "偏多"
                action = "轻仓关注"
            elif data["change_pct"] > -0.5:
                signal = "中性"
                action = "观望为主"
            elif data["change_pct"] > -2:
                signal = "偏空"
                action = "谨慎操作"
            else:
                signal = "强烈看空"
                action = "注意风控"

            data["signal"] = signal
            data["action"] = action
            results.append(data)
        time.sleep(0.1)
    return results


def get_trending_recommendation() -> list:
    """获取热点币推荐（结合涨幅与成交量筛选）"""
    candidates = []
    for symbol in TRENDING_COINS:
        data = get_ticker_24h(symbol)
        if data:
            # 热度评分：涨幅 * 0.6 + 成交量权重 * 0.4
            vol_score = min(data["volume_usdt"] / 1e8, 10)  # 最高10分
            change_score = max(min(data["change_pct"], 20), -20)
            heat_score = change_score * 0.6 + vol_score * 0.4
            data["heat_score"] = round(heat_score, 2)
            candidates.append(data)
        time.sleep(0.1)

    # 按热度评分排序，取前5
    candidates.sort(key=lambda x: x["heat_score"], reverse=True)
    top5 = candidates[:5]

    # 标注推荐等级
    for i, c in enumerate(top5):
        if i == 0:
            c["recommend_level"] = "强烈推荐"
            c["recommend_reason"] = "热度最高，资金流入明显"
        elif i <= 2:
            c["recommend_level"] = "重点关注"
            c["recommend_reason"] = "涨势良好，值得布局"
        else:
            c["recommend_level"] = "一般关注"
            c["recommend_reason"] = "有一定热度，可小仓参与"

    return top5


def get_full_market_report() -> dict:
    """获取完整市场报告（供直播话术生成使用）"""
    print("[行情引擎] 正在获取实时市场数据...")
    overview = get_market_overview()
    majors = get_major_coins_analysis()
    trending = get_trending_recommendation()

    report = {
        "overview": overview,
        "major_coins": majors,
        "trending": trending,
        "generated_at": datetime.now().isoformat(),
    }

    # 缓存到文件
    with open("/home/ubuntu/clawself_agent/data/live_market_report.json", "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"[行情引擎] 市场报告生成完成 | BTC: ${overview.get('btc_price', 0):,.0f} | 情绪: {overview.get('market_sentiment', 'N/A')}")
    return report


def load_cached_report() -> dict:
    """加载缓存的市场报告"""
    try:
        with open("/home/ubuntu/clawself_agent/data/live_market_report.json") as f:
            return json.load(f)
    except Exception:
        return get_full_market_report()


if __name__ == "__main__":
    report = get_full_market_report()
    print(json.dumps(report, ensure_ascii=False, indent=2))
