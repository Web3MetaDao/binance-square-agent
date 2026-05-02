"""
舆情嗅探模块 — v1.1 三维舆情辅助 (CLI数据源)
===============================================
为起涨点检测提供舆情热度加持。

数据源:
  1. OKX news sentiment-rank — CLI调用: 各币种推特提及数+看涨比+新闻提及 (主要数据源)
  2. 币安广场热搜 — 通过requests模拟 (带User-Agent). 可能403则fallback到CLI
  3. 推特关键词热度 — 通过OKX sentiment中的xMentionCnt指标 (无需单独调用Twitter API)

使用:
    from sentiment_scanner import SentimentScanner
    scanner = SentimentScanner()
    result = scanner.scan_sentiment()
    # { "ORDI": {"heat": 73.2, "square": ..., "okx": ..., "twitter": ...}, ... }
"""

import os
import re
import json
import time
import logging
import subprocess
from datetime import datetime
from typing import Optional

import requests
from pathlib import Path

logger = logging.getLogger("sentiment_scanner")

# ════════════════════════════════════════════════════════════
#  配置
# ════════════════════════════════════════════════════════════

# 缓存
CACHE_TTL = 120  # 2分钟缓存

# 舆情权重 (四维)
OKX_WEIGHT = 35          # OKX舆情
TWITTER_WEIGHT = 30      # 推特舆情
SQUARE_WEIGHT = 20       # 币安广场文章热度
SQUARE_W2E_WEIGHT = 15   # 币安广场W2E创作者热搜 (第四维)

# CLI路径
OKX_CLI = "okx"
OKX_PROFILE = "hermes-trader"

# 热门币种判断阈值
HOT_MENTION_THRESHOLD = 100       # 提及次数>100算热门
HOT_BULLISH_RATIO = 0.30          # 看涨比>0.3算正向

# 已知非加密标的过滤 (OKX sentiment-rank会返回美股, 过滤掉)
STOCK_FILTER = {
    "GOOGL", "AAPL", "TSLA", "SPY", "QQQ", "AMZN", "MSFT", "NFLX",
    "NVDA", "META", "GOOG", "AMD", "INTC", "MU", "SNDK", "CRM",
    "DIS", "BA", "JPM", "GS", "V", "MA", "PYPL", "SQ", "COIN",
    "HOOD", "MSTR", "CLWD", "CLAWD",
}
# 美股类前缀 (很多美股出现在sentiment-rank中)
STOCK_PREFIX = ("SPY", "QQQ", "IWM", "DIA", "XL")


class SentimentScanner:
    """三维舆情嗅探 — OKX舆情 + 推特热度 + 币安广场"""

    def __init__(self):
        self._session = requests.Session()
        self._cache: dict[str, tuple] = {}
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })

    def _get_cached(self, key: str) -> Optional[dict]:
        if key in self._cache:
            ts, data = self._cache[key]
            if time.time() - ts < CACHE_TTL:
                return data
        return None

    def _set_cache(self, key: str, data: dict):
        self._cache[key] = (time.time(), data)

    def _get_w2e_file_path(self) -> Optional[Path]:
        """获取 W2E 热点数据文件路径"""
        try:
            from pathlib import Path
            # square_sentiment.py 在 binance-square-agent 根目录下
            base = Path(__file__).resolve().parent.parent  # okx_auto_trader -> binance-square-agent
            hot_file = base / "data" / "square_hot_topics.json"
            if hot_file.exists():
                return hot_file
            return None
        except Exception:
            return None

    # ════════════════════════════════════════════════════════════
    #  1. OKX舆情 — CLI调用 (主数据源)
    # ════════════════════════════════════════════════════════════

    def scan_okx_sentiment(self) -> dict[str, dict]:
        """
        通过OKX CLI获取新闻舆情排名。

        返回:
            {
                "BTC": {
                    "okx_score": 65.4,    # 综合分 0-100
                    "mention_cnt": 1531,  # 总提及次数
                    "bullish_ratio": 0.43,# 看涨比
                    "bearish_ratio": 0.13,# 看跌比
                    "label": "neutral",   # 情绪标签
                    "x_mentions": 1474,   # 推特提及
                    "news_count": 57,     # 新闻提及
                },
                ...
            }
        """
        cached = self._get_cached("okx_sentiment_full")
        if cached:
            return cached

        result = {}

        try:
            # 使用OKX CLI
            env = os.environ.copy()
            if OKX_PROFILE:
                env["OKX_DEFAULT_PROFILE"] = OKX_PROFILE

            cmd = [OKX_CLI, "news", "sentiment-rank",
                   "--sort-by", "bullish", "--limit", "50", "--json"]

            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=15, env=env
            )

            if proc.returncode != 0:
                logger.warning(f"OKX CLI failed: {proc.stderr[:200]}")
                return {}

            data = json.loads(proc.stdout)
            # 结构: [{"period": "24h", "details": [{ccy, mentionCnt, sentiment, ...}]}]
            if isinstance(data, list) and data:
                entries = data[0].get("details", [])
                for entry in entries:
                    coin = entry.get("ccy", "").upper().strip()
                    if not coin or len(coin) > 10:
                        continue
                    # 过滤美股
                    if coin in STOCK_FILTER:
                        continue
                    if any(coin.startswith(p) for p in STOCK_PREFIX):
                        continue

                    mention_cnt = int(entry.get("mentionCnt", 0) or 0)
                    sentiment = entry.get("sentiment", {})
                    bullish_ratio = float(sentiment.get("bullishRatio", 0) or 0)
                    bearish_ratio = float(sentiment.get("bearishRatio", 0) or 0)
                    label = sentiment.get("label", "neutral")
                    x_mentions = int(entry.get("xMentionCnt", 0) or 0)
                    news_count = int(entry.get("newsMentionCnt", 0) or 0)

                    # 综合评分
                    if mention_cnt > 0:
                        # 看涨比越高越好
                        sentiment_score = bullish_ratio * 60
                        # 提及量越大越好(log)
                        volume_score = min(30, (mention_cnt ** 0.5) * 2.5)
                        # 推特加分
                        twitter_score = min(10, (x_mentions ** 0.5) * 0.8)
                        # 新闻加分
                        news_score = min(5, news_count * 0.2)
                        # 看跌惩罚
                        bearish_penalty = bearish_ratio * 20

                        total = sentiment_score + volume_score + twitter_score + news_score - bearish_penalty
                        okx_score = round(max(0, min(100, total)), 1)
                    else:
                        okx_score = 0

                    result[coin] = {
                        "okx_score": okx_score,
                        "mention_cnt": mention_cnt,
                        "bullish_ratio": round(bullish_ratio, 2),
                        "bearish_ratio": round(bearish_ratio, 2),
                        "label": label,
                        "x_mentions": x_mentions,
                        "news_count": news_count,
                    }

            logger.debug(f"OKX sentiment: {len(result)} coins")
            self._set_cache("okx_sentiment_full", result)
            return result

        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            logger.warning(f"OKX sentiment scan failed: {e}")
            return {}

    # ════════════════════════════════════════════════════════════
    #  2. 币安广场热搜 (可能403, 有fallback)
    # ════════════════════════════════════════════════════════════

    def scan_square_trending(self) -> dict[str, float]:
        """
        币安广场热门文章, 提取热门币。

        可能因IP限制返回403, 此时用OKX数据中的新闻提及作为近似替代。
        """
        cached = self._get_cached("square_trending")
        if cached:
            return cached

        coin_heat: dict[str, float] = {}

        # 尝试抓取
        try:
            resp = self._session.post(
                "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                json={"pageNo": 1, "pageSize": 20, "type": 1, "catalogId": 1},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == "000000" or data.get("success"):
                    articles = data.get("data", {}).get("catalogs", [])
                    for article in articles:
                        pairs = article.get("tradingPairsV2", [])
                        for pair in pairs:
                            coin = pair.get("code", "").upper().strip()
                            if coin and len(coin) <= 10:
                                score = float(pair.get("priceChange", 1) or 1)
                                coin_heat[coin] = coin_heat.get(coin, 0) + max(1, score)

        except Exception as e:
            logger.debug(f"Binance Square API 403/err: {e} — 使用OKX新闻作为后备")

        # 如果有数据则归一化
        if coin_heat:
            max_h = max(coin_heat.values())
            if max_h > 0:
                coin_heat = {k: round(v / max_h * 100, 1) for k, v in coin_heat.items()}

        self._set_cache("square_trending", coin_heat)
        return coin_heat

    # ════════════════════════════════════════════════════════════
    #  3. 推特热度 (从OKX sentiment中的xMentionCnt提取)
    # ════════════════════════════════════════════════════════════

    @staticmethod
    def _compute_twitter_heat(okx_data: dict[str, dict], coin: str) -> float:
        """从OKX数据的xMentionCnt计算推特热度"""
        if coin not in okx_data:
            return 0

        info = okx_data[coin]
        x_mentions = info["x_mentions"]
        bullish_ratio = info["bullish_ratio"]

        if x_mentions == 0:
            return 0

        # 量级分 (log scale): 100条=15分, 1000条=25分, 10000条=35分
        volume_heat = min(35, (x_mentions ** 0.3) * 6)

        # 情绪分: 看涨比越高越好
        sentiment_heat = bullish_ratio * 40

        # 惩罚高看跌
        bearish_penalty = info["bearish_ratio"] * 15

        return round(max(0, min(100, volume_heat + sentiment_heat - bearish_penalty)), 1)

    # ════════════════════════════════════════════════════════════
    #  4. 三维融合
    # ════════════════════════════════════════════════════════════

    def scan_sentiment(self, target_coins: Optional[list[str]] = None) -> dict[str, dict]:
        """
        全维度舆情扫描。四源融合:
        - OKX新闻情感 (35%)
        - 推特提及热度 (30%)
        - 币安广场文章热度 (20%)
        - 币安广场W2E创作者热搜 (15%)

        如果广场API/W2E不可用, 权重重分配到OKX+推特

        Args:
            target_coins: 可选 — 只关注这些币种

        返回:
            {
                "ORDI": {
                    "heat": 73.2,          # 综合热度 0-100 (越高越好)
                    "okx": 65.4,           # OKX新闻情绪分
                    "twitter": 42.1,       # 推特热度
                    "square": 0.0,         # 币安广场文章热度
                    "square_w2e": 0.0,     # 币安广场W2E创作者热搜 (第四维)
                    "details": {
                        "mention_cnt": 1531,
                        "x_mentions": 1474,
                        "bullish_ratio": 0.43,
                        "label": "neutral",
                        "square_w2e_mentions": 45,
                        "square_w2e_sentiment": "bullish",
                    }
                }
            }
        """
        # 获取OKX舆情 (主要数据源)
        okx_data = self.scan_okx_sentiment()

        # 币安广场文章热度
        square_data = self.scan_square_trending()
        square_available = bool(square_data)

        # 币安广场W2E创作者热搜 (第四维)
        square_w2e_data = self._scan_square_w2e_hot()
        square_w2e_available = bool(square_w2e_data)

        # 调整权重 (动态分配)
        available_weights = []
        if square_available:
            available_weights.append(("w_sq", SQUARE_WEIGHT))
        if square_w2e_available:
            available_weights.append(("w_w2e", SQUARE_W2E_WEIGHT))

        if square_available and square_w2e_available:
            w_okx = OKX_WEIGHT
            w_tw = TWITTER_WEIGHT
            w_sq = SQUARE_WEIGHT
            w_w2e = SQUARE_W2E_WEIGHT
        elif square_available or square_w2e_available:
            # 只有一个Square数据源可用
            remaining = SQUARE_WEIGHT + SQUARE_W2E_WEIGHT
            if square_available:
                w_sq = SQUARE_WEIGHT + int(remaining * 0.3)  # 拿回部分
                w_w2e = 0
            else:
                w_w2e = SQUARE_W2E_WEIGHT + int(remaining * 0.3)
                w_sq = 0
            # OKX+推特也分走部分剩余
            w_okx = OKX_WEIGHT + int(remaining * 0.2)
            w_tw = TWITTER_WEIGHT + int(remaining * 0.2)
        else:
            # 两路Square都不可用
            w_sq = 0
            w_w2e = 0
            w_okx = OKX_WEIGHT + SQUARE_WEIGHT + SQUARE_W2E_WEIGHT
            w_tw = TWITTER_WEIGHT

        # 归一化
        total = w_okx + w_tw + w_sq + w_w2e
        w_okx = round(w_okx / total * 100, 1)
        w_tw = round(w_tw / total * 100, 1)
        w_sq = round(w_sq / total * 100, 1)
        w_w2e = round(w_w2e / total * 100, 1)

        # 融合
        all_coins = set(okx_data.keys()) | set(square_data.keys()) | set(square_w2e_data.keys())
        if target_coins:
            all_coins |= set(c.upper().strip() for c in target_coins)

        result = {}
        for coin in all_coins:
            okx_info = okx_data.get(coin, {})
            sq_val = square_data.get(coin, 0)
            w2e_val = square_w2e_data.get(coin, 0)
            tw_val = self._compute_twitter_heat(okx_data, coin)

            # 加权
            okx_val = okx_info.get("okx_score", 0)
            weighted = (
                okx_val * w_okx / 100
                + tw_val * w_tw / 100
                + sq_val * w_sq / 100
                + w2e_val * w_w2e / 100
            )

            # 详情信息
            details = {}
            if okx_info:
                details.update({
                    "mention_cnt": okx_info.get("mention_cnt", 0),
                    "x_mentions": okx_info.get("x_mentions", 0),
                    "bullish_ratio": okx_info.get("bullish_ratio", 0),
                    "label": okx_info.get("label", "unknown"),
                })

            result[coin] = {
                "heat": round(min(100, max(0, weighted)), 1),
                "okx": round(okx_val, 1),
                "twitter": round(tw_val, 1),
                "square": round(sq_val, 1),
                "square_w2e": round(w2e_val, 1),
                "details": details,
            }

        logger.info(f"Sentiment scan: {len(result)} coins "
                    f"(square={'✓' if square_available else '✗'} | "
                    f"w2e={'✓' if square_w2e_available else '✗'} | "
                    f"w_okx={w_okx} w_tw={w_tw} w_sq={w_sq} w_w2e={w_w2e})")

        return result

    def _scan_square_w2e_hot(self) -> dict[str, float]:
        """
        从币安广场W2E创作者热搜数据集中提取币种热度 (第四维)
        读取 square_sentiment.py 输出的 data/square_hot_topics.json
        """
        cached = self._get_cached("square_w2e_hot")
        if cached:
            return cached

        coin_heat: dict[str, float] = {}
        w2e_file = self._get_w2e_file_path()
        if not w2e_file or not w2e_file.exists():
            return coin_heat

        try:
            data = json.loads(w2e_file.read_text())
            if not data.get("hot_topics"):
                return coin_heat

            max_mention = max(h["mention_count"] for h in data["hot_topics"]) if data["hot_topics"] else 1
            for topic in data["hot_topics"]:
                coin = topic["coin"]
                mention_count = topic["mention_count"]
                # 归一化到0-100
                heat = round(mention_count / max_mention * 100, 1)
                # 情感加成
                sentiment = topic.get("sentiment", "neutral")
                if sentiment == "bullish":
                    heat = min(100, heat * 1.2)
                elif sentiment == "bearish":
                    heat = heat * 0.8
                coin_heat[coin] = round(min(100, max(0, heat)), 1)

            logger.debug(f"Square W2E hot: {len(coin_heat)} coins from {w2e_file.name}")
        except Exception as e:
            logger.debug(f"Square W2E hot load failed: {e}")

        self._set_cache("square_w2e_hot", coin_heat)
        return coin_heat


# ════════════════════════════════════════════════════════════
#  CLI入口
# ════════════════════════════════════════════════════════════

def run_sentiment_scan(target_coins: Optional[list[str]] = None) -> dict:
    scanner = SentimentScanner()
    return scanner.scan_sentiment(target_coins)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    scanner = SentimentScanner()

    # 1. OKX舆情
    print("\n▸ OKX舆情 (CLI):")
    okx = scanner.scan_okx_sentiment()
    if okx:
        top = sorted(okx.items(), key=lambda x: -x[1]["okx_score"])[:10]
        for coin, info in top:
            bar = "█" * int(info["okx_score"] / 6)
            print(f"  {coin:<8s} {info['okx_score']:>5.1f} {bar} "
                  f"(提及{info['mention_cnt']} 推特{info['x_mentions']} "
                  f"看涨{info['bullish_ratio']:.0%} {info['label']})")
    else:
        print("  无数据")

    # 2. 四维融合
    print("\n▸ 四维融合 (OKX+推特+广场+W2E热搜):")
    import sys
    target = sys.argv[1:] if len(sys.argv) > 1 else None
    fused = scanner.scan_sentiment(target)
    if fused:
        top = sorted(fused.items(), key=lambda x: -x[1]["heat"])[:15]
        print(f"  {'币种':<8s} {'热度':>6s} {'OKX':>6s} {'推特':>6s} {'广场':>6s} {'W2E':>6s}  提及数")
        print(f"  {'-'*60}")
        for coin, info in top:
            bar = "█" * int(info["heat"] / 6)
            print(f"  {coin:<8s} {info['heat']:>5.1f} {bar} "
                  f"{info['okx']:>5.1f} {info['twitter']:>5.1f} {info['square']:>5.1f} {info['square_w2e']:>5.1f}  "
                  f"{info['details'].get('mention_cnt', 0)}")
    else:
        print("  无融合数据")

    print()
